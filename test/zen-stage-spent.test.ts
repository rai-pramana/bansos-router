// A stage that is itself spent must not be re-walked on every request.
//
// Measured 2026-10-30: production closed (parked, instant), the stage's "bi"
// row then drained too, and three consecutive big-pickle requests paid
// 41.7s / 45.0s / 43.7s each — tryZenStageFor walked up to 13 of the stage's
// dead relays before giving up, because only PRODUCTION's closed-wall evidence
// was consulted before a walk, never the stage's own. The fall-through must
// fail fast once a majority of the fleet has answered quota-429 for the
// STAGE's origin and this model row.
import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { RuntimeCatalog } from "../src/daemon/catalog";
import { createServer, setZenMirror, setZenStage } from "../src/daemon/server";
import { RateLimiter } from "../src/daemon/rate-limit";
import { normalizeSecurityConfig } from "../src/security/policy";
import { createZenStage } from "../src/upstreams/zen-stage";
import { BANSOS_DIR } from "../src/daemon/state";
import { resetRelayRetryState, restoreQuotaEvidenceForTest } from "../src/relay/egress";
import type { Logger } from "../src/logger";
import type { ModelDef, Upstream } from "../src/upstreams/types";

function logger(): Logger {
  return {
    info: () => {},
    debug: () => {},
    warn: () => {},
    error: () => {},
    child: () => logger(),
  } as unknown as Logger;
}

function md(id: string, source: ModelDef["source"]): ModelDef {
  return {
    id,
    name: id,
    source,
    reasoning: false,
    contextWindow: 100_000,
    maxTokens: 8_192,
    input: ["text"],
    compat: { supportsReasoningEffort: false, supportsDeveloperRole: false },
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  };
}

async function listen(server: http.Server): Promise<number> {
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  return (server.address() as { port: number }).port;
}

test("a known-spent stage is skipped instead of walked when production is parked", async () => {
  // Production: always a quota 429.
  let zenHits = 0;
  const zenServer = http.createServer((req, res) => {
    zenHits++;
    req.resume();
    req.on("end", () => {
      res.writeHead(429, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { type: "FreeUsageLimitError", message: "Free usage limit reached." } }));
    });
  });
  const zenPort = await listen(zenServer);

  // Stage: serves the catalog, but its chat endpoint answers quota 429.
  let stageChatHits = 0;
  const stageServer = http.createServer((req, res) => {
    if (req.url === "/zen/v1/models") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ data: [{ id: "big-pickle", object: "model" }] }));
      req.resume();
      return;
    }
    stageChatHits++;
    let b = "";
    req.on("data", (c) => (b += c));
    req.on("end", () => {
      res.writeHead(429, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { type: "FreeUsageLimitError", message: "Free usage limit reached." } }));
    });
  });
  const stagePort = await listen(stageServer);
  const stageOrigin = `http://127.0.0.1:${stagePort}`;

  // A relay fleet whose addresses are ALL stamped spent for the stage's "bi"
  // row in the persisted evidence — exactly what accumulates across real
  // requests over an afternoon. Relay URLs point at the stage server itself
  // (any live HTTP works; the handler answers the same way to any path).
  const N = 14;
  const relayUrls = Array.from({ length: N }, (_, i) => `${stageOrigin}/relay-${i}`);
  fs.writeFileSync(
    path.join(BANSOS_DIR, "relay-state.json"),
    JSON.stringify({ enabled: true, url: relayUrls[0], relays: relayUrls.map((url) => ({ url })) }),
  );

  const zenUpstream: Upstream = {
    id: "zen",
    kind: "remote-keyless",
    relayAllowed: true,
    chatUrl: `http://127.0.0.1:${zenPort}/v1/chat/completions`,
    async fetchCatalog() {
      return null;
    },
    requestHeaders() {
      return {};
    },
  };
  const catalog = new RuntimeCatalog([zenUpstream], logger(), normalizeSecurityConfig(undefined));
  catalog.seed([md("big-pickle", "zen")]);
  // Park the model the way a confirmed fleet-exhausted production 429 does,
  // so the request takes the parked fall-through path — the one measured at
  // 45s in the field.
  catalog.markQuotaExhausted("big-pickle", 3_600_000, {
    status: 429,
    message: "Free usage limit reached",
  });

  const handle = createZenStage({ baseUrl: stageOrigin, ttlMs: 0 });
  await handle.ready();
  setZenStage(handle);
  setZenMirror(null);

  // Stamp the spent evidence right before the request under test: the relay
  // layer restores the on-disk file into its in-memory counter at module load
  // AND on every reset, so this is what a real afternoon of accumulated
  // quota-429s looks like from the daemon's side. The stage's catalog fetch
  // (one /models hit) happens earlier and is not counted as a chat contact.
  resetRelayRetryState();
  fs.writeFileSync(
    path.join(BANSOS_DIR, "quota-evidence.json"),
    JSON.stringify({ rows: { [`${stageOrigin}|bi`]: { spent: relayUrls, until: Date.now() + 3_600_000 } } }),
  );
  restoreQuotaEvidenceForTest();

  const bansos = createServer({
    catalog,
    rateLimiter: new RateLimiter({ limit: 1000, windowMs: 60_000 }),
    port: 0,
    log: logger(),
    startedAt: Date.now(),
    security: normalizeSecurityConfig(undefined),
  });
  const bport = await listen(bansos);

  try {
    const t0 = Date.now();
    const res = await fetch(`http://127.0.0.1:${bport}/v1/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "big-pickle",
        messages: [{ role: "user", content: "hi" }],
        max_tokens: 64,
        stream: false,
      }),
    });
    const ms = Date.now() - t0;
    assert.equal(res.status, 429, "with both deployments closed the client still gets the honest 429");
    // The defect being locked: walking 13 dead stage relays at ~2s each took
    // 41-45s. Skipping the walk keeps this under a second even with the park
    // answer, the zen probe, and loopback HTTP in play.
    assert.ok(ms < 1000, `a known-spent stage must fail fast, took ${ms}ms`);
    assert.equal(stageChatHits, 0, "a known-spent stage must not be contacted at all");
  } finally {
    await new Promise<void>((r) => bansos.close(() => r()));
    setZenStage(null);
    await new Promise<void>((r) => stageServer.close(() => r()));
    await new Promise<void>((r) => zenServer.close(() => r()));
    fs.rmSync(path.join(BANSOS_DIR, "relay-state.json"), { force: true });
    fs.rmSync(path.join(BANSOS_DIR, "quota-evidence.json"), { force: true });
    resetRelayRetryState();
  }
});
