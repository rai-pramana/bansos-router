import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import zlib from "node:zlib";
import { createServer } from "../src/daemon/server";
import { RuntimeCatalog } from "../src/daemon/catalog";
import { RateLimiter } from "../src/daemon/rate-limit";
import { createLogger } from "../src/logger";
import { SEEDED_MODELS, DEFAULT_UPSTREAMS } from "../src/upstreams";
import { loadRelayState, saveRelayState, resetGzipCache } from "../src/relay/egress";
import { modelDef } from "../src/upstreams/types";

// bansos-opt4 follow-up: compressing is only safe to a relay that decompresses on
// receipt. The Vercel handler does; the Cloudflare worker and the Netlify function
// forward the body untouched and answer 400 (measured: same payload 429 plain,
// 400 gzipped on workers.dev). These tests pin the host gate so a relay that
// cannot gunzip is never handed a compressed body.

interface Seen {
  headers: http.IncomingHttpHeaders;
  wireBytes: number;
  decoded: string;
}

function recordRelay(statuses: number[] = []): Promise<{
  relayUrl: string;
  seen: Seen[];
  close: () => Promise<void>;
}> {
  const seen: Seen[] = [];
  let call = 0;
  const server = http.createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const c of req) chunks.push(c as Buffer);
    const raw = Buffer.concat(chunks);
    const enc = String(req.headers["content-encoding"] || "").toLowerCase();
    seen.push({
      headers: req.headers,
      wireBytes: raw.length,
      decoded: enc.includes("gzip") ? zlib.gunzipSync(raw).toString("utf8") : raw.toString("utf8"),
    });
    const status = statuses[call++] ?? 200;
    if (status !== 200) {
      res.writeHead(status, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { type: "FreeUsageLimitError", message: "Free usage limit reached." } }));
      return;
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        id: "chatcmpl-gzip",
        object: "chat.completion",
        created: Date.now(),
        model: "mock-model",
        choices: [{ index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" }],
        usage: { prompt_tokens: 5, completion_tokens: 1, total_tokens: 6 },
      }),
    );
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      const port = typeof addr === "object" && addr ? addr.port : 0;
      resolve({
        relayUrl: `http://127.0.0.1:${port}`,
        seen,
        close: () => new Promise<void>((r) => server.close(() => r())),
      });
    });
  });
}

function setupDaemon(): Promise<{
  baseUrl: string;
  catalog: RuntimeCatalog;
  close: () => Promise<void>;
}> {
  const log = createLogger({ level: "error" });
  const catalog = new RuntimeCatalog(DEFAULT_UPSTREAMS, log);
  catalog.seed(SEEDED_MODELS);
  const rateLimiter = new RateLimiter({ limit: 1000, windowMs: 60_000 });
  const server = createServer({ catalog, rateLimiter, port: 0, log, startedAt: Date.now() });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address() as { port: number };
      resolve({
        baseUrl: `http://127.0.0.1:${addr.port}`,
        catalog,
        close: () => new Promise<void>((r) => server.close(() => r())),
      });
    });
  });
}

const BIG_TEXT = "Hello relay ".repeat(400); // ~4.8 KB > GZIP_MIN_BYTES

test("Relay Egress: one compression per request, reused across relay attempts", async () => {
  const original = loadRelayState();
  const prevHosts = process.env.BANSOS_RELAY_GZIP_HOSTS;
  // Two independent recording relays; the first answers 503 so relayFetch walks
  // to the second. Both must receive the SAME gzip bytes — compressing per attempt
  // would burn the CPU three times on the event loop that streams SSE.
  const first = await recordRelay([503]);
  const second = await recordRelay();
  process.env.BANSOS_RELAY_GZIP_HOSTS = "127.0.0.1";
  resetGzipCache();
  const { baseUrl, catalog, close } = await setupDaemon();
  try {
    saveRelayState({
      enabled: true,
      url: first.relayUrl,
      relays: [{ url: first.relayUrl, label: "First" }, { url: second.relayUrl, label: "Second" }],
    });
    catalog.seed([
      modelDef({
        id: "mock/gzip-retry:free",
        name: "Mock Gzip Retry",
        source: "zen",
        reasoning: false,
        input: ["text"],
        contextWindow: 128000,
        maxTokens: 4096,
        compat: { supportsDeveloperRole: true, supportsReasoningEffort: false },
      }),
    ]);

    const res = await fetch(`${baseUrl}/v1/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "mock/gzip-retry:free", messages: [{ role: "user", content: BIG_TEXT }] }),
    });
    assert.equal(res.status, 200);
    assert.equal(first.seen.length, 1);
    assert.equal(second.seen.length, 1);
    const a = first.seen[0]!;
    const b = second.seen[0]!;
    assert.equal(a.headers["content-encoding"], "gzip");
    assert.equal(b.headers["content-encoding"], "gzip");
    assert.equal(a.wireBytes, b.wireBytes, "both attempts must carry the same compressed body");
    assert.equal(a.decoded, b.decoded, "both attempts must decode to the same JSON");
  } finally {
    saveRelayState(original);
    if (prevHosts === undefined) delete process.env.BANSOS_RELAY_GZIP_HOSTS;
    else process.env.BANSOS_RELAY_GZIP_HOSTS = prevHosts;
    resetGzipCache();
    await first.close();
    await second.close();
    await close();
  }
});

test("Relay Egress: a relay that cannot gunzip is sent the plain body", async () => {
  const original = loadRelayState();
  const prevHosts = process.env.BANSOS_RELAY_GZIP_HOSTS;
  const relay = await recordRelay();
  // Simulate a non-Vercel relay (Cloudflare worker / Netlify function): its host is
  // not gzip-capable, so the daemon must leave the body uncompressed.
  process.env.BANSOS_RELAY_GZIP_HOSTS = "example-worker.workers.dev";
  resetGzipCache();
  const { baseUrl, catalog, close } = await setupDaemon();
  try {
    saveRelayState({
      enabled: true,
      url: relay.relayUrl,
      relays: [{ url: relay.relayUrl, label: "Plain Only" }],
    });
    catalog.seed([
      modelDef({
        id: "mock/gzip-host:free",
        name: "Mock Gzip Host",
        source: "zen",
        reasoning: false,
        input: ["text"],
        contextWindow: 128000,
        maxTokens: 4096,
        compat: { supportsDeveloperRole: true, supportsReasoningEffort: false },
      }),
    ]);

    const res = await fetch(`${baseUrl}/v1/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "mock/gzip-host:free", messages: [{ role: "user", content: BIG_TEXT }] }),
    });
    assert.equal(res.status, 200);
    const req = relay.seen[0]!;
    assert.equal(req.headers["content-encoding"], undefined, "non-gzip relay must get plain bytes");
    assert.ok(req.decoded.includes("Hello relay"), "plain body must still arrive intact");
    assert.ok(req.wireBytes > 1024, "plain body keeps its full size");
  } finally {
    saveRelayState(original);
    if (prevHosts === undefined) delete process.env.BANSOS_RELAY_GZIP_HOSTS;
    else process.env.BANSOS_RELAY_GZIP_HOSTS = prevHosts;
    resetGzipCache();
    await relay.close();
    await close();
  }
});
