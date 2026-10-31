import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { RuntimeCatalog } from "../src/daemon/catalog";
import { createServer, setZenMirror, setZenStage } from "../src/daemon/server";
import { RateLimiter } from "../src/daemon/rate-limit";
import { normalizeSecurityConfig } from "../src/security/policy";
import { createZenStage } from "../src/upstreams/zen-stage";
import type { Logger } from "../src/logger";
import type { ModelDef, Upstream } from "../src/upstreams/types";

// Upstream opencode retired a free model id in production ("Model is
// unavailable", HTTP 400) while the stage kept serving it under the same id.
// When the stage then runs out of its own daily budget, the daemon has to
// answer with the stage's 429 -- not with production's older 400. Returning the
// 400 makes the caller read it as "malformed request" and retry forever
// against an id that production will never serve again.

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

const sse = (content: string) =>
  `data: ${JSON.stringify({
    id: "x",
    object: "chat.completion.chunk",
    created: 1,
    model: "muse-spark-1.3-contributor-free",
    choices: [{ index: 0, delta: { content }, finish_reason: null }],
  })}\n\ndata: [DONE]\n\n`;

// A stage that publishes the model id but answers 429 for it: its own bucket
// is spent. The catalog endpoint still has to list the id, otherwise the
// daemon treats it as "the stage does not carry this id" and never asks.
async function startExhaustedStage(modelIds: string[]): Promise<{ url: string; close: () => Promise<void> }> {
  const server = http.createServer((req, res) => {
    if (req.url === "/zen/v1/models") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ data: modelIds.map((id) => ({ id, object: "model" })) }));
      return;
    }
    req.on("data", () => {});
    req.on("end", () => {
      res.writeHead(429, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          error: { message: "Free usage limit reached. Free models work in OpenCode" },
        }),
      );
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
  const port = (server.address() as { port: number }).port;
  return {
    url: `http://127.0.0.1:${port}`,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

// A production upstream that has retired the id: 400 "Model is unavailable".
// It counts its hits, so a test can prove the retired id is not re-asked.
async function startRetiredProduction(): Promise<{
  url: string;
  hits: () => number;
  close: () => Promise<void>;
}> {
  let hits = 0;
  const server = http.createServer((req, res) => {
    hits++;
    req.on("data", () => {});
    req.on("end", () => {
      res.writeHead(400, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          error: { type: "server_error", message: "Upstream request failed: Model is unavailable." },
        }),
      );
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
  const port = (server.address() as { port: number }).port;
  return {
    url: `http://127.0.0.1:${port}/v1/chat/completions`,
    hits: () => hits,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

test("a retired production id whose stage is also spent answers 429, not production's 400", async () => {
  const modelId = "muse-spark-1.3-contributor-free";
  const stage = await startExhaustedStage([modelId]);
  const production = await startRetiredProduction();

  const zenUpstream: Upstream = {
    id: "zen",
    kind: "remote-keyless",
    relayAllowed: true,
    chatUrl: production.url,
    async fetchCatalog() {
      return null;
    },
    requestHeaders() {
      return {};
    },
  };

  const catalog = new RuntimeCatalog([zenUpstream], logger(), normalizeSecurityConfig(undefined));
  catalog.seed([md(modelId, "zen")]);

  const handle = createZenStage({ baseUrl: stage.url, ttlMs: 0 });
  setZenStage(handle);
  setZenMirror(null);
  await handle.ready();

  const bansos = createServer({
    catalog,
    rateLimiter: new RateLimiter({ limit: 1000, windowMs: 60_000 }),
    port: 0,
    log: logger(),
    startedAt: Date.now(),
    security: normalizeSecurityConfig(undefined),
  });
  await new Promise<void>((resolve) => bansos.listen(0, "127.0.0.1", () => resolve()));
  const bport = (bansos.address() as { port: number }).port;

  try {
    const res = await fetch(`http://127.0.0.1:${bport}/v1/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: modelId,
        messages: [{ role: "user", content: "hi" }],
        max_tokens: 64,
      }),
    });
    const text = await res.text();
    // The stage's 429 is the newest and most specific verdict: production's
    // 400 only said "this id is gone", which the stage already contradicted.
    assert.equal(res.status, 429, `expected 429, got ${res.status}: ${text.slice(0, 200)}`);
    // A 400 here is what the caller sees as "malformed request" and retries.
    assert.notEqual(res.status, 400);
  } finally {
    bansos.close();
    await production.close();
    await stage.close();
  }
});

test("the stage's verdict wins even when production's 400 came first in the walk", async () => {
  const modelId = "muse-spark-1.3-contributor-free";
  const stage = await startExhaustedStage([modelId]);
  const production = await startRetiredProduction();

  const zenUpstream: Upstream = {
    id: "zen",
    kind: "remote-keyless",
    relayAllowed: true,
    chatUrl: production.url,
    async fetchCatalog() {
      return null;
    },
    requestHeaders() {
      return {};
    },
  };

  const catalog = new RuntimeCatalog([zenUpstream], logger(), normalizeSecurityConfig(undefined));
  catalog.seed([md(modelId, "zen")]);

  const handle = createZenStage({ baseUrl: stage.url, ttlMs: 0 });
  setZenStage(handle);
  setZenMirror(null);
  await handle.ready();

  const bansos = createServer({
    catalog,
    rateLimiter: new RateLimiter({ limit: 1000, windowMs: 60_000 }),
    port: 0,
    log: logger(),
    startedAt: Date.now(),
    security: normalizeSecurityConfig(undefined),
  });
  await new Promise<void>((resolve) => bansos.listen(0, "127.0.0.1", () => resolve()));
  const bport = (bansos.address() as { port: number }).port;

  try {
    // Two rounds: the first marks the id retired in the runtime catalog, the
    // second must not resurrect production's 400 as the final answer.
    for (const round of [1, 2]) {
      const res = await fetch(`http://127.0.0.1:${bport}/v1/chat/completions`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          model: modelId,
          messages: [{ role: "user", content: "hi" }],
          max_tokens: 64,
        }),
      });
      const text = await res.text();
      assert.equal(res.status, 429, `round ${round}: expected 429, got ${res.status}: ${text.slice(0, 200)}`);
    }
  } finally {
    bansos.close();
    await production.close();
    await stage.close();
  }
});

test("a retired production id is not asked again once the stage refused it", async () => {
  const modelId = "muse-spark-1.3-contributor-free";
  const stage = await startExhaustedStage([modelId]);
  const production = await startRetiredProduction();

  const zenUpstream: Upstream = {
    id: "zen",
    kind: "remote-keyless",
    relayAllowed: true,
    chatUrl: production.url,
    async fetchCatalog() {
      return null;
    },
    requestHeaders() {
      return {};
    },
  };

  const catalog = new RuntimeCatalog([zenUpstream], logger(), normalizeSecurityConfig(undefined));
  catalog.seed([md(modelId, "zen")]);

  const handle = createZenStage({ baseUrl: stage.url, ttlMs: 0 });
  setZenStage(handle);
  setZenMirror(null);
  await handle.ready();

  const bansos = createServer({
    catalog,
    rateLimiter: new RateLimiter({ limit: 1000, windowMs: 60_000 }),
    port: 0,
    log: logger(),
    startedAt: Date.now(),
    security: normalizeSecurityConfig(undefined),
  });
  await new Promise<void>((resolve) => bansos.listen(0, "127.0.0.1", () => resolve()));
  const bport = (bansos.address() as { port: number }).port;

  try {
    const call = async () => {
      const res = await fetch(`http://127.0.0.1:${bport}/v1/chat/completions`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          model: modelId,
          messages: [{ role: "user", content: "hi" }],
          max_tokens: 64,
        }),
      });
      await res.text();
      return res.status;
    };

    assert.equal(await call(), 429);
    const afterFirst = production.hits();
    assert.equal(await call(), 429);
    const afterSecond = production.hits();

    // Once the id is retired, production will never serve it again, so asking
    // it again only buys the same rejection. Measured 2026-10-31: the second
    // request walked production for 70.9s before returning the very same 400
    // the stage had already superseded with its 429.
    assert.equal(
      afterSecond,
      afterFirst,
      `production was asked again after retirement (${afterFirst} -> ${afterSecond})`,
    );
  } finally {
    bansos.close();
    await production.close();
    await stage.close();
  }
});

test("a healthy stage answers a retired production id with 200", async () => {
  const modelId = "muse-spark-1.3-contributor-free";

  // The stage publishes the id and serves it.
  const stageServer = http.createServer((req, res) => {
    if (req.url === "/zen/v1/models") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ data: [{ id: modelId, object: "model" }] }));
      return;
    }
    req.on("data", () => {});
    req.on("end", () => {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.end(sse("PONG"));
    });
  });
  await new Promise<void>((resolve) => stageServer.listen(0, "127.0.0.1", () => resolve()));
  const sport = (stageServer.address() as { port: number }).port;
  const stage = { url: `http://127.0.0.1:${sport}`, close: () => new Promise<void>((resolve) => stageServer.close(() => resolve())) };

  const production = await startRetiredProduction();
  const zenUpstream: Upstream = {
    id: "zen",
    kind: "remote-keyless",
    relayAllowed: true,
    chatUrl: production.url,
    async fetchCatalog() {
      return null;
    },
    requestHeaders() {
      return {};
    },
  };
  const catalog = new RuntimeCatalog([zenUpstream], logger(), normalizeSecurityConfig(undefined));
  catalog.seed([md(modelId, "zen")]);

  const handle = createZenStage({ baseUrl: stage.url, ttlMs: 0 });
  setZenStage(handle);
  setZenMirror(null);
  await handle.ready();

  const bansos = createServer({
    catalog,
    rateLimiter: new RateLimiter({ limit: 1000, windowMs: 60_000 }),
    port: 0,
    log: logger(),
    startedAt: Date.now(),
    security: normalizeSecurityConfig(undefined),
  });
  await new Promise<void>((resolve) => bansos.listen(0, "127.0.0.1", () => resolve()));
  const bport = (bansos.address() as { port: number }).port;

  try {
    const res = await fetch(`http://127.0.0.1:${bport}/v1/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: modelId,
        messages: [{ role: "user", content: "hi" }],
        max_tokens: 64,
      }),
    });
    assert.equal(res.status, 200);
  } finally {
    bansos.close();
    await production.close();
    await stage.close();
  }
});
