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

function logger(): Logger {
  return {
    info: () => {},
    debug: () => {},
    warn: () => {},
    error: () => {},
    child: () => logger(),
  } as unknown as Logger;
}

function md(id: string, source: ModelDef["source"], extra: Partial<ModelDef> = {}): ModelDef {
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
    ...extra,
  };
}

// A fake opencode console: the model catalog plus a chat and a responses
// endpoint. `answers` maps a model id to what the endpoint replies with.
interface FakeStage {
  url: string;
  requests: Array<{ path: string; model: string; body: string }>;
  close: () => Promise<void>;
}

async function startStage(
  modelIds: string[],
  answers: Record<string, { status: number; body: unknown }>,
): Promise<FakeStage> {
  const requests: FakeStage["requests"] = [];
  const server = http.createServer((req, res) => {
    if (req.url === "/zen/v1/models") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ data: modelIds.map((id) => ({ id, object: "model" })) }));
      return;
    }
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      let model = "";
      try {
        model = JSON.parse(body).model ?? "";
      } catch {
        /* leave empty */
      }
      requests.push({ path: req.url ?? "", model, body });
      const answer = answers[model] ?? {
        status: 400,
        body: { error: { type: "server_error", message: "Model is unavailable." } },
      };
      res.writeHead(answer.status, { "content-type": "application/json" });
      res.end(typeof answer.body === "string" ? answer.body : JSON.stringify(answer.body));
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
  const port = (server.address() as { port: number }).port;
  return {
    url: `http://127.0.0.1:${port}`,
    requests,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

const sse = (content: string) =>
  `data: ${JSON.stringify({
    id: "x",
    object: "chat.completion.chunk",
    created: 1,
    model: "big-pickle",
    choices: [{ index: 0, delta: { content }, finish_reason: null }],
  })}\n\ndata: [DONE]\n\n`;

// A daemon whose only upstream is a zen upstream that always reports its
// daily free pool as spent, plus a stage that can serve the same model id.
async function startZenDaemon(
  zenModelId: string,
  stageUrl: string | null,
  stageModels: string[] = [],
  stageAnswers: Record<string, { status: number; body: unknown }> = {},
) {
  const zenUpstream: Upstream = {
    id: "zen",
    kind: "remote-keyless",
    relayAllowed: true,
    chatUrl: "http://zen.invalid/v1/chat/completions",
    async fetchCatalog() {
      return null;
    },
    requestHeaders() {
      return {};
    },
  };
  const server = http.createServer((req, res) => {
    req.on("data", () => {});
    req.on("end", () => {
      res.writeHead(429, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: { message: "Free usage limit reached" } }));
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
  const port = (server.address() as { port: number }).port;
  (zenUpstream as { chatUrl: string }).chatUrl = `http://127.0.0.1:${port}/v1/chat/completions`;

  const catalog = new RuntimeCatalog([zenUpstream], logger(), normalizeSecurityConfig(undefined));
  catalog.seed([md(zenModelId, "zen")]);

  const stage = stageUrl
    ? createZenStage({ baseUrl: stageUrl, ttlMs: 0 })
    : null;
  setZenStage(stage);
  setZenMirror(null);
  if (stage) await stage.ready();

  const bansos = createServer({
    catalog,
    rateLimiter: new RateLimiter({ limit: 1000, windowMs: 60_000 }),
    port: 0,
    log: logger(),
    startedAt: Date.now(),
    security: normalizeSecurityConfig(undefined),
  });
  await new Promise<void>((resolve) => bansos.listen(0, "127.0.0.1", () => resolve()));
  return {
    bansos,
    bport: (bansos.address() as { port: number }).port,
    server,
    stage,
    stageModels,
  };
}

test("the stage's catalog decides which ids it can serve", async () => {
  const stage = await startStage(["big-pickle", "muse-spark-1.3-contributor-free"], {});
  try {
    const handle = createZenStage({ baseUrl: stage.url, ttlMs: 0 });
    await handle.ready();
    assert.equal(handle.serves("big-pickle"), true);
    assert.equal(handle.serves("muse-spark-1.3-contributor-free"), true);
    // an id the stage does not carry is NOT servable there
    assert.equal(handle.serves("beam-free"), false);
    // only ids the daemon has a registry entry for are published
    assert.deepEqual(handle.ids().sort(), ["big-pickle", "muse-spark-1.3-contributor-free"]);
  } finally {
    await stage.close();
  }
});

test("a closed zen pool is answered by the stage under the SAME zen model id", async () => {
  const stage = await startStage(["big-pickle"], {
    "big-pickle": { status: 200, body: sse("PONG") },
  });
  const daemon = await startZenDaemon("big-pickle", stage.url, ["big-pickle"]);
  try {
    const res = await fetch(`http://127.0.0.1:${daemon.bport}/v1/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "big-pickle",
        messages: [{ role: "user", content: "hi" }],
        max_tokens: 64,
        stream: false,
      }),
    });
    const text = await res.text();
    assert.equal(res.status, 200, `expected the stage to answer, got ${res.status}: ${text.slice(0, 200)}`);
    // the stage was asked for the SAME id the client asked for
    assert.equal(stage.requests.length, 1);
    assert.equal(stage.requests[0]!.model, "big-pickle");
    assert.ok(text.includes("PONG"), `the stage answer must reach the client: ${text.slice(0, 200)}`);
  } finally {
    await daemon.bansos.close();
    await daemon.server.close();
    await stage.close();
    setZenStage(null);
  }
});

test("the stage is not asked for an id it does not carry", async () => {
  const stage = await startStage(["big-pickle"], {
    "big-pickle": { status: 200, body: sse("PONG") },
  });
  // the daemon knows beam-free, but the stage does not serve it
  const daemon = await startZenDaemon("beam-free", stage.url, ["big-pickle"]);
  try {
    const res = await fetch(`http://127.0.0.1:${daemon.bport}/v1/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "beam-free",
        messages: [{ role: "user", content: "hi" }],
        max_tokens: 64,
        stream: false,
      }),
    });
    assert.equal(res.status, 429, "an id the stage cannot serve keeps the upstream's own verdict");
    assert.equal(stage.requests.length, 0, "the stage must not be asked for an id it does not carry");
  } finally {
    await daemon.bansos.close();
    await daemon.server.close();
    await stage.close();
    setZenStage(null);
  }
});

test("a stage-backed id the production catalog does not list stays published", async () => {
  // The stage publishes its OWN catalog, so it can carry an id production
  // dropped. That id must stay published: the stage answers it under the
  // SAME id, so dropping it turns a working request into "unknown model".
  const upstream: Upstream = {
    id: "zen",
    kind: "remote-keyless",
    relayAllowed: false,
    chatUrl: "https://example.invalid/chat/completions",
    async fetchCatalog() {
      // production lists big-pickle but NOT muse-spark-1.3-contributor-free
      return [md("big-pickle", "zen")];
    },
    requestHeaders: () => ({}),
  };
  const catalog = new RuntimeCatalog([upstream], logger(), normalizeSecurityConfig(undefined));
  catalog.seed([md("big-pickle", "zen"), md("muse-spark-1.3-contributor-free", "zen")]);

  const stage = await startStage(["big-pickle", "muse-spark-1.3-contributor-free"], {});
  try {
    const handle = createZenStage({ baseUrl: stage.url, ttlMs: 0 });
    await handle.ready();
    catalog.setStagedModels(handle.ids(), (id) => handle.describe(id));

    assert.equal(catalog.resolve("muse-spark-1.3-contributor-free")?.id, "muse-spark-1.3-contributor-free");

    // a refresh that drops the id from the production list must not unpublish
    // it: the stage still serves it
    await catalog.refresh();
    assert.equal(
      catalog.resolve("muse-spark-1.3-contributor-free")?.id,
      "muse-spark-1.3-contributor-free",
      "a refresh must not drop a stage-backed id",
    );
    assert.deepEqual(catalog.stagedIds.sort(), ["big-pickle", "muse-spark-1.3-contributor-free"]);
  } finally {
    await stage.close();
  }
});

test("the stage answers even when cross-provider failover is disabled", async () => {
  // BANSOS_ZEN_FAILOVER=0 keeps a DIFFERENT provider from answering a zen id.
  // The stage is opencode's own console on another deployment, so it must
  // still answer under the same id - otherwise the switch silently kills the
  // only working zen transport.
  const previous = process.env.BANSOS_ZEN_FAILOVER;
  process.env.BANSOS_ZEN_FAILOVER = "0";
  const stage = await startStage(["big-pickle"], {
    "big-pickle": { status: 200, body: sse("PONG") },
  });
  const daemon = await startZenDaemon("big-pickle", stage.url, ["big-pickle"]);
  try {
    const res = await fetch(`http://127.0.0.1:${daemon.bport}/v1/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "big-pickle",
        messages: [{ role: "user", content: "hi" }],
        max_tokens: 64,
        stream: false,
      }),
    });
    const text = await res.text();
    assert.equal(res.status, 200, `the stage must answer under the zen id: ${text.slice(0, 200)}`);
    assert.ok(text.includes("PONG"));
  } finally {
    if (previous === undefined) delete process.env.BANSOS_ZEN_FAILOVER;
    else process.env.BANSOS_ZEN_FAILOVER = previous;
    await daemon.bansos.close();
    await daemon.server.close();
    await stage.close();
    setZenStage(null);
  }
});

test("a caller that pins the request to one upstream does not reach the stage", async () => {
  const stage = await startStage(["big-pickle"], {
    "big-pickle": { status: 200, body: sse("PONG") },
  });
  const daemon = await startZenDaemon("big-pickle", stage.url, ["big-pickle"]);
  try {
    const res = await fetch(`http://127.0.0.1:${daemon.bport}/v1/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-bansos-no-failover": "1" },
      body: JSON.stringify({
        model: "big-pickle",
        messages: [{ role: "user", content: "hi" }],
        max_tokens: 64,
        stream: false,
      }),
    });
    assert.equal(res.status, 429, "the per-request opt-out must be honoured");
    assert.equal(stage.requests.length, 0, "the stage must not be asked");
  } finally {
    await daemon.bansos.close();
    await daemon.server.close();
    await stage.close();
    setZenStage(null);
  }
});

test("without a stage a closed zen pool is still a 429", async () => {
  const daemon = await startZenDaemon("big-pickle", null);
  try {
    const res = await fetch(`http://127.0.0.1:${daemon.bport}/v1/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "big-pickle",
        messages: [{ role: "user", content: "hi" }],
        max_tokens: 64,
        stream: false,
      }),
    });
    assert.equal(res.status, 429);
  } finally {
    await daemon.bansos.close();
    await daemon.server.close();
    setZenStage(null);
  }
});
