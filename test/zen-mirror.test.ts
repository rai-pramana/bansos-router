import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { RuntimeCatalog } from "../src/daemon/catalog";
import { createServer, setZenMirror } from "../src/daemon/server";
import { RateLimiter } from "../src/daemon/rate-limit";
import { normalizeSecurityConfig } from "../src/security/policy";
import { createZenMirror, zenMirrorIsFreeId } from "../src/upstreams/zen-mirror";
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

// A gateway that serves an OpenAI-compatible model list and a chat endpoint.
// `answers` maps a model id to the HTTP status and body it answers with.
interface FakeGateway {
  url: string;
  close: () => Promise<void>;
  requests: Array<{ model: string; body: string }>;
}

async function startGateway(
  modelIds: string[],
  answers: Record<string, { status: number; body: unknown }>,
): Promise<FakeGateway> {
  const requests: FakeGateway["requests"] = [];
  const server = http.createServer((req, res) => {
    if (req.url === "/v1/models") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ data: modelIds.map((id) => ({ id })) }));
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
      requests.push({ model, body });
      const answer = answers[model] ?? {
        status: 404,
        body: { error: { message: `no answer for ${model}` } },
      };
      res.writeHead(answer.status, { "content-type": "application/json" });
      res.end(typeof answer.body === "string" ? answer.body : JSON.stringify(answer.body));
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
  const port = (server.address() as { port: number }).port;
  return {
    url: `http://127.0.0.1:${port}/v1`,
    requests,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

const completion = (content: string, model = "upstream-model") => ({
  id: "x",
  object: "chat.completion",
  created: 1,
  model,
  choices: [{ index: 0, message: { role: "assistant", content }, finish_reason: "stop" }],
});

// A bansos daemon whose only upstream is a zen upstream that always reports
// its daily free pool as spent. Every request must therefore be answered by
// the mirror.
async function startZenDaemon(zenModelId: string) {
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
  const bansos = createServer({
    catalog,
    rateLimiter: new RateLimiter({ limit: 1000, windowMs: 60_000 }),
    port: 0,
    log: logger(),
    startedAt: Date.now(),
    security: normalizeSecurityConfig(undefined),
  });
  await new Promise<void>((resolve) => bansos.listen(0, "127.0.0.1", () => resolve()));
  return { bansos, bport: (bansos.address() as { port: number }).port, server };
}

test("zenMirrorIsFreeId accepts the free spellings and rejects paid ones", () => {
  assert.equal(zenMirrorIsFreeId("orca/deepseek/deepseek-v4.1-flash-free"), true);
  assert.equal(zenMirrorIsFreeId("unorouter/beam:free"), true);
  assert.equal(zenMirrorIsFreeId("orca/z-ai/glm-5.3-flash"), false);
  assert.equal(zenMirrorIsFreeId("beam-free"), false, "an id without a provider prefix is not a gateway id");
});

test("the mirror maps a zen id to a free counterpart and keeps the provider order", async () => {
  const gateway = await startGateway(
    [
      "unorouter/beam:free",
      "kenari/beam-1-0:free",
      "orca/deepseek/deepseek-v4.1-flash-free",
      "unorouter/paid-model",
    ],
    {},
  );
  try {
    const mirror = createZenMirror({
      baseUrl: gateway.url,
      providers: ["orca", "unorouter", "kenari"],
    });
    await mirror.ready();

    // orca comes first and has no beam, so unorouter's beam wins.
    assert.deepEqual(mirror.mirrorsFor("beam-free"), ["unorouter/beam:free", "kenari/beam-1-0:free"]);
    // a paid id is never picked as a mirror
    assert.deepEqual(mirror.mirrorsFor("beam-free").includes("unorouter/paid-model"), false);
    assert.deepEqual(mirror.mirrorsFor("deepseek-v4.1-flash-free"), [
      "orca/deepseek/deepseek-v4.1-flash-free",
    ]);
    // a model with no counterpart has no mirror
    assert.deepEqual(mirror.mirrorsFor("some-unknown-model"), []);
  } finally {
    await gateway.close();
  }
});

test("the mirror body carries the upstream's own id, not the zen id", async () => {
  const gateway = await startGateway(["unorouter/beam:free"], {
    "unorouter/beam:free": { status: 200, body: completion("PONG") },
  });
  try {
    const mirror = createZenMirror({ baseUrl: gateway.url, providers: ["unorouter"] });
    await mirror.ready();
    const body = mirror.upstream.transformRequestBody?.({ model: "beam-free" }, md("beam-free", "zen"));
    assert.equal(body?.model, "unorouter/beam:free");
  } finally {
    await gateway.close();
  }
});

test("a closed zen pool is answered by the mirror under the SAME zen model id", async () => {
  const gateway = await startGateway(["unorouter/beam:free"], {
    "unorouter/beam:free": { status: 200, body: completion("PONG", "rewritten-upstream-id") },
  });
  const zenCalls: string[] = [];
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
    transformRequestBody(body) {
      zenCalls.push(String(body.model));
      return body;
    },
  };
  const { bansos, bport, server } = await startZenDaemon("beam-free");

  try {
    const mirror = createZenMirror({ baseUrl: gateway.url, providers: ["unorouter"] });
    await mirror.ready();
    setZenMirror(mirror);

    const res = await fetch(`http://127.0.0.1:${bport}/v1/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "beam-free",
        messages: [{ role: "user", content: "Reply PONG" }],
        max_tokens: 32,
      }),
    });
    assert.equal(res.status, 200);
    const json = (await res.json()) as { model?: string; choices?: Array<{ message: { content: string } }> };
    assert.equal(json.model, "beam-free", "the client gets the zen id back, not the upstream's id");
    assert.equal(json.choices?.[0]?.message.content, "PONG");
    assert.equal(gateway.requests[0]?.model, "unorouter/beam:free", "the mirror was asked for its own id");
  } finally {
    setZenMirror(null);
    await new Promise<void>((resolve) => bansos.close(() => resolve()));
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await gateway.close();
  }
});

test("a mirror answer that is a prose refusal is NOT served to the client", async () => {
  const gateway = await startGateway(["unorouter/beam:free"], {
    "unorouter/beam:free": {
      status: 200,
      body: completion("Sorry, to prevent abuse of free resources, please try again later."),
    },
  });
  const { bansos, bport, server } = await startZenDaemon("beam-free");

  try {
    const mirror = createZenMirror({ baseUrl: gateway.url, providers: ["unorouter"] });
    await mirror.ready();
    setZenMirror(mirror);

    const res = await fetch(`http://127.0.0.1:${bport}/v1/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "beam-free",
        messages: [{ role: "user", content: "Reply PONG" }],
        max_tokens: 32,
      }),
    });
    // The refusal must not be shipped as an answer. The daemon falls through
    // to the normal path, which reports the upstream's own failure.
    assert.notEqual(res.status, 200);
    const text = await res.text();
    assert.equal(text.includes("prevent abuse"), false, "the refusal must not reach the client");
    await new Promise<void>((resolve) => bansos.close(() => resolve()));
  } finally {
    setZenMirror(null);
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await gateway.close();
  }
});

// A mirror may answer with the answer under `reasoning` instead of `content`,
// and may label the body `text/event-stream` even though it is plain JSON
// (observed on kiosapi). Both must still reach the client as a JSON answer.
test("a mirror that answers in `reasoning` under an SSE content-type is served", async () => {
  const body = {
    id: "gen-1",
    object: "chat.completion",
    created: 1,
    model: "recursive/flint-v1.0:free",
    choices: [
      {
        index: 0,
        finish_reason: "length",
        message: { role: "assistant", content: null, reasoning: "PONG" },
      },
    ],
  };
  const gateway = await startGateway(
    ["kiosapi/flint-v1.0-free"],
    { "kiosapi/flint-v1.0-free": { status: 200, body: JSON.stringify(body) } },
  );
  // Relabel the gateway's response as SSE, the way kiosapi does.
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (input: any, init?: any) => {
    const res = await originalFetch(input, init);
    if (!String(input).includes(gateway.url)) return res;
    return new Response(await res.text(), {
      status: res.status,
      headers: { "content-type": "text/event-stream" },
    });
  }) as typeof fetch;

  const { bansos, bport, server } = await startZenDaemon("flint-40b-free");
  try {
    const mirror = createZenMirror({ baseUrl: gateway.url, providers: ["kiosapi"] });
    await mirror.ready();
    setZenMirror(mirror);

    const res = await fetch(`http://127.0.0.1:${bport}/v1/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "flint-40b-free",
        messages: [{ role: "user", content: "Reply PONG" }],
        max_tokens: 32,
      }),
    });
    assert.equal(res.status, 200);
    const json = (await res.json()) as {
      model?: string;
      choices?: Array<{ message: { content?: string; reasoning?: string } }>;
    };
    assert.equal(json.model, "flint-40b-free");
    const answer = json.choices?.[0]?.message?.content ?? json.choices?.[0]?.message?.reasoning;
    assert.equal(answer, "PONG");
  } finally {
    globalThis.fetch = originalFetch;
    setZenMirror(null);
    await new Promise<void>((resolve) => bansos.close(() => resolve()));
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await gateway.close();
  }
});

// A mirror that answers with an SSE stream while the client asked for JSON
// (observed on the glm mirror) must be reassembled into one JSON completion.
test("a mirror that answers with SSE while the client wants JSON is reassembled", async () => {
  const sse =
    'data: {"id":"1","object":"chat.completion.chunk","model":"glm-upstream","choices":[{"index":0,"delta":{"content":"PONG"},"finish_reason":null}]}\n\n' +
    'data: {"id":"1","object":"chat.completion.chunk","model":"glm-upstream","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n' +
    "data: [DONE]\n\n";
  const gateway = await startGateway(["orca/z-ai/glm-5.3-flash-free"], {
    "orca/z-ai/glm-5.3-flash-free": { status: 200, body: sse },
  });
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (input: any, init?: any) => {
    const res = await originalFetch(input, init);
    if (!String(input).includes(gateway.url)) return res;
    return new Response(await res.text(), {
      status: res.status,
      headers: { "content-type": "text/event-stream" },
    });
  }) as typeof fetch;

  const { bansos, bport, server } = await startZenDaemon("glm-5.3-flash-free");
  try {
    const mirror = createZenMirror({ baseUrl: gateway.url, providers: ["orca"] });
    await mirror.ready();
    setZenMirror(mirror);

    const res = await fetch(`http://127.0.0.1:${bport}/v1/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "glm-5.3-flash-free",
        messages: [{ role: "user", content: "Reply PONG" }],
        max_tokens: 32,
      }),
    });
    assert.equal(res.status, 200);
    assert.match(res.headers.get("content-type") ?? "", /application\/json/);
    const json = (await res.json()) as { model?: string; choices?: Array<{ message: { content: string } }> };
    assert.equal(json.model, "glm-5.3-flash-free");
    assert.equal(json.choices?.[0]?.message.content, "PONG");
  } finally {
    globalThis.fetch = originalFetch;
    setZenMirror(null);
    await new Promise<void>((resolve) => bansos.close(() => resolve()));
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await gateway.close();
  }
});
