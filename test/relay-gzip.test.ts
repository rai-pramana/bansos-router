import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import zlib from "node:zlib";
import { createServer } from "../src/daemon/server";
import { RuntimeCatalog } from "../src/daemon/catalog";
import { RateLimiter } from "../src/daemon/rate-limit";
import { createLogger } from "../src/logger";
import { SEEDED_MODELS, DEFAULT_UPSTREAMS } from "../src/upstreams";
import { DEFAULT_SECURITY_CONFIG } from "../src/security/policy";
import { loadRelayState, saveRelayState, resetGzipCache, gzipBodyForTest } from "../src/relay/egress";
import { modelDef } from "../src/upstreams/types";

// bansos-opt4: Vercel's Fast Origin Transfer meter counts bytes on the WIRE, so
// the daemon compresses the outbound chat body. Measured on a real probe
// function: identical 30 MB of JSON cost +36.5 MB FOT as plain text and +0.1 MB
// gzipped. These tests pin the contract — a body over the threshold must arrive
// gzip-encoded, and a tiny one must be left alone (compressing it would only add
// overhead).

interface Seen {
  headers: http.IncomingHttpHeaders;
  wireBytes: number;
  decoded: string;
}

function recordRelay(): Promise<{
  relayUrl: string;
  seen: Seen[];
  close: () => Promise<void>;
}> {
  const seen: Seen[] = [];
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
    res.writeHead(200, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        id: "chatcmpl-gzip",
        object: "chat.completion",
        created: Date.now(),
        model: "mock-model",
        choices: [
          { index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" },
        ],
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
  const server = createServer({
    catalog,
    rateLimiter,
    port: 0,
    log,
    startedAt: Date.now(),
  });
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
const TINY_TEXT = "hi"; // well under the threshold

test("Relay Egress: a body over 1 KB is gzipped on the wire and still arrives as JSON", async () => {
  const original = loadRelayState();
  resetGzipCache();
  const relay = await recordRelay();
  const { baseUrl, catalog, close } = await setupDaemon();
  try {
    saveRelayState({
      enabled: true,
      url: relay.relayUrl,
      relays: [{ url: relay.relayUrl, label: "Gzip Test Relay" }],
    });

    catalog.seed([
      modelDef({
        id: "mock/gzip-test:free",
        name: "Mock Gzip Test",
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
      body: JSON.stringify({
        model: "mock/gzip-test:free",
        messages: [{ role: "user", content: BIG_TEXT }],
      }),
    });
    assert.equal(res.status, 200);

    assert.equal(relay.seen.length, 1);
    const req = relay.seen[0]!;
    assert.equal(req.headers["content-encoding"], "gzip", "wire body must be gzip");
    // Vercel's edge buffers a CHUNKED gzip stream and answers 400 with an empty
    // body before the function runs, so the compressed length must be declared.
    assert.equal(
      Number(req.headers["content-length"]),
      req.wireBytes,
      "gzip body must declare its compressed content-length",
    );
    assert.ok(
      req.wireBytes < req.decoded.length / 3,
      `wire ${req.wireBytes} should be far smaller than decoded ${req.decoded.length}`,
    );
    assert.ok(req.decoded.includes("Hello relay"), "decoded body must be the original JSON");
    // the relay must be able to parse it back into a request
    const parsed = JSON.parse(req.decoded) as { messages: Array<{ content: string }> };
    assert.ok(parsed.messages[0]?.content.includes("Hello relay"));
  } finally {
    saveRelayState(original);
    resetGzipCache();
    await relay.close();
    await close();
  }
});

test("Relay Egress: small bodies and non-strings are never compressed", async () => {
  // A real chat body is always >1 KB once transformed, so this path cannot be
  // reached through HTTP — assert the helper directly instead.
  resetGzipCache();

  const tiny = JSON.stringify({ messages: [{ role: "user", content: "hi" }] });
  const small = await gzipBodyForTest(tiny);
  assert.equal(small.gzipped, false, "body under the threshold must stay plain");
  assert.equal(small.body, tiny);

  // exactly at the threshold compresses; one byte under does not
  const atLimit = "x".repeat(1024);
  assert.equal((await gzipBodyForTest(atLimit)).gzipped, true, "1024 B must compress");
  assert.equal((await gzipBodyForTest("x".repeat(1023))).gzipped, false, "1023 B must not");

  // non-string bodies (streams, Uint8Array) are passed through untouched: they
  // cannot be compressed without consuming them, and relayFetch already skips
  // those on its own single-attempt path.
  const bytes = new Uint8Array([1, 2, 3]);
  const passthrough = await gzipBodyForTest(bytes);
  assert.equal(passthrough.gzipped, false);
  assert.equal(passthrough.body, bytes);
  assert.equal((await gzipBodyForTest(null)).gzipped, false);
  assert.equal((await gzipBodyForTest(undefined)).gzipped, false);
});

test("Relay Egress: BANSOS_RELAY_GZIP=0 disables compression", async () => {
  const original = loadRelayState();
  const prev = process.env.BANSOS_RELAY_GZIP;
  process.env.BANSOS_RELAY_GZIP = "0";
  resetGzipCache();
  const relay = await recordRelay();
  const { baseUrl, catalog, close } = await setupDaemon();
  try {
    saveRelayState({
      enabled: true,
      url: relay.relayUrl,
      relays: [{ url: relay.relayUrl, label: "Gzip Test Relay" }],
    });

    catalog.seed([
      modelDef({
        id: "mock/gzip-test:free",
        name: "Mock Gzip Test",
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
      body: JSON.stringify({
        model: "mock/gzip-test:free",
        messages: [{ role: "user", content: BIG_TEXT }],
      }),
    });
    assert.equal(res.status, 200);

    const req = relay.seen[0]!;
    assert.equal(req.headers["content-encoding"], undefined, "kill switch must keep the body plain");
    assert.ok(req.decoded.includes("Hello relay"));
  } finally {
    saveRelayState(original);
    if (prev === undefined) delete process.env.BANSOS_RELAY_GZIP;
    else process.env.BANSOS_RELAY_GZIP = prev;
    resetGzipCache();
    await relay.close();
    await close();
  }
});
