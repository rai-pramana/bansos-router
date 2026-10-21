import { test, beforeEach } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  relayFetch,
  relayRetryState,
  resetRelayRetryState,
  type RelayState,
} from "../src/relay/egress";

// zen meters its free tier per relay AND per model, so a 429 from one relay is
// not a verdict on the model: measured 148/159 relays served deepseek while 11
// (an entire region's worth) did not. relayFetch must therefore fail over to the
// next relay instead of handing the user a 429 it could have avoided.

const TARGET = "https://opencode.ai/inference/v1/chat/completions";

function state(url: string, extras: string[] = []): RelayState {
  return {
    enabled: true,
    url,
    relays: [url, ...extras].map((u) => ({ url: u })),
  };
}

type Call = { url: string; body: unknown };

function stubFetch(reply: (url: string, n: number) => number): { calls: Call[]; restore: () => void } {
  const calls: Call[] = [];
  const original = globalThis.fetch;
  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    calls.push({ url, body: init?.body });
    const status = reply(url, calls.length);
    return new Response(status === 200 ? '{"ok":true}' : '{"error":{"type":"FreeUsageLimitError"}}', {
      status,
      headers: { "content-type": "application/json" },
    });
  }) as typeof fetch;
  return { calls, restore: () => { globalThis.fetch = original; } };
}

// Hermetic: this machine publishes a real ranking at ~/.bansos/logs/.relay-rank.json
// and it would otherwise outrank the fixture relays.
const NO_RANK = path.join(os.tmpdir(), "bansos-test-no-rank.json");
beforeEach(() => {
  resetRelayRetryState();
  process.env.BANSOS_RELAY_RANK = NO_RANK;
});

test("a 429 from the active relay is retried on the next relay and served", async () => {
  const { calls, restore } = stubFetch((url) => (url.includes("r1") ? 429 : 200));
  try {
    const res = await relayFetch(state("https://r1.example", ["https://r2.example"]), TARGET, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: '{"model":"deepseek-v4.1-flash-free"}',
    });
    assert.equal(res.status, 200);
    assert.deepEqual(calls.map((c) => c.url), ["https://r1.example", "https://r2.example"]);
    // the body must survive the retry verbatim — a replayed request with an empty
    // body would "succeed" at the transport level and fail at the model
    assert.equal(calls[1]!.body, '{"model":"deepseek-v4.1-flash-free"}');
    const st = relayRetryState();
    assert.equal(st.preferred, "https://r2.example");
    assert.deepEqual(st.penalized, ["https://r1.example"]);
  } finally {
    restore();
  }
});

test("the relay that served the retry is tried first on the next request", async () => {
  const { calls, restore } = stubFetch((url) => (url.includes("r1") ? 429 : 200));
  try {
    const st = state("https://r1.example", ["https://r2.example"]);
    const init = { method: "POST", body: "{}" };
    await relayFetch(st, TARGET, init);
    const afterFirst = calls.length;
    const res = await relayFetch(st, TARGET, init);
    assert.equal(res.status, 200);
    // one hop, not two: without stickiness every request pays the dead relay
    assert.equal(calls.length, afterFirst + 1);
    assert.equal(calls[calls.length - 1]!.url, "https://r2.example");
  } finally {
    restore();
  }
});

test("a non-retryable status is returned as-is without probing another relay", async () => {
  const { calls, restore } = stubFetch(() => 400);
  try {
    const res = await relayFetch(state("https://r1.example", ["https://r2.example"]), TARGET, {
      method: "POST",
      body: "{}",
    });
    assert.equal(res.status, 400);
    assert.equal(calls.length, 1);
    assert.deepEqual(relayRetryState().penalized, []);
  } finally {
    restore();
  }
});

test("when every relay is spent the last 429 is returned, not an invented success", async () => {
  const { calls, restore } = stubFetch(() => 429);
  try {
    const res = await relayFetch(
      state("https://r1.example", ["https://r2.example", "https://r3.example", "https://r4.example"]),
      TARGET,
      { method: "POST", body: "{}" },
    );
    assert.equal(res.status, 429);
    // one active + two alternates: a global 429 must not fan out over the fleet
    assert.equal(calls.length, 3);
  } finally {
    restore();
  }
});

test("an unreadable body is sent exactly once (no replay of a stream)", async () => {
  const { calls, restore } = stubFetch((url) => (url.includes("r1") ? 429 : 200));
  try {
    const stream = new ReadableStream({
      start(c) {
        c.enqueue(new TextEncoder().encode("{}"));
        c.close();
      },
    });
    const res = await relayFetch(state("https://r1.example", ["https://r2.example"]), TARGET, {
      method: "POST",
      body: stream,
      duplex: "half",
    } as RequestInit);
    assert.equal(res.status, 429);
    assert.equal(calls.length, 1);
  } finally {
    restore();
  }
});

test("the scheduler's FOT ranking decides which alternate is tried first", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "relay-rank-"));
  const rank = path.join(dir, "rank.json");
  // r3 is the coolest account, r2 is hotter: fleet order says r2, ranking says r3
  fs.writeFileSync(rank, JSON.stringify({ at: Date.now() / 1000, rank: ["https://r3.example", "https://r2.example"] }));
  process.env.BANSOS_RELAY_RANK = rank;
  const { calls, restore } = stubFetch((url) => (url.includes("r1") ? 429 : 200));
  try {
    const res = await relayFetch(
      state("https://r1.example", ["https://r2.example", "https://r3.example"]),
      TARGET,
      { method: "POST", body: "{}" },
    );
    assert.equal(res.status, 200);
    assert.deepEqual(calls.map((c) => c.url), ["https://r1.example", "https://r3.example"]);
    assert.equal(relayRetryState().preferred, "https://r3.example");
  } finally {
    restore();
    process.env.BANSOS_RELAY_RANK = NO_RANK;
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("a stale ranking is ignored rather than trusted", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "relay-rank-"));
  const rank = path.join(dir, "rank.json");
  const old = Date.now() / 1000 - 3 * 3600;
  fs.writeFileSync(rank, JSON.stringify({ at: old, rank: ["https://r3.example", "https://r2.example"] }));
  process.env.BANSOS_RELAY_RANK = rank;
  const { calls, restore } = stubFetch((url) => (url.includes("r1") ? 429 : 200));
  try {
    await relayFetch(state("https://r1.example", ["https://r2.example", "https://r3.example"]), TARGET, {
      method: "POST",
      body: "{}",
    });
    // falls back to fleet order: r2, not the stale ranking's r3
    assert.deepEqual(calls.map((c) => c.url), ["https://r1.example", "https://r2.example"]);
  } finally {
    restore();
    process.env.BANSOS_RELAY_RANK = NO_RANK;
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
