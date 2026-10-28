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

test("the stage spreads requests across relays instead of pinning one", async () => {
  // The stage's upstream bucket is per egress address, so consecutive
  // requests must land on DIFFERENT relays. Pinning one relay would drain
  // one bucket and waste the other ~165.
  const seen: string[] = [];
  const { calls, restore } = stubFetch((url) => {
    seen.push(url);
    return 200;
  });
  try {
    const st = state("https://r1.example", ["https://r2.example", "https://r3.example"]);
    for (let i = 0; i < 3; i++) {
      const res = await relayFetch(st, "https://dev.opencode.ai/inference/openai/v1/chat/completions", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: '{"model":"big-pickle"}',
      });
      assert.equal(res.status, 200);
    }
    const relays = seen.filter((u) => u.includes(".example"));
    assert.equal(relays.length, 3);
    assert.equal(new Set(relays).size, 3, "each request must use a different relay");
  } finally {
    restore();
  }
});

test("a relay spent on production is still tried on the stage", async () => {
  // The quota bucket is per (egress IP, deployment). Production and the
  // STAGE are separate deployments with separate databases, so the SAME
  // relay has a separate bucket on each. A production outage must not hide
  // the stage's fresh buckets - that is exactly what the stage exists for.
  const { calls, restore } = stubFetch((url) => (url.includes("r1") ? 429 : 200));
  try {
    // production: r1 answers 429, so it gets penalised
    const prod = await relayFetch(state("https://r1.example", ["https://r2.example"]), TARGET, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: '{"model":"big-pickle"}',
    });
    assert.equal(prod.status, 200);
    assert.ok(calls.some((c) => c.url === "https://r2.example"));

    // the stage: r1 must be tried again, because its penalty was for
    // opencode.ai, not for dev.opencode.ai
    calls.length = 0;
    const stage = await relayFetch(
      state("https://r1.example", ["https://r2.example"]),
      "https://dev.opencode.ai/inference/openai/v1/chat/completions",
      { method: "POST", headers: { "content-type": "application/json" }, body: '{"model":"big-pickle"}' },
    );
    assert.equal(stage.status, 200);
    assert.equal(calls[0]!.url, "https://r1.example", "the stage must try the relay the production penalty hid");
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
    const relayCalls = calls.filter((c) => c.url !== TARGET);
    assert.equal(relayCalls.length, 3);
    // ...but the machine's own address is a DIFFERENT client identity with its
    // own allowance, so it gets exactly one last attempt before the 429 surfaces.
    assert.equal(calls.filter((c) => c.url === TARGET).length, 1);
    assert.equal(calls.length, 4);
  } finally {
    restore();
  }
});

test("a spent fleet falls back to the direct path and serves the request", async () => {
  // every relay is out of allowance, the local address is not
  const { calls, restore } = stubFetch((url) => (url === TARGET ? 200 : 429));
  try {
    const res = await relayFetch(
      state("https://r1.example", ["https://r2.example", "https://r3.example"]),
      TARGET,
      { method: "POST", body: '{"model":"deepseek-v4.1-flash-free"}' },
    );
    assert.equal(res.status, 200);
    assert.equal(calls[calls.length - 1]!.url, TARGET);
    // the direct attempt replays the body, not an empty request
    assert.equal(calls[calls.length - 1]!.body, '{"model":"deepseek-v4.1-flash-free"}');
  } finally {
    restore();
  }
});

test("a spent relay stays parked for the upstream Retry-After, not 5 minutes", async () => {
  const original = globalThis.fetch;
  const calls: string[] = [];
  globalThis.fetch = (async (input: RequestInfo | URL) => {
    const url = String(input);
    calls.push(url);
    if (url.includes("r1")) {
      return new Response('{"error":{"type":"FreeUsageLimitError"}}', {
        status: 429,
        headers: { "retry-after": "3600" },
      });
    }
    return new Response('{"ok":true}', { status: 200 });
  }) as typeof fetch;
  try {
    const st = state("https://r1.example", ["https://r2.example"]);
    await relayFetch(st, TARGET, { method: "POST", body: "{}" });
    const afterFirst = calls.length;
    // r1 is parked for an hour, r2 is preferred: the spent relay cannot be
    // charged a round-trip again (that is what burned the pool all day).
    await relayFetch(st, TARGET, { method: "POST", body: "{}" });
    assert.equal(calls.length, afterFirst + 1);
    assert.ok(!calls.slice(afterFirst).some((u) => u.includes("r1")));
  } finally {
    globalThis.fetch = original;
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
