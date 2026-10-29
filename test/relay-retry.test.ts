import { test, beforeEach } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  relayFetch,
  relayRetryState,
  resetRelayRetryState,
  deploymentEssentiallySpent,
  FLEET_EXHAUSTED_HEADER,
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

test("a relay that refuses the target does not end the request", async () => {
  // The fleet's allowlist is per deployment. A relay deployed before a
  // target was added answers 403 "target not allowlisted" - that is a
  // property of THAT relay, not of the target. The request must continue to
  // the next relay instead of failing outright.
  const { calls, restore } = stubFetch((url) => (url.includes("r1") ? 403 : 200));
  try {
    const res = await relayFetch(
      state("https://r1.example", ["https://r2.example"]),
      "https://dev.opencode.ai/inference/openai/v1/chat/completions",
      { method: "POST", headers: { "content-type": "application/json" }, body: '{"model":"big-pickle"}' },
    );
    assert.equal(res.status, 200);
    assert.ok(calls.some((c) => c.url === "https://r2.example"), "must try the next relay");
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

// PRODUCTION is a per-egress-address bucket too. The upstream limiter keys
// its daily row on the request's x-real-ip (util_handler.ts:99 ->
// util_ipRateLimiter.ts:22) and the fleet handler strips x-real-ip, so the
// relay's own egress address is what lands in that row. Measured 2026-10-29
// with the canonical contract, same model, same body, same minute: through a
// relay 200 billing_source=free, direct from HOME 429 FreeUsageLimitError.
//
// Pinning production to one relay therefore drains one address's bucket and
// leaves the other ~159 untouched. One address's allowance was measured at
// >= 800 requests/day, so the fleet is worth two orders of magnitude more
// than a pin.
test("production spreads across relays too, because its bucket is per egress address", async () => {
  const seen: string[] = [];
  const { restore } = stubFetch((url) => {
    seen.push(url);
    return 200;
  });
  try {
    const st = state("https://r1.example", ["https://r2.example", "https://r3.example"]);
    for (let i = 0; i < 3; i++) {
      const res = await relayFetch(st, TARGET, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: '{"model":"deepseek-v4.1-flash-free"}',
      });
      assert.equal(res.status, 200);
    }
    const relays = seen.filter((u) => u.includes(".example"));
    assert.equal(relays.length, 3);
    assert.equal(
      new Set(relays).size,
      3,
      "production must not pin one relay: each address is an independent daily bucket",
    );
  } finally {
    restore();
  }
});

// The production bucket is also split PER MODEL (util_ipRateLimiter.ts:
// dailyInterval = YYYYMMDD + modelId.slice(0,2) for a model with its own
// rateLimit), so a 429 on one id says nothing about the others. Measured:
// deepseek-v4.1-flash-free answered 200 on 10/160 addresses in the same
// minute that four other ids answered 429 on 160/160.
//
// The penalty is therefore keyed by address AND model: an address that is
// spent for one id stays usable for another.
test("a 429 on one model does not park that address for the other models", async () => {
  const { calls, restore } = stubFetch((url, n) => (n === 1 ? 429 : 200));
  try {
    const st = state("https://r1.example", ["https://r2.example"]);
    const first = await relayFetch(st, TARGET, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: '{"model":"deepseek-v4.1-flash-free"}',
    });
    assert.equal(first.status, 200);
    // a DIFFERENT model must not inherit r1's penalty
    calls.length = 0;
    const second = await relayFetch(st, TARGET, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: '{"model":"beam-free"}',
    });
    assert.equal(second.status, 200);
    assert.ok(
      calls.some((c) => c.url === "https://r1.example"),
      "an address spent for one model must stay usable for another model",
    );
  } finally {
    restore();
  }
});

// A 429 on the addresses this request happened to try must NOT claim the
// whole fleet is spent. The daemon parks a model fleet-wide on that signal,
// and a false "exhausted" kills a lane whose other ~147 addresses still have
// room (measured: one address served >= 800 requests/day without a 429).
test("a 429 on part of the fleet is not reported as fleet exhaustion", async () => {
  const { restore } = stubFetch((url, n) => (n <= 13 ? 429 : 200));
  try {
    const relays = Array.from({ length: 40 }, (_, i) => `https://r${i}.example`);
    const res = await relayFetch(state(relays[0]!, relays.slice(1)), TARGET, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: '{"model":"deepseek-v4.1-flash-free"}',
    });
    assert.equal(res.status, 200);
    assert.equal(
      res.headers.get(FLEET_EXHAUSTED_HEADER),
      null,
      "a served request must not claim the fleet is exhausted",
    );
  } finally {
    restore();
  }
});

test("every address answering 429 is reported as fleet exhaustion", async () => {
  const { restore } = stubFetch(() => 429);
  try {
    const relays = ["https://r1.example", "https://r2.example"];
    const res = await relayFetch(state(relays[0]!, relays.slice(1)), TARGET, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: '{"model":"deepseek-v4.1-flash-free"}',
    });
    assert.equal(res.status, 429);
    assert.equal(
      res.headers.get(FLEET_EXHAUSTED_HEADER),
      "1",
      "when every address is spent the model may be parked fleet-wide",
    );
  } finally {
    restore();
  }
});

// A deployment that is closed for a model must be recognisable BEFORE the
// request is sent, so the daemon can go to another deployment (the stage)
// instead of paying the dead round-trips.
//
// Measured 2026-10-29: production opencode.ai answered 429 on all 160
// relays for all 8 anonymous-free ids, so every request paid ~26s of
// round-trips before the stage answered in ~3s.
//
// The evidence is per (deployment, model ROW): a 429 on one deployment says
// nothing about the other one, and the upstream keys the row by the first
// two characters of the id.
test("a closed deployment is recognised for its own model only", async () => {
  const { restore } = stubFetch(() => 429);
  try {
    const relays = Array.from({ length: 13 }, (_, i) => `https://r${i}.example`);
    const st = state(relays[0]!, relays.slice(1));
    const res = await relayFetch(st, TARGET, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: '{"model":"deepseek-v4.1-flash-free"}',
    });
    assert.equal(res.status, 429);
    assert.equal(
      deploymentEssentiallySpent(st, new URL(TARGET).origin, "deepseek-v4.1-flash-free"),
      true,
      "every reachable address answered 429, so the deployment is closed for this model",
    );
    assert.equal(
      deploymentEssentiallySpent(st, new URL(TARGET).origin, "beam-free"),
      false,
      "a different model row has its own budget and must not inherit the verdict",
    );
    assert.equal(
      deploymentEssentiallySpent(st, "https://dev.opencode.ai", "deepseek-v4.1-flash-free"),
      false,
      "a different deployment has its own rows and must not inherit the verdict",
    );
  } finally {
    restore();
  }
});

// One address is not evidence about a fleet of ~160. A single 429 must not
// close the deployment.
test("a single rejected address does not close a deployment", async () => {
  const { calls, restore } = stubFetch((url, n) => (n === 1 ? 429 : 200));
  try {
    const relays = Array.from({ length: 40 }, (_, i) => `https://r${i}.example`);
    const st = state(relays[0]!, relays.slice(1));
    const res = await relayFetch(st, TARGET, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: '{"model":"deepseek-v4.1-flash-free"}',
    });
    assert.equal(res.status, 200);
    assert.equal(
      deploymentEssentiallySpent(st, new URL(TARGET).origin, "deepseek-v4.1-flash-free"),
      false,
      "one spent address must not close a 40-address fleet",
    );
  } finally {
    restore();
  }
});

// The upstream row is keyed by the FIRST TWO CHARACTERS of the model id
// (util_ipRateLimiter.ts:25: dailyInterval = YYYYMMDD + modelId.slice(0,2)),
// so two ids sharing a prefix are ONE budget. A penalty on
// "deepseek-v4.1-flash-free" must therefore also hide
// "deepseek-v4-flash" on that address - they spend the same "de" row.
test("a penalty covers the ids that share the upstream's two-character row", async () => {
  const { calls, restore } = stubFetch((url, n) => (n === 1 ? 429 : 200));
  try {
    const st = state("https://r1.example", ["https://r2.example"]);
    const first = await relayFetch(st, TARGET, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: '{"model":"deepseek-v4.1-flash-free"}',
    });
    assert.equal(first.status, 200);
    // a DIFFERENT prefix must still be usable on the same address
    calls.length = 0;
    const other = await relayFetch(st, TARGET, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: '{"model":"beam-free"}',
    });
    assert.equal(other.status, 200);
    assert.ok(calls.some((c) => c.url === "https://r1.example"), "a different prefix must not be hidden");
    // ...but the SAME prefix must be hidden on that address
    calls.length = 0;
    const same = await relayFetch(st, TARGET, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: '{"model":"deepseek-v4-flash"}',
    });
    assert.equal(same.status, 200);
    assert.ok(
      !calls.some((c) => c.url === "https://r1.example"),
      "an id sharing the two-character prefix must not reuse the spent address",
    );
  } finally {
    restore();
  }
});

test("a quota 429 on a few addresses does NOT declare the fleet exhausted", async () => {
  const { calls, restore } = stubFetch((url, n) => (n <= 2 ? 429 : 200));
  try {
    const st = state("https://r1.example", [
      "https://r2.example",
      "https://r3.example",
      "https://r4.example",
      "https://r5.example",
    ]);
    const res = await relayFetch(st, TARGET, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: '{"model":"deepseek-v4.1-flash-free"}',
    });
    assert.equal(res.status, 200);
    assert.notEqual(
      res.headers.get("x-bansos-fleet-exhausted"),
      "1",
      "a handful of spent addresses must not park the model fleet-wide",
    );
  } finally {
    restore();
  }
});

test("the fleet is only declared exhausted when every address is spent", async () => {
  const { calls, restore } = stubFetch(() => 429);
  try {
    const st = state("https://r1.example", ["https://r2.example", "https://r3.example"]);
    // drain every address so each one carries a penalty
    for (let i = 0; i < 3; i++) {
      const res = await relayFetch(st, TARGET, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: '{"model":"deepseek-v4.1-flash-free"}',
      });
      assert.equal(res.status, 429);
    }
    const res = await relayFetch(st, TARGET, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: '{"model":"deepseek-v4.1-flash-free"}',
    });
    assert.equal(res.headers.get("x-bansos-fleet-exhausted"), "1");
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
    // Production spreads now (its bucket is per egress address), so the
    // request walks the whole candidate list instead of a fixed two
    // alternates. What matters is that every relay answered 429 and the
    // answer is the LAST one, not an invented success.
    const relayCalls = calls.filter((c) => c.url !== TARGET);
    assert.equal(relayCalls.length, 4);
    assert.deepEqual(relayCalls.map((c) => c.url), [
      "https://r1.example",
      "https://r2.example",
      "https://r3.example",
      "https://r4.example",
    ]);
    // ...and the machine's own address is a DIFFERENT client identity with its
    // own allowance, so it gets exactly one last attempt before the 429 surfaces.
    assert.equal(calls.filter((c) => c.url === TARGET).length, 1);
    assert.equal(calls.length, 5);
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
