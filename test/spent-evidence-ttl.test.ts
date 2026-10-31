import { test, beforeEach } from "node:test";
import assert from "node:assert/strict";
import {
  relayFetch,
  resetRelayRetryState,
  deploymentEssentiallySpent,
  expirePenaltyForTest,
  ageSpentEvidenceForTest,
  resetSpreadCursorForTest,
  spentEvidenceTtlMs,
  type RelayState,
} from "../src/relay/egress";

// A relay URL is NOT an egress identity.
//
// The upstream meters its free tier per (egress IP, model row), but the fleet's
// relays do not own a stable address: the address is stamped by the deployment's
// edge and rotates per request. The "spent" evidence is keyed by relay URL, so it
// is an observation about a MOMENT, and it must expire on its own short clock.
//
// Measured 2026-11-01: 90 of 164 relays carried a same-day mark for
// dev.opencode.ai|mu (threshold 82), so deploymentEssentiallySpent() was true for
// BOTH zen deployments and every request - from Hermes included - answered 429
// in 7ms. Probing those exact 90 relays with the canonical contract answered 200
// on all 90: the lane was dead only in the daemon's own bookkeeping.

const TARGET = "https://dev.opencode.ai/inference/openai/v1/chat/completions";
const MODEL = "muse-spark-1.3-contributor-free";

function state(url: string, extras: string[] = []): RelayState {
  return { enabled: true, url, relays: [url, ...extras].map((u) => ({ url: u })) };
}

function stubFetch(reply: (url: string) => number): { calls: string[]; restore: () => void } {
  const calls: string[] = [];
  const original = globalThis.fetch;
  globalThis.fetch = (async (input: RequestInfo | URL) => {
    const url = String(input);
    calls.push(url);
    const status = reply(url);
    return new Response('{"error":{"type":"FreeUsageLimitError"}}', {
      status,
      headers: { "content-type": "application/json", ...(status === 429 ? { "retry-after": "3600" } : {}) },
    });
  }) as typeof fetch;
  return { calls, restore: () => { globalThis.fetch = original; } };
}

beforeEach(() => {
  resetRelayRetryState();
  resetSpreadCursorForTest();
});

// Fill a row's spent evidence with a majority of the fleet, the way a long
// session of exhausted requests does.
async function wearDown(row: RelayState, init: RequestInit, rounds: number): Promise<void> {
  for (let i = 0; i < rounds; i++) {
    expirePenaltyForTest(Date.now() - 60 * 60_000);
    await relayFetch(row, TARGET, init);
  }
}

test("an aged spent mark stops closing the deployment, so the fleet is probed again", async () => {
  const relays = Array.from({ length: 20 }, (_, i) => `https://r${i}.example`);
  const st = state(relays[0]!, relays.slice(1));
  const init = {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: `{"model":"${MODEL}"}`,
  };

  // Everything answers 429 until the bucket "comes back".
  let spent = true;
  const stub = stubFetch(() => (spent ? 429 : 200));
  try {
    await wearDown(st, init, 25);
    assert.equal(
      deploymentEssentiallySpent(st, new URL(TARGET).origin, MODEL),
      true,
      "a fleet that has answered 429 for most addresses must be treated as closed",
    );

    // The bucket comes back while the daemon is still holding same-day marks.
    spent = false;

    // The marks are what must expire. Ageing them past the TTL models the real
    // situation: the relay's egress address rotated away, so the observation no
    // longer describes the fleet. Before this fix the mark lived until 00:00 UTC
    // and the deployment stayed "closed" for the rest of the day.
    ageSpentEvidenceForTest(spentEvidenceTtlMs() + 60_000);
    assert.equal(
      deploymentEssentiallySpent(st, new URL(TARGET).origin, MODEL),
      false,
      "an aged mark must stop closing the deployment, or the lane is dead until UTC midnight",
    );

    stub.calls.length = 0;
    const res = await relayFetch(st, TARGET, init);
    assert.equal(res.status, 200, "a recovered bucket must be reachable after the marks age out");
    assert.ok(stub.calls.length > 0, "the request must have probed the fleet again");
  } finally {
    stub.restore();
  }
});

test("a fresh spent mark still short-circuits, so a closed lane stays cheap", async () => {
  const relays = Array.from({ length: 20 }, (_, i) => `https://r${i}.example`);
  const st = state(relays[0]!, relays.slice(1));
  const init = {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: `{"model":"${MODEL}"}`,
  };
  const stub = stubFetch(() => 429);
  try {
    await wearDown(st, init, 25);
    assert.equal(
      deploymentEssentiallySpent(st, new URL(TARGET).origin, MODEL),
      true,
      "a mostly-spent fleet must be known as spent",
    );
    // With the marks still young, a request must NOT re-walk the whole fleet.
    stub.calls.length = 0;
    await relayFetch(st, TARGET, init);
    assert.ok(
      stub.calls.length <= 13,
      `a closed lane must stay bounded (walked ${stub.calls.length} candidates)`,
    );
  } finally {
    stub.restore();
  }
});

test("a relay that answers 200 has its spent mark forgotten immediately", async () => {
  const st = state("https://r0.example", ["https://r1.example"]);
  const init = {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: `{"model":"${MODEL}"}`,
  };
  let first = true;
  const stub = stubFetch((url) => {
    if (url.includes("r0") && first) return 429;
    return 200;
  });
  try {
    await relayFetch(st, TARGET, init);
    first = false;
    // The same address now answers. Its mark must go, or the TTL would keep a
    // live relay out of the walk for nothing.
    expirePenaltyForTest(Date.now() - 60 * 60_000);
    resetSpreadCursorForTest();
    stub.calls.length = 0;
    const res = await relayFetch(st, TARGET, init);
    assert.equal(res.status, 200);
    assert.ok(
      stub.calls.includes("https://r0.example"),
      "a relay that answered 200 must be a candidate again on the next request",
    );
  } finally {
    stub.restore();
  }
});