import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

// The zen-gate module reads BANSOS_DIR at import time, so the marker lives
// under a temp HOME for these tests.
const TMP_HOME = fs.mkdtempSync(path.join(os.tmpdir(), "zen-gate-test-"));
process.env.BANSOS_DIR = path.join(TMP_HOME, ".bansos");

const { noteZenQuotaExhausted, noteZenQuotaRecovered, nextUtcMidnight, zenQuotaClosedForMs } = await import(
  "../src/daemon/zen-gate"
);

const MARKER = path.join(process.env.BANSOS_DIR!, "logs", ".zen-quota.json");

function readMarker(): Record<string, unknown> {
  return JSON.parse(fs.readFileSync(MARKER, "utf8"));
}

test("a quota rejection closes the probe lane for the upstream's own window", () => {
  noteZenQuotaExhausted(6 * 60 * 60_000, "test exhaustion");
  const marker = readMarker();
  assert.equal(marker.freeze, false);
  assert.equal(marker.source, "bansosd");
  assert.ok(
    Number(marker.until) * 1000 - Date.now() > 5.9 * 60 * 60_000,
    `until should be ~6h out, got ${Number(marker.until) * 1000 - Date.now()}ms`,
  );
});

test("the marker uses epoch SECONDS so zen_quota_gate.py can read it", () => {
  // A fresh marker: the rule "a shorter window never shortens an existing one"
  // would otherwise keep the 6h window from the previous test.
  fs.rmSync(MARKER, { force: true });
  noteZenQuotaExhausted(60_000);
  const marker = readMarker();
  // zen_quota_gate.py compares marker["until"] against `date +%s` (seconds).
  const delta = Number(marker.until) - Math.floor(Date.now() / 1000);
  assert.ok(delta > 30 && delta <= 61, `until must be epoch seconds ~60s ahead, got ${delta}`);
});

test("a later, longer window wins and a shorter one never shortens it", () => {
  noteZenQuotaExhausted(6 * 60 * 60_000);
  const long = Number(readMarker().until);
  noteZenQuotaExhausted(60_000);
  assert.equal(Number(readMarker().until), long, "a shorter window must not shorten the gate");
  noteZenQuotaExhausted(12 * 60 * 60_000);
  assert.ok(Number(readMarker().until) > long, "a longer window extends the gate");
});

test("a successful zen request reopens the lane", () => {
  noteZenQuotaExhausted(60_000);
  assert.ok(fs.existsSync(MARKER));
  noteZenQuotaRecovered();
  assert.equal(fs.existsSync(MARKER), false, "the marker is removed so the lane is open again");
});

test("a deliberate freeze is never touched by the daemon", () => {
  fs.mkdirSync(path.dirname(MARKER), { recursive: true });
  fs.writeFileSync(
    MARKER,
    JSON.stringify({ until: 4102444800, freeze: true, reason: "human freeze" }),
  );
  noteZenQuotaExhausted(60_000);
  assert.equal(readMarker().freeze, true, "the freeze survives an exhaustion notice");
  assert.equal(readMarker().reason, "human freeze");
  noteZenQuotaRecovered();
  assert.equal(readMarker().freeze, true, "the freeze survives a recovery notice");
  assert.ok(fs.existsSync(MARKER));
});

test("nextUtcMidnight lands on the next 00:00 UTC", () => {
  const now = Date.UTC(2026, 9, 24, 23, 30, 0);
  const delta = nextUtcMidnight(now);
  assert.equal(delta, 30 * 60_000);
  const iso = new Date(now + delta).toISOString();
  assert.equal(iso, "2026-10-25T00:00:00.000Z");
});

test("the closed window is readable so a restart does not forget the park", () => {
  // The daemon's per-model park lives in memory; only this window survives a
  // restart. It must report the REMAINING time, so boot code can re-park for
  // exactly what is left instead of starting from zero (a wasted round trip)
  // or from a fresh full day (a park that outlives the real refill).
  // Prior tests can leave a FROZEN marker behind, which deliberately makes an
  // exhaustion notice a no-op; start from a clean, unfrozen lane.
  fs.rmSync(MARKER, { force: true });
  noteZenQuotaExhausted(6 * 60 * 60_000, "restart test");
  const remaining = zenQuotaClosedForMs();
  // `until` is stored as ceil(epoch seconds), so a fresh window can read up to
  // one second PAST the requested length; anything meaningfully longer would
  // mean the window was re-stamped from now instead of from the refill moment.
  assert.ok(remaining > 5.9 * 60 * 60_000 && remaining <= 6 * 60 * 60_000 + 1_000, `unexpected window ${remaining}`);

  // Past the deadline it reports open, so the lane cannot stay closed forever.
  assert.equal(zenQuotaClosedForMs(Date.now() + 7 * 60 * 60_000), 0);

  // No marker at all is an open lane, not an error.
  noteZenQuotaRecovered();
  assert.equal(zenQuotaClosedForMs(), 0);
});

test("a human freeze leaves the closed-window question to the human", () => {
  fs.mkdirSync(path.dirname(MARKER), { recursive: true });
  fs.writeFileSync(MARKER, JSON.stringify({ until: 4102444800, freeze: true, reason: "human freeze" }));
  // A freeze is a human decision, not a measured refill moment: reporting it as
  // a closed window would let boot code re-park every zen model indefinitely.
  assert.equal(zenQuotaClosedForMs(), 0);
  noteZenQuotaRecovered();
  fs.unlinkSync(MARKER);
});
