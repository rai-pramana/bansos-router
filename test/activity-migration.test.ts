import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { ActivityStore } from "../src/daemon/activity";
import { BANSOS_DIR } from "../src/daemon/state";

// HARD GUARD, same rule as activity-store.test.ts: these tests write and rename
// files in BANSOS_DIR. Run through `npm test` (which preloads test/isolate.ts) or
// the paths below would point at the operator's live history.
const _dir = process.env.BANSOS_DIR;
const _tmp = process.env.TMPDIR || "/tmp";
assert.ok(_dir, "BANSOS_DIR must be set (run via `npm test`, which preloads test/isolate.ts)");
assert.ok(
  _dir!.startsWith(_tmp) || _dir!.includes("bansos-test-"),
  `refusing to touch a non-temp BANSOS_DIR: ${_dir}`,
);

const LEGACY = path.join(BANSOS_DIR, "activity.jsonl");

function line(over: Record<string, unknown> = {}): string {
  return `${JSON.stringify({
    id: 0,
    timestamp: Date.now(),
    kind: "chat",
    model: "deepseek-v4.1-flash-free",
    requestedModel: "deepseek-v4.1-flash-free",
    upstream: "zen",
    inputTokens: 100,
    outputTokens: 10,
    cachedTokens: 40,
    durationMs: 1200,
    status: "ok",
    ...over,
  })}\n`;
}

function freshStore(): ActivityStore {
  // start from an empty month-file store for every case
  const s = new ActivityStore();
  s.clear();
  return s;
}

test("the flat pre-0.6.2 activity.jsonl is migrated into the month files", () => {
  freshStore();
  const old = Date.parse("2026-11-01T10:00:00+08:00");
  const newer = Date.parse("2026-11-02T10:00:00+08:00");
  fs.writeFileSync(LEGACY, line({ id: 1, timestamp: old }) + line({ id: 2, timestamp: newer, model: "big-pickle" }));

  const store = new ActivityStore();
  const all = store.getAllEvents();
  assert.equal(all.length, 2, "both old events were imported");
  assert.deepEqual(all.map((e) => e.model).sort(), ["big-pickle", "deepseek-v4.1-flash-free"]);
  assert.ok(!fs.existsSync(LEGACY), "the flat file is renamed away after migration");
  assert.ok(fs.existsSync(`${LEGACY}.migrated`), "and kept for recovery, not deleted");
});

test("imported ids are renumbered so they cannot collide with the month files", async () => {
  freshStore();
  const store = new ActivityStore();
  // an event recorded natively first: it owns id 1 in the month files
  store.record({ timestamp: Date.now(), model: "native", requestedModel: "native", upstream: "kilo", inputTokens: 1, outputTokens: 1, durationMs: 5, status: "ok" });
  await store.whenIdle();
  store.flush();

  // the legacy file also starts at id 1 — the collision that made a naive
  // id-preserving import drop everything as a duplicate
  fs.writeFileSync(LEGACY, line({ id: 1, timestamp: Date.now() - 60_000, model: "legacy-a" }));
  const reopened = new ActivityStore();

  const ids = reopened.getAllEvents().map((e) => e.id);
  assert.equal(new Set(ids).size, ids.length, "ids stay unique across the merge");
  assert.equal(reopened.getAllEvents().length, 2, "the legacy event survived the id collision");
  assert.equal(reopened.getUsage("all").totalRequests, 2);
});

test("a second boot does not double count (the rename is the guard)", () => {
  freshStore();
  const t = Date.parse("2026-11-03T10:00:00+08:00");
  fs.writeFileSync(LEGACY, line({ id: 7, timestamp: t }));

  const first = new ActivityStore();
  assert.equal(first.getUsage("all").totalRequests, 1);
  const second = new ActivityStore();
  assert.equal(second.getUsage("all").totalRequests, 1, "no duplicate after a restart");
});

test("re-importing the same content is a no-op even without the rename", () => {
  freshStore();
  const t = Date.parse("2026-11-04T10:00:00+08:00");
  const body = line({ id: 1, timestamp: t });

  fs.writeFileSync(LEGACY, body);
  const store = new ActivityStore();
  assert.equal(store.getUsage("all").totalRequests, 1);

  // simulate the rename having failed: same content, different id
  fs.writeFileSync(LEGACY, body.replace('"id":1', '"id":999'));
  const again = new ActivityStore();
  assert.equal(again.getUsage("all").totalRequests, 1, "content dedupe catches it");
});

test("two genuinely distinct requests that differ only in timing are both kept", () => {
  freshStore();
  const t = Date.parse("2026-11-05T10:00:00+08:00");
  fs.writeFileSync(LEGACY, line({ id: 1, timestamp: t, durationMs: 1000 }) + line({ id: 2, timestamp: t, durationMs: 1001 }));

  const store = new ActivityStore();
  assert.equal(store.getUsage("all").totalRequests, 2, "near-identical events are not collapsed");
});

test("a torn trailing line is skipped, not fatal", () => {
  freshStore();
  fs.writeFileSync(LEGACY, line({ id: 1 }) + '{"id":2,"timestamp":');
  const store = new ActivityStore();
  assert.equal(store.getUsage("all").totalRequests, 1, "the good line still imports");
  assert.ok(fs.existsSync(`${LEGACY}.migrated`), "the torn file is still retired");
});

test("an unparseable legacy file is left in place rather than retired", () => {
  freshStore();
  fs.writeFileSync(LEGACY, "not json at all\nnor this\n");
  const store = new ActivityStore();
  assert.equal(store.getUsage("all").totalRequests, 0);
  assert.ok(fs.existsSync(LEGACY), "nothing was salvageable, so the file stays for a manual look");
});
