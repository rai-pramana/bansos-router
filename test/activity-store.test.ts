import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import {
  ActivityStore,
  ACTIVITY_FILE,
  LEGACY_ACTIVITY_FILE,
  type ActivityEvent,
} from "../src/daemon/activity";

function reset(): void {
  // HARD GUARD. These tests delete and rewrite the activity file. Run through
  // `npm test` (which preloads test/isolate.ts and points BANSOS_DIR at a temp
  // dir) and this is harmless; run directly with `tsx --test` and it would wipe
  // the operator's real history. That happened once: it deleted 5000 events.
  const dir = process.env.BANSOS_DIR;
  const tmp = process.env.TMPDIR || "/tmp";
  assert.ok(dir, "BANSOS_DIR must be set (run via `npm test`, which preloads test/isolate.ts)");
  assert.ok(
    dir!.startsWith(tmp) || dir!.includes("bansos-test-"),
    `refusing to touch a non-temp BANSOS_DIR: ${dir}`,
  );
  for (const p of [ACTIVITY_FILE, LEGACY_ACTIVITY_FILE, `${LEGACY_ACTIVITY_FILE}.migrated`]) {
    try {
      fs.rmSync(p, { force: true });
    } catch {
      // ignore
    }
  }
}

function ev(over: Partial<ActivityEvent> = {}): ActivityEvent {
  return {
    id: 1,
    timestamp: Date.now(),
    kind: "chat",
    model: "m",
    requestedModel: "m",
    upstream: "zen",
    inputTokens: 10,
    outputTokens: 20,
    durationMs: 100,
    status: "ok",
    ...over,
  };
}


// Seed history by writing the JSONL file the loader actually reads: record() is
// for live events and always stamps them with the current time.
function seed(events: Array<Partial<ActivityEvent>>): void {
  const lines = events
    .map((e, i) => JSON.stringify({ ...ev({ id: i + 1 }), ...e }))
    .join("\n");
  fs.writeFileSync(ACTIVITY_FILE, `${lines}\n`);
}

test("records append one line per event and never rewrite the file", () => {
  reset();
  const store = new ActivityStore();
  const sizes: number[] = [];
  for (let i = 0; i < 5; i++) {
    store.record(ev({ id: i + 1 }));
    sizes.push(fs.statSync(ACTIVITY_FILE).size);
  }
  const lines = fs.readFileSync(ACTIVITY_FILE, "utf8").trim().split("\n");
  assert.equal(lines.length, 5, "one line per event");
  // a rewrite would make every step cost the whole file; appends only grow
  const deltas = sizes.map((s, i) => s - (i === 0 ? 0 : sizes[i - 1]!));
  const max = Math.max(...deltas);
  const min = Math.min(...deltas);
  assert.ok(max / min < 2, `each append costs ~the same (${min}..${max})`);

  const last = JSON.parse(lines[4]!) as ActivityEvent;
  assert.equal(last.id, 5);
  assert.equal(last.status, "ok");
});

test("a torn trailing line is skipped instead of losing the file", () => {
  reset();
  const store = new ActivityStore();
  store.record(ev({ id: 1 }));
  store.record(ev({ id: 2 }));
  fs.appendFileSync(ACTIVITY_FILE, '{"id":3,"timestamp":'); // crash mid-append

  const reloaded = new ActivityStore();
  const events = reloaded.getEvents(10);
  assert.equal(events.length, 2, "both complete events survive");
  assert.deepEqual(
    events.map((e) => e.id),
    [2, 1],
    "newest first",
  );
});

test("the legacy single-array file is migrated once and renamed", () => {
  reset();
  const legacy = {
    updatedAt: Date.now(),
    events: [ev({ id: 7, model: "legacy-a" }), ev({ id: 8, model: "legacy-b" })],
  };
  fs.writeFileSync(LEGACY_ACTIVITY_FILE, JSON.stringify(legacy));

  const store = new ActivityStore();
  const events = store.getEvents(10);
  assert.equal(events.length, 2, "legacy events carried over");
  assert.deepEqual(events.map((e) => e.model), ["legacy-b", "legacy-a"]);
  assert.ok(!fs.existsSync(LEGACY_ACTIVITY_FILE), "legacy file renamed away");
  assert.ok(fs.existsSync(`${LEGACY_ACTIVITY_FILE}.migrated`), "kept as .migrated");
  assert.ok(fs.existsSync(ACTIVITY_FILE), "new JSONL file created");

  // ids continue past the migrated ones
  store.record(ev({ model: "fresh" }));
  assert.equal(store.getEvents(1)[0]!.id, 9, "nextId resumes after the max migrated id");
  assert.equal(store.getEvents(1)[0]!.model, "fresh");
});

test("retention is actually 60 days, independent of the feed cap", () => {
  reset();
  const day = 24 * 60 * 60 * 1000;
  const now = Date.now();
  // 40 days old, and more events than the cap so the feed cannot hold them all
  seed([
    { timestamp: now - 40 * day, model: "old" },
    { timestamp: now - 39 * day, model: "old" },
    { timestamp: now - 60_000, model: "recent" },
    { timestamp: now - 30_000, model: "recent" },
  ]);
  const store = new ActivityStore(2); // feed keeps only the newest two
  const usage = store.getUsage("60d");
  assert.equal(usage.totalRequests, 4, "40-day-old events still counted");
  assert.equal(usage.perModel.old?.requests, 2, "and attributed per model");

  // beyond retention: dropped on load
  seed([
    { timestamp: now - 61 * day, model: "expired" },
    { timestamp: now - 60_000, model: "fresh" },
  ]);
  const reloaded = new ActivityStore();
  assert.equal(reloaded.getUsage("60d").perModel.expired, undefined, "61-day-old event dropped");
  assert.equal(reloaded.getUsage("60d").perModel.fresh?.requests, 1, "the fresh one survives");
});

test("wide windows return data instead of collapsing onto the feed cap", () => {
  reset();
  const day = 24 * 60 * 60 * 1000;
  const now = Date.now();
  // MORE events than the feed cap, spread across the retention window. This is
  // the actual production shape: 5000 events cover ~1.4 days, so a 30d/60d
  // aggregate computed from the feed silently returned the same 5000 rows.
  seed([
    { timestamp: now - 50 * day, model: "m" },
    { timestamp: now - 40 * day, model: "m" },
    { timestamp: now - 30 * day, model: "m" },
    { timestamp: now - 20 * day, model: "m" },
    { timestamp: now - 10 * day, model: "m" },
    { timestamp: now - 2 * day, model: "m" },
  ]);
  const store = new ActivityStore(3); // feed keeps 3
  assert.equal(store.getEvents(100).length, 3, "the feed is capped");
  assert.equal(store.getUsage("60d").totalRequests, 6, "60d counts every retained event");
  assert.equal(store.getUsage("30d").totalRequests, 4, "30d counts the four inside it");
  assert.equal(store.getUsage("7d").totalRequests, 1, "7d counts only the 2-day-old one");
  assert.equal(store.getUsage("all").totalRequests, 6, "all counts every retained event");
});

test("usage aggregates match a hand sum over the events", () => {
  reset();
  const store = new ActivityStore();
  const now = Date.now();
  store.record(ev({ timestamp: now, model: "a", status: "ok", inputTokens: 5, outputTokens: 7, durationMs: 200 }));
  store.record(ev({ timestamp: now, model: "a", status: "error", inputTokens: 1, outputTokens: 0, durationMs: 400 }));
  store.record(ev({ timestamp: now, model: "b", status: "ok", inputTokens: 3, outputTokens: 9, durationMs: 300, failoverFrom: "a" }));

  const u = store.getUsage("all");
  assert.equal(u.totalRequests, 3);
  assert.equal(u.okRequests, 2);
  assert.equal(u.errorRequests, 1);
  assert.equal(u.totalInputTokens, 9);
  assert.equal(u.totalOutputTokens, 16);
  assert.equal(u.failoverCount, 1);
  assert.equal(u.avgDurationMs, 300);
  assert.equal(u.perModel.a?.requests, 2);
  assert.equal(u.perModel.a?.ok, 1);
  assert.equal(u.perModel.a?.inputTokens, 6);
  assert.equal(u.perUpstream.zen?.requests, 3);
  assert.equal(u.perUpstream.zen?.ok, 2);
});

test("'today' counts from local midnight and excludes yesterday", () => {
  reset();
  const midnight = new Date();
  midnight.setHours(0, 0, 0, 0);
  seed([
    { timestamp: midnight.getTime() - 60_000, model: "yesterday" },
    { timestamp: midnight.getTime() + 60_000, model: "today" },
  ]);
  const store = new ActivityStore();
  const u = store.getUsage("today");
  assert.equal(u.totalRequests, 1, "only today's event");
  assert.equal(u.perModel.today?.requests, 1);
  assert.equal(u.perModel.yesterday, undefined);
});

test("counters survive a restart (rebuilt from the file)", () => {
  reset();
  const store = new ActivityStore();
  store.record(ev({ model: "kept", inputTokens: 4 }));
  store.record(ev({ model: "kept", inputTokens: 6 }));

  const reloaded = new ActivityStore();
  const u = reloaded.getUsage("all");
  assert.equal(u.totalRequests, 2);
  assert.equal(u.totalInputTokens, 10);
  assert.equal(u.perModel.kept?.requests, 2);
});

test("relay attribution round-trips through the append-only file", () => {
  reset();
  const store = new ActivityStore();
  const relay = "https://bansos-relay-hnd1-rwk5.vercel.app";
  store.record(ev({ relay }));
  store.record(ev()); // direct egress: no field at all

  const reloaded = new ActivityStore();
  const [direct, attributed] = reloaded.getEvents(2);
  assert.equal(attributed!.relay, relay, "relay survives the write/read cycle");
  assert.ok(!("relay" in direct!), "no relay field when serving direct");
});
