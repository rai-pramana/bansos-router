import fs from "node:fs";
import path from "node:path";
import { BANSOS_DIR, ensureBansosDir } from "./state";

export type ActivityKind = "chat" | "responses" | "anthropic";
export type ActivityStatus = "ok" | "error";

export interface ActivityEvent {
  id: number;
  timestamp: number;
  kind: ActivityKind;
  model: string;
  requestedModel: string;
  upstream: string;
  // Egress relay that served the request ("" when direct). Without it the
  // watchdog sees "upstream=zen failed" and cannot tell a gated relay from a
  // gated upstream, so it cannot decide whether switching egress would help.
  relay?: string;
  inputTokens: number;
  outputTokens: number;
  durationMs: number;
  status: ActivityStatus;
  statusCode?: number;
  failoverFrom?: string;
}

export interface ModelActivity {
  requests: number;
  ok: number;
  inputTokens: number;
  outputTokens: number;
}

export interface UsageStats {
  totalRequests: number;
  okRequests: number;
  errorRequests: number;
  totalInputTokens: number;
  totalOutputTokens: number;
  failoverCount: number;
  avgDurationMs: number;
  perModel: Record<string, ModelActivity>;
  perUpstream: Record<string, { requests: number; ok: number }>;
}

export type TimeWindow = "today" | "7d" | "30d" | "60d" | "all";

export interface ActivityInput {
  kind: ActivityKind;
  model: string;
  requestedModel: string;
  upstream: string;
  relay?: string;
  inputTokens: number;
  outputTokens: number;
  durationMs: number;
  status: ActivityStatus;
  statusCode?: number;
  failoverFrom?: string;
}

// Append-only JSONL, one event per line. The old single-JSON-array file forced a
// full rewrite of every retained event on each chat: measured 1071 KiB written
// per chat against a 1067 KiB file (~3.7 GiB/day at 3618 events/day). One line
// is ~200 bytes, so the same traffic writes ~0.7 MiB/day instead.
export const ACTIVITY_FILE = path.join(BANSOS_DIR, "activity.jsonl");
// Pre-JSONL layout, converted on first boot and then left as *.migrated.
export const LEGACY_ACTIVITY_FILE = path.join(BANSOS_DIR, "activity.json");

const MAX_RETENTION_MS = 60 * 24 * 60 * 60 * 1000; // 60 days
// Compaction trigger. At ~200 B/event this file reaches 64 MiB after ~330k
// events (~90 days at this machine's rate), so compaction is a rare
// boot/outlier cost rather than a per-request one.
const MAX_FILE_BYTES = 64 * 1024 * 1024;
// Detail retained for the activity feed. Aggregates do NOT depend on this: they
// come from hourly buckets covering the full retention. Only the feed is capped,
// and getEvents always returns the newest `limit` rows, which are by definition
// inside the newest `cap`.
const DEFAULT_EVENT_CAP = 5000;

// Per-hour aggregate. Wide windows (7d/30d/60d/all) are summed from these rather
// than scanning events: measured 45.9 ms for a 217k-event scan at full 60-day
// retention (and 618 ms to parse 50 MiB of JSONL for it), versus O(1) per write.
interface Bucket {
  requests: number;
  ok: number;
  errors: number;
  inputTokens: number;
  outputTokens: number;
  durationMs: number;
  failovers: number;
  perModel: Record<string, ModelActivity>;
  perUpstream: Record<string, { requests: number; ok: number }>;
}

function emptyBucket(): Bucket {
  return {
    requests: 0,
    ok: 0,
    errors: 0,
    inputTokens: 0,
    outputTokens: 0,
    durationMs: 0,
    failovers: 0,
    perModel: {},
    perUpstream: {},
  };
}

// Local-hour key, so "today" (which starts at local midnight) lands exactly on a
// bucket boundary. Rolling windows are hour-granular at their trailing edge —
// they can include up to one hour of extra events, which is immaterial for
// totals and is the price of not rescanning events.
function hourKey(ts: number): number {
  const d = new Date(ts);
  d.setMinutes(0, 0, 0);
  return d.getTime();
}

export class ActivityStore {
  private events: ActivityEvent[] = [];
  private nextId = 1;
  private readonly cap: number;
  private buckets = new Map<number, Bucket>();
  private fd: number | null = null;
  private bytes = 0;
  private _reloadTimer: NodeJS.Timeout | null = null;
  private _freshAt = 0;

  constructor(cap = DEFAULT_EVENT_CAP) {
    this.cap = cap;
    this.loadFromDisk();
  }

  // ---------- boot ----------

  /** Convert the legacy single-object file to JSONL, once. */
  private migrateLegacy(): void {
    if (!fs.existsSync(LEGACY_ACTIVITY_FILE)) return;
    try {
      const data = JSON.parse(fs.readFileSync(LEGACY_ACTIVITY_FILE, "utf8")) as {
        events?: ActivityEvent[];
      };
      const list = Array.isArray(data?.events) ? data.events : [];
      const now = Date.now();
      const lines = list
        .filter((e) => typeof e?.timestamp === "number" && now - e.timestamp <= MAX_RETENTION_MS)
        .map((e) => `${JSON.stringify(e)}\n`)
        .join("");
      if (lines) fs.appendFileSync(ACTIVITY_FILE, lines);
      fs.renameSync(LEGACY_ACTIVITY_FILE, `${LEGACY_ACTIVITY_FILE}.migrated`);
    } catch {
      // a corrupt legacy file must not block the daemon; leave it untouched
    }
  }

  private loadFromDisk(): void {
    this.migrateLegacy();
    this.events = [];
    this.buckets.clear();
    this.nextId = 1;
    this.bytes = 0;
    let raw: string;
    try {
      raw = fs.readFileSync(ACTIVITY_FILE, "utf8");
    } catch {
      return;
    }
    this.bytes = Buffer.byteLength(raw);
    const now = Date.now();
    const kept: ActivityEvent[] = [];
    for (const line of raw.split("\n")) {
      if (!line) continue;
      let e: ActivityEvent;
      try {
        e = JSON.parse(line) as ActivityEvent;
      } catch {
        continue; // torn trailing line from a crash mid-append
      }
      if (typeof e?.timestamp !== "number") continue;
      if (now - e.timestamp > MAX_RETENTION_MS) continue;
      kept.push(e);
      if (e.id >= this.nextId) this.nextId = e.id + 1;
      this.addToBucket(e);
    }
    // Buckets cover the whole retention; the event array only feeds the feed.
    this.events = kept.length > this.cap ? kept.slice(-this.cap) : kept;
  }

  // ---------- write ----------

  private addToBucket(e: ActivityEvent): void {
    const key = hourKey(e.timestamp);
    let b = this.buckets.get(key);
    if (!b) {
      b = emptyBucket();
      this.buckets.set(key, b);
    }
    b.requests++;
    if (e.status === "ok") b.ok++;
    else b.errors++;
    b.inputTokens += e.inputTokens ?? 0;
    b.outputTokens += e.outputTokens ?? 0;
    b.durationMs += e.durationMs ?? 0;
    if (e.failoverFrom) b.failovers++;

    const m =
      b.perModel[e.model] ??
      (b.perModel[e.model] = { requests: 0, ok: 0, inputTokens: 0, outputTokens: 0 });
    m.requests++;
    if (e.status === "ok") m.ok++;
    m.inputTokens += e.inputTokens ?? 0;
    m.outputTokens += e.outputTokens ?? 0;

    const u = b.perUpstream[e.upstream] ?? (b.perUpstream[e.upstream] = { requests: 0, ok: 0 });
    u.requests++;
    if (e.status === "ok") u.ok++;
  }

  private appendLine(e: ActivityEvent): void {
    const line = `${JSON.stringify(e)}\n`;
    try {
      ensureBansosDir();
      if (this.fd === null) this.fd = fs.openSync(ACTIVITY_FILE, "a");
      fs.writeSync(this.fd, line);
      this.bytes += Buffer.byteLength(line);
    } catch {
      // a failed append must never break a request; drop the fd so the next
      // event retries the open
      this.closeFd();
    }
  }

  private closeFd(): void {
    if (this.fd === null) return;
    try {
      fs.closeSync(this.fd);
    } catch {
      // already closed
    }
    this.fd = null;
  }

  /** Rewrite the file with only retained events. Rare: boot, or file oversized. */
  private compact(): void {
    const now = Date.now();
    const kept: ActivityEvent[] = [];
    try {
      const raw = fs.readFileSync(ACTIVITY_FILE, "utf8");
      for (const line of raw.split("\n")) {
        if (!line) continue;
        try {
          const e = JSON.parse(line) as ActivityEvent;
          if (typeof e?.timestamp === "number" && now - e.timestamp <= MAX_RETENTION_MS) kept.push(e);
        } catch {
          // torn line: dropping it is the point of compaction
        }
      }
      const body = kept.map((e) => `${JSON.stringify(e)}\n`).join("");
      this.closeFd();
      const tmp = `${ACTIVITY_FILE}.${process.pid}.tmp`;
      fs.writeFileSync(tmp, body, { mode: 0o600 });
      fs.renameSync(tmp, ACTIVITY_FILE);
      this.bytes = Buffer.byteLength(body);
    } catch {
      return;
    }
    this.buckets.clear();
    for (const e of kept) this.addToBucket(e);
    this.events = kept.length > this.cap ? kept.slice(-this.cap) : kept;
  }

  /**
   * Historical name kept for callers. Appends are already durable, so this only
   * forces them to disk and drops expired hourly buckets.
   */
  public flushToDisk(): void {
    if (this.fd !== null) {
      try {
        fs.fsyncSync(this.fd);
      } catch {
        // fsync is best-effort
      }
    }
    this.pruneBuckets();
  }

  private pruneBuckets(): void {
    const cutoff = hourKey(Date.now() - MAX_RETENTION_MS);
    for (const key of this.buckets.keys()) {
      if (key < cutoff) this.buckets.delete(key);
    }
  }

  record(input: ActivityInput): void {
    const now = Date.now();
    // id/timestamp after the spread: ActivityInput has no id, but a caller (or a
    // test fixture) passing one must never rewrite the identity of an event.
    const event: ActivityEvent = { ...input, id: this.nextId++, timestamp: now };
    this.events.push(event);
    if (this.events.length > this.cap) this.events = this.events.slice(-this.cap);
    this.addToBucket(event);
    this.appendLine(event);
    if (this.bytes > MAX_FILE_BYTES) this.compact();
  }

  // ---------- read ----------

  /**
   * Only the daemon appends now, so this has to notice just a shrink or a
   * replacement (manual edit, compaction by another process). Our own appends
   * are tracked in `bytes`, so they never trigger a reload.
   */
  public checkDiskFreshness(): void {
    const now = Date.now();
    if (now - this._freshAt < 1000) return; // stat throttle
    this._freshAt = now;
    let size: number;
    try {
      size = fs.statSync(ACTIVITY_FILE).size;
    } catch {
      if (this.bytes > 0) {
        this.bytes = 0;
        this.events = [];
        this.buckets.clear();
      }
      return;
    }
    if (size === this.bytes) return;
    if (this._reloadTimer) return; // one pending reload is enough
    this._reloadTimer = setTimeout(() => {
      this._reloadTimer = null;
      this.closeFd();
      this.loadFromDisk();
    }, 0);
  }

  getEvents(limit = 100, window: TimeWindow = "all"): ActivityEvent[] {
    const threshold = this.thresholdFor(window);
    const filtered =
      threshold === null ? this.events : this.events.filter((e) => e.timestamp >= threshold);
    return filtered.slice(-limit).reverse();
  }

  private thresholdFor(window: TimeWindow): number | null {
    if (window === "all") return null;
    const now = Date.now();
    if (window === "today") {
      const startOfDay = new Date();
      startOfDay.setHours(0, 0, 0, 0);
      return startOfDay.getTime();
    }
    if (window === "7d") return now - 7 * 24 * 60 * 60 * 1000;
    if (window === "30d") return now - 30 * 24 * 60 * 60 * 1000;
    return now - 60 * 24 * 60 * 60 * 1000;
  }

  /**
   * Summed from hourly buckets, so this is O(hours) instead of O(events): a full
   * 60-day window is ~1440 map lookups instead of a 217k-event scan.
   */
  getUsage(window: TimeWindow = "all"): UsageStats {
    const threshold = this.thresholdFor(window);
    const from = threshold === null ? -Infinity : hourKey(threshold);
    let requests = 0;
    let ok = 0;
    let errors = 0;
    let inputTokens = 0;
    let outputTokens = 0;
    let durationMs = 0;
    let failovers = 0;
    const perModel: Record<string, ModelActivity> = {};
    const perUpstream: Record<string, { requests: number; ok: number }> = {};

    for (const [key, b] of this.buckets) {
      if (key < from) continue;
      requests += b.requests;
      ok += b.ok;
      errors += b.errors;
      inputTokens += b.inputTokens;
      outputTokens += b.outputTokens;
      durationMs += b.durationMs;
      failovers += b.failovers;
      for (const [id, m] of Object.entries(b.perModel)) {
        const acc =
          perModel[id] ?? (perModel[id] = { requests: 0, ok: 0, inputTokens: 0, outputTokens: 0 });
        acc.requests += m.requests;
        acc.ok += m.ok;
        acc.inputTokens += m.inputTokens;
        acc.outputTokens += m.outputTokens;
      }
      for (const [id, u] of Object.entries(b.perUpstream)) {
        const acc = perUpstream[id] ?? (perUpstream[id] = { requests: 0, ok: 0 });
        acc.requests += u.requests;
        acc.ok += u.ok;
      }
    }

    return {
      totalRequests: requests,
      okRequests: ok,
      errorRequests: errors,
      totalInputTokens: inputTokens,
      totalOutputTokens: outputTokens,
      failoverCount: failovers,
      avgDurationMs: requests ? Math.round(durationMs / requests) : 0,
      perModel,
      perUpstream,
    };
  }
}
