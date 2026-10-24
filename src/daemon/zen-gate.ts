import fs from "node:fs";
import path from "node:path";
import { BANSOS_DIR, LOG_DIR } from "./state";

// The zen free-tier quota gate.
//
// OpenCode Zen meters its anonymous free models as a REQUEST-COUNT budget per
// real client IP, refilling at 00:00 UTC (the upstream's Retry-After points
// there). When that bucket is empty every relay answers 429
// FreeUsageLimitError — so the watchers (relay heal/rotate/audit/watch,
// zen-relay-rotate, the 9Router free-tier probe) must stop probing: each of
// their probes that SUCCEEDS is one more request taken from a bucket that is
// already close to empty, and their fan-out turns a 200-request daily budget
// into scrap within hours.
//
// Until this module existed the gate marker was written by hand, which meant
// the lane stayed dead long after the bucket refilled. The daemon sees the
// upstream's Retry-After on every quota rejection, so the daemon owns the
// marker: it closes the gate for exactly the upstream's window and reopens it
// when a request finally succeeds.
//
// The marker file is shared with ~/.hermes/scripts/zen_quota_gate.py, which
// every probing job calls before it spends a request. Keep the shape
// compatible: {"until": epoch, "reason": str, "freeze": bool, ...}.
//
// A FROZEN marker is a human decision ("no zen probes until I say so") and is
// never touched here: the daemon neither closes nor reopens it.

const MARKER = path.join(LOG_DIR, ".zen-quota.json");
const FREEZE_UNTIL = 4102444800;

interface GateMarker {
  // Epoch SECONDS — the same unit ~/.hermes/scripts/zen_quota_gate.py uses.
  until?: number;
  reason?: string;
  freeze?: boolean;
  set_at?: number;
  set_at_h?: string;
  source?: string;
}

function readMarker(): GateMarker {
  try {
    return JSON.parse(fs.readFileSync(MARKER, "utf8")) as GateMarker;
  } catch {
    return {};
  }
}

function writeMarker(marker: GateMarker): void {
  try {
    fs.mkdirSync(LOG_DIR, { recursive: true });
    const tmp = `${MARKER}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(marker, null, 2));
    fs.renameSync(tmp, MARKER);
  } catch {
    // a missing marker only means "the lane is open"; never fail a request
  }
}

function stamp(until: number, reason: string): void {
  const now = Date.now();
  writeMarker({
    until,
    reason,
    freeze: false,
    set_at: Math.floor(now / 1000),
    set_at_h: new Date(now).toISOString().replace("T", " ").slice(0, 19),
    source: "bansosd",
  });
}

/**
 * Close the zen probe lane for exactly the upstream's refill window.
 *
 * Called when the daemon sees a quota-exhaustion 429 (FreeUsageLimitError or a
 * Retry-After of a day or more). `retryAfterMs` is the upstream's own answer;
 * when it is missing fall back to the next UTC midnight, which is where the
 * upstream's counter resets.
 */
export function noteZenQuotaExhausted(retryAfterMs?: number, reason?: string): void {
  const marker = readMarker();
  if (marker.freeze) return; // a deliberate freeze wins over automation
  const now = Date.now();
  const fallback = nextUtcMidnight(now);
  const untilMs = Math.max(now + (retryAfterMs ?? fallback), now + 60_000);
  const until = Math.ceil(untilMs / 1000);
  // Never shorten an existing window: the upstream window is the truth, and a
  // later retry may carry a slightly different rounding.
  if (typeof marker.until === "number" && marker.until > until && marker.until * 1000 > now) {
    return;
  }
  stamp(
    until,
    reason ??
      `zen free-tier bucket exhausted (upstream Retry-After); probes resume at ${new Date(untilMs).toISOString()}`,
  );
}

/**
 * Reopen the probe lane once a zen request finally succeeds.
 *
 * A deliberate freeze is left alone.
 */
export function noteZenQuotaRecovered(): void {
  const marker = readMarker();
  if (marker.freeze) return;
  if (!marker.until && !marker.reason) return;
  try {
    fs.unlinkSync(MARKER);
  } catch {
    // already gone
  }
}

/** Milliseconds until the next 00:00 UTC — where the upstream's daily bucket refills. */
export function nextUtcMidnight(nowMs = Date.now()): number {
  const d = new Date(nowMs);
  const midnight = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() + 1, 0, 0, 0, 0);
  return midnight - nowMs;
}

/** Test seam: the marker path the daemon and the gate script must agree on. */
export function zenGateMarkerPath(): string {
  return MARKER;
}

/** Test seam: BANSOS_DIR is read at import time, so expose the base for assertions. */
export function zenGateBaseDir(): string {
  return BANSOS_DIR;
}
