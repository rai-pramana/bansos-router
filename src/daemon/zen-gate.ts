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
  // Per-prefix windows. The upstream's budget is keyed by (egress IP, the
  // model's FIRST TWO CHARACTERS), so "bi" and "ne" are different rows with
  // different refill states. A single global window therefore closes rows
  // that still have room - and that is not a conservative mistake, it is a
  // functional one: the closed row's 429s can never be re-probed, so the
  // per-prefix spent evidence for it is never collected, so the daemon walks
  // the dead addresses again on every request.
  //
  // The legacy top-level `until`/`reason` stay as the DEFAULT window (what a
  // human or the gate script sets, and what applies to a prefix with no entry
  // of its own). A prefix listed here overrides it.
  prefixes?: Record<string, { until?: number; reason?: string }>;
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
 *
 * When the marker carries PER-PREFIX windows, only the recovered row's window
 * is removed: a successful "bi" says the "bi" row has room, not that the
 * "ne" row does. Clearing the whole marker on any success would reopen rows
 * the upstream has already reported as spent - and each of those rows then
 * pays a full dead-address walk before the daemon learns it is still closed.
 *
 * The legacy global-only marker is removed entirely, as before.
 */
export function noteZenQuotaRecovered(modelId?: string): void {
  const marker = readMarker();
  if (marker.freeze) return;
  const prefix = zenRowPrefix(modelId);
  if (prefix && marker.prefixes && Object.keys(marker.prefixes).length > 0) {
    if (!marker.prefixes[prefix]) return; // that row was never closed
    const prefixes = { ...marker.prefixes };
    delete prefixes[prefix];
    writeMarker({ ...marker, prefixes });
    return;
  }
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

// The upstream's budget row for a model id: the FIRST TWO CHARACTERS of the id.
// "big-pickle" and "beam-free" are different rows; "big-pickle" and
// "big-pickle-2" are the SAME row.
export function zenRowPrefix(modelId: string | undefined): string {
  if (!modelId) return "";
  return modelId.slice(0, 2);
}

// The window that applies to ONE model row.
//
// A prefix-specific window wins over the global one: the global window is
// what a human or the gate script set for "the whole lane", and the
// per-prefix entries are what the daemon learned from the upstream's own
// per-row answers. A row that the upstream says still has room must not be
// closed by another row's exhaustion.
export function zenQuotaClosedForModel(modelId: string | undefined, nowMs = Date.now()): number {
  const marker = readMarker();
  if (marker.freeze) return 0;
  const prefix = zenRowPrefix(modelId);
  const scoped = prefix ? marker.prefixes?.[prefix] : undefined;
  const until = typeof scoped?.until === "number" ? scoped.until : marker.until;
  if (typeof until !== "number") return 0;
  const remaining = until * 1000 - nowMs;
  return remaining > 0 ? remaining : 0;
}

// The reason that applies to ONE model row, for the same reason.
export function zenQuotaReasonForModel(modelId: string | undefined): string | undefined {
  const marker = readMarker();
  if (marker.freeze) return undefined;
  const prefix = zenRowPrefix(modelId);
  const scoped = prefix ? marker.prefixes?.[prefix] : undefined;
  const reason = scoped?.reason ?? marker.reason;
  return typeof reason === "string" && reason ? reason : undefined;
}

/**
 * Close ONE model row of the zen probe lane for exactly the upstream's refill
 * window.
 *
 * The window is recorded per PREFIX, not globally. The upstream meters each
 * row separately, so closing "bi" must not close "mu" - and a global close
 * has a functional cost, not just a conservative one: while the lane is
 * closed the daemon never re-probes production, so the per-prefix spent
 * evidence for that row is never collected, so the daemon cannot learn that
 * the row is closed and keeps walking the dead addresses on every request
 * (measured 2026-10-31: a 24s production walk for a row the upstream had
 * already reported as spent).
 */
export function noteZenQuotaExhaustedForModel(
  modelId: string | undefined,
  retryAfterMs?: number,
  reason?: string,
): void {
  const marker = readMarker();
  if (marker.freeze) return; // a deliberate freeze wins over automation
  const prefix = zenRowPrefix(modelId);
  if (!prefix) {
    noteZenQuotaExhausted(retryAfterMs, reason);
    return;
  }
  const now = Date.now();
  const fallback = nextUtcMidnight(now);
  const untilMs = Math.max(now + (retryAfterMs ?? fallback), now + 60_000);
  const until = Math.ceil(untilMs / 1000);
  const existing = marker.prefixes?.[prefix];
  // Never shorten an existing window: the upstream window is the truth, and a
  // later retry may carry a slightly different rounding.
  if (typeof existing?.until === "number" && existing.until > until && existing.until * 1000 > now) {
    return;
  }
  const text =
    reason ??
    `zen free-tier bucket exhausted (upstream Retry-After); probes resume at ${new Date(untilMs).toISOString()}`;
  const prefixes = { ...(marker.prefixes ?? {}) };
  prefixes[prefix] = { until, reason: text };
  // The daemon's OWN legacy global window is superseded by this per-prefix
  // one. It was written by the same automation (source "bansosd") before the
  // windows became per-row, and leaving it in place would keep closing every
  // zen model - the exact bug this change fixes.
  //
  // A HUMAN's whole-lane close (zen_quota_gate.py set) has no `source`, so it
  // survives: the per-prefix entries are an addition, not a replacement.
  const supersededDaemonGlobal = marker.source === "bansosd";
  writeMarker({
    ...marker,
    ...(supersededDaemonGlobal ? { until: undefined, reason: undefined } : {}),
    prefixes,
    set_at: Math.floor(now / 1000),
    set_at_h: new Date(now).toISOString().replace("T", " ").slice(0, 19),
    source: "bansosd",
  });
}

/**
 * Read the zen probe lane's remaining closed window, in milliseconds.
 *
 * The marker is the only SURVIVOR of a daemon restart, so it is what keeps a
 * daily free-tier park from being forgotten: the model's cooldown lives in
 * memory, and without this the first request after every restart pays for the
 * same already-known 429. Returns 0 when the lane is open (or on a frozen
 * marker, whose window is not ours to interpret).
 */
export function zenQuotaClosedForMs(nowMs = Date.now()): number {
  const marker = readMarker();
  if (marker.freeze) return 0;
  if (typeof marker.until !== "number") return 0;
  const remaining = marker.until * 1000 - nowMs;
  return remaining > 0 ? remaining : 0;
}

/** The refill reason recorded on disk, for a repeat request to quote verbatim. */
export function zenQuotaReason(): string | undefined {
  const marker = readMarker();
  if (marker.freeze) return undefined;
  return typeof marker.reason === "string" && marker.reason ? marker.reason : undefined;
}

/** Test seam: the marker path the daemon and the gate script must agree on. */
export function zenGateMarkerPath(): string {
  return MARKER;
}

/** Test seam: BANSOS_DIR is read at import time, so expose the base for assertions. */
export function zenGateBaseDir(): string {
  return BANSOS_DIR;
}
