import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import zlib from "node:zlib";
import { RELAY_STATE_FILE, readJson, writeJsonAtomic } from "../daemon/state";

// only known keyless upstream origins may be targeted via the relay; each entry must equal an upstream chatUrl origin
export const ALLOWED_TARGETS = [
  "https://opencode.ai",
  "https://api.kilo.ai",
  "https://api.llm7.io",
];

export interface KnownRelay {
  url: string;
  label?: string;
  addedAt?: string;
}

export interface RelayState {
  enabled: boolean;
  url: string;
  relays: KnownRelay[];
}

// bansos-opt3: loadRelayState runs on the chat path; cache it and stat the file
// per call, re-parsing only when mtime/size changed
let _relayCache: RelayState | null = null;
let _relayCacheKey = "";

export function loadRelayState(): RelayState {
  let st: fs.Stats | null = null;
  try {
    st = fs.statSync(RELAY_STATE_FILE);
  } catch {
    st = null;
  }
  const key = st ? `${st.mtimeMs}:${st.size}` : "missing";
  if (_relayCache !== null && _relayCacheKey === key) return _relayCache;

  const s = readJson<RelayState>(RELAY_STATE_FILE);
  const out: RelayState = {
    enabled: Boolean(s?.enabled),
    url: typeof s?.url === "string" ? s.url : "",
    relays: Array.isArray(s?.relays) ? s.relays : [],
  };
  _relayCache = out;
  _relayCacheKey = key;
  return out;
}

export function saveRelayState(state: RelayState): void {
  writeJsonAtomic(RELAY_STATE_FILE, state);
  _relayCache = null; // bansos-opt3: drop cache on every in-process write
}

function ensureRelay(state: RelayState, url: string, label?: string): void {
  const clean = url.trim();
  if (!clean || state.relays.some((r) => r.url === clean)) return;
  state.relays.push({ url: clean, label, addedAt: new Date().toISOString() });
}

// zen meters its free tier per CLIENT IP (its own egress address) and per model
// prefix, and the limit is a server-side secret that moves with capacity. Two
// consequences drive this file:
//
//   - A relay that answers 429 is NOT broken: its egress IP's daily allowance is
//     spent. The response carries Retry-After pointing at the next UTC reset,
//     so that relay is skipped until then instead of being probed again.
//   - The relay's egress IP is shared with every other customer of the same
//     platform (all Vercel functions of that account region land in one NAT
//     pool), so a fleet of relays can be exhausted together while the machine's
//     OWN address still has allowance. The direct fetch is therefore a real
//     fallback with its own bucket, not a degraded path.
//
// Two pieces of state make that cheap enough to sit on the hot path:
//   _penalty  — a relay that just answered 429/5xx is skipped for a while instead
//               of being preferred again on the next request.
//   _preferred — the relay that actually served the last retry. Without it every
//               request would pay the dead relay's round-trip first.
const RETRY_STATUS = new Set([429, 502, 503, 504]);
const PENALTY_MS = 5 * 60_000;
// A quota answer (429 + FreeUsageLimitError) means "this egress IP is spent
// until the reset", not "this relay is broken". Honour the upstream Retry-After
// so the relay is not re-probed for the rest of the window, but keep the window
// bounded: the limit is dynamic and can recover earlier.
const QUOTA_PENALTY_MIN_MS = 10 * 60_000;
const QUOTA_PENALTY_MAX_MS = 6 * 60 * 60_000;
const PREFER_MS = 2 * 60_000;
// Two alternates: enough to cross a dead region, small enough that a global 429
// (every relay spent) does not turn into a stampede of probes per request.
const MAX_ALTERNATES = 2;

// Is this a free-tier quota rejection (as opposed to a burst rate limit)?
// Both arrive as 429; only the quota one carries a long Retry-After or the
// FreeUsageLimitError body.
function quotaPenaltyMs(res: Response, bodyText: string): number {
  const ra = res.headers.get("retry-after");
  let ms = 0;
  if (ra) {
    const secs = Number.parseInt(ra.trim(), 10);
    if (Number.isFinite(secs) && secs > 0) ms = secs * 1000;
  }
  const quotaBody = /FreeUsageLimitError|Free usage limit reached/i.test(bodyText);
  if (!quotaBody && ms < QUOTA_PENALTY_MIN_MS) return PENALTY_MS;
  if (ms === 0) ms = QUOTA_PENALTY_MIN_MS;
  return Math.min(Math.max(ms, QUOTA_PENALTY_MIN_MS), QUOTA_PENALTY_MAX_MS);
}

let _preferred: { url: string; until: number } | null = null;
const _penalty = new Map<string, number>();

// The fleet scheduler (bansos_relay_rotate.sh) publishes the ranking it computed
// each tick: ready for the model in use, under the FOT cap, lowest usage first.
// Honouring it here is what makes the retry and the scheduled rotation agree —
// otherwise a retry picks whatever sits next in relay-state.json, which can be the
// account the scheduler is actively resting (main/neko were 73% and 84% of cap
// while ytk7 idled at 17%).
// Resolved per call, not at import: tests and `bansos doctor` point it elsewhere.
function rankFile(): string {
  return process.env.BANSOS_RELAY_RANK
    || path.join(os.homedir(), ".bansos", "logs", ".relay-rank.json");
}
const RANK_MAX_AGE_MS = 30 * 60_000;
let _rankCache: { key: string; urls: string[] } | null = null;

function rankedRelays(): string[] {
  try {
    const st = fs.statSync(rankFile());
    const key = `${st.mtimeMs}:${st.size}`;
    if (_rankCache && _rankCache.key === key) return _rankCache.urls;
    const data = JSON.parse(fs.readFileSync(rankFile(), "utf8")) as { at?: number; rank?: unknown };
    const urls = Array.isArray(data.rank) ? data.rank.filter((u): u is string => typeof u === "string") : [];
    const at = typeof data.at === "number" ? data.at * 1000 : 0;
    // A stale ranking is worse than none: it would pin the retry to accounts the
    // hourly FOT refresh has since seen fill up.
    if (urls.length === 0 || (at > 0 && Date.now() - at > RANK_MAX_AGE_MS)) {
      // Drop it: returning the cached copy would keep a stale ranking alive for the
      // rest of the process, which is the exact failure this check exists to avoid.
      _rankCache = null;
      return [];
    }
    _rankCache = { key, urls };
    return urls;
  } catch {
    return [];   // no ranking published yet: fleet order is the fallback
  }
}

function penalized(url: string, now: number): boolean {
  const until = _penalty.get(url);
  if (until === undefined) return false;
  if (until <= now) {
    _penalty.delete(url);
    return false;
  }
  return true;
}

// Exposed for `bansos doctor` and tests; not part of the request path.
export function relayRetryState(): { preferred: string; penalized: string[] } {
  const now = Date.now();
  return {
    preferred: _preferred && _preferred.until > now ? _preferred.url : "",
    penalized: [..._penalty.entries()].filter(([, u]) => u > now).map(([u]) => u),
  };
}

export function resetRelayRetryState(): void {
  _preferred = null;
  _penalty.clear();
}

function relayCandidates(state: RelayState, now: number): string[] {
  const out: string[] = [];
  const push = (u: string) => {
    if (!u || out.includes(u) || penalized(u, now)) return;
    out.push(u);
  };
  if (_preferred && _preferred.until > now) push(_preferred.url);
  push(state.url);
  // Scheduler ranking first (coolest healthy accounts), then the raw fleet order as
  // a fallback for relays the last tick did not rank.
  for (const u of rankedRelays()) push(u);
  for (const r of state.relays) push(r.url);
  return out;
}

// relay-aware fetch: direct when disabled, else POST via the relay, falling back
// to the next relay on a retryable status and finally to a direct fetch on a
// transport error. `init.body` must be replayable (server.ts passes a string).
export async function relayFetch(
  state: RelayState,
  targetUrl: string,
  init: RequestInit = {},
  relayPermitted = true,
): Promise<Response> {
  if (!relayPermitted || !state.enabled || !state.url) return fetch(targetUrl, init);

  const u = new URL(targetUrl);
  if (!ALLOWED_TARGETS.includes(u.origin)) return fetch(targetUrl, init);

  // A streaming/async body can only be sent once; keep the single-attempt path.
  if (init.body && typeof init.body !== "string" && !(init.body instanceof Uint8Array)) {
    return relayOnce(state.url!, u, init);
  }

  const now = Date.now();
  const candidates = relayCandidates(state, now).slice(0, 1 + MAX_ALTERNATES);
  // Every candidate is currently marked spent: its egress IP's allowance is
  // used up. Do NOT re-probe one of them — go straight to this machine's own
  // address, which is a different client identity with its own allowance.
  if (candidates.length === 0) return fetch(targetUrl, init);

  let last: Response | null = null;
  let quotaHit = false;
  for (let i = 0; i < candidates.length; i++) {
    const url = candidates[i]!;
    let res: Response;
    try {
      res = await relayOnce(url, u, init);
    } catch {
      // Transport failure (dead deployment, DNS, reset). Try the next relay; if
      // none is left, the caller's direct-fetch fallback still applies.
      _penalty.set(url, Date.now() + PENALTY_MS);
      if (i === candidates.length - 1) return fetch(targetUrl, init);
      continue;
    }
    if (!RETRY_STATUS.has(res.status)) {
      if (url !== state.url) _preferred = { url, until: Date.now() + PREFER_MS };
      else _preferred = null;
      return res;
    }
    // Read the body BEFORE cancelling it. Returning the cancelled Response as
    // `last` used to surface as "Body is unusable: Body has already been read"
    // at the caller's res.text() — every exhausted retry turned into a 502 that
    // hid the real upstream status. Re-wrap the text so `last` stays readable.
    let lastText = "";
    try {
      lastText = await res.text();
    } catch {
      /* body already gone */
    }
    // A quota 429 (free allowance spent on that egress IP) is not a relay
    // failure: park the relay until the upstream Retry-After instead of the flat
    // 5 minutes, so the next call does not pay the same dead round-trip again.
    const penalty = quotaPenaltyMs(res, lastText);
    if (res.status === 429 && penalty > PENALTY_MS) quotaHit = true;
    _penalty.set(url, Date.now() + penalty);
    last = new Response(lastText, { status: res.status, headers: res.headers });
    try {
      await res.body?.cancel();
    } catch {
      /* already closed */
    }
  }
  // The fleet could not serve it and the reason was a spent allowance. The local
  // address is a SEPARATE client identity (its own bucket), so it is worth one
  // more request before the caller sees the quota error.
  if (quotaHit) {
    try {
      const direct = await fetch(targetUrl, init);
      if (direct.status !== 429) return direct;
      await direct.body?.cancel();
    } catch {
      /* transport failure on the direct path: keep the relay's answer */
    }
  }
  return last ?? fetch(targetUrl, init);
}

// bansos-opt4: Vercel's Fast Origin Transfer meter counts bytes on the WIRE, not
// decoded bytes. Measured on a probe function with bodyParser off: the same
// 30.0 MB of JSON cost +36.5 MB of FOT as plain text and +0.1 MB gzipped, because
// the platform hands the function the still-compressed stream. Compressing the
// outbound chat body therefore cuts FOT ~10x without touching context length.
// The relay handler gunzips when it sees content-encoding: gzip.
const GZIP_MIN_BYTES = 1024;
let _gzipEnabled: boolean | null = null;

function gzipEnabled(): boolean {
  if (_gzipEnabled === null) {
    const raw = (process.env.BANSOS_RELAY_GZIP ?? "1").trim().toLowerCase();
    _gzipEnabled = !(raw === "0" || raw === "false" || raw === "off");
  }
  return _gzipEnabled;
}

export function resetGzipCache(): void {
  _gzipEnabled = null;
}

// Exported so the threshold and kill switch can be tested without standing up a
// daemon: a real chat body is always >1 KB after transformation, so the small-body
// path is unreachable through the HTTP surface.
export function gzipBodyForTest(body: BodyInit | null | undefined): { body: BodyInit | null | undefined; gzipped: boolean } {
  return maybeGzip(body);
}

function maybeGzip(body: BodyInit | null | undefined): { body: BodyInit | null | undefined; gzipped: boolean } {
  if (!gzipEnabled() || typeof body !== "string" || body.length < GZIP_MIN_BYTES) {
    return { body, gzipped: false };
  }
  return { body: new Uint8Array(zlib.gzipSync(Buffer.from(body, "utf8"))), gzipped: true };
}

async function relayOnce(relayUrl: string, u: URL, init: RequestInit): Promise<Response> {
  const headers = new Headers(init.headers);
  headers.set("x-relay-target", u.origin);
  headers.set("x-relay-path", `${u.pathname}${u.search}`);
  const packed = maybeGzip(init.body);
  if (packed.gzipped) {
    // Drop any inbound length/encoding the caller set: the wire body is now gzip.
    headers.set("content-encoding", "gzip");
    // Set the compressed length EXPLICITLY. Without it the request goes out
    // chunked, the Vercel edge buffers the stream and answers 400 with an empty
    // body before the function runs (measured: identical gzip payload is 200
    // when curl sends content-length, 400 when node sends it chunked).
    headers.set("content-length", String((packed.body as Uint8Array).byteLength));
  }
  return fetch(relayUrl, {
    ...init,
    headers,
    body: packed.body,
    duplex: "half",
  });
}

export function addRelay(state: RelayState, url: string, label?: string): RelayState {
  ensureRelay(state, url, label);
  return state;
}

export function removeRelay(state: RelayState, url: string): RelayState {
  state.relays = state.relays.filter((r) => r.url !== url);
  if (state.url === url) state.url = "";
  return state;
}
