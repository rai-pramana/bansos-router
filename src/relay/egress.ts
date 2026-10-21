import fs from "node:fs";
import os from "node:os";
import path from "node:path";
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

// zen meters its free tier per RELAY (per egress pool) and per model, so a 429
// from one relay says nothing about the next one: 148 of 159 relays served
// deepseek while 11 did not, and the failing 11 clustered in one region. Retrying
// a second relay therefore turns a user-visible 429 into a served request.
//
// Two pieces of state make that cheap enough to sit on the hot path:
//   _penalty  — a relay that just answered 429/5xx is skipped for a while instead
//               of being preferred again on the next request.
//   _preferred — the relay that actually served the last retry. Without it every
//               request would pay the dead relay's round-trip first.
const RETRY_STATUS = new Set([429, 502, 503, 504]);
const PENALTY_MS = 5 * 60_000;
const PREFER_MS = 2 * 60_000;
// Two alternates: enough to cross a dead region, small enough that a global 429
// (every relay spent) does not turn into a stampede of probes per request.
const MAX_ALTERNATES = 2;

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
  if (candidates.length === 0) return relayOnce(state.url!, u, init);

  let last: Response | null = null;
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
    _penalty.set(url, Date.now() + PENALTY_MS);
    last = res;
    // Nothing was generated on a retryable status, but a fully consumed body must
    // not leak the socket; discard before retrying.
    try {
      await res.body?.cancel();
    } catch {
      /* already closed */
    }
  }
  return last ?? relayOnce(state.url!, u, init);
}

async function relayOnce(relayUrl: string, u: URL, init: RequestInit): Promise<Response> {
  const headers = new Headers(init.headers);
  headers.set("x-relay-target", u.origin);
  headers.set("x-relay-path", `${u.pathname}${u.search}`);
  return fetch(relayUrl, {
    ...init,
    headers,
    body: init.body,
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
