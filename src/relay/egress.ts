import fs from "node:fs";
import { RELAY_STATE_FILE, readJson, writeJsonAtomic } from "../daemon/state";

// only known keyless upstream origins may be targeted via the relay
// must match each upstream's base-URL origin: LLM7_BASE_URL is
// https://api.llm7.io/v1, so the allowlist entry has to be the api host —
// a bare "https://llm7.io" silently routes llm7 direct (relayFetch's
// origin check misses and falls through to fetch). test/relay-targets.test.ts
// asserts this invariant against the real base URLs.
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

// relay-aware fetch: direct when disabled, else POST via the relay, falling back to direct
export async function relayFetch(
  state: RelayState,
  targetUrl: string,
  init: RequestInit = {},
  relayPermitted = true,
): Promise<Response> {
  if (!relayPermitted || !state.enabled || !state.url) return fetch(targetUrl, init);

  const u = new URL(targetUrl);
  if (!ALLOWED_TARGETS.includes(u.origin)) return fetch(targetUrl, init);

  try {
    const headers = new Headers(init.headers);
    headers.set("x-relay-target", u.origin);
    headers.set("x-relay-path", `${u.pathname}${u.search}`);
    return await fetch(state.url, {
      ...init,
      headers,
      body: init.body,
      duplex: "half",
    });
  } catch {
    return fetch(targetUrl, init); // non-strict fallback
  }
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
