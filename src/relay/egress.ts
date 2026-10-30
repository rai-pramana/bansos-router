import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import zlib from "node:zlib";
import { promisify } from "node:util";
import { BANSOS_DIR, RELAY_STATE_FILE, readJson, writeJsonAtomic } from "../daemon/state";

// only known keyless upstream origins may be targeted via the relay; each entry must equal an upstream chatUrl origin
// must match each upstream's base-URL origin: LLM7_BASE_URL is
// https://api.llm7.io/v1, so the allowlist entry has to be the api host —
// a bare "https://llm7.io" silently routes llm7 direct (relayFetch's
// origin check misses and falls through to fetch). test/relay-targets.test.ts
// asserts this invariant against the real base URLs.
export const ALLOWED_TARGETS = [
  "https://opencode.ai",
  // The zen STAGE deployment. It is a SEPARATE opencode app with its own
  // database, so the per-IP daily bucket is a different row set than
  // production's. The fleet's egress addresses have never been counted
  // there, so a relay that is spent on production still has room on the
  // stage.
  "https://dev.opencode.ai",
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
// A spread target (the zen STAGE) has one bucket PER RELAY, so a 429 there
// means "this relay's bucket is spent", not "every relay is spent". Trying
// more alternates finds a fresh bucket instead of giving up.
//
// The count is generous because two different things skip a relay: a spent
// bucket (429) and a deployment that does not know the target yet (403). A
// fleet mid-deploy can have several of the latter, so the alternates have to
// be enough to get past them.
const MAX_SPREAD_ALTERNATES = 12;

// How many candidates may be in flight at once once the first one failed.
//
// The first candidate is always tried alone: on a healthy fleet it answers,
// and racing would spend a request from every other address's budget for
// nothing (a 200 consumes one request of that address's row).
//
// After a failure the fleet may be spent, and a SPENT fleet is the expensive
// case: measured 2026-10-30, a relay answers 429 in ~2.1s, so walking 13
// candidates one by one cost ~27s per request (the daemon log showed 26.5s
// and 29.0s for two ids). The same candidates raced answered in ~1.5s.
//
// The width is bounded so a big fleet cannot stampede the upstream: 6
// concurrent requests is below the point where the upstream starts refusing
// for concurrency rather than for quota (the audit measured 6 workers turning
// healthy relays into 503s, and 2 workers keeping all but one real 404).
const RACE_WIDTH = 6;

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

let _preferred: { url: string; until: number; origin?: string } | null = null;
const _penalty = new Map<string, number>();

// The penalty map is keyed by relay URL only, but the quota bucket is NOT:
// the upstream limiter keys it on the request's x-real-ip, and the relay's
// egress address is what the edge stamps there. Production
// (opencode.ai) and the STAGE (dev.opencode.ai) are separate deployments
// with separate databases, so the SAME relay has a separate bucket on each.
//
// A relay spent on production is therefore NOT spent on the stage. Keying
// the penalty by target as well keeps a production outage from hiding the
// stage's fresh buckets - which is the exact situation the stage exists for.
//
// The bucket is ALSO split per model, but not by the whole id: the upstream
// row for a model that carries its own rateLimit is
// YYYYMMDD + modelId.slice(0,2) (util_ipRateLimiter.ts:25), so the row is
// keyed by the FIRST TWO CHARACTERS of the id. Ids sharing a prefix share a
// row: "deepseek-v4.1-flash-free" and "deepseek-v4-flash" both land on "de".
//
// Keying the penalty by the same prefix is what makes the penalty mean
// something upstream agrees with. Keying by the whole id would let the
// daemon keep using an address whose "de" row is already spent, and would
// miss the fact that two different ids are one budget.
//
// Measured 2026-10-29: a 429 on one id must not hide the other ids on the
// same address, but it MUST hide the ids that share its two-character
// prefix.
function penaltyKey(url: string, target: URL, model?: string): string {
  // The prefix is what the upstream keys on. Two characters is what
  // util_ipRateLimiter.ts uses, so "de" is the row for every deepseek-*.
  const prefix = model ? model.slice(0, 2) : "";
  return prefix ? `${target.origin}|${url}|${prefix}` : `${target.origin}|${url}`;
}

// The fleet scheduler (bansos_relay_rotate.sh) publishes the ranking it computed
// each tick: ready for the model in use, under the FOT cap, lowest usage first.
// Honouring it here is what makes the retry and the scheduled rotation agree —
// otherwise a retry picks whatever sits next in relay-state.json, which can be the
// account the scheduler is actively resting (main/neko were 73% and 84% of cap
// while ytk7 idled at 17%).
// Resolved per call, not at import: tests and `bansos doctor` point it elsewhere.
//
// BANSOS_DIR must win over the home directory: a test run with an isolated
// state dir would otherwise read the PRODUCTION ranking and splice live
// relays into the candidate list, so a test that expects "stub A fails,
// stub B answers" would send the second attempt to a real deployment.
function rankFile(): string {
  return process.env.BANSOS_RELAY_RANK
    || (process.env.BANSOS_DIR
      ? path.join(process.env.BANSOS_DIR, "logs", ".relay-rank.json")
      : path.join(os.homedir(), ".bansos", "logs", ".relay-rank.json"));
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

function penalized(url: string, now: number, target?: URL, model?: string): boolean {
  // A model-scoped penalty must not hide the address for the other models
  // (the bucket is per address AND per model), and an address-scoped penalty
  // must not hide it for another deployment. So a candidate is skipped when
  // ANY of its keys is live: the unscoped key (deployment-wide), the
  // model-scoped key, and the bare URL (the legacy shape).
  const keys = target
    ? [penaltyKey(url, target), ...(model ? [penaltyKey(url, target, model)] : []), url]
    : [url];
  for (const key of keys) {
    const until = _penalty.get(key);
    if (until === undefined) continue;
    if (until <= now) {
      _penalty.delete(key);
      continue;
    }
    return true;
  }
  return false;
}
// Exposed for `bansos doctor` and tests; not part of the request path.
export function relayRetryState(): { preferred: string; penalized: string[] } {
  const now = Date.now();
  return {
    preferred: _preferred && _preferred.until > now ? _preferred.url : "",
    // The penalty keys are "<target origin>|<relay url>" or
    // "<target origin>|<relay url>|<model id>" so a relay spent on production
    // is not hidden from the stage, and a relay spent for one model is not
    // hidden for the others. Report the relay URL alone: that is what a
    // caller wants to know.
    penalized: [..._penalty.entries()]
      .filter(([, u]) => u > now)
      .map(([key]) => key.slice(key.indexOf("|") + 1).split("|")[0] ?? key),
  };
}

// Round-robin cursors, one per (target origin, model).
//
// A relay's upstream bucket is per egress address, so a target that is
// reached through many relays (both opencode deployments: the fleet's
// addresses each land in their own daily row) can serve far more traffic
// when consecutive requests land on DIFFERENT relays. The cursor is what
// makes that happen: without it the first candidate keeps winning and a
// single relay's bucket is the only one ever used.
//
// Per model as well: each model has its own bucket per address, so two
// models must not share one cursor position - they would hit the same
// address together.
const _spreadCursor = new Map<string, number>();

function nextSpreadOffset(origin: string, model: string | undefined, count: number): number {
  if (count <= 0) return 0;
  const key = model ? `${origin}|${model}` : origin;
  const cur = _spreadCursor.get(key) ?? 0;
  _spreadCursor.set(key, (cur + 1) % count);
  return cur % count;
}

// Targets whose upstream quota bucket is per egress address, so spreading
// requests across relays multiplies the allowance.
//
// The bucket key is the request's x-real-ip header (upstream
// util_handler.ts:99 -> util_ipRateLimiter.ts:22), falling back to the
// literal "unknown" when the header is absent. The fleet handler strips
// x-real-ip, so every relay's request arrives WITHOUT it and the upstream
// stamps the RELAY's own egress address there. One address = one bucket.
//
// Verified 2026-10-29 with the canonical contract, same model, same body,
// same minute: relay = 200 billing_source=free, HOME-direct = 429
// FreeUsageLimitError. Two addresses, two buckets. The bucket is also split
// PER MODEL (dailyInterval = YYYYMMDD + modelId.slice(0,2) for a model
// with its own rateLimit) and resets at 00:00 UTC.
//
// So BOTH opencode deployments qualify:
//   * production (opencode.ai) - the fleet's addresses are independent
//     buckets there too. Pinning one relay for 2 minutes drains one bucket
//     and leaves 159 untouched; spreading multiplies the lane's daily
//     capacity by the fleet size.
//   * the STAGE (dev.opencode.ai) - a separate deployment with its own
//     database, so its buckets are a different row set entirely.
//
// The old comment here claimed production "is shared with every anonymous
// client on the internet" and therefore does not qualify. That was the
// misread that kept the fleet pinned to one address: a shared "unknown" row
// exists for requests that arrive with NO x-real-ip, but a relayed request
// is stamped with the relay's address, so it never touches that row.
const SPREAD_TARGETS = new Set<string>([
  "https://opencode.ai",
  "https://dev.opencode.ai",
]);

// A relay can refuse a target it has not been taught about: the fleet's
// allowlist is per deployment, so a relay deployed before a new target was
// added answers 403 "forbidden: target not allowlisted".
//
// That is a property of THAT relay, not of the target: another relay in the
// fleet may well forward it. So a refusal must move on to the next relay
// instead of ending the request - otherwise one stale deployment turns into
// a fleet-wide failure for that target.
//
// Only 403 qualifies. A 400 is the UPSTREAM rejecting the request body, and
// the upstream will reject the same body through every relay, so retrying it
// just multiplies the round trips.
function isRelayRefusal(status: number): boolean {
  return status === 403;
}

function isSpreadTarget(origin: string): boolean {
  return SPREAD_TARGETS.has(origin);
}

export function resetRelayRetryState(): void {
  _preferred = null;
  _penalty.clear();
  _spreadCursor.clear();
  // The spent-evidence counter is per (deployment, model): a reset means the
  // operator wants the fleet re-probed from scratch, so the accumulated
  // "this deployment is closed" evidence must go with it - on disk too.
  _quotaRejected.clear();
  try {
    fs.rmSync(QUOTA_EVIDENCE_FILE, { force: true });
  } catch {
    // A missing file is the expected state.
  }
}

function relayCandidates(state: RelayState, now: number, target?: URL, model?: string): string[] {
  const out: string[] = [];
  const push = (u: string) => {
    if (!u || out.includes(u) || penalized(u, now, target, model)) return;
    out.push(u);
  };
  // The preferred relay is only pinned when it is NOT a per-egress-bucket
  // target. Both opencode deployments qualify now: the upstream bucket is
  // keyed on the request's x-real-ip, and the fleet strips that header, so
  // the relay's own egress address lands in the row. Sticking to one relay
  // throws away the other ~159 buckets.
  const spread = target ? isSpreadTarget(target.origin) : false;
  if (!spread && _preferred && _preferred.until > now && (!target || !_preferred.origin || _preferred.origin === target.origin)) {
    push(_preferred.url);
  }
  push(state.url);
  // Scheduler ranking first (coolest healthy accounts), then the raw fleet order as
  // a fallback for relays the last tick did not rank.
  const ranked = rankedRelays();
  for (const u of ranked) push(u);
  for (const r of state.relays) push(r.url);
  if (spread && out.length > 1) {
    // Rotate the list so consecutive requests start on a different relay.
    // The cursor is per (origin, model): each model has its own daily bucket
    // per address, so two different models must not share one rotation
    // position - they would land on the same address together and drain it
    // twice as fast.
    const off = nextSpreadOffset(target!.origin, model, out.length);
    return out.slice(off).concat(out.slice(0, off));
  }
  return out;
}

// The model id in a request body, when the body is a JSON string.
//
// The upstream counts a request against a daily bucket keyed by (egress
// address, model), so the penalty and the spread rotation must know which
// model is being asked for. The body is a string on this path (the daemon
// passes a JSON string so a retry can replay it), and a non-JSON or
// stream body simply yields undefined - the penalty then stays
// deployment-scoped, which is the conservative choice.
function bodyModel(body: BodyInit | null | undefined): string | undefined {
  if (typeof body !== "string") return undefined;
  // A cheap pre-check: the model id is always inside a JSON object, so a
  // body that cannot contain it is skipped without parsing.
  if (!body.includes('"model"')) return undefined;
  try {
    const parsed = JSON.parse(body) as { model?: unknown };
    return typeof parsed.model === "string" && parsed.model ? parsed.model : undefined;
  } catch {
    return undefined;
  }
}

// relay-aware fetch: direct when disabled, else POST via the relay, falling back
// to the next relay on a retryable status and finally to a direct fetch on
// a transport error. `init.body` must be replayable (server.ts passes a string).
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
    return relayOnce(state.url!, u, init, null);
  }

  const now = Date.now();
  const spread = isSpreadTarget(u.origin);
  const maxAlternates = spread ? MAX_SPREAD_ALTERNATES : MAX_ALTERNATES;
  // The model id decides WHICH daily bucket the upstream counts this request
  // against (util_ipRateLimiter.ts: dailyInterval = YYYYMMDD +
  // modelId.slice(0,2) for a model with its own rateLimit), so the penalty
  // and the spread cursor are both scoped to it.
  const model = bodyModel(init.body);
  // The FULL candidate list is the denominator for "is this model really
  // exhausted?": the fleet has ~160 addresses, and a 429 on the first 13 of
  // them says nothing about the other 147. The daemon must not park a model
  // fleet-wide on that evidence, or the lane dies while most of the fleet is
  // still fresh.
  const allCandidates = relayCandidates(state, now, u, model);
  const candidates = allCandidates.slice(0, 1 + maxAlternates);
  // Every candidate is currently marked spent: its egress IP's allowance is
  // used up. Do NOT re-probe one of them — go straight to this machine's own
  // address, which is a different client identity with its own allowance.
  if (candidates.length === 0) {
    const res = await fetch(targetUrl, init);
    // The fleet is spent for this model (every relay carries a live penalty),
    // so the verdict still has to travel to the daemon - otherwise the model
    // would never be parked and every request would keep paying for a direct
    // fetch that also fails.
    return markFleetExhausted(res, fleetIsSpent(state, u, model));
  }
  // Compress once and reuse across attempts (a retry must not re-pay the CPU).
  const packed = await packBody(init.body);

  // One candidate's outcome. The body is read BEFORE the response is cancelled:
  // returning a cancelled Response as `last` used to surface as "Body is
  // unusable: Body has already been read" at the caller's res.text() — every
  // exhausted retry turned into a 502 that hid the real upstream status.
  type Attempt =
    | { kind: "ok"; url: string; res: Response }
    | { kind: "quota"; url: string; res: Response; text: string }
    | { kind: "retry"; url: string; res: Response; text: string }
    | { kind: "refusal"; url: string }
    | { kind: "transport"; url: string };

  const attempt = async (url: string): Promise<Attempt> => {
    let res: Response;
    try {
      res = await relayOnce(url, u, init, packed);
    } catch {
      // Transport failure (dead deployment, DNS, reset).
      return { kind: "transport", url };
    }
    if (!RETRY_STATUS.has(res.status) && !isRelayRefusal(res.status)) {
      return { kind: "ok", url, res };
    }
    if (isRelayRefusal(res.status)) {
      // A refusal is not a quota verdict and not a broken relay: it is a
      // deployment that does not know this target.
      try {
        await res.body?.cancel();
      } catch {
        /* already closed */
      }
      return { kind: "refusal", url };
    }
    let text = "";
    try {
      text = await res.text();
    } catch {
      /* body already gone */
    }
    return res.status === 429
      ? { kind: "quota", url, res, text }
      : { kind: "retry", url, res, text };
  };

  let last: Response | null = null;
  let quotaHit = false;

  // Record one attempt's consequence: the penalty, the spent evidence, and the
  // preferred relay. Kept in one place so the sequential attempt and the raced
  // attempts cannot drift apart.
  const record = (a: Attempt): void => {
    if (a.kind === "ok") {
      if (a.url !== state.url) _preferred = { url: a.url, until: Date.now() + PREFER_MS, origin: u.origin };
      else _preferred = null;
      // This address answered: its bucket still has room, so it is not part of
      // the spent evidence for this (deployment, model).
      noteQuotaServed(u, model, a.url);
      return;
    }
    if (a.kind === "transport" || a.kind === "refusal") {
      // A caller-side abort is not the relay's fault: penalising the address
      // would throw away a healthy relay because the client hung up.
      if (a.kind === "transport" && init.signal?.aborted) return;
      _penalty.set(penaltyKey(a.url, u), Date.now() + PENALTY_MS);
      return;
    }
    // A quota 429 (free allowance spent on that egress IP) is not a relay
    // failure: park the relay until the upstream Retry-After instead of the flat
    // 5 minutes, so the next call does not pay the same dead round-trip again.
    //
    // The park is scoped to (address, model): the upstream counts each request
    // against a bucket keyed by the egress address AND the model id, so an
    // address that is spent for one id still has room for the others. Measured
    // 2026-10-29: deepseek-v4.1-flash-free answered 200 on 10/160 addresses
    // in the same minute that four other ids answered 429 on 160/160.
    //
    // A NON-quota 429 (a burst limit) stays deployment-scoped: it is not
    // evidence about the model's bucket, and hiding the address for every
    // model would throw away the fleet.
    const penalty = quotaPenaltyMs(a.res, a.text);
    if (a.kind === "quota" && penalty > PENALTY_MS) quotaHit = true;
    const quotaScoped = a.kind === "quota" && penalty > PENALTY_MS && model;
    if (quotaScoped) noteQuotaRejected(u, model, a.url);
    _penalty.set(
      quotaScoped ? penaltyKey(a.url, u, model) : penaltyKey(a.url, u),
      Date.now() + penalty,
    );
    last = new Response(a.text, { status: a.res.status, headers: a.res.headers });
  };

  // The FIRST candidate goes alone: on a healthy fleet it is the one that
  // answers, and racing would spend a request from every other address's
  // budget for nothing.
  const first = await attempt(candidates[0]!);
  record(first);
  if (first.kind === "ok") return first.res;

  // A QUOTA 429 means the fleet may be spent, so the REST of the candidates can
  // be raced instead of walked one by one.
  //
  // Racing is safe here and only here: a quota 429 is FREE upstream (the
  // limiter's check() throws before track(), so a rejected request consumes no
  // budget) and the next candidate is a DIFFERENT egress address with its own
  // row. A 5xx or a transport error is NOT free — a 200 from the raced
  // candidate would spend that address's budget on a request nobody asked for
  // — so those keep the sequential walk.
  //
  // Racing is also only worth it when the fleet is ALREADY known to be spent
  // for this model. Otherwise the sequential walk finds the next address with
  // room after one round-trip (~2.1s), while racing would spend one request
  // from up to RACE_WIDTH addresses that all had room. So the race is gated on
  // the spent-evidence counter, which is exactly the condition under which the
  // daemon is about to fall through to the stage anyway.
  //
  // The bar is the same majority-of-the-fleet rule the daemon's park uses, so
  // the two agree: the daemon parks the model (and the stage takes over) only
  // after a majority of the fleet has answered 429, and the race opens at the
  // same point. A fixed bar would desynchronise them - with 160 addresses, a
  // bar of 12 would race on the very first request, spending one request from
  // up to 6 healthy addresses on every call.
  const fleetKnownSpent =
    quotaRejectedCount(u, model) >= fleetSpentThreshold(state.relays.length);
  const raceable = first.kind === "quota" && fleetKnownSpent;
  const rest = candidates.slice(1);
  if (rest.length > 0 && raceable) {
    const attempts: Attempt[] = [];
    let next = 0;
    let served = false;
    const worker = async (): Promise<void> => {
      while (!served) {
        const i = next++;
        if (i >= rest.length) return;
        const a = await attempt(rest[i]!);
        attempts.push(a);
        record(a);
        if (a.kind === "ok") served = true;
      }
    };
    const width = Math.min(RACE_WIDTH, rest.length);
    await Promise.all(Array.from({ length: width }, () => worker()));
    const winner = attempts.find((a) => a.kind === "ok");
    if (winner && winner.kind === "ok") return winner.res;
  } else {
    // Not a quota rejection: walk the rest one by one. Each step keeps the
    // old semantics, including the direct fallback when the fleet is exhausted.
    for (let i = 0; i < rest.length; i++) {
      const a = await attempt(rest[i]!);
      record(a);
      if (a.kind === "ok") return a.res;
      if (a.kind === "transport" || a.kind === "refusal") {
        if (i === rest.length - 1) {
          // No relay in the fleet forwards this target. Fall back to direct.
          return fetch(targetUrl, init);
        }
      }
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
  const final = last ?? (await fetch(targetUrl, init));
  // Did the WHOLE fleet run out, or only the addresses this request happened
  // to try? The caller parks the model fleet-wide on a quota 429, so it must
  // know the difference: a 429 on 13 of 160 addresses is not exhaustion.
  //
  // Two pieces of evidence, and the stronger one wins:
  //   * every relay carries a live penalty for this deployment and model;
  //   * every address a single request can WALK has answered a quota 429.
  //
  // The second is the one that works in practice: the walk is bounded, so
  // the penalty map only covers the addresses that request touched. The
  // counter accumulates across requests, so a genuinely closed deployment is
  // recognised after a handful of requests instead of never.
  //
  // Until then the model stays alive and the NEXT request lands on another
  // address.
  const fleetExhausted =
    quotaHit && (fleetIsSpent(state, u, model) || fleetIsEssentiallySpent(state, u, model));
  return markFleetExhausted(final, fleetExhausted);
}

// Is EVERY relay in the fleet spent for this deployment and model?
//
// This is the question the daemon needs answered before it parks a model
// for the whole upstream refill window: the upstream counts each request
// against a daily bucket keyed by (egress address, model), and the fleet
// has ~160 addresses with their own rows. A 429 on the addresses one
// request happened to try says nothing about the rest, so the model must
// stay alive and the NEXT request must land on another address.
//
// An address counts as spent when it carries a live penalty for this
// deployment and this model.
function fleetIsSpent(state: RelayState, target: URL, model?: string): boolean {
  if (!state.relays.length) return false;
  const now = Date.now();
  return state.relays.every((r) => penalized(r.url, now, target, model));
}

// How many DISTINCT addresses have answered a quota 429 for this
// (deployment, model) since the last reset.
//
// This is the cross-request evidence that the penalty map alone cannot give:
// the penalty map is keyed by relay URL, and the fleet-exhaustion test asks
// whether EVERY relay carries a live penalty. But the penalty map is only
// populated for the addresses a request actually touched, and one request
// touches 13 of the ~160, so the fleet never looks exhausted on its own.
//
// Counting distinct quota-rejected addresses gives a real answer: production
// opencode.ai was measured at 429 on all 160 relays for all 8 anonymous-free
// ids, so the fleet IS exhausted there, and the daemon should park the model
// and fall through to the stage instead of paying the dead round-trips on
// every request.
//
// The counter is per (deployment, model ROW) and resets as soon as an address
// answers with anything else, so a deployment that recovers is noticed.
const _quotaRejected = new Map<string, Set<string>>();

// How many (deployment, model row) pairs are tracked at once. The catalog is
// small, but relayFetch takes the model id from the request body, so an
// unbounded key space is reachable from outside. The cap keeps a flood of
// one-off ids from growing the map without limit; the oldest entries go
// first, and a real lane's entry is refreshed on every use.
const MAX_QUOTA_TRACK_KEYS = 256;

// The spent evidence is written to disk so a daemon restart does not have to
// relearn it.
//
// Measured 2026-10-29: the first request after a restart paid ~26s of dead
// production round-trips before the stage answered, because the evidence was
// in memory only. The upstream's daily row refills at 00:00 UTC, so the
// evidence is valid for the rest of the UTC day - exactly as long as the
// upstream's own penalty.
const QUOTA_EVIDENCE_FILE = path.join(BANSOS_DIR, "quota-evidence.json");

interface QuotaEvidenceFile {
  // "<origin>|<prefix>" -> { spent: string[]; until: epochMs }
  rows: Record<string, { spent: string[]; until: number }>;
}

function quotaEvidenceUntil(now = Date.now()): number {
  // The upstream's row refills at 00:00 UTC, so the evidence cannot outlive
  // the UTC day it was collected in.
  const nextMidnight = Date.UTC(
    new Date(now).getUTCFullYear(),
    new Date(now).getUTCMonth(),
    new Date(now).getUTCDate() + 1,
  );
  return nextMidnight;
}

function persistQuotaEvidence(): void {
  const until = quotaEvidenceUntil();
  const rows: Record<string, { spent: string[]; until: number }> = {};
  for (const [key, set] of _quotaRejected) {
    if (set.size === 0) continue;
    rows[key] = { spent: [...set], until };
  }
  try {
    writeJsonAtomic(QUOTA_EVIDENCE_FILE, { rows }, 0);
  } catch {
    // A failed write only costs the next restart its shortcut.
  }
}

function restoreQuotaEvidence(): void {
  const file = readJson<QuotaEvidenceFile>(QUOTA_EVIDENCE_FILE);
  if (!file || typeof file.rows !== "object" || file.rows === null) return;
  const now = Date.now();
  for (const [key, entry] of Object.entries(file.rows)) {
    if (!entry || typeof entry.until !== "number" || entry.until <= now) continue;
    const set = new Set<string>((entry.spent ?? []).filter((u) => typeof u === "string"));
    if (set.size) _quotaRejected.set(key, set);
  }
}

function quotaTrackKey(target: URL, model?: string): string {
  // The upstream row is keyed by the FIRST TWO CHARACTERS of the id
  // (util_ipRateLimiter.ts:25), so two ids sharing a prefix are ONE budget.
  // The tracker uses the same key, otherwise "deepseek-v4.1-flash-free" and
  // "deepseek-v4-flash" would each look like their own row and the spent
  // evidence would be undercounted.
  const prefix = model ? model.slice(0, 2) : "";
  return prefix ? `${target.origin}|${prefix}` : target.origin;
}

function noteQuotaRejected(target: URL, model: string | undefined, url: string): void {
  const key = quotaTrackKey(target, model);
  let set = _quotaRejected.get(key);
  if (!set) {
    set = new Set();
    _quotaRejected.set(key, set);
    // Bound the key space: the oldest key is dropped when the cap is hit.
    while (_quotaRejected.size > MAX_QUOTA_TRACK_KEYS) {
      const oldest = _quotaRejected.keys().next();
      if (oldest.done) break;
      _quotaRejected.delete(oldest.value);
    }
  }
  set.add(url);
  persistQuotaEvidence();
}

function noteQuotaServed(target: URL, model: string | undefined, url: string): void {
  const key = quotaTrackKey(target, model);
  const set = _quotaRejected.get(key);
  if (!set) return;
  if (set.delete(url) && set.size === 0) _quotaRejected.delete(key);
  persistQuotaEvidence();
}

// How many DISTINCT addresses are known to be spent for this
// (deployment, model)?
//
// The count drives the per-request probe budget: the more addresses are
// known to be spent, the fewer probes a request should pay for before it
// gives up and lets the daemon fall through to another deployment.
function quotaRejectedCount(target: URL, model?: string): number {
  return _quotaRejected.get(quotaTrackKey(target, model))?.size ?? 0;
}

// Has a MAJORITY of the fleet's addresses answered a quota 429 for this
// (deployment, model)?
//
// A majority - not all - is the threshold, because the fleet is large and
// addresses are heterogeneous: one relay may still have room while the rest
// are spent. But a majority means the deployment is essentially closed, and
// the remaining minority is worth probing only occasionally.
//
// The count is over DISTINCT addresses, so repeating the same relay does not
// manufacture evidence.
function fleetIsEssentiallySpent(state: RelayState, target: URL, model?: string): boolean {
  return deploymentEssentiallySpent(state, target.origin, model);
}

// How many distinct addresses must have answered a quota 429 before the
// deployment counts as closed for that model row.
//
// A MAJORITY of the fleet, not the per-request walk size. The threshold used
// to be MAX_SPREAD_ALTERNATES + 1 = 13, on the reasoning that "the walk visits
// 13 relays, so 13 distinct rejections mean the whole reachable set is
// spent". That reasoning was wrong in a way that cost the lane its whole day:
//
//   * one request only walks 13 of ~160 addresses, so 13 rejections say
//     nothing about the other 147;
//   * the fleet ROTATES the candidates, so the next request's 13 are
//     different addresses. The count climbed slowly but it DID climb: the
//     spent-evidence file on 2026-10-30 held 162 distinct addresses for the
//     "de" row;
//   * the verdict therefore needed ~13 SEPARATE requests to fire, each paying
//     ~27s of dead round-trips. Measured 2026-10-30: beam-free took 26.5s
//     and nemotron-3.5-super-vl-free 29.0s per request.
//
// The floor keeps the verdict from firing on a tiny fleet where one address
// is half the fleet: on a fleet smaller than 26 addresses the walk can cover
// the whole fleet in one request, so 13 rejections already are a majority.
//
// A false "closed" is harmless: the stage-first attempt falls through to the
// normal path when the stage does not serve the model, so a wrong verdict
// costs one wasted stage lookup, not a broken lane.
function fleetSpentThreshold(relayCount: number): number {
  return Math.max(MAX_SPREAD_ALTERNATES + 1, Math.ceil(relayCount / 2));
}

// Is this deployment closed for this model, as far as the daemon can tell?
//
// The daemon needs this BEFORE it sends the request, because a closed
// deployment should not be tried first: the prod path costs ~2s per relay
// and, measured on 2026-10-29, production opencode.ai answered 429 on all
// 160 relays for all 8 anonymous-free ids. Trying it first means every
// request pays ~26s of dead round-trips before the stage - the one
// deployment that still has room - is reached.
//
// A majority - not all - is the threshold, because the fleet is large and
// addresses are heterogeneous: one relay may still have room while the rest
// are spent. But a majority means the deployment is essentially closed, and
// the remaining minority is worth probing only occasionally.
//
// The count is over DISTINCT addresses, so repeating the same relay does not
// manufacture evidence.
export function deploymentEssentiallySpent(
  state: RelayState,
  origin: string,
  model?: string,
): boolean {
  if (!state.relays.length) return false;
  const set = _quotaRejected.get(quotaTrackKey(new URL(origin), model));
  if (!set || set.size === 0) return false;
  return set.size >= fleetSpentThreshold(state.relays.length);
}

// The fleet-exhaustion verdict travels on a response header, because the
// response body is the upstream's own error text and the daemon reads it
// verbatim for the client.
export const FLEET_EXHAUSTED_HEADER = "x-bansos-fleet-exhausted";

// Mark a response as "the whole fleet is spent for this model". The daemon
// reads this to decide whether the model may be parked fleet-wide.
function markFleetExhausted(res: Response, exhausted: boolean): Response {
  if (!exhausted) return res;
  try {
    const headers = new Headers(res.headers);
    headers.set(FLEET_EXHAUSTED_HEADER, "1");
    return new Response(res.body, { status: res.status, statusText: res.statusText, headers });
  } catch {
    // A body that cannot be re-wrapped (already consumed): the verdict is
    // lost, and the caller falls back to the conservative behaviour.
    return res;
  }
}

// bansos-opt4: Vercel's Fast Origin Transfer meter counts bytes on the WIRE, not
// decoded bytes. Measured on a probe function with bodyParser off: the same
// 30.0 MB of JSON cost +36.5 MB of FOT as plain text and +0.1 MB gzipped, because
// the platform hands the function the still-compressed stream.
//
// Real-world ratio is 4x, not 10x: a 1.0 MB body of non-repeating source files and
// diffs lands at 255 KB (0.238) at level 6, and 302 KB at level 1. The 10x figure
// came from a low-entropy probe body. Level 9 buys 1 KB over level 6 for 3 ms more
// CPU, so level 6 stays.
const GZIP_MIN_BYTES = 1024;
const GZIP_LEVEL = 6;
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

// A relay can only take a compressed body if it decompresses on receipt. The
// Vercel handler does (api/index.js: CommonJS + bodyParser:false + it reads the
// raw stream and gunzips). The Cloudflare worker and the Netlify function hand
// the body through untouched: measured on the worker, the same payload answers
// 429 plain and 400 gzipped, so compressing to a non-Vercel relay turns a quota
// error into a dead relay. Loopback is listed for the local relay (express.json
// inflates gzip) and for the tests. Unknown hosts stay plain — plain always works.
const GZIP_CAPABLE_HOSTS = [".vercel.app", "127.0.0.1", "localhost", "::1"];

function relayTakesGzip(relayUrl: string): boolean {
  let host: string;
  try {
    host = new URL(relayUrl).hostname.toLowerCase();
  } catch {
    return false;
  }
  const override = process.env.BANSOS_RELAY_GZIP_HOSTS;
  const hosts = override === undefined
    ? GZIP_CAPABLE_HOSTS
    : override.split(",").map((h) => h.trim().toLowerCase()).filter(Boolean);
  return hosts.some((h) => (h.startsWith(".") ? host.endsWith(h) : host === h));
}

// The compressed wire body. Node's `BodyInit` type does not accept a bare
// Uint8Array (it models `Iterable<Uint8Array>`, not the buffer itself), so the
// bytes travel in a small wrapper and get one documented cast at the fetch call.
interface PackedBody {
  bytes: Uint8Array;
}

const gzipAsync = promisify(zlib.gzip);

// Compress ONCE per request, not once per relay attempt: relayFetch can try three
// candidates, and a 1.6 MB agent body costs ~11 ms of CPU per pass (4 MB: ~36 ms)
// on the same event loop that streams SSE to the client. zlib.gzip is async for
// the same reason — gzipSync blocks the loop for the whole compression.
async function packBody(body: BodyInit | null | undefined): Promise<PackedBody | null> {
  if (!gzipEnabled() || typeof body !== "string" || body.length < GZIP_MIN_BYTES) return null;
  const buf = await gzipAsync(Buffer.from(body, "utf8"), { level: GZIP_LEVEL });
  return { bytes: new Uint8Array(buf) };
}

// Exported so the threshold and kill switch can be tested without standing up a
// daemon: a real chat body is always >1 KB after transformation, so the small-body
// path is unreachable through the HTTP surface.
export async function gzipBodyForTest(
  body: BodyInit | null | undefined,
): Promise<{ body: BodyInit | null | undefined; gzipped: boolean }> {
  const packed = await packBody(body);
  return packed ? { body: packed.bytes as unknown as BodyInit, gzipped: true } : { body, gzipped: false };
}

async function relayOnce(
  relayUrl: string,
  u: URL,
  init: RequestInit,
  packed: PackedBody | null,
): Promise<Response> {
  const headers = new Headers(init.headers);
  headers.set("x-relay-target", u.origin);
  headers.set("x-relay-path", `${u.pathname}${u.search}`);
  const wire = packed !== null && relayTakesGzip(relayUrl) ? packed : null;
  if (wire !== null) {
    // Drop any inbound length/encoding the caller set: the wire body is now gzip.
    headers.set("content-encoding", "gzip");
    // Set the compressed length EXPLICITLY. Without it the request goes out
    // chunked, the Vercel edge buffers the stream and answers 400 with an empty
    // body before the function runs (measured: identical gzip payload is 200
    // when curl sends content-length, 400 when node sends it chunked).
    headers.set("content-length", String(wire.bytes.byteLength));
  }
  return fetch(relayUrl, {
    ...init,
    headers,
    body: wire !== null ? (wire.bytes as unknown as BodyInit) : init.body,
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

// Restore the spent evidence from disk at module load.
//
// The daemon is restarted often (an install, a config change, a crash), and
// without this every restart pays the full dead-production walk once more:
// measured 2026-10-29, ~26s before the stage answered.
//
// The file is only written by this module, and it expires at 00:00 UTC with
// the upstream's own row, so a stale file cannot close a lane for longer
// than the upstream's own budget lasts.
restoreQuotaEvidence();
