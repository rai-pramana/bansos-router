import { ZEN_MODELS, zenUpstream } from "./zen";
import type { ModelDef, Upstream } from "./types";

// The zen STAGE deployment.
//
// opencode.ai runs the same console code on several stages. Upstream
// infra/stage.ts maps them:
//
//     production -> opencode.ai
//     dev        -> dev.opencode.ai
//     <other>    -> <other>.dev.opencode.ai
//
// Every stage is a separate SST app with its own database (sst.config.ts
// picks the "opencode-dev" AWS profile for every non-production stage), so
// every stage has its OWN free-tier bucket.
//
// Why production is exhausted while the stage still answers:
// the upstream limiter keys the daily bucket on the request's `x-real-ip`
// header (util_handler.ts:99 -> util_ipRateLimiter.ts:22), falling back to
// the literal "unknown" only when that header is ABSENT. The fleet handler
// strips x-real-ip from the inbound request (bansos-relay/api/index.js:30),
// so the edge stamps the RELAY's own egress address there
// (inference-ip.ts: cf-connecting-ip). One relay = one address = one bucket.
//
// Measured 2026-10-29:
//   * 160 relay URLs -> 159 distinct egress addresses;
//   * one relay URL -> one stable egress address (100.52.203.58, 60/60 calls
//     on it), so a relay is a bucket, not a lottery;
//   * address A answered 2446 requests then 429'd while address B was
//     polled 193 times in the same window and kept answering 200 - so the
//     rows are PER ADDRESS;
//   * the bucket is also split PER MODEL, keyed by the FIRST TWO CHARACTERS
//     of the id (util_ipRateLimiter.ts:25), so "deepseek-*" ids share one
//     row and "bi" (big-pickle) / "be" (beam-free) are different rows;
//   * the row resets at 00:00 UTC (retry-after points there).
//
// So production is not a single shared bucket - it is 159 buckets that each
// fill up. The stage is the same thing with a different database, so its
// buckets are a separate row set entirely.
//
// The stage is the SAME product and the SAME model ids (big-pickle,
// muse-spark-1.3-contributor-free). It is a transport change, not a provider
// change, so it keeps the zen lane pure: a zen id is still answered by
// opencode's own console, never by a different provider under that id.
//
// What it is NOT: a way to get more model ids. The stage publishes its own
// catalog, and an id it does not carry answers "Model is unavailable" (400).
// Only the ids the stage actually serves are usable there.

export interface ZenStageConfig {
  // base url of the stage deployment, e.g. "https://dev.opencode.ai"
  baseUrl: string;
  // how long the stage's model list stays trusted before it is refetched
  ttlMs?: number;
}

export interface ZenStageHandle {
  upstream: Upstream;
  // resolve the stage's model list (network call, cached); call before using
  ready(): Promise<void>;
  // does the stage serve this model id?
  serves(id: string): boolean;
  // the stage's ids that this daemon already knows (the ones it may publish)
  ids(): string[];
  // the catalog entry for a stage-served id
  describe(id: string): ModelDef | undefined;
}

// The ids this daemon already has a registry entry for. A stage id outside
// this set is not published: the daemon would have no metadata (context
// window, modalities, wire) for it.
const KNOWN = new Map(ZEN_MODELS.map((m) => [m.id, m]));

export function createZenStage(cfg: ZenStageConfig): ZenStageHandle {
  const base = cfg.baseUrl.replace(/\/+$/, "");
  const ttl = cfg.ttlMs ?? 10 * 60_000;
  let served = new Set<string>();
  let fetchedAt = 0;
  let inflight: Promise<void> | null = null;

  async function refresh(): Promise<void> {
    try {
      // The catalog lookup is quota-free (it answers 200 even when the pool
      // is empty), so it is safe to call on a timer.
      const res = await fetch(`${base}/zen/v1/models`, {
        headers: zenUpstream.requestHeaders(),
        signal: AbortSignal.timeout(10_000),
      });
      if (!res.ok) return;
      const data = (await res.json()) as { data?: Array<{ id?: string }> };
      const ids = new Set((data.data ?? []).map((m) => m.id ?? "").filter(Boolean));
      if (ids.size === 0) return;
      served = ids;
      fetchedAt = Date.now();
    } catch {
      // keep the last known list: an unreachable stage must not unpublish
      // the ids it served a minute ago
    }
  }

  async function ready(): Promise<void> {
    if (Date.now() - fetchedAt < ttl && served.size > 0) return;
    if (inflight) return inflight;
    inflight = refresh().finally(() => {
      inflight = null;
    });
    return inflight;
  }

  const upstream: Upstream = {
    id: "zen-stage",
    kind: "remote-keyless",
    // The stage IS reachable through the relay fleet, and that is the point.
    //
    // The upstream limiter keys its daily bucket on the request's x-real-ip
    // (util_ipRateLimiter.ts:22) and the edge stamps the RELAY's egress
    // address there when the client does not send x-real-ip itself
    // (inference-ip.ts). The fleet handler strips x-real-ip, so every relay
    // gets its OWN bucket.
    //
    // The stage is a SEPARATE deployment with its own database, so those
    // buckets are virgin: measured, the same relay answered 429 on
    // opencode.ai and 200 with billing_source=free on dev.opencode.ai in the
    // same minute. Routing the stage through the fleet therefore turns one
    // bucket into 166.
    relayAllowed: true,
    chatUrl: `${base}/inference/openai/v1/chat/completions`,
    responsesUrl: `${base}/inference/openai/v1/responses`,

    async fetchCatalog(): Promise<ModelDef[] | null> {
      // The stage decides WHICH ids it can serve; it does not publish new
      // models. An id the daemon does not know is not usable here.
      const out: ModelDef[] = [];
      for (const id of served) {
        const known = KNOWN.get(id);
        if (known) out.push(known);
      }
      return out.length > 0 ? out : null;
    },

    requestHeaders(model, callerHeaders) {
      // The stage speaks the same console contract as production: same
      // session identity, same fingerprint headers, same body shape.
      return zenUpstream.requestHeaders(model, callerHeaders);
    },

    transformRequestBody(body, model) {
      return zenUpstream.transformRequestBody!(body, model);
    },
  };

  return {
    upstream,
    ready,
    serves: (id) => served.has(id),
    ids: () => [...served].filter((id) => KNOWN.has(id)),
    describe: (id) => KNOWN.get(id),
  };
}
