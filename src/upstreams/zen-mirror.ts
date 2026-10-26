import { modelDef, type ModelDef, type Upstream } from "./types";

// A zen mirror: the SAME model id that opencode.ai serves, answered by a
// different free upstream that still has quota.
//
// Why this exists
// ---------------
// OpenCode Zen meters its anonymous free models as a request-count budget per
// real client IP, refilling at 00:00 UTC. When that bucket is empty every
// egress identity answers 429 FreeUsageLimitError — one IP, one thousand IPs,
// Node or Bun, official headers or a replayed capture: all the same verdict.
// The upstream pool is CLOSED, not mis-routed.
//
// But the same underlying models are served by other free upstreams under
// different ids. `orca/deepseek/deepseek-v4.1-flash-free`,
// `unorouter/beam:free` and `kenari/beam-1-0:free` all answer 200 while
// `oc/beam-free` answers 429. Each of those routers has its own quota bucket,
// so the model is reachable — just not through opencode.ai.
//
// This upstream keeps the zen model IDs stable (a client asking for
// "beam-free" keeps asking for "beam-free") and swaps the transport to a
// mirror that currently has room. The zen IDs stay the public contract; the
// mirror list is the mechanism.
//
// It is deliberately NOT a transparent swap of every zen request: a model with
// no live mirror keeps 429ing exactly as before, so the daemon's own failover
// logic still has something to do, and a caller that insists on the real zen
// upstream can still ask for it.

export interface ZenMirrorConfig {
  // base url of an OpenAI-compatible gateway that proxies many free routers,
  // e.g. "http://127.0.0.1:20128/v1"
  baseUrl: string;
  // bearer token for that gateway
  apiKey?: string;
  // upstream prefixes to try, in order, when the zen pool is closed
  // (e.g. ["orca", "unorouter", "kenari", "tokenharbor", "cl"])
  providers: string[];
  // how long the discovered mirror list stays trusted before it is refetched
  ttlMs?: number;
}

// The zen models a mirror may serve, and the family stem used to find its
// counterpart on a mirror provider. The id on the mirror provider differs
// from the zen id (":free" vs "-free", a vendor prefix, a version suffix),
// so the match is on the model family, not the literal id.
const MIRRORABLE = new Map<string, string[]>([
  ["beam-free", ["beam"]],
  ["deepseek-v4.1-flash-free", ["deepseek-v4.1-flash"]],
  ["glm-5.3-flash-free", ["glm-5.3-flash"]],
  // The zen flash id has no free mirror of its own: every `mimo-v2.6-flash`
  // free id answers 402 "subscription only". The pro model is the same
  // family with a different tier, and it does have free quota — so the
  // stem list carries both, best first.
  ["mimo-v2.6-flash-free", ["mimo-v2.6-flash", "mimo-v2.6-pro"]],
  // `big-pickle` is the stealth name for the same underlying model as
  // `mimo-v2.6-flash-free` — identical tokenizer fingerprints (587/594/700
  // prompt tokens on three fixed strings) and the same self-identification.
  // It is therefore served by the same free mirrors, and the zen id stays
  // the client's contract.
  ["big-pickle", ["big-pickle", "mimo-v2.6-flash", "mimo-v2.6-pro"]],
  ["nemotron-3.5-super-vl-free", ["nemotron-3.5-super-vl"]],
  ["nemotron-3.5-lightning-free", ["nemotron-3.5-lightning"]],
  ["flint-40b-free", ["flint-v1.0", "flint-40b"]],
  ["longcat-2.5-preview-free", ["longcat-2.5-preview"]],
  ["minimax-m3.1-flash-free", ["minimax-m3.1-flash"]],
  ["muse-spark-1.3-contributor-free", ["muse-spark"]],
  ["space-bunny-free", ["space-bunny"]],
  ["cat-jam-free", ["cat-jam"]],
  // Zen-exclusive models. No free mirror exists today, but keeping them in the
  // map costs nothing and picks them up the moment a provider offers one.
  ["nemotron-3-ultra-free", ["nemotron-3-ultra"]],
  ["step-5-preview-free", ["step-5-preview"]],
]);

// A provider's 200 is not a success: some free routers answer 200 with a
// refusal string ("Sorry, to prevent abuse of free resources..."). That is
// worse than an error, because the client receives a valid-looking completion
// that says nothing. Treat it as a failure.
export const ZEN_MIRROR_REFUSAL =
  /prevent abuse|free resources|rate limit|try again|exceeded|quota|not available/i;

// Is this gateway id a free one?
//
// The free marker is not always the last segment: a provider may namespace
// its free tier inside the path (`apinex/free/mimo-v2.6-pro`), not just
// suffix the model name (`unorouter/beam:free`, `orca/…-flash-free`). So
// every segment is checked, not just the tail.
export function zenMirrorIsFreeId(id: string): boolean {
  const segments = id.split("/");
  // A gateway id always carries a provider prefix; an id without one is not
  // a gateway id at all and must never be treated as a free mirror.
  if (segments.length < 2) return false;
  return segments.some((segment) => /:free$|-free$|^free$/.test(segment));
}

// How well a candidate matches the zen model. The family stem must match the
// WHOLE tail, not a prefix of a longer family: "deepseek-v4-flash-free" must
// lose to "deepseek-v4.1-flash-free". The best-scoring stem wins, so a model
// may have several acceptable spellings.
//
// Providers disagree on punctuation: kenari writes `flint-v1-0:free` where
// zen writes `flint-40b-free` and orca writes `flint-v1.0:free`. Dots and
// dashes are interchangeable in a model name, so both sides are normalised
// before comparing — otherwise a provider's spelling of the same model is
// simply not recognised.
function score(tail: string, zenId: string): number {
  const t = normalizeName(tail);
  let best = 10;
  for (const stem of MIRRORABLE.get(zenId) ?? [zenId]) {
    const s = normalizeName(stem);
    const score = t === s ? 100 : t.startsWith(s) ? 80 : t.includes(s) ? 60 : 10;
    if (score > best) best = score;
  }
  return best;
}

// A model name with its punctuation flattened, so "flint-v1.0" and
// "flint-v1-0" compare equal.
function normalizeName(name: string): string {
  return name.toLowerCase().replace(/[._]/g, "-");
}

export interface ZenMirrorHandle {
  upstream: Upstream;
  // zen id -> candidate mirror ids, best first
  readonly mirrors: ReadonlyMap<string, string[]>;
  // resolve the mirror list (network call, cached); call before using
  ready(): Promise<void>;
  // the candidate mirror ids for a zen id, best first
  mirrorsFor(zenId: string): string[];
  // A catalog entry for a zen id this mirror can serve.
  //
  // Most zen ids are pinned in the registry and the daemon uses that entry.
  // Some ids are mirror-only: they exist as a free mirror but the upstream
  // never listed them, so there is no pin to restore. Their entry is built
  // from the gateway's own model metadata, so the daemon can publish them.
  describe(zenId: string): ModelDef | undefined;
}

export function createZenMirror(cfg: ZenMirrorConfig): ZenMirrorHandle {
  const base = cfg.baseUrl.replace(/\/+$/, "");
  const ttl = cfg.ttlMs ?? 10 * 60_000;
  const mirrors = new Map<string, string[]>();
  let modelIds: string[] = [];
  // The gateway's own metadata per model id, used to describe a mirror-only
  // zen id (one the upstream never listed, so the registry has no pin).
  const modelMeta = new Map<string, { contextWindow?: number; maxTokens?: number; reasoning?: boolean; input?: string[] }>();
  let fetchedAt = 0;
  let inflight: Promise<void> | null = null;

  async function fetchModelIds(): Promise<string[]> {
    if (Date.now() - fetchedAt < ttl && modelIds.length > 0) return modelIds;
    try {
      const res = await fetch(`${base}/models`, {
        headers: cfg.apiKey ? { authorization: `Bearer ${cfg.apiKey}` } : {},
        signal: AbortSignal.timeout(15_000),
      });
      if (!res.ok) return modelIds;
      const data = (await res.json()) as {
        data?: Array<{
          id?: string;
          context_length?: number;
          max_completion_tokens?: number;
          capabilities?: { reasoning?: boolean; vision?: boolean; tools?: boolean };
        }>;
      };
      const ids = (data.data ?? []).map((m) => m.id ?? "").filter(Boolean);
      if (ids.length > 0) {
        modelIds = ids;
        modelMeta.clear();
        for (const m of data.data ?? []) {
          if (!m.id) continue;
          const caps = m.capabilities ?? {};
          const input: string[] = ["text"];
          if (caps.vision) input.push("image");
          modelMeta.set(m.id, {
            contextWindow: m.context_length,
            maxTokens: m.max_completion_tokens,
            reasoning: caps.reasoning === true,
            input,
          });
        }
        fetchedAt = Date.now();
      }
      return modelIds;
    } catch {
      return modelIds;
    }
  }

  function resolve(): void {
    mirrors.clear();
    if (modelIds.length === 0) return;
    // The provider order is the preference order, but a provider is not a
    // guarantee: one provider's free id can answer 402 "exclusive to a paid
    // subscription" while the next one serves the same model for free. So
    // candidates are collected from EVERY configured provider, in preference
    // order, and the daemon walks the list until one actually answers.
    for (const zenId of MIRRORABLE.keys()) {
      const stems = MIRRORABLE.get(zenId)!;
      const found: string[] = [];
      for (const provider of cfg.providers) {
        const candidates = modelIds.filter((id) => {
          const slash = id.indexOf("/");
          if (slash < 0) return false;
          if (id.slice(0, slash) !== provider) return false;
          if (!zenMirrorIsFreeId(id)) return false;
          const tail = normalizeName(id.slice(slash + 1));
          return stems.some((s) => tail.includes(normalizeName(s)));
        });
        if (candidates.length === 0) continue;
        candidates.sort((a, b) => {
          const ta = a.split("/").slice(1).join("/");
          const tb = b.split("/").slice(1).join("/");
          return score(tb, zenId) - score(ta, zenId);
        });
        found.push(...candidates);
      }
      if (found.length > 0) mirrors.set(zenId, found);
    }
  }

  async function ready(): Promise<void> {
    if (inflight) return inflight;
    inflight = (async () => {
      await fetchModelIds();
      resolve();
    })().finally(() => {
      inflight = null;
    });
    return inflight;
  }

  const upstream: Upstream = {
    id: "zen-mirror",
    kind: "local-openai",
    // The mirror is a local gateway; it must never be sent through the relay
    // fleet, whose egress is the very thing that is closed.
    relayAllowed: false,
    chatUrl: `${base}/chat/completions`,

    async fetchCatalog(): Promise<ModelDef[] | null> {
      // The mirror is a TRANSPORT, not a catalog source. It never publishes
      // models: a model id belongs to whichever upstream currently owns it, and
      // publishing the same id from two sources would make the catalog's
      // replace-by-source bookkeeping fight itself. The mirror is instead
      // consulted by the daemon when the zen upstream reports its pool as spent.
      return null;
    },

    requestHeaders(): Record<string, string> {
      return cfg.apiKey ? { Authorization: `Bearer ${cfg.apiKey}` } : {};
    },

    transformRequestBody(body: Record<string, unknown>, model: ModelDef): Record<string, unknown> {
      // The body arrives with the ZEN id (that is the public contract). The
      // mirror speaks a different id, so it is swapped here. When no mirror
      // is known the zen id is passed through unchanged — the gateway then
      // answers with its own error, which is honest.
      const candidates = mirrors.get(model.id);
      if (!candidates || candidates.length === 0) return body;
      return { ...body, model: candidates[0]! };
    },
  };

  return {
    upstream,
    mirrors,
    ready,
    mirrorsFor: (zenId: string) => mirrors.get(zenId) ?? [],
    describe: (zenId: string): ModelDef | undefined => {
      const candidates = mirrors.get(zenId);
      if (!candidates || candidates.length === 0) return undefined;
      // The first candidate is the best match; its metadata describes the
      // model. A missing field falls back to a conservative default, so an
      // incomplete gateway entry still yields a usable catalog row.
      const meta = modelMeta.get(candidates[0]!) ?? {};
      return modelDef({
        id: zenId,
        name: zenId,
        source: "zen",
        reasoning: meta.reasoning ?? false,
        contextWindow: meta.contextWindow ?? 200_000,
        maxTokens: meta.maxTokens ?? 32_768,
        input: (meta.input ?? ["text"]) as ModelDef["input"],
        compat: { supportsReasoningEffort: false, supportsDeveloperRole: false },
      });
    },
  };
}

export { MIRRORABLE as ZEN_MIRRORABLE_MODELS };
