import type { Logger } from "../logger";
import {
  DEFAULT_SECURITY_CONFIG,
  isUpstreamAllowed,
  type SecurityConfig,
} from "../security/policy";
import type { ModelDef, Upstream, UpstreamSource } from "../upstreams/types";
import { CATALOG_FILE, readJson, writeJsonAtomic } from "./state";

const DEFAULT_COOLDOWN_MS = 60_000;
const MAX_COOLDOWN_MS = 15 * 60_000;
// A daily free-tier quota is not a burst limit: the upstream says exactly when
// the bucket refills (Retry-After points at the next UTC midnight, up to 24h).
// Parking such a model for the flat 15 minutes above means the next request
// pays the full round-trip again to relearn the same "spent" answer, and every
// probe/watch tick burns one more request from the bucket that is already
// empty. Honour the upstream window instead — bounded so a wild header cannot
// park a model forever.
const QUOTA_COOLDOWN_MAX_MS = 24 * 60 * 60_000;
// 401/403 is a policy refusal, not a burst limit: back off longer, but not forever
const REFUSED_COOLDOWN_MS = 30 * 60_000;
// a snapshot older than this says nothing about today's catalog
const SNAPSHOT_MAX_AGE_MS = 7 * 24 * 60 * 60_000;

export interface RefreshReport {
  checked: number;
  alive: number;
  dead: number;
  degraded: string[];
}

export class RuntimeCatalog {
  private byId = new Map<string, ModelDef>();
  private readonly bySource = new Map<string, Upstream>();
  private readonly upstreams: Upstream[];
  private refreshInFlight: Promise<RefreshReport> | null = null;
  private readonly coolUntil = new Map<string, number>();
  // The pinned registry, kept so a model the upstream retired can be
  // published again when a mirror takes it over.
  private readonly pins = new Map<string, ModelDef>();
  // Model ids a free mirror can serve under the SAME id. The upstream may
  // retire such an id (410 ModelDeprecated) while the mirror still answers
  // it, so the catalog keeps publishing it instead of dropping it as
  // unknown. The client asked for this id and the mirror answers under it.
  private readonly mirrored = new Set<string>();
  // The catalog entry used for each mirror-backed id, so a refresh can
  // re-publish it after the upstream dropped it.
  private readonly mirrorDefs = new Map<string, ModelDef>();
  // Model ids the zen STAGE can serve under the SAME id. The stage is
  // opencode's own console on another deployment with its own catalog, so it
  // may carry an id the production catalog does not list (measured:
  // muse-spark-1.3-contributor-free is on the stage and gone from
  // production). Such an id stays published for the same reason a mirrored
  // one does: the client asked for this id and the stage answers under it.
  private readonly staged = new Set<string>();
  // The catalog entry used for each stage-backed id.
  private readonly stageDefs = new Map<string, ModelDef>();
  // Model ids the upstream retired (410 / "Model is unavailable"). Asking
  // that upstream again only buys the same rejection, so the request goes
  // straight to the mirror while the id stays published.
  private readonly retired = new Set<string>();
  // Why a model is parked, so a repeated request to it can repeat the upstream's
  // own rejection (message + status) instead of paying for the same round trip.
  private readonly parkReason = new Map<string, { status: number; message: string }>();
  // Models parked by a DAILY free-tier exhaustion (as opposed to a burst rate
  // limit or a policy refusal). Only these defer to the on-disk refill marker:
  // a burst park is the daemon's own timing and must not be cleared just
  // because no marker exists.
  private readonly quotaParked = new Set<string>();

  constructor(
    upstreams: Upstream[],
    private readonly log: Logger,
    private readonly security: SecurityConfig = DEFAULT_SECURITY_CONFIG,
  ) {
    this.upstreams = upstreams;
    for (const u of upstreams) {
      this.bySource.set(u.id, u);
      if (u.kind === "local-openai") this.bySource.set("local", u);
    }
  }

  // upstream that serves a model source (zen/kilo/llm7/local)
  upstreamBySource(source: UpstreamSource): Upstream | undefined {
    return this.bySource.get(source);
  }

  // seed the pinned registry so the daemon works before any network call
  seed(models: ModelDef[]): void {
    for (const m of models) {
      this.byId.set(m.id, m);
      this.pins.set(m.id, m);
    }
  }

  // Declare which ids a free mirror can serve under the same id.
  //
  // A retired upstream id stays published when the mirror takes it over: the
  // mirror answers the SAME id from a different transport with its own quota,
  // so the id is not dead — only that upstream's copy of it is. Any such id
  // the upstream has already dropped is published again from the pins.
  //
  // `describe` supplies the entry for a mirror-only id: one the upstream never
  // listed, so the registry has no pin for it. Without such an entry the id
  // stays unknown and every request for it fails with "unknown model".
  setMirroredModels(ids: Iterable<string>, describe?: (id: string) => ModelDef | undefined): void {
    this.mirrored.clear();
    for (const id of ids) this.mirrored.add(id);
    for (const id of this.mirrored) {
      // The entry is remembered even when the id is already published: a
      // later refresh drops it (the upstream no longer lists it), and the
      // remembered entry is what puts it back.
      const entry = this.byId.get(id) ?? this.pins.get(id) ?? describe?.(id);
      if (!entry) continue;
      this.mirrorDefs.set(id, entry);
      if (!this.byId.has(id)) this.byId.set(id, entry);
    }
  }

  // The ids a mirror can serve, and that are published here.
  get mirroredIds(): string[] {
    return [...this.mirrored].filter((id) => this.byId.has(id));
  }

  // Declare which ids the zen stage can serve under the same id.
  //
  // The stage publishes its own catalog, which can carry an id the production
  // catalog does not list. Such an id must stay published: the stage answers
  // it under the SAME id, so dropping it would turn a working request into
  // "unknown model".
  setStagedModels(ids: Iterable<string>, describe?: (id: string) => ModelDef | undefined): void {
    this.staged.clear();
    for (const id of ids) this.staged.add(id);
    for (const id of this.staged) {
      const entry = this.byId.get(id) ?? this.pins.get(id) ?? describe?.(id);
      if (!entry) continue;
      this.stageDefs.set(id, entry);
      if (!this.byId.has(id)) this.byId.set(id, entry);
    }
  }

  // The ids the stage can serve, and that are published here.
  get stagedIds(): string[] {
    return [...this.staged].filter((id) => this.byId.has(id));
  }

  // Remember that the upstream retired this id, so the next request skips
  // that upstream instead of paying for the same rejection again.
  markRetired(id: string): void {
    this.retired.add(id);
  }

  isRetired(id: string): boolean {
    return this.retired.has(id);
  }

  // the last live pass is a better offline seed than the build-time pins: a cold
  // boot with no egress otherwise serves catalog entries that upstream retired
  loadSnapshot(now = Date.now()): boolean {
    const saved = readJson<{ savedAt?: number; models?: ModelDef[] }>(CATALOG_FILE);
    if (!saved?.models?.length) return false;
    if (typeof saved.savedAt === "number" && now - saved.savedAt > SNAPSHOT_MAX_AGE_MS) {
      this.log.warn("catalog snapshot older than 7 days - ignoring it");
      return false;
    }
    for (const m of saved.models) this.byId.set(m.id, m);
    // the snapshot speaks for its sources: drop the pins there, or retired ids
    // come back alongside it (muse-spark, space-bunny, ...)
    const covered = new Set(saved.models.map((m) => m.source));
    for (const [id, m] of this.byId) {
      if (covered.has(m.source) && !saved.models.some((s) => s.id === id)) {
        // A mirror-backed id stays: the snapshot predates the retirement, and
        // the mirror still answers this id.
        if (this.mirrored.has(id)) continue;
        // So does a stage-backed id: the stage publishes its own catalog, so
        // an id it carries may never appear in a production snapshot.
        if (this.staged.has(id)) continue;
        this.byId.delete(id);
      }
    }
    this.log.info(`catalog snapshot: ${saved.models.length} model(s) from the last live pass`);
    return true;
  }

  get models(): ModelDef[] {
    return [...this.byId.values()];
  }

  // models whose upstream the security policy lets a request reach
  get servableModels(): ModelDef[] {
    return this.models.filter((m) => isUpstreamAllowed(this.security, this.bySource.get(m.source)?.id ?? m.source));
  }

  get upstreamIds(): string[] {
    return this.upstreams.map((u) => u.id);
  }

  // park a 429'd model so the next request skips it; Retry-After sets the time, else a minute
  markRateLimited(id: string, retryAfterMs?: number, reason?: { status: number; message: string }): void {
    const ms = Math.min(Math.max(retryAfterMs ?? DEFAULT_COOLDOWN_MS, 1_000), MAX_COOLDOWN_MS);
    this.coolUntil.set(id, Date.now() + ms);
    if (reason) this.parkReason.set(id, reason);
  }

  // park a model whose DAILY free-tier bucket is spent. The upstream's
  // Retry-After points at the exact refill moment (next UTC midnight), so the
  // park lasts that long instead of the flat 15 minutes a burst limit gets:
  // every probe in the meantime would spend another request from the bucket
  // that is already empty, and the fleet would keep marking live relays dead.
  markQuotaExhausted(id: string, retryAfterMs?: number, reason?: { status: number; message: string }): void {
    const ms = Math.min(Math.max(retryAfterMs ?? DEFAULT_COOLDOWN_MS, 1_000), QUOTA_COOLDOWN_MAX_MS);
    this.coolUntil.set(id, Date.now() + ms);
    this.quotaParked.add(id);
    if (reason) this.parkReason.set(id, reason);
  }

  // upstream refused this model (401/403), usually about egress location, not the request
  markRefused(id: string, reason?: { status: number; message: string }): void {
    this.coolUntil.set(id, Date.now() + REFUSED_COOLDOWN_MS);
    if (reason) this.parkReason.set(id, reason);
  }

  isCoolingDown(id: string, now = Date.now()): boolean {
    const until = this.coolUntil.get(id);
    if (until === undefined) return false;
    if (until <= now) {
      this.coolUntil.delete(id);
      this.parkReason.delete(id);
      return false;
    }
    return true;
  }

  // Drop a park whose reason no longer holds (e.g. an expired zen gate marker),
  // so the next request actually probes the upstream again.
  clearPark(id: string): void {
    this.coolUntil.delete(id);
    this.parkReason.delete(id);
    this.quotaParked.delete(id);
  }

  // True when this model's park came from a daily free-tier exhaustion, i.e.
  // the on-disk refill marker — not the daemon's own burst timing — is the
  // authority on when it may be probed again.
  isQuotaParked(id: string): boolean {
    return this.quotaParked.has(id);
  }

  // The upstream rejection that parked this model, when one was recorded.
  parkMessage(id: string): { status: number; message: string } | undefined {
    return this.parkReason.get(id);
  }

  /**
   * Re-park every zen model for a window that survived a restart.
   *
   * The per-model park lives in memory; the refill deadline on disk does not.
   * Without this, a daemon restart (including `bansos-install-local`) forgets
   * that the daily bucket is spent until the bucket is spent AGAIN — one paid
   * round trip per model, per restart.
   *
   * Only models that are actually served by zen are touched, and an existing
   * (longer) park is never shortened.
   */
  restoreZenPark(windowMs: number, reason?: string, now = Date.now()): void {
    if (windowMs <= 0) return;
    for (const model of this.models) {
      if (model.source !== "zen") continue;
      const until = this.coolUntil.get(model.id);
      if (until !== undefined && until >= now + windowMs) continue;
      this.coolUntil.set(model.id, now + windowMs);
      this.quotaParked.add(model.id);
      // Carry the recorded reason so a repeat request still explains WHY,
      // instead of falling back to the generic cooldown sentence.
      if (reason) this.parkReason.set(model.id, { status: 429, message: reason });
    }
  }

  /**
   * Restore the zen park PER MODEL ROW.
   *
   * The upstream meters (egress IP, the model's first two characters)
   * separately, so a row that is still open must not be closed by another
   * row's exhaustion. The resolver answers for one model id.
   */
  restoreZenParkPerModel(
    resolve: (modelId: string) => { windowMs: number; reason?: string } | undefined,
    now = Date.now(),
  ): void {
    for (const model of this.models) {
      if (model.source !== "zen") continue;
      const entry = resolve(model.id);
      if (!entry || entry.windowMs <= 0) continue;
      const until = this.coolUntil.get(model.id);
      if (until !== undefined && until >= now + entry.windowMs) continue;
      this.coolUntil.set(model.id, now + entry.windowMs);
      this.quotaParked.add(model.id);
      if (entry.reason) this.parkReason.set(model.id, { status: 429, message: entry.reason });
    }
  }

  // Milliseconds left on a model's park, 0 when it is not parked. The daemon
  // uses this to keep the zen probe gate closed for exactly as long as the
  // model stays parked, even if the gate marker was cleared by hand.
  cooldownRemaining(id: string, now = Date.now()): number {
    const until = this.coolUntil.get(id);
    if (until === undefined) return 0;
    return Math.max(0, until - now);
  }

  resolve(id: string): ModelDef | undefined {
    const direct = this.byId.get(id);
    if (direct) return direct;
    // tolerant fallback: "laguna-xs-2.1:free" matches "poolside/laguna-xs-2.1:free"
    for (const [k, m] of this.byId) {
      if (k.endsWith(`/${id}`)) return m;
    }
    return undefined;
  }

  // unreachable upstream keeps last-known models; concurrent callers share the in-flight pass
  async refresh(): Promise<RefreshReport> {
    if (this.refreshInFlight) return this.refreshInFlight;
    this.refreshInFlight = this.runRefresh();
    try {
      return await this.refreshInFlight;
    } finally {
      this.refreshInFlight = null;
    }
  }

  private async runRefresh(): Promise<RefreshReport> {
    const report: RefreshReport = { checked: 0, alive: 0, dead: 0, degraded: [] };

    const allowed = this.upstreams.filter((upstream) => {
      if (isUpstreamAllowed(this.security, upstream.id)) return true;
      report.degraded.push(upstream.id);
      this.log.warn(`upstream ${upstream.id}: blocked by strict allowlist`);
      return false;
    });

    // upstreams run independently so one slow gateway costs its own latency, not the sum
    const fetched = await Promise.all(
      allowed.map(async (upstream) => ({
        upstream,
        live: await upstream.fetchCatalog().catch(() => null),
      })),
    );

    // applied in registry order so the catalog does not reshuffle per pass
    const liveSources = new Set<UpstreamSource>();
    for (const { upstream, live } of fetched) {
      if (live === null) {
        report.degraded.push(upstream.id);
        this.log.warn(`upstream ${upstream.id}: no live catalog - keeping last-known models`);
        continue;
      }
      const source: UpstreamSource =
        upstream.kind === "local-openai" ? "local" : (upstream.id as UpstreamSource);
      this.replaceBySource(source, live);
      liveSources.add(source);
      report.checked += live.length;
    }

    this.saveSnapshot(liveSources);
    report.alive = this.byId.size;
    this.log.info(`catalog refresh: ${report.alive} model(s) alive`);
    return report;
  }

  // ponytail: one flat snapshot, written only from the sources that answered this
  // pass; the build-time pins cover the rest. Per-source files only if that bites.
  private saveSnapshot(live: Set<UpstreamSource>): void {
    const models = this.models.filter((m) => live.has(m.source));
    if (models.length === 0) return;
    try {
      writeJsonAtomic(CATALOG_FILE, { savedAt: Date.now(), models });
    } catch (err) {
      this.log.warn(`catalog snapshot write failed: ${String((err as Error)?.message ?? err)}`);
    }
  }

  private replaceBySource(source: UpstreamSource, models: ModelDef[]): void {
    for (const [id, m] of this.byId) {
      if (m.source === source) this.byId.delete(id);
    }
    for (const m of models) this.byId.set(m.id, m);
    // A model the upstream retired but a mirror can serve under the SAME id
    // stays published. The upstream's copy is gone, but the id is not: the
    // mirror answers it from a different transport with its own quota, so
    // dropping it here would turn a working lane into "unknown model".
    for (const id of this.mirrored) {
      if (this.byId.has(id)) continue;
      const entry = this.mirrorDefs.get(id);
      if (entry) {
        this.byId.set(id, entry);
        this.log.info(`catalog: keeping retired model id, a mirror serves it`, {
          model: id,
          source,
        });
      }
    }
    // A stage-backed id stays published for the same reason: the stage has
    // its own catalog and may carry an id the production upstream dropped.
    for (const id of this.staged) {
      if (this.byId.has(id)) continue;
      const entry = this.stageDefs.get(id);
      if (entry) {
        this.byId.set(id, entry);
        this.log.info(`catalog: keeping model id the stage still serves`, {
          model: id,
          source,
        });
      }
    }
  }
}
