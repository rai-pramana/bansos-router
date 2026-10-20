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
    for (const m of models) this.byId.set(m.id, m);
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
      if (covered.has(m.source) && !saved.models.some((s) => s.id === id)) this.byId.delete(id);
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
  markRateLimited(id: string, retryAfterMs?: number): void {
    const ms = Math.min(Math.max(retryAfterMs ?? DEFAULT_COOLDOWN_MS, 1_000), MAX_COOLDOWN_MS);
    this.coolUntil.set(id, Date.now() + ms);
  }

  // upstream refused this model (401/403), usually about egress location, not the request
  markRefused(id: string): void {
    this.coolUntil.set(id, Date.now() + REFUSED_COOLDOWN_MS);
  }

  isCoolingDown(id: string, now = Date.now()): boolean {
    const until = this.coolUntil.get(id);
    if (until === undefined) return false;
    if (until <= now) {
      this.coolUntil.delete(id);
      return false;
    }
    return true;
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
  }
}
