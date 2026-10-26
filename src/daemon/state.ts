import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  DEFAULT_SECURITY_CONFIG,
  normalizeSecurityConfig,
  securityConfigWarnings,
  type SecurityConfig,
} from "../security/policy";

// overridable so the test suite never touches the real daemon state
export const BANSOS_DIR = process.env.BANSOS_DIR || path.join(os.homedir(), ".bansos");

export const CONFIG_FILE = path.join(BANSOS_DIR, "config.json");
export const STATE_FILE = path.join(BANSOS_DIR, "state.json");
export const RELAY_STATE_FILE = path.join(BANSOS_DIR, "relay-state.json");
export const LOG_DIR = path.join(BANSOS_DIR, "logs");
export const LOG_FILE = path.join(LOG_DIR, "bansosd.log");
export const CATALOG_FILE = path.join(BANSOS_DIR, "catalog.json");

export interface BansosConfig {
  port: number;
  bind: string;
  refreshIntervalMs: number;
  security: SecurityConfig;
  // opt-in local gateways (freebuff-proxy, litellm, ...)
  localUpstreams: Array<{ name: string; baseUrl: string; apiKey?: string }>;
  // OpenCode Zen's free pool is a daily per-IP request budget. When it is
  // spent, the same model is often still served by another free upstream under
  // a different id. This mirror lets the daemon answer the SAME zen model id
  // from such an upstream instead of returning 429.
  zenMirror?: {
    baseUrl: string;
    apiKey?: string;
    // provider prefixes to try, in order (e.g. ["orca", "unorouter", "kenari"])
    providers: string[];
  };
}

export const DEFAULT_CONFIG: BansosConfig = {
  port: 17070,
  bind: "127.0.0.1",
  refreshIntervalMs: 30 * 60_000,
  security: { ...DEFAULT_SECURITY_CONFIG },
  localUpstreams: [],
};

export function ensureBansosDir(): void {
  fs.mkdirSync(BANSOS_DIR, { recursive: true });
}

// the port/bind a running daemon actually bound (from state.json), preferred over config
export function effectivePort(): number {
  const state = readJson<{ port?: number }>(STATE_FILE);
  return typeof state?.port === "number" ? state.port : loadConfig().port;
}

export function effectiveBind(): string {
  const state = readJson<{ bind?: string }>(STATE_FILE);
  return typeof state?.bind === "string" ? state.bind : loadConfig().bind;
}

// base url of the daemon answering /healthz at the recorded port; an explicit other port never collides with it
export async function runningDaemonUrl(port?: number): Promise<string | null> {
  if (port !== undefined && port !== effectivePort()) return null;
  const base = `http://${effectiveBind()}:${effectivePort()}`;
  try {
    const res = await fetch(`${base}/healthz`, { signal: AbortSignal.timeout(800) });
    return res.ok ? base : null;
  } catch {
    return null;
  }
}

export function readRawConfig(file = CONFIG_FILE): { raw: Partial<BansosConfig> | null; invalid: boolean } {
  let text: string;
  try {
    text = fs.readFileSync(file, "utf8");
  } catch {
    return { raw: null, invalid: false };
  }
  try {
    const raw = JSON.parse(text) as unknown;
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) return { raw: null, invalid: true };
    return { raw: raw as Partial<BansosConfig>, invalid: false };
  } catch {
    return { raw: null, invalid: true };
  }
}

export function loadConfigWarnings(file = CONFIG_FILE): string[] {
  const { raw, invalid } = readRawConfig(file);
  if (invalid) {
    return [`${file} is not a valid JSON object; using defaults (normal mode, strict protections OFF).`];
  }
  return securityConfigWarnings(raw?.security);
}

export function loadConfig(file = CONFIG_FILE): BansosConfig {
  const { raw } = readRawConfig(file);
  if (!raw) return { ...DEFAULT_CONFIG };
  // The mirror is opt-in: without a base URL and at least one provider there
  // is nothing to mirror to, and a half-configured mirror must not make the
  // daemon send zen traffic somewhere unexpected.
  const mirror = raw.zenMirror;
  const zenMirror =
    mirror &&
    typeof mirror === "object" &&
    typeof mirror.baseUrl === "string" &&
    mirror.baseUrl.trim().length > 0 &&
    Array.isArray(mirror.providers) &&
    mirror.providers.length > 0
      ? {
          baseUrl: mirror.baseUrl.trim(),
          ...(typeof mirror.apiKey === "string" && mirror.apiKey ? { apiKey: mirror.apiKey } : {}),
          providers: mirror.providers.filter(
            (p): p is string => typeof p === "string" && p.trim().length > 0,
          ),
        }
      : undefined;
  return {
    ...DEFAULT_CONFIG,
    ...raw,
    security: normalizeSecurityConfig(raw.security),
    localUpstreams: raw.localUpstreams ?? [],
    ...(zenMirror ? { zenMirror } : {}),
  };
}

// atomic write: temp file in the same dir, then rename over target
// indent: 0 writes compact JSON. The daemon rewrites activity.json (5000 events,
// ~1.5MB) on every chat batch; nothing parses it by eye, so the 30% of bytes that
// were pretty-printing whitespace was pure write amplification. Everything else
// stays indented because it is meant to be read and hand-edited.
export function writeJsonAtomic(file: string, data: unknown, indent = 2): void {
  ensureBansosDir();
  const tmp = `${file}.${process.pid}.tmp`;
  const body = indent > 0 ? JSON.stringify(data, null, indent) : JSON.stringify(data);
  fs.writeFileSync(tmp, `${body}\n`, { mode: 0o600 });
  fs.renameSync(tmp, file);
}

export function readJson<T>(file: string): T | null {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8")) as T;
  } catch {
    return null;
  }
}
