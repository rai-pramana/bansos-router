import fs from "node:fs";
import { ADAPTERS } from "../adapters";
import { effectiveBind, effectivePort, loadConfig, loadConfigWarnings } from "../daemon/state";
import { START_MARKER } from "../adapters/types";
import { expandHome, parseJsonc } from "./write";
import { checkUpdate } from "../update";

export async function runDoctor(_argv: string[]): Promise<number> {
  const base = `http://${effectiveBind()}:${effectivePort()}`;
  let failures = 0;

  // 1. daemon reachable?
  try {
    const res = await fetch(`${base}/healthz`, { signal: AbortSignal.timeout(2000) });
    const body = (await res.json()) as { modelCount?: number; relay?: { enabled: boolean; url: string } };
    console.log(`✓ daemon      ok at ${base} (${body.modelCount ?? "?"} models)`);
    if (body.relay?.enabled && body.relay.url) {
      console.log(`✓ relay       on (${body.relay.url})`);
    } else {
      console.log(`· relay       disabled (direct)`);
    }
  } catch {
    console.error(`✗ daemon      not reachable at ${base}`);
    console.error(`  fix: run "bansos start" (or "bansosd") in another terminal`);
    failures++;
  }

  const warnings = loadConfigWarnings();
  if (warnings.length > 0) {
    for (const w of warnings) console.error(`✗ security    ${w}`);
    failures++;
  } else {
    const { mode, dlp } = loadConfig().security;
    console.log(`✓ security    ${mode} mode${mode === "strict" ? ` (dlp: ${dlp})` : ""}`);
  }

  // 2. per-harness config files.
  for (const h of harnessStatuses()) {
    if (h.status === "missing") {
      console.log(`· ${h.id.padEnd(12)} not configured (bansos setup ${h.id})`);
    } else if (h.status === "configured") {
      console.log(`✓ ${h.id.padEnd(12)} configured (${h.path})`);
    } else {
      console.log(`· ${h.id.padEnd(12)} file exists but no bansos config (${h.path})`);
    }
  }

  // 3. update available?
  const update = await checkUpdate();
  if (update.hasUpdate) {
    console.log(`\nUpdate available: ${update.current} -> ${update.latest} (run: npm i -g bansos-router)`);
  }

  return failures > 0 ? 1 : 0;
}

export type HarnessStatus = { id: string; status: "configured" | "unconfigured" | "missing"; path?: string };

// bansos-opt4: adapter config paths can point at SQLite blobs (9router keeps its
// registry in data.sqlite). Never feed a binary or huge file to parseJsonc —
// see hasKeys.
const TEXT_CONFIG_LIMIT = 1 << 20;

export function harnessStatuses(): HarnessStatus[] {
  return ADAPTERS.map((adapter) => {
    const found = adapter.configPaths.map(expandHome).find((p) => fs.existsSync(p));
    if (!found) return { id: adapter.id, status: "missing" };
    let content = "";
    try {
      content = fs.readFileSync(found, "utf8");
    } catch {
      return { id: adapter.id, status: "unconfigured", path: found };
    }
    const configured = adapter.undoKeys ? hasKeys(content, adapter.undoKeys) : content.includes(START_MARKER);
    return { id: adapter.id, status: configured ? "configured" : "unconfigured", path: found };
  });
}

function hasKeys(content: string, keys: string[]): boolean {
  // bansos-opt4: 9router keeps its registry in SQLite, so `configPaths` can point
  // at binary. parseJsonc is a per-char scanner whose brace-trim replace is O(n^2):
  // slurping the 27MB data.sqlite wedged the event loop and the daemon stopped
  // answering /healthz. Binary or oversized payloads fall back to a leaf-id
  // substring hit (the ids appear verbatim in both db.json and the sqlite blob).
  if (content.length > TEXT_CONFIG_LIMIT || content.slice(0, 4096).includes("\u0000")) {
    return keys.some((k) => content.includes((k.split(".").pop() ?? k).replace(/\*$/, "")));
  }
  try {
    const obj = parseJsonc(content);
    const first = keys[0];
    if (!first) return false;
    let cur: unknown = obj;
    for (const p of first.split(".")) {
      const match = p.endsWith("*")
        ? (k: unknown) => typeof k === "string" && k.startsWith(p.slice(0, -1))
        : (k: unknown) => k === p;
      if (Array.isArray(cur)) {
        cur = cur.find((item) => {
          const rec = item as Record<string, unknown> | null;
          return rec && typeof rec === "object" && match(rec.id ?? rec.title ?? rec.model);
        });
      } else if (cur && typeof cur === "object") {
        const k = Object.keys(cur).find(match);
        cur = k === undefined ? undefined : (cur as Record<string, unknown>)[k];
      } else {
        cur = undefined;
      }
      if (cur === undefined) return false;
    }
    return true;
  } catch {
    return false;
  }
}
