import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { RuntimeCatalog } from "../src/daemon/catalog";
import { RETIRED_FILE } from "../src/daemon/state";
import { createLogger } from "../src/logger";
import { modelDef, type Upstream } from "../src/upstreams/types";

// A retirement verdict ("Model is unavailable") is a fact about the UPSTREAM's
// registry, not about one daemon process.
//
// It used to live in memory only, so every restart forgot it and the next
// request for the id paid the production round-trip again before the stage could
// answer. Measured 2026-11-01: a cold boot spent 1.9s on production's 400 before
// the stage served the same request, on every restart.

function devNull(): NodeJS.WritableStream {
  return { write: () => true } as unknown as NodeJS.WritableStream;
}

function model(id: string) {
  return modelDef({
    id,
    name: id,
    source: "zen",
    reasoning: false,
    contextWindow: 4096,
    maxTokens: 1024,
    input: ["text"],
    compat: { supportsDeveloperRole: false, supportsReasoningEffort: false },
  });
}

const upstream: Upstream = {
  id: "zen",
  kind: "remote-keyless",
  relayAllowed: false,
  chatUrl: "https://example.invalid/chat/completions",
  async fetchCatalog() {
    return [model("zen/m1")];
  },
  requestHeaders: () => ({}),
};

function fresh(): RuntimeCatalog {
  const catalog = new RuntimeCatalog([upstream], createLogger({ level: "error" }));
  catalog.seed([model("muse-spark-1.3-contributor-free")]);
  return catalog;
}

test("a retirement verdict survives a daemon restart", () => {
  fs.rmSync(RETIRED_FILE, { force: true });

  const before = fresh();
  assert.equal(before.isRetired("muse-spark-1.3-contributor-free"), false);
  before.markRetired("muse-spark-1.3-contributor-free");
  assert.ok(fs.existsSync(RETIRED_FILE), "the verdict must be written to disk");

  // A new process: same state dir, brand-new in-memory catalog.
  const after = fresh();
  assert.equal(
    after.isRetired("muse-spark-1.3-contributor-free"),
    true,
    "a cold boot must not forget which ids the upstream already retired",
  );
  after.loadRetired();
  assert.equal(after.isRetired("muse-spark-1.3-contributor-free"), true);
});

test("an expired retirement file is not trusted", () => {
  fs.rmSync(RETIRED_FILE, { force: true });
  fs.writeFileSync(
    RETIRED_FILE,
    JSON.stringify({
      // Older than the 7-day snapshot window: upstream registries do re-add ids
      // (that is how muse-spark came back), so a permanent veto is wrong.
      savedAt: Date.now() - 30 * 24 * 60 * 60_000,
      ids: ["some-old-free-id"],
    }),
  );
  const catalog = fresh();
  catalog.loadRetired();
  assert.equal(
    catalog.isRetired("some-old-free-id"),
    false,
    "an ancient retirement verdict must not hide a re-added id forever",
  );
  fs.rmSync(RETIRED_FILE, { force: true });
});

test("markRetired is idempotent and keeps every id", () => {
  fs.rmSync(RETIRED_FILE, { force: true });
  const catalog = fresh();
  catalog.markRetired("a-free-id");
  catalog.markRetired("a-free-id");
  catalog.markRetired("b-free-id");

  const saved = JSON.parse(fs.readFileSync(RETIRED_FILE, "utf8")) as { ids: string[] };
  assert.deepEqual([...saved.ids].sort(), ["a-free-id", "b-free-id"]);
  fs.rmSync(RETIRED_FILE, { force: true });
});