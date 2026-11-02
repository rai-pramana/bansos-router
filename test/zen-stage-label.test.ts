import test from "node:test";
import assert from "node:assert/strict";
import { RuntimeCatalog } from "../src/daemon/catalog";
import { createLogger } from "../src/logger";
import { modelDef, type ModelDef, type Upstream } from "../src/upstreams/types";

// The stage answers the SAME ids as production, so a single id cannot name both
// buckets. "zen-stage/<id>" is how a caller says "spend the stage's quota"; the
// catalog must publish it, route it to the stage upstream, and leave the bare
// production id untouched.

const log = createLogger({ level: "error" });
const BASE = "muse-spark-1.3-contributor-free";

function model(id: string, source: ModelDef["source"] = "zen"): ModelDef {
  return modelDef({
    id,
    name: id,
    source,
    reasoning: false,
    contextWindow: 4096,
    maxTokens: 1024,
    input: ["text"],
    compat: { supportsDeveloperRole: false, supportsReasoningEffort: false },
  });
}

function upstream(id: string): Upstream {
  return {
    id,
    kind: "remote-keyless",
    relayAllowed: true,
    chatUrl: `https://${id}.invalid/inference/openai/v1/chat/completions`,
    async fetchCatalog() {
      return null;
    },
    requestHeaders: () => ({}),
  };
}

function build() {
  const prod = upstream("zen");
  const stage = upstream("zen-stage");
  const catalog = new RuntimeCatalog([prod], log);
  catalog.seed([model(BASE)]);
  return { catalog, prod, stage };
}

test("a labeled id is published next to the production one", () => {
  const { catalog, stage } = build();
  const published = catalog.setLabeledModels("zen-stage", stage, [BASE], () => model(BASE));

  assert.deepEqual(published, [`zen-stage/${BASE}`]);
  const ids = catalog.models.map((m) => m.id).sort();
  assert.deepEqual(ids, [BASE, `zen-stage/${BASE}`].sort());
});

test("the labeled entry routes to the stage and carries the bare wire id", () => {
  const { catalog, stage } = build();
  catalog.setLabeledModels("zen-stage", stage, [BASE], () => model(BASE));

  const entry = catalog.resolve(`zen-stage/${BASE}`);
  assert.ok(entry, "labeled id must resolve");
  assert.equal(entry!.source, "zen-stage");
  assert.equal(entry!.id, `zen-stage/${BASE}`, "the caller's id is echoed back");
  assert.equal(entry!.upstreamId, BASE, "the wire must carry the bare id");
  assert.equal(catalog.upstreamBySource("zen-stage"), stage);
});

test("publishing a label does not shadow the production id", () => {
  const { catalog, stage, prod } = build();
  catalog.setLabeledModels("zen-stage", stage, [BASE], () => model(BASE));

  const bare = catalog.resolve(BASE);
  assert.equal(bare!.source, "zen", "the bare id must stay production");
  assert.equal(bare!.upstreamId, undefined);
  assert.equal(catalog.upstreamBySource("zen"), prod);
});

test("an id the label's describe() cannot describe is skipped, not guessed", () => {
  const { catalog, stage } = build();
  const published = catalog.setLabeledModels("zen-stage", stage, ["not-in-registry"], () => undefined);

  assert.deepEqual(published, []);
  assert.equal(catalog.resolve("zen-stage/not-in-registry"), undefined);
});

test("republishing the label is idempotent", () => {
  const { catalog, stage } = build();
  const first = catalog.setLabeledModels("zen-stage", stage, [BASE], () => model(BASE));
  const second = catalog.setLabeledModels("zen-stage", stage, [BASE], () => model(BASE));

  assert.deepEqual(first, second);
  const labeled = catalog.models.filter((m) => m.id === `zen-stage/${BASE}`);
  assert.equal(labeled.length, 1, "the id must not be duplicated in the list");
});
