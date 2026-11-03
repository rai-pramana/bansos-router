import test from "node:test";
import assert from "node:assert/strict";
import { createZenStage } from "../src/upstreams/zen-stage";

// A restart must publish the zen-stage label without waiting for the network.
// The id list is cached on disk for exactly that reason: systemd starts the
// daemon before DNS is up, and a cold boot that published nothing left every
// `zen-stage/` slot answering "unknown model" until an unrelated request
// happened to trigger a refetch (measured 2026-11-04, 30 minutes).

test("zen-stage serves the cached ids before any network call", () => {
  const stage = createZenStage({ baseUrl: "https://stage.invalid" });
  const ids = stage.ids();
  assert.ok(Array.isArray(ids));
  for (const id of ids) {
    assert.equal(id.includes("/"), false, `${id} is a label, not a stage id`);
    assert.ok(stage.describe(id), `${id} has no registry entry`);
  }
});

test("zen-stage drops a cached id the registry does not know", () => {
  const stage = createZenStage({ baseUrl: "https://stage.invalid" });
  assert.equal(stage.serves("brand-new-model-not-in-registry"), false);
  assert.equal(stage.describe("brand-new-model-not-in-registry"), undefined);
});

test("zen-stage publishes a label for every id it serves", () => {
  const stage = createZenStage({ baseUrl: "https://stage.invalid" });
  for (const id of stage.ids()) {
    // The label literal the caller dials is what setLabeledModels builds.
    assert.equal(stage.describe(id)?.id, id);
    assert.equal(stage.upstream.id, "zen-stage");
  }
});