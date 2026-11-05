import test from "node:test";
import assert from "node:assert/strict";
import { ZEN_MODELS } from "../src/upstreams/zen";

// The stage publishes a model only if this registry has an entry for it
// (zen-stage.describe -> KNOWN -> ZEN_MODELS), and every `zen-stage/<id>` label
// Hermes dials depends on that. Upstream rewrote ZEN_MODELS for 0.6.x and
// dropped muse-spark-1.3 while dev.opencode.ai kept answering it for free: the
// id disappeared from /v1/models and 18 aux slots answered "unknown model"
// with no error anywhere in the lane. This test is the tripwire for that merge.

const STAGE_IDS_WE_DEPEND_ON = ["big-pickle", "muse-spark-1.3-contributor-free"];

test("registry keeps every id the zen stage is relied on for", () => {
  const ids = new Set(ZEN_MODELS.map((m) => m.id));
  for (const id of STAGE_IDS_WE_DEPEND_ON) {
    assert.ok(ids.has(id), `${id} missing from ZEN_MODELS: the zen-stage label cannot be built`);
  }
});

test("muse-spark keeps the responses wire and its multimodal inputs", () => {
  const muse = ZEN_MODELS.find((m) => m.id === "muse-spark-1.3-contributor-free");
  assert.ok(muse);
  // A chat-wire request to this id answers 500; the translation is what makes
  // it usable, and image/pdf are what make it the only vision-capable stage id.
  assert.equal(muse.wireApi, "responses");
  assert.deepEqual(muse.input, ["text", "image", "pdf"]);
  assert.ok((muse.contextWindow ?? 0) >= 1_000_000);
});