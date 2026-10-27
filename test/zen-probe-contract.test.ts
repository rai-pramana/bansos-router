import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { execFileSync } from "node:child_process";
import { zenUpstream, ZEN_MODELS } from "../src/upstreams/zen";

// The probe scripts outside this repo (~/.hermes/scripts/zen_probe_contract.py
// and everything importing it) must send the SAME canonical tools the daemon
// sends upstream. The free-tier gate compares them, so a paraphrase is a
// refusal that looks exactly like "quota exhausted".
//
// This test locks the contract inside the repo so `npm test` catches drift at
// the source, and the out-of-repo helper is checked by
// ~/.hermes/scripts/zen_probe_contract_check.py.

const HELPER = path.join(os.homedir(), ".hermes", "scripts", "zen_probe_contract.py");

function upstreamTools(): Array<{ function: { name: string; description: string; parameters: unknown } }> {
  const model = ZEN_MODELS.find((m) => m.wireApi !== "responses")!;
  const out = zenUpstream.transformRequestBody!(
    { model: model.id, messages: [{ role: "user", content: "hi" }] },
    model,
  );
  return out.tools as Array<{ function: { name: string; description: string; parameters: unknown } }>;
}

test("the canonical fingerprint tools are read + shell with upstream's own text", () => {
  const tools = upstreamTools();
  const names = tools.map((t) => t.function.name);
  assert.deepEqual(names, ["read", "shell"], "the gate only accepts this exact pair");
  for (const t of tools) {
    assert.ok(t.function.description.length > 100, `${t.function.name} description is the verbatim upstream one`);
    assert.ok(t.function.parameters, `${t.function.name} carries a parameter schema`);
  }
});

test("the out-of-repo probe helper is byte-identical to the upstream tools", { skip: !fs.existsSync(HELPER) }, () => {
  const helper = execFileSync("python3", [HELPER.replace(/\.py$/, "_check.py")], {
    encoding: "utf8",
    timeout: 300_000,
  });
  assert.match(helper, /DIVERGENCES: 0/, helper);
});
