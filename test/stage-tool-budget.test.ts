import test from "node:test";
import assert from "node:assert/strict";
import { zenUpstream } from "../src/upstreams/zen";

// The stage's free-tier gate counts tools, and refuses over-long lists with the
// SAME FreeUsageLimitError as a spent bucket.
//
// Measured 2026-11-01 against dev.opencode.ai on muse-spark-1.3-contributor-free,
// 10 identical calls per case: read+shell plus 20 other tools answered 200 on
// every call; the same list with one more tool answered 429 on every call. The
// daemon used to ADD its own read/shell to whatever the caller sent, so a caller
// with 21 tools (Hermes) reached 23 and was refused - every Hermes request failed
// while a 2-tool dashboard ping answered 200 through the same relay.

const up = zenUpstream;

function tool(name: string) {
  return {
    type: "function",
    function: {
      name,
      description: `${name} tool`,
      parameters: { type: "object", properties: {} },
    },
  };
}

function names(body: Record<string, unknown>): string[] {
  const tools = (body.tools ?? []) as Array<Record<string, any>>;
  return tools.map((t) => t.function?.name ?? t.name ?? "");
}

function body(toolNames: string[]) {
  return { model: "muse-spark-1.3-contributor-free", tools: toolNames.map(tool) };
}

test("a caller's own read_file and terminal are renamed, never duplicated", () => {
  const b = body(["read_file", "terminal", "web_search"]);
  const out = (up.transformRequestBody as any)(b, { id: "muse-spark-1.3-contributor-free" });
  const list = names(out);
  assert.ok(list.includes("read"), "the gate must see `read`");
  assert.ok(list.includes("shell"), "the gate must see `shell`");
  assert.equal(
    list.filter((n) => n === "read").length,
    1,
    "renaming must not leave a duplicate read behind",
  );
  assert.equal(
    list.filter((n) => n === "shell").length,
    1,
    "renaming must not leave a duplicate shell behind",
  );
  assert.equal(out.tools.length, 3, "renaming must not grow the caller's list");
});

test("a caller that ships no file/shell tool gets the canonical pair added", () => {
  const b = body(["web_search", "memory"]);
  const out = (up.transformRequestBody as any)(b, { id: "muse-spark-1.3-contributor-free" });
  const list = names(out);
  assert.ok(list.includes("read"));
  assert.ok(list.includes("shell"));
});

test("an over-long list is trimmed to what the gate accepts", () => {
  // Hermes ships 21 tools, which is over the budget once the gate's own pair is
  // accounted for. The request must still be sent, with the list cut down.
  const many = Array.from({ length: 30 }, (_, i) => `tool_${i}`);
  const b = body(["read_file", "terminal", ...many]);
  const out = (up.transformRequestBody as any)(b, { id: "muse-spark-1.3-contributor-free" });
  const list = names(out);
  assert.ok(list.length <= 22, `tools must be trimmed to the gate budget, got ${list.length}`);
  assert.ok(list.includes("read"), "the trim must keep read");
  assert.ok(list.includes("shell"), "the trim must keep shell");
});

test("a list already inside the budget is left alone", () => {
  const b = body(["read_file", "terminal", "web_search"]);
  const out = (up.transformRequestBody as any)(b, { id: "muse-spark-1.3-contributor-free" });
  assert.equal(out.tools.length, 3, "nothing may be added or dropped from a valid list");
});

test("the chat wire transform follows the same rule", () => {
  const b = {
    model: "big-pickle",
    messages: [{ role: "user", content: "hi" }],
    tools: [tool("read_file"), tool("terminal"), ...Array.from({ length: 25 }, (_, i) => tool(`t_${i}`))],
  };
  const out = (up.transformRequestBody as any)(b, { id: "big-pickle" });
  const list = names(out);
  assert.ok(list.length <= 22, `chat wire must respect the budget too, got ${list.length}`);
  assert.ok(list.includes("read"));
  assert.ok(list.includes("shell"));
});