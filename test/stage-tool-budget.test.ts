import test from "node:test";
import assert from "node:assert/strict";
import { zenUpstream, setToolTrimReporter } from "../src/upstreams/zen";

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

// Upstream now owns this case: a name outside opencode's own vocabulary collapses the whole set
// to the canonical pair, so a rename pass is unreachable and was removed. What stays ours is the
// invariant that the caller's own array is never mutated.
test("a foreign tool set collapses to the pair and leaves the caller's array alone", () => {
  const b = body(["read_file", "terminal", "web_search"]);
  const out = (up.transformRequestBody as any)(b, { id: "muse-spark-1.3-contributor-free" });
  const list = names(out);
  assert.deepEqual(list, ["read", "shell"], "the gate only ever sees its own pair");
  assert.equal((b.tools as unknown[]).length, 3, "the caller's tools array is untouched");
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
  // all three names are in opencode's own vocabulary, so nothing collapses and nothing needs
  // trimming: a valid list must come through exactly as sent
  const b = body(["bash", "grep", "glob"]);
  const out = (up.transformRequestBody as any)(b, { id: "muse-spark-1.3-contributor-free" });
  // the gate still requires its own read+shell pair, so a caller that ships neither gains exactly
  // those two - nothing else is added and nothing the caller sent is dropped
  assert.deepEqual(names(out), ["bash", "grep", "glob", "read", "shell"]);
  assert.equal(out.tools.length, 5, "only the gate's required pair may be added");
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
test("a trim is reported with the names it dropped, not swallowed", () => {
  // The trim keeps the request working, which is why it used to be invisible: a
  // caller that ships 37 tools (this CLI profile's full surface) gets a 200 and
  // its model simply never sees `execute_code`, `memory`, `delegate_task`, ...
  // An agentic caller then plans around capabilities it no longer has. Every
  // trim must therefore name what it removed.
  const seen: Array<{ model: string; total: number; kept: number; dropped: string[] }> = [];
  setToolTrimReporter((info) => seen.push(info));
  try {
    // 23 names, every one inside opencode's vocabulary: restrictGatedTools() keeps them all, so
    // the only thing that can cut the list is the budget - which is the behaviour under test
    const many = [
      "write", "edit", "glob", "grep", "list", "ls", "find", "view", "fetch", "todo",
      "task", "skill", "patch", "question", "plan", "batch", "scroll", "snap",
      "multiedit", "str_replace", "apply_edit",
    ];
    const b = body(["read", "shell", ...many]);
    const out = (up.transformRequestBody as any)(b, { id: "muse-spark-1.3-contributor-free" });
    assert.equal(out.tools.length, 22, "the list still has to fit the gate");
    assert.equal(seen.length, 1, "exactly one trim must be reported");
    assert.equal(seen[0]!.total, 23);
    assert.equal(seen[0]!.kept, 22);
    assert.equal(seen[0]!.dropped.length, 1, "the one dropped name must be reported");
    assert.ok(
      typeof seen[0]!.dropped[0] === "string" && seen[0]!.dropped[0]!.length > 0,
      `the reported name must be the dropped tool, got ${seen[0]!.dropped[0]}`,
    );
  } finally {
    setToolTrimReporter(null);
  }
});

test("a list inside the budget reports nothing", () => {
  const seen: unknown[] = [];
  setToolTrimReporter((info) => seen.push(info));
  try {
    const b = body(["read_file", "terminal", "web_search"]);
    (up.transformRequestBody as any)(b, { id: "muse-spark-1.3-contributor-free" });
    assert.equal(seen.length, 0, "no trim means no report - silence must mean silence");
  } finally {
    setToolTrimReporter(null);
  }
});
