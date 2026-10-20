import { test } from "node:test";
import assert from "node:assert/strict";
import { extractReasoningText, renderResponse } from "../src/protocols/responses";

test("extractReasoningText: reads reasoning_content (Zen/DeepSeek shape)", () => {
  const got = extractReasoningText({
    role: "assistant",
    content: "",
    reasoning_content: '{"count":1}',
  });
  assert.equal(got, '{"count":1}');
});

test("extractReasoningText: still prefers the answer marker when present", () => {
  const got = extractReasoningText({ reasoning_content: "thinking...\nAnswer: 42" });
  assert.equal(got, "42");
});

test("extractReasoningText: legacy reasoning field keeps working", () => {
  assert.equal(extractReasoningText({ reasoning: "thinking...\nAnswer: 7" }), "7");
  assert.equal(extractReasoningText({ reasoning: "Answer: 7" }), "Answer: 7");
});

test("extractReasoningText: no reasoning at all yields empty string", () => {
  assert.equal(extractReasoningText({ content: "hi" }), "");
});

test("renderResponse: empty content is rescued from reasoning_content", () => {
  const out: any = renderResponse(
    {
      choices: [
        {
          message: { role: "assistant", content: "", reasoning_content: '{"count":1}' },
          finish_reason: "stop",
        },
      ],
    },
    "deepseek-v4.1-flash-free",
  );
  const text = out?.output?.[0]?.content?.[0]?.text;
  assert.equal(text, '{"count":1}');
});

test("renderResponse: real content wins over reasoning", () => {
  const out: any = renderResponse(
    {
      choices: [
        {
          message: { role: "assistant", content: "the answer", reasoning_content: "thinking" },
          finish_reason: "stop",
        },
      ],
    },
    "m",
  );
  assert.equal(out?.output?.[0]?.content?.[0]?.text, "the answer");
});
