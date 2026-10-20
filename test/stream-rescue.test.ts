import { test } from "node:test";
import assert from "node:assert/strict";
import { streamToChatResponse } from "../src/daemon/server";

function sseResponse(frames: object[]): Response {
  const body = frames.map((f) => `data: ${JSON.stringify(f)}\n\n`).join("") + "data: [DONE]\n\n";
  return new Response(body, { status: 200, headers: { "content-type": "text/event-stream" } });
}

const envelope = { id: "chatcmpl-x", created: 1, model: "deepseek-v4.1-flash-free" };

test("streamToChatResponse: reasoning-only upstream is rescued into content", async () => {
  const upstream = sseResponse([
    { ...envelope, choices: [{ index: 0, delta: { role: "assistant", content: null, reasoning_content: "" }, finish_reason: null }] },
    { ...envelope, choices: [{ index: 0, delta: { reasoning_content: '{"count":' }, finish_reason: null }] },
    { ...envelope, choices: [{ index: 0, delta: { reasoning_content: "1}" }, finish_reason: null }] },
    { ...envelope, choices: [{ index: 0, delta: { content: "" }, finish_reason: "stop" }] },
  ]);
  const res = await streamToChatResponse(upstream, "deepseek-v4.1-flash-free");
  const json: any = await res.json();
  const msg = json.choices[0].message;
  assert.equal(msg.content, '{"count":1}');
  assert.equal(msg.reasoning, undefined);
  assert.equal(json.choices[0].finish_reason, "stop");
});

test("streamToChatResponse: real content is never overwritten by reasoning", async () => {
  const upstream = sseResponse([
    { ...envelope, choices: [{ index: 0, delta: { reasoning_content: "thinking hard" }, finish_reason: null }] },
    { ...envelope, choices: [{ index: 0, delta: { content: "the answer" }, finish_reason: null }] },
    { ...envelope, choices: [{ index: 0, delta: {}, finish_reason: "stop" }] },
  ]);
  const res = await streamToChatResponse(upstream, "m");
  const json: any = await res.json();
  assert.equal(json.choices[0].message.content, "the answer");
  assert.equal(json.choices[0].message.reasoning, "thinking hard");
});

test("streamToChatResponse: tool calls suppress the reasoning rescue", async () => {
  const upstream = sseResponse([
    { ...envelope, choices: [{ index: 0, delta: { reasoning_content: "I should read the file" }, finish_reason: null }] },
    {
      ...envelope,
      choices: [
        {
          index: 0,
          delta: { tool_calls: [{ index: 0, id: "call_1", type: "function", function: { name: "read_file", arguments: '{"path":"a"}' } }] },
          finish_reason: null,
        },
      ],
    },
    { ...envelope, choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }] },
  ]);
  const res = await streamToChatResponse(upstream, "m");
  const json: any = await res.json();
  const msg = json.choices[0].message;
  assert.equal(msg.content, "");
  const kept = msg.reasoning_content ?? msg.reasoning ?? "";
  assert.ok(kept.length > 0, "reasoning kept when tool calls exist");
  assert.equal(json.choices[0].finish_reason, "tool_calls");
});
