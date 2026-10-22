import test from "node:test";
import assert from "node:assert/strict";
import { Readable, Transform } from "node:stream";
import { logUsageTransform } from "../src/daemon/server";
import type { ActivityStore } from "../src/daemon/activity";
import type { Logger } from "../src/logger";

// Regression guard for the 2026-10-22 turn abort:
// "stream_disconnected: upstream stream ended without finish_reason".
// zen's free pool cut the SSE body mid-generation; the daemon used to pipe the truncation
// through and only append [DONE]. 9router's openai-responses translator then had a stream
// that ended with no verdict, reported response.failed/stream_disconnected, and Hermes
// retried the turn until it gave up. The daemon must close the stream itself.

type Warn = { msg: string; fields: Record<string, unknown> };

function harness() {
  const warns: Warn[] = [];
  const infos: Warn[] = [];
  const log = {
    info: (msg: string, fields?: Record<string, unknown>) => infos.push({ msg, fields: fields ?? {} }),
    warn: (msg: string, fields?: Record<string, unknown>) => warns.push({ msg, fields: fields ?? {} }),
    debug: () => {},
    error: () => {},
    child: () => log,
  } as unknown as Logger;
  const recorded: Array<Record<string, unknown>> = [];
  const activity = { record: (e: Record<string, unknown>) => recorded.push(e) } as unknown as ActivityStore;
  return { log, warns, infos, recorded, activity };
}

async function run(chunks: string[]): Promise<string> {
  const { log, activity } = harness();
  const source = Readable.from(chunks.map((c) => Buffer.from(c, "utf8")));
  const out: Buffer[] = [];
  await new Promise<void>((resolve, reject) => {
    source
      .pipe(logUsageTransform("deepseek-v4.1-flash-free", "zen", log, Date.now(), undefined, activity))
      .pipe(
        new Transform({
          transform(chunk, _enc, cb) {
            out.push(chunk as Buffer);
            cb();
          },
        }),
      )
      .on("finish", resolve)
      .on("error", reject);
  });
  return Buffer.concat(out).toString("utf8");
}

// every frame the client sees must be a parseable `data:` payload, except [DONE]
function frames(body: string): string[] {
  return body
    .split("\n\n")
    .map((f) => f.trim())
    .filter((f) => f.length > 0);
}

function lastVerdict(body: string): string | undefined {
  let verdict: string | undefined;
  for (const frame of frames(body)) {
    const payload = frame.replace(/^data:\s*/, "");
    if (payload === "[DONE]") continue;
    const json = JSON.parse(payload);
    const reason = json?.choices?.[0]?.finish_reason;
    if (reason) verdict = reason;
  }
  return verdict;
}

const chunk = (content: string) =>
  `data: ${JSON.stringify({
    id: "chatcmpl-1",
    object: "chat.completion.chunk",
    created: 1,
    model: "deepseek-v4.1-flash-free",
    choices: [{ index: 0, delta: { content }, finish_reason: null }],
  })}\n\n`;

test("a stream cut before finish_reason closes with finish_reason=length", async () => {
  const body = await run([chunk("partial answer"), chunk(" and then")]);
  assert.equal(lastVerdict(body), "length", "client gets a verdict it can continue from");
  assert.equal(body.split("[DONE]").length - 1, 1, "exactly one [DONE]");
  for (const frame of frames(body)) {
    const payload = frame.replace(/^data:\s*/, "");
    if (payload === "[DONE]") continue;
    assert.doesNotThrow(() => JSON.parse(payload), "no broken frame reaches the client");
  }
});

test("a body cut mid-frame drops the dangling bytes instead of forwarding invalid JSON", async () => {
  const body = await run([
    chunk("hello"),
    'data: {"id":"chatcmpl-1","choices":[{"delta":{"content":"wor',
  ]);
  assert.ok(!body.includes("wor"), "partial frame is dropped");
  assert.equal(lastVerdict(body), "length");
  assert.equal(body.split("[DONE]").length - 1, 1);
  for (const frame of frames(body)) {
    const payload = frame.replace(/^data:\s*/, "");
    if (payload === "[DONE]") continue;
    assert.doesNotThrow(() => JSON.parse(payload));
  }
});

test("usage without a finish frame closes with stop, not length", async () => {
  const body = await run([
    chunk("done answering"),
    `data: ${JSON.stringify({ choices: [], usage: { prompt_tokens: 10, completion_tokens: 4 } })}\n\n`,
  ]);
  assert.equal(lastVerdict(body), "stop", "a generation that reported usage was not cut short");
  assert.equal(body.split("[DONE]").length - 1, 1);
});

test("a complete stream is forwarded byte-exact with a single [DONE]", async () => {
  const finish =
    `data: ${JSON.stringify({
      id: "chatcmpl-1",
      object: "chat.completion.chunk",
      created: 1,
      model: "deepseek-v4.1-flash-free",
      choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
      usage: { prompt_tokens: 9, completion_tokens: 3 },
    })}\n\n`;
  const body = await run([chunk("answer"), finish, "data: [DONE]\n\n"]);
  assert.equal(body, chunk("answer") + finish + "data: [DONE]\n\n", "no synthetic frame, no duplicate [DONE]");
});

test("frames split across chunk boundaries are reassembled", async () => {
  const whole = chunk("split me") + `data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: "stop" }] })}\n\n`;
  const bodies: string[] = [];
  for (let i = 1; i < whole.length; i += 7) bodies.push(whole.slice(i, i + 7));
  const body = await run([whole.slice(0, 1), ...bodies]);
  assert.equal(lastVerdict(body), "stop");
  assert.ok(body.includes('"content":"split me"'));
  assert.equal(body.split("[DONE]").length - 1, 1);
});

test("a finish frame missing its blank separator is salvaged, not dropped", async () => {
  const body = await run([chunk("answer"), `data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: "stop" }] })}\n`]);
  assert.equal(lastVerdict(body), "stop");
  assert.equal(body.split("[DONE]").length - 1, 1, "exactly one [DONE] even when the terminal frame needed salvage");
});
