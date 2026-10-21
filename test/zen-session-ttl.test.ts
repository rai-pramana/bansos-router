import test from "node:test";
import assert from "node:assert/strict";

// zen meters its free tier per opencode session id, so a session pinned for the
// whole daemon life is what turns a spent window into fleet-wide 429s. The TTL
// is read at module load, and node's test runner gives each file its own process,
// so setting it here does not leak into the other zen tests.
process.env.OPENCODE_SESSION_TTL_MS = "30";

const { zenUpstream, ZEN_MODELS } = await import("../src/upstreams/zen");

const sessionOf = () => zenUpstream.requestHeaders(ZEN_MODELS[0]!)["x-opencode-session"];
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

test("session id is stable inside the TTL window", () => {
  assert.equal(sessionOf(), sessionOf());
});

test("session id rotates once the TTL lapses", async () => {
  const before = sessionOf();
  await sleep(50);
  const after = sessionOf();

  assert.notEqual(after, before, "a lapsed window must get a fresh session");
  assert.match(after ?? "", /^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$/);
});

test("affinity and id headers follow the rotated session", async () => {
  const headers = zenUpstream.requestHeaders(ZEN_MODELS[0]!);
  assert.equal(headers["x-opencode-session-id"], headers["x-opencode-session"]);
  assert.equal(headers["x-session-affinity"], headers["x-opencode-session"]);
});
