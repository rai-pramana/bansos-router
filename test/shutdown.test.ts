import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import net from "node:net";
import { shutdownServer } from "../src/daemon/shutdown";

// Shutdown used to rely on server.close() alone, which waits for every open
// connection to end by itself. A streaming chat or an idle UI keep-alive never
// does, so systemd's TimeoutStopSec expired and SIGKILLed the daemon (7 times in
// 7 days, measured) — truncating the live stream and skipping the activity
// flush. These tests pin the three behaviours that fix it.

function listen(server: http.Server): Promise<number> {
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address() as net.AddressInfo;
      resolve(addr.port);
    });
  });
}

function healthServer(handler: http.RequestListener): http.Server {
  return http.createServer(handler);
}

function connect(port: number): Promise<net.Socket> {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host: "127.0.0.1", port }, () => resolve(socket));
    socket.on("error", reject);
  });
}

function waitForClose(socket: net.Socket): Promise<void> {
  return new Promise((resolve) => socket.once("close", () => resolve()));
}

test("an idle keep-alive connection does not delay shutdown", { timeout: 10_000 }, async () => {
  const server = healthServer((_req, res) => res.end("ok"));
  const port = await listen(server);

  // A UI poll leaves the socket open between polls: the server is idle but the
  // connection is alive, and close() alone would wait for it forever.
  const socket = await connect(port);
  socket.on("data", () => {}); // drain, or the request never completes server-side
  socket.write("GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: keep-alive\r\n\r\n");
  await new Promise((r) => setTimeout(r, 100));

  const startedAt = Date.now();
  const done = await new Promise<number>((resolve) => {
    shutdownServer(server, 30_000, () => resolve(Date.now() - startedAt));
  });

  assert.ok(done < 1_000, `idle keep-alive should be dropped at once, took ${done}ms`);
  socket.destroy();
});

test("an in-flight request gets the grace window, then is force-closed", { timeout: 10_000 }, async () => {
  let release!: () => void;
  const gate = new Promise<void>((r) => (release = r));
  const server = healthServer(async (_req, res) => {
    // a stream that never finishes on its own
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write("data: partial\n\n");
    await gate;
    res.end();
  });
  const port = await listen(server);

  const socket = await connect(port);
  socket.write("GET /stream HTTP/1.1\r\nHost: x\r\n\r\n");
  await new Promise((r) => setTimeout(r, 150));

  const startedAt = Date.now();
  const elapsed = await new Promise<number>((resolve) => {
    shutdownServer(server, 300, () => resolve(Date.now() - startedAt));
  });

  // It waits for the grace window rather than hanging forever...
  assert.ok(elapsed >= 250, `should honour the grace window, returned in ${elapsed}ms`);
  assert.ok(elapsed < 2_000, `should not overshoot the grace window, took ${elapsed}ms`);
  socket.destroy();
  release();
});

test("a request that finishes inside the grace window closes the server early", async () => {
  const server = healthServer((_req, res) => {
    setTimeout(() => res.end("late but fine"), 100);
  });
  const port = await listen(server);

  const socket = await connect(port);
  socket.write("GET /slow HTTP/1.1\r\nHost: x\r\n\r\n");
  await new Promise((r) => setTimeout(r, 20));

  const startedAt = Date.now();
  const elapsed = await new Promise<number>((resolve) => {
    shutdownServer(server, 5_000, () => resolve(Date.now() - startedAt));
  });

  assert.ok(elapsed < 1_000, `should close as soon as the request drains, took ${elapsed}ms`);
  socket.destroy();
});

test("onDone fires exactly once even when both paths race", async () => {
  const server = healthServer((_req, res) => res.end("ok"));
  const port = await listen(server);

  let calls = 0;
  await new Promise<void>((resolve) => {
    shutdownServer(server, 50, () => {
      calls += 1;
      resolve();
    });
  });
  await new Promise((r) => setTimeout(r, 200));
  assert.equal(calls, 1, "the completion callback must not run twice");
  void port;
});
