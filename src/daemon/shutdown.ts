/**
 * Close an HTTP server without waiting on connections that will never end.
 *
 * `server.close()` stops accepting new connections and then waits for every open
 * connection to finish on its own. Two kinds of connection never do: a streaming
 * chat (minutes) and an idle keep-alive (the desktop UI polls and leaves the
 * socket open between polls, never closing it by itself). Relying on close()
 * alone meant every `systemctl restart` burned the full 20s TimeoutStopSec and
 * systemd SIGKILLed the daemon — measured 7 times in 7 days, each one truncating
 * whatever stream was live and skipping the activity flush.
 *
 * Grace first, so a request already in flight can finish, then a hard cut. Idle
 * keep-alives are dropped immediately — and swept again on every tick, because
 * closing one only marks it: a connection that was busy at close() time becomes
 * idle a moment later and would otherwise hold the process for the whole grace
 * window (measured: a 100ms request still kept the server open for the full
 * grace period).
 */

// How long an in-flight request may keep the process alive during shutdown.
// systemd's TimeoutStopSec is 20s; staying well under it means systemd never has
// to SIGKILL us, which would cut a stream and skip the final flush.
export const SHUTDOWN_GRACE_MS = 8_000;

const SWEEP_INTERVAL_MS = 250;

export function shutdownServer(
  server: import("node:http").Server,
  graceMs: number,
  onDone: () => void,
): void {
  let done = false;
  let sweep: NodeJS.Timeout | undefined;
  let deadline: NodeJS.Timeout | undefined;

  const finish = () => {
    if (done) return;
    done = true;
    if (sweep) clearInterval(sweep);
    if (deadline) clearTimeout(deadline);
    onDone();
  };

  server.close(finish);
  server.closeIdleConnections();

  sweep = setInterval(() => server.closeIdleConnections(), SWEEP_INTERVAL_MS);
  sweep.unref();

  deadline = setTimeout(() => {
    server.closeAllConnections();
    finish();
  }, graceMs);
  deadline.unref();
}
