import http from "node:http";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Readable, Transform } from "node:stream";
import type { Logger } from "../logger";
import {
  DEFAULT_SECURITY_CONFIG,
  isCrossProviderFailoverAllowed,
  isRelayAllowed,
  isLoopbackBind,
  isSensitiveNetworkHost,
  isStrictSecurity,
  isUpstreamAllowed,
  type SecurityConfig,
} from "../security/policy";
import { redactRequestBody, scanRequestBody, type SecretType } from "../security/secret-guard";
import { ActivityStore, type ActivityKind } from "./activity";
import {
  noteZenQuotaExhausted,
  noteZenQuotaExhaustedForModel,
  noteZenQuotaRecovered,
  zenQuotaClosedForModel,
  zenQuotaClosedForMs,
} from "./zen-gate";
import type { ModelDef, Upstream } from "../upstreams/types";
import { pickSmartDefaultModel } from "../upstreams/types";
import { parseChatTurn, sanitizeChatBody } from "../protocols/openai-chat";
import { parseResponsesTurn, renderResponse, extractReasoningText, ResponsesStreamEncoder } from "../protocols/responses";
import { chatToResponsesBody, toChatResponse } from "../protocols/responses-upstream";
import {
  parseAnthropicRequest,
  openAiCompletionToAnthropicMessage,
  AnthropicStreamEncoder,
} from "../protocols/anthropic";
import {
  loadRelayState,
  saveRelayState,
  addRelay,
  removeRelay,
  relayFetch,
  relayRetryState,
  deploymentEssentiallySpent,
  FLEET_EXHAUSTED_HEADER,
  type RelayState,
} from "../relay/egress";
import { ADAPTERS, buildSetupContext, findAdapter } from "../adapters";
import { harnessStatuses } from "../cli/doctor";
import { ADMIN_PATHS, handleAdmin, type AdminOptions } from "./admin";
import { createSseFrameSplitter, readSseStream } from "../protocols/stream";
import { callerToolsFromBody, type CallerTools, type Translator } from "../protocols/tool-bridge";
import type { RuntimeCatalog } from "./catalog";
import type { RateLimiter } from "./rate-limit";
import {
  ZEN_MIRROR_REFUSAL,
  type ZenMirrorHandle,
} from "../upstreams/zen-mirror";
import type { ZenStageHandle } from "../upstreams/zen-stage";

let activity: ActivityStore = new ActivityStore();

// The zen mirror: when opencode.ai's daily free pool is spent, the SAME zen
// model id can be answered by another free upstream that still has quota.
// It is a transport, not a catalog source, so it lives here rather than in
// the upstream registry.
let zenMirror: ZenMirrorHandle | null = null;

// The zen STAGE deployment: opencode's own console, running against its own
// database, so it has its own daily free-tier bucket. It serves the SAME
// model ids production serves - only the transport changes - which keeps the
// zen lane pure: a zen id is still answered by opencode, never by a
// different provider under that id.
//
// It is the answer to a closed production bucket: the production bucket is
// keyed on the literal "unknown" (the edge does not forward x-real-ip), so it
// is shared with every anonymous OpenCode user on the internet and no egress
// identity escapes it. The stage's bucket is separate and still has room.
let zenStage: ZenStageHandle | null = null;

export function setZenMirror(handle: ZenMirrorHandle | null): void {
  zenMirror = handle;
}

export function setZenStage(handle: ZenStageHandle | null): void {
  zenStage = handle;
}

export interface StatusPayload {
  status: "ok";
  uptimeSeconds: number;
  port: number;
  modelCount: number;
  models: string[];
  providers: Array<{ id: string; models: number; allowed: boolean }>;
  relay?: { enabled: boolean; url: string };
  // zen model id -> the free upstream ids that can serve it when opencode.ai's
  // daily pool is spent
  zenMirror?: Record<string, string[]>;
  // the zen stage deployment: opencode's own console on a separate
  // deployment, with its own daily free-tier bucket
  zenStage?: { upstream: string; models: string[] };
  security?: {
    mode: "normal" | "strict";
    dlp: "redact" | "block";
    allowedUpstreams: string[];
    allowCrossProviderFailover: boolean;
  };
}

function providerCounts(
  catalog: RuntimeCatalog,
  security: SecurityConfig,
): Array<{ id: string; models: number; allowed: boolean }> {
  const counts = new Map<string, number>();
  for (const m of catalog.models) counts.set(m.source, (counts.get(m.source) ?? 0) + 1);
  return [...counts].map(([id, models]) => ({ id, models, allowed: isUpstreamAllowed(security, id) }));
}

export interface ServerOptions {
  catalog: RuntimeCatalog;
  rateLimiter: RateLimiter;
  port: number;
  log: Logger;
  startedAt: number;
  security?: SecurityConfig;
  activity?: ActivityStore;
  admin?: Partial<Omit<AdminOptions, "security" | "catalog" | "port">>;
}

const ALLOWED_METHODS = new Set(["GET", "POST", "OPTIONS"]);

export { pickSmartDefaultModel };

// reject a malformed relay url here, not at use time (it is fetched on every egress)
function applyRelayMutation(
  initialState: RelayState,
  body: Record<string, unknown>,
): RelayState | { error: string } {
  let current: RelayState = {
    ...initialState,
    relays: initialState.relays.map((r) => ({ ...r })),
  };

  if (Array.isArray(body.relays)) {
    const relays: RelayState["relays"] = [];
    for (const entry of body.relays) {
      if (!entry || typeof entry !== "object") return { error: "relays must be an array of objects" };
      const rec = entry as Record<string, unknown>;
      if (!isValidRelayUrl(rec.url)) return { error: "invalid relay url in relays list" };
      relays.push({
        url: (rec.url as string).trim(),
        ...(typeof rec.label === "string" && rec.label.trim() ? { label: rec.label.trim().slice(0, 100) } : {}),
        ...(typeof rec.addedAt === "string" ? { addedAt: rec.addedAt } : {}),
      });
    }
    current.relays = relays;
  }

  if (typeof body.enabled === "boolean") {
    current.enabled = body.enabled;
  }
  const label = typeof body.label === "string" ? body.label.trim().slice(0, 100) || undefined : undefined;
  const hasUrl = typeof body.url === "string" && body.url.trim() !== "";
  if (hasUrl && !isValidRelayUrl(body.url)) return { error: "invalid relay url" };

  if (hasUrl) {
    const cleanUrl = (body.url as string).trim();
    if (!body.action) {
      current.url = cleanUrl;
      if (!current.relays.some((r: import("../relay/egress").KnownRelay) => r.url === cleanUrl)) {
        current = addRelay(current, cleanUrl, label);
      }
    } else if (body.action === "add") {
      current = addRelay(current, cleanUrl, label);
    } else if (body.action === "remove") {
      current = removeRelay(current, cleanUrl);
    }
  }
  return current;
}
const STATIC_ROOT_FILES = new Set([
  "/",
  "/index.html",
  "/favicon.ico",
  "/favicon.svg",
  "/favicon.png",
  "/apple-touch-icon.png",
  "/manifest.json",
  "/robots.txt",
]);

const API_EXACT_PATHS = new Set([
  "/healthz",
  "/healthz/",
  "/bansos/status",
  "/bansos/status/",
  "/bansos/refresh",
  "/bansos/refresh/",
  "/bansos/adapters",
  "/bansos/adapters/",
  "/bansos/adapters/render",
  "/bansos/adapters/render/",
  "/bansos/relay",
  "/bansos/relay/",
  "/bansos/relay/probe",
  "/bansos/relay/probe/",
  "/bansos/usage",
  "/bansos/usage/",
  "/bansos/events",
  "/bansos/events/",
  ...[...ADMIN_PATHS].flatMap((p) => [p, `${p}/`]),
  "/chat/completions",
  "/chat/completions/",
  "/messages",
  "/messages/",
  "/responses",
  "/responses/",
  "/models",
  "/models/",
  "/v1/chat/completions",
  "/v1/chat/completions/",
  "/v1/messages",
  "/v1/messages/",
  "/v1/responses",
  "/v1/responses/",
  "/v1/models",
  "/v1/models/",
]);

function isAllowedInboundPath(pathname: string): boolean {
  if (STATIC_ROOT_FILES.has(pathname) || API_EXACT_PATHS.has(pathname)) {
    return true;
  }
  return pathname.startsWith("/assets/") && /^[\w.-]+$/.test(pathname.slice(8));
}

// fallback models to try after the primary is rejected; total attempts = 1 + this
const MAX_FAILOVER_RETRIES = 2;

// only loopback origins get CORS; a wildcard would let any website drive the daemon
function corsHeadersForOrigin(originHeader: string | undefined): Record<string, string> {
  if (!originHeader) return {};
  let hostname = "";
  try {
    hostname = new URL(originHeader).hostname;
  } catch {
    return {};
  }
  if (!isLoopbackBind(hostname)) return {};
  return {
    "access-control-allow-origin": originHeader,
    "access-control-allow-methods": "GET, POST, OPTIONS",
    "access-control-allow-headers": "*",
    "access-control-max-age": "86400",
    vary: "Origin",
  };
}

interface CorsAwareResponse extends http.ServerResponse {
  // per-request CORS headers, attached at the top of the connection handler
  bansosCors?: Record<string, string>;
}

function corsFor(res: http.ServerResponse): Record<string, string> {
  return (res as CorsAwareResponse).bansosCors ?? {};
}

// DNS-rebinding guard: loopback clients must present a loopback Host
let ownAddresses: Set<string> | null = null;

function isOwnAddress(hostname: string): boolean {
  if (ownAddresses === null) {
    const set = new Set<string>();
    for (const ifaces of Object.values(os.networkInterfaces())) {
      for (const iface of ifaces ?? []) set.add(iface.address.toLowerCase());
    }
    ownAddresses = set;
  }
  return ownAddresses.has(hostname.toLowerCase());
}

function isHostTrusted(req: http.IncomingMessage): boolean {
  const host = req.headers.host;
  if (!host) return false;
  let hostname = "";
  try {
    hostname = new URL(`http://${host}`).hostname;
  } catch {
    return false;
  }
  if (isLoopbackBind(hostname)) return true;
  const peer = req.socket.remoteAddress ?? "";
  if (isLoopbackBind(peer)) return isOwnAddress(hostname);
  return true;
}

// CSRF guard: browsers always send Origin cross-site; native clients send none
function isOriginTrusted(req: http.IncomingMessage): boolean {
  const origin = req.headers.origin;
  if (origin === undefined) return true;
  let url: URL;
  try {
    url = new URL(origin);
  } catch {
    return false;
  }
  if (isLoopbackBind(url.hostname)) return true;
  return url.host === req.headers.host;
}

// validates a user-supplied relay URL before it is stored or probed
function isValidRelayUrl(value: unknown): value is string {
  if (typeof value !== "string") return false;
  const trimmed = value.trim();
  if (trimmed.length === 0 || trimmed.length > 2048) return false;
  let u: URL;
  try {
    u = new URL(trimmed);
  } catch {
    return false;
  }
  if (u.protocol !== "http:" && u.protocol !== "https:") return false;
  if (u.username || u.password) return false;
  return u.hostname.length > 0;
}

// The origin of an upstream URL, or "" when the URL cannot be parsed.
//
// The relay layer keys its spent-evidence by the deployment's origin, so the
// daemon has to name that origin exactly. A malformed URL must not throw
// here: the request path would die before it could answer.
function safeOrigin(url: string): string {
  try {
    return new URL(url).origin;
  } catch {
    return "";
  }
}

const MIME_TYPES: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".ico": "image/x-icon",
  ".woff2": "font/woff2",
  ".woff": "font/woff",
  ".ttf": "font/ttf",
};

function getUiDistDir(): string {
  try {
    const currentFile = fileURLToPath(import.meta.url);
    const currentDir = path.dirname(currentFile);
    const candidate1 = path.resolve(currentDir, "../../dist/ui");
    if (fs.existsSync(candidate1)) return candidate1;
    const candidate2 = path.resolve(currentDir, "../ui");
    if (fs.existsSync(candidate2)) return candidate2;
  } catch {
    // resolution can throw on odd import.meta.url shapes; cwd still works
  }
  const candidate3 = path.resolve(process.cwd(), "dist/ui");
  if (fs.existsSync(candidate3)) return candidate3;
  return path.resolve(process.cwd(), "dist/ui");
}

// the SPA is self-contained, so a strict CSP costs nothing here (C4)
function securityHeaders(): Record<string, string> {
  return {
    "x-content-type-options": "nosniff",
    "x-frame-options": "DENY",
    "referrer-policy": "no-referrer",
    "content-security-policy":
      "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
  };
}

function serveStaticUi(res: http.ServerResponse, reqPath: string): void {
  const uiDir = getUiDistDir();
  let relativePath = reqPath.replace(/^\/+/, "");
  if (!relativePath || relativePath === "index.html") {
    relativePath = "index.html";
  }

  const filePath = path.join(uiDir, relativePath);

  // path traversal guard: a crafted url must not escape uiDir
  if (!filePath.startsWith(uiDir)) {
    sendJson(res, 403, { error: { message: "forbidden" } });
    return;
  }

  if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
    const ext = path.extname(filePath).toLowerCase();
    const contentType = MIME_TYPES[ext] ?? "application/octet-stream";
    res.writeHead(200, {
      "content-type": contentType,
      ...securityHeaders(),
      ...corsFor(res),
    });
    const rs = fs.createReadStream(filePath);
    rs.on("error", () => res.destroy());
    rs.pipe(res);
    return;
  }

  if (relativePath.startsWith("assets/")) {
    sendJson(res, 404, { error: { message: "asset not found" } });
    return;
  }

  // any other path is an SPA route, so hand back index.html
  const indexPath = path.join(uiDir, "index.html");
  if (fs.existsSync(indexPath) && fs.statSync(indexPath).isFile()) {
    res.writeHead(200, {
      "content-type": "text/html; charset=utf-8",
      ...securityHeaders(),
      ...corsFor(res),
    });
    const rs = fs.createReadStream(indexPath);
    rs.on("error", () => res.destroy());
    rs.pipe(res);
    return;
  }

  // no build present: say so instead of 404ing, the daemon API still works
  const fallbackHtml = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>bansos-router</title>
</head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;background:#111113;color:#f4f4f6;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0;padding:1.5rem;box-sizing:border-box;">
  <div style="max-width:460px;width:100%;text-align:center;background:#16161a;border:1px solid #23232a;border-radius:1rem;padding:2.5rem 2rem;box-shadow:0 20px 25px -5px rgba(0,0,0,0.5);">
    <div style="display:inline-flex;align-items:center;justify-content:center;width:3rem;height:3rem;border-radius:0.75rem;background:#202028;border:1px solid #2c2c36;margin-bottom:1.25rem;">
      <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#3b82f6" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"></polygon></svg>
    </div>
    <h1 style="font-size:1.25rem;font-weight:700;margin:0 0 0.5rem 0;color:#ffffff;letter-spacing:-0.025em;">bansos-router daemon is online</h1>
    <p style="font-size:0.875rem;color:#9393a0;margin:0 0 1.5rem 0;line-height:1.5;">Web UI bundle is not built yet. Run <code style="background:#202028;border:1px solid #2c2c36;padding:0.2rem 0.4rem;border-radius:0.375rem;color:#60a5fa;font-family:monospace;font-size:0.8125rem;">npm run build</code> to compile the dashboard.</p>
    <div style="font-size:0.75rem;color:#71717a;border-top:1px solid #23232a;padding-top:1rem;line-height:1.6;">
      API live at <span style="font-family:monospace;color:#a1a1aa;">/v1/chat/completions</span> & <span style="font-family:monospace;color:#a1a1aa;">/v1/models</span>
    </div>
  </div>
</body>
</html>`;
  res.writeHead(200, {
    "content-type": "text/html; charset=utf-8",
    "content-length": Buffer.byteLength(fallbackHtml),
    ...securityHeaders(),
    ...corsFor(res),
  });
  res.end(fallbackHtml);
}

function validatePath(rawUrl: string): boolean {
  const pathname = rawUrl.split("?")[0] ?? "/";
  const cleaned = pathname.replace(/^\/+/, "");
  const withSlash = `/${cleaned}`;
  if (withSlash.includes("..")) return false;
  try {
    const decoded = decodeURIComponent(withSlash);
    if (decoded !== withSlash) return false; // encoded variants not accepted (v1)
  } catch {
    return false;
  }
  return true;
}

// limit relay targets to the active/saved relay or a public address, to block SSRF probing
function probeTargetAllowed(
  targetUrl: string,
  state: RelayState,
): { allowed: boolean; reason?: string } {
  const trimmed = targetUrl.trim();
  let u: URL;
  try {
    u = new URL(trimmed);
  } catch {
    return { allowed: false, reason: "invalid url" };
  }
  if (u.protocol !== "http:" && u.protocol !== "https:") {
    return { allowed: false, reason: "only http(s) targets can be probed" };
  }
  if (u.username || u.password) {
    return { allowed: false, reason: "url must not contain credentials" };
  }
  if (state.url === trimmed || state.relays.some((r) => r.url === trimmed)) {
    return { allowed: true };
  }
  if (isSensitiveNetworkHost(u.hostname)) {
    return { allowed: false, reason: "target resolves to a loopback/private network address" };
  }
  return { allowed: true };
}

// guard the daemon from an unhandled rejection/stream error: send 502 if unwritten, else close
function runRequest(
  promise: Promise<void>,
  res: http.ServerResponse,
  log: Logger,
  label: string,
): void {
  res.on("error", () => {
    // client vanished mid-response; nothing left to send
  });
  promise.catch((err: unknown) => {
    log.warn(`${label}: request failed`, {
      status: 502,
      upstreamError: String((err as Error)?.message || err).slice(0, 300),
      from: String((err as Error)?.stack || "")
        .split("\n")
        .slice(1, 3)
        .join(" | ")
        .slice(0, 200),
    });
    try {
      if (res.headersSent) res.end();
      else {
        sendJson(res, 502, {
          error: { message: "upstream request failed", type: "upstream_error", status: 502 },
        });
      }
    } catch {
      // connection already gone
    }
  });
}

function clientDisconnectSignal(res: http.ServerResponse): AbortSignal {
  const controller = new AbortController();
  res.on("close", () => {
    if (!res.writableFinished) controller.abort();
  });
  return controller.signal;
}

function sendJson(
  res: http.ServerResponse,
  status: number,
  body: unknown,
  extraHeaders: Record<string, string> = {},
): void {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    "content-type": "application/json",
    "content-length": Buffer.byteLength(payload),
    ...extraHeaders,
    ...corsFor(res),
  });
  res.end(payload);
}

async function readBody(
  req: http.IncomingMessage,
  cap = 10 * 1024 * 1024,
): Promise<string> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    const buf = chunk as Buffer;
    size += buf.length;
    if (size > cap) throw new Error("request body too large");
    chunks.push(buf);
  }
  return Buffer.concat(chunks).toString("utf8");
}

// openai chat in -> resolve model -> forward raw body -> stream the reply back unchanged

interface TokenUsage {
  inputTokens: number;
  outputTokens: number;
}

export function extractUsage(json: unknown): TokenUsage | null {
  const usage = (json as { usage?: { prompt_tokens?: number; completion_tokens?: number } })
    ?.usage;
  if (!usage || usage.prompt_tokens == null || usage.completion_tokens == null) return null;
  return { inputTokens: usage.prompt_tokens, outputTokens: usage.completion_tokens };
}

// fold reasoning into content when a model streams reasoning_content but no content (C5)
type ChatFoldState = { sawContent: boolean; sawToolCalls: boolean; reasoning: string; envelope: Record<string, unknown> };

function inspectChatFrame(frame: string, state: ChatFoldState): { finished: boolean } {
  let finished = false;
  for (const line of frame.split("\n")) {
    if (!line.startsWith("data:")) continue;
    const payload = line.slice(5).trim();
    if (payload === "[DONE]") continue;
    let json: any;
    try {
      json = JSON.parse(payload);
    } catch {
      continue;
    }
    if (json?.id || json?.model) {
      state.envelope = { id: json.id, model: json.model, created: json.created };
    }
    const choice = json?.choices?.[0];
    if (choice?.finish_reason) finished = true;
    const delta = choice?.delta;
    if (!delta || typeof delta !== "object") continue;
    if (Array.isArray(delta.tool_calls) && delta.tool_calls.length > 0) state.sawToolCalls = true;
    if (typeof delta.content === "string" && delta.content.length > 0) {
      state.sawContent = true;
      continue;
    }
    const reasoning =
      typeof delta.reasoning_content === "string" && delta.reasoning_content.length > 0
        ? delta.reasoning_content
        : typeof delta.reasoning === "string" && delta.reasoning.length > 0
          ? delta.reasoning
          : "";
    state.reasoning += reasoning;
  }
  return { finished };
}

function foldedContentFrame(state: { reasoning: string; envelope: Record<string, unknown> }): string {
  return `data: ${JSON.stringify({
    id: state.envelope.id ?? "chatcmpl-reasoning-fold",
    object: "chat.completion.chunk",
    created: state.envelope.created ?? Math.floor(Date.now() / 1000),
    model: state.envelope.model ?? "",
    choices: [{ index: 0, delta: { role: "assistant", content: state.reasoning }, finish_reason: null }],
  })}\n\n`;
}

// Terminal chunk for a stream whose upstream never sent finish_reason. Clients need a verdict
// to close their translation; without one 9router reports stream_disconnected and Hermes drops
// the turn. "length" is the verdict that means "cut short, continue" (9router maps it to
// response.incomplete/max_output_tokens); "stop" is used when usage already arrived.
function terminalChatFrame(state: ChatFoldState, fallbackModel: string, reason: string): string {
  return `data: ${JSON.stringify({
    id: state.envelope.id ?? "chatcmpl-synthetic-finish",
    object: "chat.completion.chunk",
    created: state.envelope.created ?? Math.floor(Date.now() / 1000),
    model: state.envelope.model ?? fallbackModel,
    choices: [{ index: 0, delta: {}, finish_reason: reason }],
  })}\n\n`;
}

// A body can end right after a complete `data:` line but before its blank separator. Keep it
// when it still parses, drop it when it was cut mid-JSON.
function salvageSseFrame(rest: string): string | undefined {
  const lines = rest.split("\n").filter((line) => line.trim().length > 0);
  if (lines.length === 0) return undefined;
  for (const line of lines) {
    if (!line.startsWith("data:")) return undefined;
    const payload = line.slice(5).trim();
    if (payload === "[DONE]") continue;
    try {
      JSON.parse(payload);
    } catch {
      return undefined;
    }
  }
  return `${lines.join("\n")}\n\n`;
}

// stream transform: reassemble split SSE frames, fold reasoning-only deltas, pass the rest
export function declaredToolNames(body: unknown): Set<string> {
  const tools = (body as Record<string, unknown> | null)?.tools;
  return new Set(
    (Array.isArray(tools) ? tools : [])
      .map((t: any) => t?.function?.name ?? t?.name)
      .filter((n: unknown): n is string => typeof n === "string"),
  );
}

export function filterInjectedToolCallsTransform(callerTools: CallerTools): Transform {
  const verdict = new Map<number, boolean>();
  const bridged = new Map<number, { id?: string; args: string; translate: Translator }>();
  let forwarded = false;
  let envelope: Record<string, unknown> = {};

  const flushBridged = (): string => {
    const toolCalls: unknown[] = [];
    for (const [index, b] of bridged) {
      const id = b.id ?? `call_${index}_${Date.now().toString(36)}`;
      const call = b.translate(b.args, id);
      if (call) toolCalls.push({ index, id, type: "function", function: call });
    }
    bridged.clear();
    if (toolCalls.length === 0) return "";
    forwarded = true;
    return `data: ${JSON.stringify({
      id: envelope.id ?? "chatcmpl-bridge",
      object: "chat.completion.chunk",
      created: envelope.created ?? Math.floor(Date.now() / 1000),
      model: envelope.model ?? "",
      choices: [{ index: 0, delta: { tool_calls: toolCalls }, finish_reason: null }],
    })}\n\n`;
  };

  const rewriteFrame = (frame: string): string => {
    const line = frame.trimStart();
    if (!line.startsWith("data:")) return frame;
    const payload = line.slice(5).trim();
    if (payload === "[DONE]") return flushBridged() + frame;
    if (payload === "") return frame;

    let json: any;
    try {
      json = JSON.parse(payload);
    } catch {
      return frame;
    }
    if (json?.id || json?.model) envelope = { id: json.id, model: json.model, created: json.created };

    const choice = json?.choices?.[0];
    if (!choice) return frame;
    let changed = false;

    const calls = choice.delta?.tool_calls;
    if (Array.isArray(calls)) {
      const kept = calls.filter((tc: any) => {
        const name = tc?.function?.name;
        if (typeof name === "string" && !verdict.has(tc.index)) {
          const own = callerTools.has(name);
          verdict.set(tc.index, own);
          const translate = own ? null : callerTools.bridge?.(name);
          if (translate) bridged.set(tc.index, { id: tc.id, args: "", translate });
        }
        const b = bridged.get(tc.index);
        if (b && typeof tc?.function?.arguments === "string") b.args += tc.function.arguments;
        return verdict.get(tc.index) ?? true;
      });
      if (kept.length > 0) forwarded = true;
      if (kept.length !== calls.length) {
        changed = true;
        if (kept.length === 0) delete choice.delta.tool_calls;
        else choice.delta.tool_calls = kept;
      }
    }

    let prefix = "";
    if (choice.finish_reason) {
      prefix = flushBridged();
      if (prefix && choice.finish_reason !== "tool_calls") {
        choice.finish_reason = "tool_calls";
        changed = true;
      }
    }
    if (choice.finish_reason === "tool_calls" && !forwarded) {
      choice.finish_reason = "stop";
      changed = true;
    }

    return prefix + (changed ? `data: ${JSON.stringify(json)}\n\n` : frame);
  };

  const splitter = createSseFrameSplitter();
  return new Transform({
    transform(chunk, _enc, cb) {
      for (const frame of splitter.push(chunk)) this.push(Buffer.from(rewriteFrame(frame), "utf8"));
      cb();
    },
    flush(cb) {
      const rest = splitter.rest();
      if (rest) this.push(Buffer.from(rewriteFrame(rest), "utf8"));
      const tail = flushBridged();
      if (tail) this.push(Buffer.from(tail, "utf8"));
      cb();
    },
  });
}

export function reasoningToContentTransform(): Transform {
  const splitter = createSseFrameSplitter();
  let folded = false;
  const state: ChatFoldState = { sawContent: false, sawToolCalls: false, reasoning: "", envelope: {} };

  // promote reasoning to the answer only when nothing else came, and before the finish frame
  const foldIfNothingElse = (push: (frame: string) => void) => {
    if (folded) return;
    folded = true;
    if (!state.sawContent && !state.sawToolCalls && state.reasoning.length > 0) push(foldedContentFrame(state));
  };

  return new Transform({
    transform(chunk, _enc, cb) {
      for (const frame of splitter.push(chunk)) {
        if (/^data: ?\[DONE\]/.test(frame) || inspectChatFrame(frame, state).finished) {
          foldIfNothingElse((f) => this.push(Buffer.from(f, "utf8")));
        }
        this.push(Buffer.from(frame, "utf8"));
      }
      cb();
    },
    flush(cb) {
      const rest = splitter.rest();
      if (rest) {
        if (inspectChatFrame(rest, state).finished) foldIfNothingElse((f) => this.push(Buffer.from(f, "utf8")));
        this.push(Buffer.from(rest, "utf8"));
      }
      foldIfNothingElse((f) => this.push(Buffer.from(f, "utf8")));
      cb();
    },
  });
}

// Pass-through that logs the usage object once and guarantees a well-formed terminal frame.
//
// Upstreams — zen's free pool above all — sometimes close the SSE body without ever sending
// finish_reason. A raw pipe then hands the client a stream that ended with no verdict, and
// strict translators (9router's openai-responses) turn that into
// response.failed/stream_disconnected: Hermes retries the whole turn and eventually aborts it
// with "stream_disconnected: upstream stream ended without finish_reason". Re-frame instead:
// drop a half-written trailing frame and close with finish_reason=length, the one verdict every
// client treats as "truncated, continue". Usage present means the generation actually finished
// and only its terminal chunk was lost, so that case closes with stop.
export function logUsageTransform(
  model: string,
  upstream: string,
  log: Logger,
  startedAt: number,
  requestedModel?: string,
  activity?: ActivityStore,
  kind: ActivityKind = "chat",
  relay = "",
): Transform {
  const splitter = createSseFrameSplitter();
  const state: ChatFoldState = { sawContent: false, sawToolCalls: false, reasoning: "", envelope: {} };
  let usage: TokenUsage | null = null;
  let reported = false;
  let finished = false;
  let done = false;

  const reportUsage = () => {
    if (reported || !usage) return;
    reported = true;
    log.info("chat done", { model, upstream, durationMs: Date.now() - startedAt, ...usage });
    activity?.record({
      kind,
      model,
      requestedModel: requestedModel ?? model,
      upstream,
      ...(relay ? { relay } : {}),
      inputTokens: usage.inputTokens,
      outputTokens: usage.outputTokens,
      durationMs: Date.now() - startedAt,
      status: "ok",
      ...(requestedModel && requestedModel !== model ? { failoverFrom: requestedModel } : {}),
    });
  };

  // read verdict + usage off one complete frame; [DONE] is tracked but still forwarded verbatim
  const absorb = (frame: string) => {
    if (inspectChatFrame(frame, state).finished) finished = true;
    if (/^data: ?\[DONE\]/.test(frame)) {
      done = true;
      return;
    }
    if (usage) return;
    for (const line of frame.split("\n")) {
      if (!line.startsWith("data:")) continue;
      const payload = line.slice(5).trim();
      if (!payload || payload === "[DONE]") continue;
      try {
        usage = extractUsage(JSON.parse(payload)) ?? usage;
      } catch {
        // keep the frame untouched; a non-JSON data line is not ours to judge
      }
    }
  };

  return new Transform({
    transform(chunk, _enc, cb) {
      for (const frame of splitter.push(chunk)) {
        absorb(frame);
        if (usage) reportUsage();
        this.push(Buffer.from(frame, "utf8"));
      }
      cb();
    },
    flush(cb) {
      // A frame cut mid-write is not a frame: forwarding it hands the client invalid JSON.
      const rest = splitter.rest();
      const salvaged = rest ? salvageSseFrame(rest) : undefined;
      if (salvaged) {
        absorb(salvaged);
        if (usage) reportUsage();
        if (!done) this.push(Buffer.from(salvaged, "utf8"));
      } else if (rest.trim()) {
        log.warn("dropped partial sse frame", {
          model,
          upstream,
          bytes: Buffer.byteLength(rest),
        });
      }
      if (!finished) {
        const reason = usage ? "stop" : "length";
        log.warn("chat stream ended without finish_reason", {
          model,
          upstream,
          durationMs: Date.now() - startedAt,
          synthesised: reason,
          ...(relay ? { relay } : {}),
        });
        this.push(terminalChatFrame(state, model, reason));
      }
      // strict SSE parsers hang without a terminating [DONE]
      if (!done) this.push("data: [DONE]\n\n");
      cb();
    },
  });
}

export function parseRetryAfterMs(header: string | null, now = Date.now()): number | undefined {
  if (!header) return undefined;
  const seconds = Number(header.trim());
  if (Number.isFinite(seconds)) return seconds > 0 ? seconds * 1000 : undefined;
  const at = Date.parse(header);
  if (Number.isNaN(at)) return undefined;
  const delta = at - now;
  return delta > 0 ? delta : undefined;
}

export function retryAfterFromMessage(message: string): number | undefined {
  const m = /retry (?:after|in) (\d+(?:\.\d+)?) ?(s|sec|seconds?|ms|m|min|minutes?)\b/i.exec(message);
  if (!m) return undefined;
  const n = Number(m[1]);
  const unit = m[2]!.toLowerCase();
  if (unit === "ms") return n;
  if (unit.startsWith("m")) return n * 60_000;
  return n * 1000;
}

// A daily free-tier exhaustion, not a burst limit. OpenCode Zen answers with
// FreeUsageLimitError and a Retry-After that points at the next UTC midnight
// (up to 24h). The body type is the reliable signal; the long Retry-After is
// the fallback for a gateway that drops the type but keeps the header.
export function isQuotaExhaustion(status: number, body: string, retryAfterMs?: number): boolean {
  if (status !== 429) return false;
  if (/FreeUsageLimitError|Free usage limit reached/i.test(body)) return true;
  return retryAfterMs !== undefined && retryAfterMs >= 15 * 60_000;
}

export const FAILOVER_CONTEXT_TOLERANCE = 0.9;

export function pickFailover(
  catalog: RuntimeCatalog,
  origin: ModelDef,
  attempts: ReadonlySet<string> = new Set(),
  allowed: (candidate: ModelDef) => boolean = () => true,
): ModelDef | undefined {
  let best: ModelDef | undefined;
  let bestShort = true;
  let bestGap = Number.POSITIVE_INFINITY;
  for (const candidate of catalog.models) {
    if (candidate.id === origin.id) continue;
    if (attempts.has(candidate.id)) continue;
    if (!allowed(candidate)) continue;
    if (candidate.source === origin.source) continue;
    if (candidate.reasoning !== origin.reasoning) continue;
    if (candidate.compat.supportsDeveloperRole !== origin.compat.supportsDeveloperRole) continue;
    if (candidate.compat.supportsReasoningEffort !== origin.compat.supportsReasoningEffort) continue;
    if (candidate.contextWindow < origin.contextWindow * FAILOVER_CONTEXT_TOLERANCE) continue;

    const short = candidate.contextWindow < origin.contextWindow;
    const gap = Math.abs(candidate.contextWindow - origin.contextWindow);
    if (best !== undefined) {
      if (short !== bestShort) {
        if (short) continue;
      } else if (gap !== bestGap) {
        if (gap > bestGap) continue;
      } else if (candidate.maxTokens <= best.maxTokens) continue;
    }
    best = candidate;
    bestShort = short;
    bestGap = gap;
  }
  return best;
}
// shared by chat and responses: resolve, sanitize, then retry on 429/5xx against another upstream
type ForwardResult = { response: Response; model: ModelDef; upstream: Upstream; relay: string };
type ForwardError = {
  status: number;
  message: string;
  type?: "upstream_error" | "security_policy_error";
  secretTypes?: SecretType[];
  relay?: string;
  // Seconds the client should wait before retrying. Carried from the upstream's
  // Retry-After so a daily free-tier exhaustion is not retried in a tight loop.
  retryAfter?: number;
};

function isExternalUpstream(upstream: Upstream): boolean {
  try {
    const url = new URL(upstream.chatUrl);
    return !isLoopbackBind(url.hostname);
  } catch {
    // unparseable destinations count as external so strict DLP still applies
    return true;
  }
}

function selectFailover(
  catalog: RuntimeCatalog,
  current: ModelDef,
  currentUpstream: Upstream,
  tried: ReadonlySet<string>,
  security: SecurityConfig,
  failoverAllowed: boolean,
  status: number,
  requestStartedAt: number,
  log: Logger,
  upstreamError?: string,
): ModelDef | undefined {
  if (!failoverAllowed) {
    log.warn("upstream rejected", {
      model: current.id,
      upstream: currentUpstream.id,
      status,
      durationMs: Date.now() - requestStartedAt,
      failoverBlocked: true,
      ...(upstreamError ? { upstreamError } : {}),
    });
    return undefined;
  }

  return pickFailover(catalog, current, tried, (candidate) => {
    if (catalog.isCoolingDown(candidate.id)) return false;
    const candidateUpstream = catalog.upstreamBySource(candidate.source);
    return Boolean(candidateUpstream && isUpstreamAllowed(security, candidateUpstream.id));
  });
}

interface ToolCallAcc {
  id?: string;
  name?: string;
  args: string;
}

export function sseErrorMessage(json: any): string | null {
  if (!json || typeof json !== "object") return null;
  if (json.error) return json.error.message ?? String(json.error);
  if (json.choices) return null;
  if (json.type === "error") return json.message ?? json.error?.message ?? "upstream stream error";
  if (json.type === "response.failed") return json.response?.error?.message ?? "upstream response failed";
  return null;
}

const SSE_PEEK_LIMIT = 65536;
const SSE_PEEK_TIMEOUT_MS = 5000;

export const VISION_UNSUPPORTED_RE = /(does not|doesn't|not) support(s)? (vision|image|images|image input|multimodal)|image input is not supported/i;

export function hasImageInput(body: unknown): boolean {
  const messages = (body as Record<string, unknown> | null)?.messages;
  if (!Array.isArray(messages)) return false;
  return messages.some((m: any) =>
    Array.isArray(m?.content) &&
    m.content.some((p: any) => p?.type === "image_url" || p?.type === "input_image" || p?.type === "image"),
  );
}

export const TOOLS_UNSUPPORTED_RE = /(does not|doesn't|not) support(s)? (tools|tool use|tool calls|function calling)|support tool use/i;

// The upstream retired this model id: it will never answer it again, so the
// request belongs to a mirror that still serves the same id. Zen answers a
// retired id with 410 ModelDeprecated, or with a 400 whose body names the
// model as unavailable.
export const ZEN_MODEL_RETIRED_RE = /ModelDeprecated|Model is unavailable|model is not available|deprecated/i;

// some upstreams answer 200 and then put the failure in the first SSE frame
export async function peekStreamError(
  res: Response,
  timeoutMs = SSE_PEEK_TIMEOUT_MS,
): Promise<{ response: Response; error: string | null }> {
  if (!res.body || (res.headers.get("content-type") ?? "").includes("json")) return { response: res, error: null };
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  const chunks: Uint8Array[] = [];
  const deadline = Date.now() + timeoutMs;
  let text = "";
  let scanned = 0;
  let error: string | null = null;
  let done = false;
  let pending: Promise<ReadableStreamReadResult<Uint8Array>> | null = null;
  scan: while (text.length < SSE_PEEK_LIMIT) {
    pending = reader.read();
    const wait = deadline - Date.now();
    if (wait <= 0) break;
    let timer: NodeJS.Timeout | undefined;
    const next = await Promise.race([
      pending,
      new Promise<null>((resolve) => (timer = setTimeout(() => resolve(null), wait))),
    ]);
    clearTimeout(timer);
    if (next === null) break;
    pending = null;
    if (next.done) {
      done = true;
      break;
    }
    chunks.push(next.value);
    text = (text + decoder.decode(next.value, { stream: true })).replace(/\r\n/g, "\n");
    let end = text.indexOf("\n\n", scanned);
    while (end !== -1) {
      const lines = text.slice(scanned, end).split("\n");
      scanned = end + 2;
      for (const line of lines) {
        if (!line.startsWith("data:")) continue;
        const payload = line.slice(5).trim();
        if (!payload) continue;
        if (payload === "[DONE]") break scan;
        let json: any;
        try {
          json = JSON.parse(payload);
        } catch {
          break scan;
        }
        error = sseErrorMessage(json);
        if (error || json?.choices || typeof json?.type === "string") break scan;
      }
      end = text.indexOf("\n\n", scanned);
    }
  }

  if (error) {
    void reader.cancel().catch(() => {});
    return { response: res, error };
  }
  const replay = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const c of chunks) controller.enqueue(c);
      if (done) controller.close();
    },
    async pull(controller) {
      const next = await (pending ?? reader.read());
      pending = null;
      if (next.done) controller.close();
      else controller.enqueue(next.value);
    },
    cancel(reason) {
      return reader.cancel(reason);
    },
  });
  return {
    response: new Response(replay, { status: res.status, statusText: res.statusText, headers: res.headers }),
    error: null,
  };
}

export async function streamToChatResponse(
  upstreamRes: Response,
  modelId: string,
  callerTools?: CallerTools,
): Promise<Response> {
  if (!upstreamRes.body) return upstreamRes;
  const text = await upstreamRes.text();
  if (text.trim().startsWith("{")) {
    return new Response(text, {
      status: upstreamRes.status,
      headers: { "content-type": "application/json" },
    });
  }
  const lines = text.split("\n");
  let content = "";
  let reasoning = "";
  let finishReason = "stop";
  let usage: unknown = undefined;
  let streamError: string | null = null;
  const calls = new Map<number, ToolCallAcc>();
  for (const line of lines) {
    if (!line.startsWith("data:")) continue;
    const data = line.slice(5).trim();
    if (data === "[DONE]") break;
    try {
      const json = JSON.parse(data);
      streamError ??= sseErrorMessage(json);
      const choice = json.choices?.[0];
      if (choice?.delta?.content) content += choice.delta.content;
      if (choice?.delta?.reasoning) reasoning += choice.delta.reasoning;
      else if (choice?.delta?.reasoning_content) reasoning += choice.delta.reasoning_content;
      for (const tc of choice?.delta?.tool_calls ?? []) {
        const slot = calls.get(tc.index) ?? { args: "" };
        if (tc.id) slot.id = tc.id;
        if (tc.function?.name) slot.name = tc.function.name;
        if (tc.function?.arguments) slot.args += tc.function.arguments;
        calls.set(tc.index, slot);
      }
      if (choice?.finish_reason) finishReason = choice.finish_reason;
      if (json.usage) usage = json.usage;
    } catch {
      // ignore frame parse errors
    }
  }

  const toolCalls = [...calls.entries()]
    .sort(([a], [b]) => a - b)
    .flatMap(([, c]) => {
      if (c.name === undefined) return [];
      const id = c.id ?? `call_${Math.random().toString(36).slice(2, 12)}`;
      const fn = !callerTools || callerTools.has(c.name)
        ? { name: c.name, arguments: c.args || "{}" }
        : callerTools.bridge?.(c.name)?.(c.args, id);
      if (!fn) return [];
      return [{ id, type: "function" as const, function: fn }];
    });
  if (toolCalls.length === 0 && finishReason === "tool_calls") finishReason = "stop";
  // Same rescue as the non-stream path: when upstream answers entirely inside
  // `reasoning_content` (Zen/DeepSeek do this routinely) the reassembled message
  // would ship an empty `content` and the caller sees a blank answer. Promote the
  // reasoning only when nothing else arrived, so real answers are never overwritten.
  if (content.length === 0 && toolCalls.length === 0 && reasoning.length > 0) {
    content = reasoning;
    reasoning = "";
  }
  if (streamError && !content && !reasoning && toolCalls.length === 0) {
    return new Response(JSON.stringify({ error: { message: streamError, type: "upstream_error" } }), {
      status: 502,
      headers: { "content-type": "application/json" },
    });
  }
  return new Response(
    JSON.stringify({
      id: `chatcmpl-${Math.random().toString(36).slice(2, 12)}`,
      object: "chat.completion",
      created: Math.floor(Date.now() / 1000),
      model: modelId,
      choices: [
        {
          index: 0,
          message: {
            role: "assistant",
            content,
            ...(reasoning ? { [toolCalls.length < calls.size ? "reasoning_content" : "reasoning"]: reasoning } : {}),
            ...(toolCalls.length > 0 ? { tool_calls: toolCalls } : {}),
          },
          finish_reason: finishReason,
        },
      ],
      ...(usage ? { usage } : {}),
    }),
    {
      status: upstreamRes.status,
      headers: { "content-type": "application/json" },
    },
  );
}

// Try the zen mirror for a model whose zen upstream reported its daily free
// pool as spent.
//
// The mirror is a DIFFERENT free upstream serving the same model under a
// different id, so the request keeps the client's model id in its body and
// only the transport changes. The response is checked for two traps that a
// plain status check would miss:
//
//   1. Some free routers answer HTTP 200 with a refusal string ("Sorry, to
//      prevent abuse of free resources..."). That is a valid-looking
//      completion that says nothing — worse than an error, because the client
//      cannot tell it apart from a real answer.
//   2. The gateway rewrites the `model` field to the upstream's own id
//      ("deepseek-v4-1-flash-economy-261017"). The client asked for the zen
//      id and must get the zen id back.
//
// A free id on the gateway's model list is not a promise that the upstream
// still has quota for it: one hangs without answering, one answers 402
// "exclusive to a paid subscription", one answers a prose refusal. The
// candidates are therefore raced — the first usable answer wins — instead of
// walked in order, so one dead candidate does not hold up the ones behind it.
//
// Returns the usable response, or null when no mirror can serve it (the
// caller then falls through to the normal failover path).
async function tryZenMirror(
  model: ModelDef,
  candidates: string[],
  mirror: ZenMirrorHandle,
  sanitizedBody: Record<string, unknown>,
  req: http.IncomingMessage,
  log: Logger,
  requestStartedAt: number,
): Promise<Response | null> {
  if (candidates.length === 0) return null;

  // The losers are cancelled as soon as one candidate wins: a hanging
  // candidate must not keep a socket (and the upstream's quota) alive.
  const race = new AbortController();
  const attempts = candidates.map((mirrorId) =>
    tryZenMirrorOnce(model, mirrorId, mirror, sanitizedBody, req, log, requestStartedAt, race.signal).then(
      (response) => ({ mirrorId, response }),
    ),
  );

  // The first candidate that produces a usable answer wins; the rest are
  // abandoned (their responses are dropped, not shipped).
  const winner = await Promise.any(
    attempts.map((p) =>
      p.then(({ response }) => {
        if (response) return response;
        // A failed candidate must not win the race.
        throw new Error("mirror candidate unusable");
      }),
    ),
  )
    .catch(() => null)
    .finally(() => race.abort());

  if (!winner) {
    log.warn("zen mirror exhausted all candidates", { model: model.id, candidates });
  }
  return winner;
}

// The text a mirror actually produced. A free router is free to answer in
// any of the three places the OpenAI-compatible shape allows:
//
//   content           — the normal answer
//   reasoning_content — DeepSeek/Nemotron style reasoning models
//   reasoning         — the same thing under the other name (observed on
//                       kiosapi, which also sends `content: null` and labels
//                       a plain JSON body as `text/event-stream`)
//
// Whichever carries text is the answer.
function mirrorAnswerText(parsed: any): string {
  const message = parsed?.choices?.[0]?.message ?? {};
  return String(
    message.content ?? message.reasoning_content ?? message.reasoning ?? "",
  );
}

// Parse the JSON object at the start of a body that has trailing junk glued to
// it (observed: `...}data: [DONE]`). The object's extent is found by walking
// the braces with string awareness, so a `}` inside a string value does not
// end the object early.
function parseJsonPrefix(raw: string): any {
  const start = raw.indexOf("{");
  if (start < 0) return null;
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let i = start; i < raw.length; i++) {
    const ch = raw[i]!;
    if (escaped) {
      escaped = false;
      continue;
    }
    if (ch === "\\") {
      escaped = true;
      continue;
    }
    if (ch === '"') {
      inString = !inString;
      continue;
    }
    if (inString) continue;
    if (ch === "{") depth++;
    else if (ch === "}" && --depth === 0) {
      try {
        return JSON.parse(raw.slice(start, i + 1));
      } catch {
        return null;
      }
    }
  }
  return null;
}

async function tryZenMirrorOnce(
  model: ModelDef,
  mirrorId: string,
  mirror: ZenMirrorHandle,
  sanitizedBody: Record<string, unknown>,
  req: http.IncomingMessage,
  log: Logger,
  requestStartedAt: number,
  raceSignal?: AbortSignal,
): Promise<Response | null> {
  const headers = new Headers({
    "content-type": "application/json",
    ...mirror.upstream.requestHeaders(model, req.headers as Record<string, string | string[] | undefined>),
  });
  // The body keeps the client's model id; the mirror's transport maps it to
  // the upstream's own id. The candidate id is written directly here: the
  // transport's own mapping always picks the FIRST candidate, but this call
  // may be about the second one.
  const transformed = { ...sanitizedBody, model: mirrorId };
  const body = JSON.stringify(transformed);

  let res: Response;
  const ctl = new AbortController();
  // A free id that the upstream cannot actually serve may HANG instead of
  // answering (observed: unorouter/mimo-v2.6-flash:free never returns a
  // response). The deadline is per candidate, and it is short enough that a
  // hung candidate does not eat the whole request budget.
  const timer = setTimeout(() => ctl.abort(), 25_000);
  const onRaceAbort = () => ctl.abort();
  raceSignal?.addEventListener("abort", onRaceAbort, { once: true });
  try {
    res = await fetch(mirror.upstream.chatUrl, {
      method: "POST",
      headers,
      body,
      signal: ctl.signal,
    });
  } catch (err) {
    clearTimeout(timer);
    raceSignal?.removeEventListener("abort", onRaceAbort);
    // A candidate that lost the race is not a failure worth reporting.
    if (!raceSignal?.aborted) {
      log.warn("zen mirror request failed", {
        model: model.id,
        mirror: mirrorId,
        error: String((err as Error)?.message ?? err).slice(0, 120),
        durationMs: Date.now() - requestStartedAt,
      });
    }
    return null;
  }
  clearTimeout(timer);
  raceSignal?.removeEventListener("abort", onRaceAbort);

  if (res.status >= 400) {
    const text = await res.text().catch(() => "");
    log.warn("zen mirror rejected", {
      model: model.id,
      mirror: mirrorId,
      status: res.status,
      upstreamError: text.slice(0, 160),
      durationMs: Date.now() - requestStartedAt,
    });
    return null;
  }

  const contentType = res.headers.get("content-type") ?? "";
  const wantsStream = sanitizedBody.stream === true;

  // A mirror may label a plain JSON body as `text/event-stream` (observed on
  // kiosapi and orca), so the content-type is not trusted on its own. The
  // body is sniffed instead: an SSE body starts with `data:`.
  //
  // The sniff is not enough on its own either: a body may be JSON followed by
  // a stray SSE terminator (`data: [DONE]`), which is neither pure JSON nor
  // pure SSE. That shape is parsed as JSON with the terminator cut off.
  const sniffed = contentType.includes("text/event-stream")
    ? await res.clone().text().catch(() => "")
    : "";
  const isSse = /^\s*(data:|event:)/.test(sniffed);

  // The client asked for a stream and the mirror sent one: the model id sits
  // in every SSE frame, so it is rewritten in the stream.
  if (wantsStream && isSse) {
    const framed = rewriteStreamModelId(res, model.id);
    log.info("zen mirror served model", {
      model: model.id,
      mirror: mirrorId,
      upstream: mirror.upstream.id,
      durationMs: Date.now() - requestStartedAt,
    });
    return framed;
  }

  // The client asked for a stream but the mirror sent a plain JSON body. It
  // is turned back into the SSE shape the client expects, with the zen model
  // id in the chunk.
  if (wantsStream) {
    const framed = await jsonToEventStream(res, model.id);
    log.info("zen mirror served model", {
      model: model.id,
      mirror: mirrorId,
      upstream: mirror.upstream.id,
      durationMs: Date.now() - requestStartedAt,
    });
    return framed;
  }

  // The client asked for JSON. A mirror may still answer with an SSE stream
  // (observed: the glm mirror), which is reassembled into one completion —
  // with the zen model id, not the upstream's.
  if (isSse) {
    const assembled = await streamToChatResponse(res, model.id);
    const text = await assembled.text();
    let parsed: any;
    try {
      parsed = JSON.parse(text);
    } catch {
      log.warn("zen mirror stream did not reassemble", { model: model.id, mirror: mirrorId });
      return null;
    }
    const answer = mirrorAnswerText(parsed);
    if (!answer.trim()) {
      log.warn("zen mirror returned an empty answer", { model: model.id, mirror: mirrorId });
      return null;
    }
    if (ZEN_MIRROR_REFUSAL.test(answer)) {
      log.warn("zen mirror returned a refusal", {
        model: model.id,
        mirror: mirrorId,
        answer: answer.slice(0, 120),
      });
      return null;
    }
    parsed.model = model.id;
    log.info("zen mirror served model", {
      model: model.id,
      mirror: mirrorId,
      upstream: mirror.upstream.id,
      durationMs: Date.now() - requestStartedAt,
    });
    return new Response(JSON.stringify(parsed), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  }

  // A body that is neither pure SSE nor pure JSON: JSON followed by a stray
  // SSE terminator. The terminator is cut off so the JSON parses.
  const raw = await res.text().catch(() => "");
  let parsed: any;
  try {
    parsed = JSON.parse(raw);
  } catch {
    parsed = parseJsonPrefix(raw);
    if (!parsed) {
      log.warn("zen mirror returned unreadable body", { model: model.id, mirror: mirrorId });
      return null;
    }
  }

  const choices = parsed?.choices;
  if (!Array.isArray(choices) || choices.length === 0) {
    log.warn("zen mirror returned no choices", { model: model.id, mirror: mirrorId });
    return null;
  }

  const answer = mirrorAnswerText(parsed);
  if (!answer.trim()) {
    log.warn("zen mirror returned an empty answer", { model: model.id, mirror: mirrorId });
    return null;
  }
  if (ZEN_MIRROR_REFUSAL.test(answer)) {
    // A 200 that refuses in prose is not an answer. Treat it as a failure so
    // the caller keeps looking instead of shipping the refusal to the client.
    log.warn("zen mirror returned a refusal", {
      model: model.id,
      mirror: mirrorId,
      answer: answer.slice(0, 120),
    });
    return null;
  }

  // Put the zen model id back: the client asked for it and the daemon's
  // bookkeeping expects it.
  parsed.model = model.id;

  log.info("zen mirror served model", {
    model: model.id,
    mirror: mirrorId,
    upstream: mirror.upstream.id,
    durationMs: Date.now() - requestStartedAt,
  });

  return new Response(JSON.stringify(parsed), {
    status: res.status,
    headers: { "content-type": "application/json" },
  });
}

// Try the zen STAGE deployment for a model id.
//
// The stage is opencode's own console on a different deployment, so it
// speaks the same wire protocol and the same model ids - only the database
// (and therefore the daily free-tier bucket) differs. It is used when
// production's bucket is closed.
//
// The request is built exactly like the production one: the same session
// identity, the same fingerprint headers, the same body shape (including the
// read+shell tool names the console's gate expects), and the same wire per
// model (chat vs responses). The answer is converted back to the shape the
// caller asked for, exactly like the production path does.
//
// Returns the answer, or null when the stage cannot serve this request (the
// caller then falls through to its normal error path).
async function tryZenStage(
  model: ModelDef,
  stage: ZenStageHandle,
  sanitizedBody: Record<string, unknown>,
  req: http.IncomingMessage,
  log: Logger,
  requestStartedAt: number,
  callerTools: CallerTools,
  security: SecurityConfig,
  relay: RelayState,
): Promise<Response | null> {
  // The stage's catalog decides which ids it can serve. An id it does not
  // carry answers "Model is unavailable" (400), which is a refusal, not a
  // transport failure - so the id is checked BEFORE a request is spent.
  if (!stage.serves(model.id)) return null;

  const responsesWire = model.wireApi === "responses" && !!stage.upstream.responsesUrl;
  const outboundUrl = responsesWire ? stage.upstream.responsesUrl! : stage.upstream.chatUrl;
  const headers = new Headers({
    "content-type": "application/json",
    ...stage.upstream.requestHeaders(model, req.headers as Record<string, string | string[] | undefined>),
  });
  const transformedBody = stage.upstream.transformRequestBody
    ? stage.upstream.transformRequestBody({ ...sanitizedBody, model: model.id }, model)
    : { ...sanitizedBody, model: model.id };
  let outboundBody = JSON.stringify(
    responsesWire ? chatToResponsesBody(transformedBody, model.id) : transformedBody,
  );

  // The stage is an external upstream, so the same secret guard applies:
  // answering from the stage must not become a way around the DLP.
  if (isStrictSecurity(security) && !security.disableSecretGuard) {
    const secretScan = scanRequestBody(outboundBody);
    const redaction = secretScan.blocked && security.dlp === "redact" ? redactRequestBody(outboundBody) : null;
    if (redaction) {
      outboundBody = redaction.body;
      log.warn("secret guard redacted request", {
        model: model.id,
        upstream: stage.upstream.id,
        dlpRedacted: true,
        redactions: redaction.count,
        secretTypes: redaction.secretTypes,
      });
    } else if (secretScan.blocked) {
      log.warn("request blocked by secret guard", {
        model: model.id,
        upstream: stage.upstream.id,
        status: 422,
        durationMs: Date.now() - requestStartedAt,
        dlpBlocked: true,
        secretTypes: secretScan.secretTypes,
      });
      return new Response(
        JSON.stringify({
          error: { message: "request blocked by secret guard", type: "security_policy_error" },
        }),
        { status: 422, headers: { "content-type": "application/json" } },
      );
    }
  }

  let res: Response;
  const ctl = new AbortController();
  // A headers deadline, like the production path: a stage that accepts the
  // connection and then stalls must not hold the request open forever.
  const timer = setTimeout(() => ctl.abort(), 90_000);
  try {
    // The stage goes through the relay fleet, exactly like production.
    //
    // The upstream limiter keys its daily bucket on the request's x-real-ip,
    // and the edge stamps the RELAY's egress address there when the client
    // does not send x-real-ip itself. The fleet handler strips x-real-ip, so
    // every relay gets its own bucket - and the stage's database has never
    // seen those addresses, so they are all virgin. A direct fetch would put
    // every request in THIS box's single bucket and the stage would drain
    // within minutes.
    res = await relayFetch(
      relay,
      outboundUrl,
      {
        method: "POST",
        headers,
        body: outboundBody,
        duplex: "half",
        signal: ctl.signal,
      },
      stage.upstream.relayAllowed && isRelayAllowed(security),
    );
  } catch (err) {
    clearTimeout(timer);
    log.warn("zen stage request failed", {
      model: model.id,
      upstream: stage.upstream.id,
      error: String((err as Error)?.message ?? err).slice(0, 120),
      durationMs: Date.now() - requestStartedAt,
    });
    return null;
  }
  clearTimeout(timer);

  if (res.status >= 400) {
    const text = await res.text().catch(() => "");
    log.warn("zen stage rejected", {
      model: model.id,
      upstream: stage.upstream.id,
      status: res.status,
      upstreamError: text.slice(0, 160),
      // A stage that is itself spent must not be retried on every request:
      // the next attempt would walk the stage's dead relays and then pay the
      // production walk, which measured 31s on 2026-10-30.
      fleetExhausted: res.headers.get(FLEET_EXHAUSTED_HEADER) === "1",
      durationMs: Date.now() - requestStartedAt,
    });
    return null;
  }

  // The answer is converted back to what the caller asked for, the same way
  // the production path does it.
  const forcedStream = !sanitizedBody.stream && transformedBody.stream === true;
  let finalResponse = res;
  if (responsesWire) {
    finalResponse = await toChatResponse(
      res,
      model.id,
      sanitizedBody.stream === true || forcedStream,
    );
    if (forcedStream) {
      finalResponse = await streamToChatResponse(finalResponse, model.id, callerTools);
    }
  } else if (forcedStream) {
    finalResponse = await streamToChatResponse(res, model.id, callerTools);
  }

  log.info("zen stage served model", {
    model: model.id,
    upstream: stage.upstream.id,
    durationMs: Date.now() - requestStartedAt,
  });
  return finalResponse;
}

// Try the zen stage for a model whose production bucket is closed. The
// stage's model list is resolved on demand: the daemon may have restarted
// while the park survived on disk.
async function tryZenStageFor(
  model: ModelDef,
  stage: ZenStageHandle,
  sanitizedBody: Record<string, unknown>,
  req: http.IncomingMessage,
  log: Logger,
  requestStartedAt: number,
  callerTools: CallerTools,
  security: SecurityConfig,
  relay: RelayState,
): Promise<Response | null> {
  if (!stage.serves(model.id)) {
    await stage.ready();
    if (!stage.serves(model.id)) return null;
  }
  // The stage has its OWN daily rows, and the relay layer counts distinct
  // quota-429 addresses per (deployment, model row) — production's closed
  // wall is consulted before the prod walk (see the pre-walk check), but a
  // CLOSED STAGE was never checked. When it closes, every fall-through still
  // walked up to 13 of its dead relays one by one before giving up: measured
  // 2026-10-30, three consecutive big-pickle requests paid 41.7s / 45.0s /
  // 43.7s before the park finally engaged. A majority-spent stage must fail
  // fast; the next UTC midnight reopens it with the upstream's own rows.
  const stageOrigin = safeOrigin(stage.upstream.chatUrl);
  if (
    stageOrigin !== "" &&
    deploymentEssentiallySpent(relay, stageOrigin, model.id)
  ) {
    log.warn("zen stage known spent, skipping the dead walk", {
      model: model.id,
      upstream: stage.upstream.id,
      durationMs: Date.now() - requestStartedAt,
    });
    return null;
  }
  return tryZenStage(model, stage, sanitizedBody, req, log, requestStartedAt, callerTools, security, relay);
}

// The mirror for a parked zen model. Unlike the 429 path, the mirror list may
// not have been resolved yet (the daemon may have restarted while the park
// survived on disk), so the lookup refreshes once when it comes up empty.
async function tryZenMirrorForParked(
  model: ModelDef,
  mirror: ZenMirrorHandle,
  sanitizedBody: Record<string, unknown>,
  req: http.IncomingMessage,
  log: Logger,
  requestStartedAt: number,
): Promise<Response | null> {
  let candidates = mirror.mirrorsFor(model.id);
  if (candidates.length === 0) {
    await mirror.ready();
    candidates = mirror.mirrorsFor(model.id);
  }
  return tryZenMirror(model, candidates, mirror, sanitizedBody, req, log, requestStartedAt);
}

// Turn a plain JSON completion into the SSE shape a streaming client expects.
// The mirror's own model id is replaced with the zen id the client asked for.
async function jsonToEventStream(res: Response, modelId: string): Promise<Response> {
  const raw = await res.text().catch(() => "");
  let parsed: any;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return new Response(raw, { status: res.status, headers: res.headers });
  }
  const message = parsed?.choices?.[0]?.message ?? {};
  const content = String(message.content ?? message.reasoning_content ?? message.reasoning ?? "");
  const chunk = {
    id: parsed.id ?? `chatcmpl-${Math.random().toString(36).slice(2, 12)}`,
    object: "chat.completion.chunk",
    created: parsed.created ?? Math.floor(Date.now() / 1000),
    model: modelId,
    choices: [{ index: 0, delta: { role: "assistant", content }, finish_reason: "stop" }],
  };
  const body = `data: ${JSON.stringify(chunk)}\n\ndata: [DONE]\n\n`;
  return new Response(body, {
    status: 200,
    headers: { "content-type": "text/event-stream", "cache-control": "no-cache" },
  });
}

// Rewrite the `model` field inside every SSE frame of a streaming answer.
function rewriteStreamModelId(res: Response, modelId: string): Response {
  if (!res.body) return res;
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();
  const transform = new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      const text = decoder.decode(chunk, { stream: true });
      const rewritten = text.replace(
        /("model"\s*:\s*)"[^"]*"/g,
        (_m, prefix: string) => `${prefix}${JSON.stringify(modelId)}`,
      );
      controller.enqueue(encoder.encode(rewritten));
    },
  });
  return new Response(res.body.pipeThrough(transform), {
    status: res.status,
    headers: res.headers,
  });
}

async function runChatForward(
  req: http.IncomingMessage,
  catalog: RuntimeCatalog,
  log: Logger,
  security: SecurityConfig,
  parsedModel: string,
  sanitizedBody: Record<string, unknown>,
  signal: AbortSignal,
): Promise<ForwardResult | ForwardError> {
  const model = catalog.resolve(parsedModel);
  if (!model) {
    return { status: 400, message: `unknown model: ${parsedModel}` };
  }
  const upstream = catalog.upstreamBySource(model.source);
  if (!upstream) {
    return { status: 502, message: `no upstream for source: ${model.source}` };
  }

  const requestStartedAt = Date.now();
  const relay = loadRelayState();
  // BANSOS_ZEN_FAILOVER=0 keeps the zen lane PURE: a zen id is answered by
  // opencode.ai or the call fails. The mirror stays registered (so the catalog
  // still publishes the id) but it may not take the request. This is the
  // operator-level switch; the per-request header below still works.
  const zenFailoverAllowed = !["0", "false", "off", "no"].includes(
    (process.env.BANSOS_ZEN_FAILOVER ?? "").trim().toLowerCase(),
  );
  // The zen STAGE is governed by its OWN switch (BANSOS_ZEN_STAGE), not by
  // the cross-provider failover switch: the stage is opencode's own console on
  // a different deployment, so a zen id is still answered by opencode under
  // that id. BANSOS_ZEN_FAILOVER=0 exists to keep a DIFFERENT provider from
  // answering a zen id, and the stage is not that.
  //
  // The per-request opt-out therefore does NOT apply to the stage. It was
  // written for probes ("report the pinned upstream's own status"), but the
  // dashboard ping and `bansos ping` both send it — which made every probe of
  // a stage-served id answer 429 while real traffic served 200 through the
  // very same daemon. Measured 2026-10-30: big-pickle pinged like the
  // dashboard = 429 in 1.8ms; the identical request without the header = 200
  // via zen-stage. A probe that reports a deployment the operator deliberately
  // closed as "down" is worse than useless: it trains people to ignore the
  // dashboard. Probing ONLY the pinned production row remains possible with
  // x-bansos-no-prod-fallback (strict form of the old behaviour).
  //
  // The mirror keeps honouring the plain opt-out: it IS a different provider
  // answering under the same id, exactly what a pinning caller excludes.
  const zenStageAllowed =
    zenStage !== null && req.headers["x-bansos-no-prod-fallback"] !== "1";
  // The caller's own tool names, needed by every path that has to rebuild the
  // answer (the stage and the mirror both answer under a different transport).
  const callerTools = callerToolsFromBody(sanitizedBody);
  // An upstream that RETIRED this model id will never answer it again, and a
  // stage or mirror serves the SAME id from another transport. So a known
  // retirement goes straight to the stage, then the mirror: asking the
  // upstream again would only buy the same rejection.
  if (catalog.isRetired(model.id) && upstream.id === "zen") {
    // The stage is opencode's own console, so it answers under its own gate.
    if (zenStage && zenStageAllowed) {
      const staged = await tryZenStageFor(
        model,
        zenStage,
        sanitizedBody,
        req,
        log,
        requestStartedAt,
        callerTools,
        security,
        relay,
      );
      if (staged) {
        return { response: staged, model, upstream: zenStage.upstream, relay: "" };
      }
    }
    // The mirror is a DIFFERENT provider, so it keeps the cross-provider gate.
    if (zenMirror && zenFailoverAllowed) {
      const mirrored = await tryZenMirrorForParked(
        model,
        zenMirror,
        sanitizedBody,
        req,
        log,
        requestStartedAt,
      );
      if (mirrored) {
        return { response: mirrored, model, upstream: zenMirror.upstream, relay: "" };
      }
    }
  }
  // Which egress served this request. The watchdog can only attribute failures
  // to a relay if the event says which relay was in use, otherwise it sees
  // "upstream=zen failed" and cannot tell a gated relay from a gated upstream.
  // Computed once: relay state is loaded here and the failover loop below only
  // swaps model/upstream, never the egress.
  const servingRelay = relay.enabled && relay.url ? relay.url : "";
  const failoverAllowed =
    req.headers["x-bansos-no-failover"] !== "1" &&
    // Per-request opt-in. The global switch stays off (the operator manages
    // which model may answer), but a caller that KNOWS the requested model's
    // bucket is spent — e.g. the gateway lane that just saw the upstream's
    // Retry-After — can ask for a substitute on this one request instead of
    // failing the call. The answer's origin still changes, so it stays opt-in
    // rather than a default.
    (isCrossProviderFailoverAllowed(security) || req.headers["x-bansos-failover"] === "1");
  const tried = new Set<string>([model.id]);
  let current: ModelDef = model;
  let currentUpstream = upstream;
  let transientError: ForwardError | null = null;
  const needsTools = declaredToolNames(sanitizedBody).size > 0;
  const needsVision = hasImageInput(sanitizedBody);
  const incapable = (m: ModelDef) =>
    (needsTools && m.supportsTools === false) || (needsVision && !m.input.includes("image"));
  for (const m of catalog.models) if (incapable(m)) tried.add(m.id);

  if (failoverAllowed && incapable(current)) {
    transientError = {
      status: 400,
      message: `model ${current.id} does not support ${needsVision && !current.input.includes("image") ? "image input" : "tool calls"}`,
    };
    const capable = selectFailover(
      catalog, current, currentUpstream, tried, security, true,
      400, requestStartedAt, log, transientError.message,
    );
    if (capable) {
      tried.add(capable.id);
      current = capable;
      currentUpstream = catalog.upstreamBySource(current.source)!;
    }
  }

  // A parked model must not be re-probed, even when failover is off.
  //
  // The upstream already told us when its bucket refills (a daily
  // FreeUsageLimitError carries Retry-After to 00:00 UTC). Re-learning that on
  // every request costs a round trip, spends a request from a bucket that is
  // empty by definition, and — for zen — keeps the probe gate churning. So the
  // park is consulted unconditionally, and a caller that asked for a parked
  // model gets the upstream's own cooldown answer instead of a fresh 429.
  //
  // Failover, by contrast, stays opt-in: `failoverAllowed` below still decides
  // whether this request may be answered by a DIFFERENT model.
  if (catalog.isCoolingDown(current.id) && current.id === model.id) {
    let parkedMs = catalog.cooldownRemaining(current.id);
    // The on-disk marker is the authority for a ZEN QUOTA park: it is what
    // survives a restart and what a human clears by hand to force a retry. If
    // the marker is gone (or has expired), that park must yield instead of
    // re-stamping itself forever, otherwise clearing the gate could never open
    // the lane. A burst park is the daemon's own timing and keeps its window.
    if (currentUpstream.id === "zen" && catalog.isQuotaParked(current.id)) {
      // The window is PER MODEL ROW: the upstream meters (egress IP, the
      // model's first two characters) separately, so "bi" and "ne" are
      // different budgets with different refill states. Reading the global
      // window here closed every zen model when ANY one of them was spent.
      //
      // That was not merely conservative: while the lane was closed the daemon
      // never re-probed production, so the per-prefix spent evidence for that
      // row was never collected, so the daemon could not learn the row was
      // closed and walked the dead addresses on every request (measured
      // 2026-10-31: a 24s walk for a row the upstream had already reported as
      // spent).
      const closedMs = zenQuotaClosedForModel(current.id);
      if (closedMs > 0) {
        // Keep the lane closed for the rest of the park. Re-stamping from the
        // REMAINING time (never from now + full window) means repeated requests
        // cannot push the deadline forward.
        noteZenQuotaExhaustedForModel(current.id, closedMs);
      } else {
        // No open window on disk: the park is stale, let the request through so
        // a recovered bucket is discovered instead of being hidden forever.
        parkedMs = 0;
        catalog.clearPark(current.id);
      }
    }
    if (parkedMs > 0) {
      transientError = {
        status: 429,
        message: catalog.parkMessage(current.id)?.message ?? "model is cooling down after a recent rate limit",
        retryAfter: Math.ceil(parkedMs / 1000),
      };
    }
    if (failoverAllowed) {
      const warm = selectFailover(
        catalog, current, currentUpstream, tried, security, true,
        429, requestStartedAt, log,
      );
      if (warm) {
        tried.add(warm.id);
        current = warm;
        currentUpstream = catalog.upstreamBySource(current.source)!;
      }
    }
    if (parkedMs > 0 && (!failoverAllowed || current.id === model.id)) {
      // No substitute: answer from the park instead of paying for the same 429
      // again. The client gets the original model's own refill deadline.
      //
      // The zen STAGE is the first place to look: it is opencode's own
      // console on a different deployment, so it answers the SAME model id
      // from its own (separate) daily bucket. It comes before the mirror
      // because it keeps the lane pure - the answer still comes from
      // opencode, not from a different provider under the same id.
      if (currentUpstream.id === "zen" && zenStage && zenStageAllowed) {
        const staged = await tryZenStageFor(
          current,
          zenStage,
          sanitizedBody,
          req,
          log,
          requestStartedAt,
          callerTools,
          security,
        relay,
        );
        if (staged) {
          return { response: staged, model: current, upstream: zenStage.upstream, relay: "" };
        }
      }
      // The zen mirror is the next place: it answers the SAME model id from
      // an upstream whose own bucket still has room, so it is not "paying for
      // the same 429 again" - it is taking the request somewhere that can
      // actually serve it. Trying it here is what makes a closed zen pool a
      // working lane instead of a 429 wall.
      if (currentUpstream.id === "zen" && zenMirror && zenFailoverAllowed) {
        const mirrored = await tryZenMirrorForParked(
          current,
          zenMirror,
          sanitizedBody,
          req,
          log,
          requestStartedAt,
        );
        if (mirrored) {
          return { response: mirrored, model: current, upstream: zenMirror.upstream, relay: "" };
        }
      }
      log.warn("model parked after a recent rate limit", {
        model: current.id,
        upstream: currentUpstream.id,
        status: 429,
        durationMs: Date.now() - requestStartedAt,
        failoverBlocked: !failoverAllowed,
        retryAfter: Math.ceil(parkedMs / 1000),
      });
      return transientError ?? { status: 429, message: "model is cooling down after a recent rate limit" };
    }
  }

  for (let attempt = 0; attempt <= MAX_FAILOVER_RETRIES; attempt++) {
    if (current.id !== model.id || attempt > 0) {
      log.warn("upstream rejected - fallback used", {
        from: model.id,
        to: current.id,
        // name the originally requested model's upstream (currentUpstream has moved to the fallback)
        fromUpstream: upstream.id,
        status: transientError?.status,
        durationMs: Date.now() - requestStartedAt,
        attempt,
      });
    }

    if (!isUpstreamAllowed(security, currentUpstream.id)) {
      log.warn("upstream blocked by strict security policy", {
        model: current.id,
        upstream: currentUpstream.id,
        status: 403,
        durationMs: Date.now() - requestStartedAt,
        failoverBlocked: true,
      });
      return {
        status: 403,
        type: "security_policy_error",
        message: security.allowedUpstreams.length === 0
          ? "strict security mode blocks external LLM requests until security.allowedUpstreams explicitly permits a provider"
          : `upstream "${currentUpstream.id}" is not allowed by strict security policy`,
      };
    }

    // The STAGE first, when production is known to be closed for this model.
    //
    // Production opencode.ai and the stage dev.opencode.ai are separate
    // deployments with separate daily rows, and the stage still has room.
    // But production is tried FIRST, and every relay round-trip to a spent
    // production address costs ~2s. Measured 2026-10-29: production answered
    // 429 on all 160 relays for all 8 anonymous-free ids, so every request
    // paid 13 x ~2s = ~26s of dead round-trips before the stage answered.
    //
    // The stage is opencode's own console, so this is not a cross-provider
    // failover: it is the SAME provider on a different deployment, serving
    // the SAME model id. It therefore stays under the stage's own switch
    // (BANSOS_ZEN_STAGE) and the per-request x-bansos-no-failover opt-out.
    //
    // It sits AFTER the security check above: a strict policy that does not
    // allow the zen upstream must not be answered through the stage either.
    //
    // The "known to be closed" evidence is the relay layer's distinct-address
    // quota counter: every address this request could reach has answered a
    // quota 429 for this (deployment, model row).
    const zenOrigin = safeOrigin(currentUpstream.chatUrl);
    if (
      currentUpstream.id === "zen" &&
      zenStage &&
      zenStageAllowed &&
      attempt === 0 &&
      zenOrigin !== "" &&
      deploymentEssentiallySpent(relay, zenOrigin, current.id)
    ) {
      const staged = await tryZenStageFor(
        current,
        zenStage,
        sanitizedBody,
        req,
        log,
        requestStartedAt,
        callerTools,
        security,
        relay,
      );
      if (staged) {
        return { response: staged, model: current, upstream: zenStage.upstream, relay: "" };
      }
    }

    const headers = new Headers({
      "content-type": "application/json",
      ...currentUpstream.requestHeaders(current, req.headers as Record<string, string | string[] | undefined>),
    });
    // responses-wire model: body/reply translated at the edges so the pipeline only sees chat
    const responsesWire = current.wireApi === "responses" && !!currentUpstream.responsesUrl;
    const outboundUrl = responsesWire ? currentUpstream.responsesUrl! : currentUpstream.chatUrl;
    const transformedBody = currentUpstream.transformRequestBody
      ? currentUpstream.transformRequestBody({ ...sanitizedBody, model: current.id }, current)
      : { ...sanitizedBody, model: current.id };
    let outboundBody = JSON.stringify(
      responsesWire
        ? chatToResponsesBody(transformedBody, current.id)
        : transformedBody,
    );

    if (isStrictSecurity(security) && !security.disableSecretGuard && isExternalUpstream(currentUpstream)) {
      const secretScan = scanRequestBody(outboundBody);
      const redaction = secretScan.blocked && security.dlp === "redact" ? redactRequestBody(outboundBody) : null;
      if (redaction) {
        outboundBody = redaction.body;
        log.warn("secret guard redacted request", {
          model: current.id,
          upstream: currentUpstream.id,
          dlpRedacted: true,
          redactions: redaction.count,
          secretTypes: redaction.secretTypes,
        });
      } else if (secretScan.blocked) {
        log.warn("request blocked by secret guard", {
          model: current.id,
          upstream: currentUpstream.id,
          status: 422,
          durationMs: Date.now() - requestStartedAt,
          dlpBlocked: true,
          secretTypes: secretScan.secretTypes,
        });
        return {
          status: 422,
          type: "security_policy_error",
          message: "request blocked by secret guard",
          secretTypes: secretScan.secretTypes,
        };
      }
    }

    let upstreamRes: Response;
    // bansos-opt2: `signal` alone was client-disconnect-only, so a dead or hanging
    // relay (serverless function killed mid-rotation) waited forever and the daemon
    // wedged. This is a HEADERS deadline: AbortSignal.timeout() keeps ticking after
    // the headers land and used to kill the body read at 90s, truncating any long
    // generation (measured: 1000-word answer cut at 90.009s, 383 words, no [DONE]).
    // A controller we disarm on header arrival is the deadline that was intended.
    const chatCtl = new AbortController();
    const headersTimer = setTimeout(() => chatCtl.abort(), 90_000);
    const onClientAbort = () => chatCtl.abort();
    if (signal) {
      if (signal.aborted) chatCtl.abort();
      else signal.addEventListener("abort", onClientAbort, { once: true });
    }
    const disarmHeadersTimer = () => {
      clearTimeout(headersTimer);
      signal?.removeEventListener("abort", onClientAbort);
    };
    try {
      upstreamRes = await relayFetch(relay, outboundUrl, {
        method: "POST",
        headers,
        body: outboundBody,
        duplex: "half",
        signal: chatCtl.signal,
      }, isRelayAllowed(security));
      disarmHeadersTimer();
    } catch {
      disarmHeadersTimer();
      transientError = { status: 502, message: "upstream request failed" };
      const next = selectFailover(
        catalog, current, currentUpstream, tried, security, failoverAllowed,
        502, requestStartedAt, log,
      );
      if (!next) break;
      tried.add(next.id);
      current = next;
      currentUpstream = catalog.upstreamBySource(current.source)!;
      continue;
    }

    if (upstreamRes.status >= 400) {
      const text = await upstreamRes.text();
      let errorMsg = `upstream returned HTTP ${upstreamRes.status}`;
      try {
        const json = JSON.parse(text);
        if (!isStrictSecurity(security)) {
          errorMsg = json?.error?.message ?? json?.message ?? text.slice(0, 256) ?? errorMsg;
        }
      } catch {
        if (!isStrictSecurity(security) && text) errorMsg = text.slice(0, 256);
      }

      const refused = upstreamRes.status === 401 || upstreamRes.status === 403;
      const capabilityRejected =
        (needsTools && TOOLS_UNSUPPORTED_RE.test(errorMsg)) || (needsVision && VISION_UNSUPPORTED_RE.test(errorMsg));
      // The upstream retired this model id. It will never answer it again, so
      // the request belongs to the mirror that still serves the SAME id. This
      // is checked before the transient verdict because a retirement is not
      // transient — a 410 would otherwise return straight to the client.
      const zenRetired =
        currentUpstream.id === "zen" &&
        (upstreamRes.status === 410 ||
          (upstreamRes.status === 400 && ZEN_MODEL_RETIRED_RE.test(errorMsg)));
      const transient = refused || capabilityRejected || upstreamRes.status === 429 || upstreamRes.status >= 500;
      // Seconds the client should wait before retrying this model, taken from
      // the upstream's Retry-After. Kept even when failover finds a
      // replacement, so the caller learns the bucket is a daily one.
      const upstreamRetryAfterMs =
        upstreamRes.status === 429
          ? parseRetryAfterMs(upstreamRes.headers.get("retry-after")) ?? retryAfterFromMessage(errorMsg)
          : undefined;
      if (!transient && !zenRetired) {
        log.warn("upstream rejected", {
          model: current.id,
          upstream: currentUpstream.id,
          status: upstreamRes.status,
          durationMs: Date.now() - requestStartedAt,
          upstreamError: errorMsg,
        });
        return { status: upstreamRes.status, message: errorMsg, relay: servingRelay };
      }

      if (zenRetired) {
        // Remember it: the next request goes straight to the mirror instead of
        // paying for the same rejection again.
        catalog.markRetired(current.id);
        log.warn("upstream retired the model - handing it to the mirror", {
          model: current.id,
          upstream: currentUpstream.id,
          status: upstreamRes.status,
          durationMs: Date.now() - requestStartedAt,
          upstreamError: errorMsg,
        });
      }

      if (refused) {
        catalog.markRefused(current.id, { status: upstreamRes.status, message: errorMsg });
      } else if (upstreamRes.status === 429) {
        const retryAfterMs = upstreamRetryAfterMs;
        // Free-tier exhaustion is a daily bucket, not a burst limit: park the
        // model until the upstream's refill moment instead of the flat 15
        // minutes, so the next request and every probe tick stop paying to
        // relearn that the bucket is empty.
        //
        // But the upstream's bucket is keyed by (egress address, model) - the
        // fleet has ~160 addresses, each with its own daily row (measured:
        // one address served >= 800 requests/day without a 429). A 429 on the
        // addresses this request happened to try is therefore NOT evidence
        // that the model is exhausted fleet-wide, and parking it on that
        // evidence kills a lane whose other addresses still have room.
        //
        // relayFetch says which case it is: the FLEET_EXHAUSTED_HEADER is set
        // only when every candidate address was tried and every one of them
        // answered 429. Without that header the model stays alive and the
        // NEXT request spreads onto a different address.
        const fleetExhausted = upstreamRes.headers.get(FLEET_EXHAUSTED_HEADER) === "1";
        if (isQuotaExhaustion(upstreamRes.status, text, retryAfterMs) && fleetExhausted) {
          catalog.markQuotaExhausted(current.id, retryAfterMs, { status: upstreamRes.status, message: errorMsg });
          // Close the probe lane for the upstream's own refill window so the
          // watchers stop spending requests from an empty bucket. Without this
          // the lane stays quiet only until someone runs the gate by hand.
          if (currentUpstream.id === "zen") {
            noteZenQuotaExhaustedForModel(current.id, retryAfterMs);
          }
        } else if (upstreamRes.status === 429) {
          // Either a burst limit, or one address's bucket is spent while the
          // rest of the fleet still has room. Park it briefly (not for the
          // daily window) so the next request moves on to another address.
          catalog.markRateLimited(current.id, retryAfterMs, { status: upstreamRes.status, message: errorMsg });
        }
      }
      transientError = {
        status: upstreamRes.status,
        message: errorMsg,
        ...(upstreamRetryAfterMs !== undefined
          ? { retryAfter: Math.ceil(upstreamRetryAfterMs / 1000) }
          : {}),
      };

      // The zen pool is a daily per-IP request budget. When it is spent, the
      // upstream is CLOSED for every egress identity — there is no route to
      // the model left. But the same model is often served by another free
      // upstream under a different id, and that upstream has its own quota.
      //
      // So before giving up on the model, try the mirror: the SAME model id,
      // answered by a different free transport. This is not failover to a
      // substitute model — the client asked for "beam-free" and gets an
      // answer for "beam-free", from an upstream that still has room.
      //
      // The same applies when the upstream RETIRED the model (410
      // ModelDeprecated, or 400 "Model is unavailable"): that upstream will
      // never answer this id again, but a mirror still can, under the same id.
      //
      // The zen STAGE comes first: it is opencode's own console on a
      // different deployment, so it answers the SAME id from its own daily
      // bucket. A closed production bucket is exactly the case it exists for.
      if (
        currentUpstream.id === "zen" &&
        zenStage &&
        zenStageAllowed &&
        (upstreamRes.status === 429 || zenRetired)
      ) {
        const staged = await tryZenStageFor(
          current,
          zenStage,
          sanitizedBody,
          req,
          log,
          requestStartedAt,
          callerTools,
          security,
        relay,
        );
        if (staged) {
          return { response: staged, model: current, upstream: zenStage.upstream, relay: "" };
        }
      }
      if (
        currentUpstream.id === "zen" &&
        zenMirror &&
        zenFailoverAllowed &&
        (upstreamRes.status === 429 || zenRetired)
      ) {
        const mirrored = await tryZenMirrorForParked(
          current,
          zenMirror,
          sanitizedBody,
          req,
          log,
          requestStartedAt,
        );
        if (mirrored) {
          return { response: mirrored, model: current, upstream: zenMirror.upstream, relay: "" };
        }
      }

      const next = selectFailover(
        catalog, current, currentUpstream, tried, security, failoverAllowed,
        upstreamRes.status, requestStartedAt, log, errorMsg,
      );
      if (!next) break;
      tried.add(next.id);
      current = next;
      currentUpstream = catalog.upstreamBySource(current.source)!;
      continue;
    }

    // some upstreams answer 200 with an error envelope and no choices; fail over on it
    if ((upstreamRes.headers.get("content-type") ?? "").includes("json")) {
      let envelopeError: string | null = null;
      try {
        const peek = (await upstreamRes.clone().json()) as { error?: { message?: string }; choices?: unknown };
        if (peek && typeof peek === "object" && peek.error && !peek.choices) {
          envelopeError = peek.error.message ?? "upstream returned an error under HTTP 200";
        }
      } catch {
        // unreadable or not json: let the normal path handle the original
      }
      if (envelopeError) {
        transientError = { status: 502, message: envelopeError };
        const next = selectFailover(
          catalog, current, currentUpstream, tried, security, failoverAllowed,
          502, requestStartedAt, log, envelopeError,
        );
        if (!next) break;
        tried.add(next.id);
        current = next;
        currentUpstream = catalog.upstreamBySource(current.source)!;
        continue;
      }
    }

    const peeked = await peekStreamError(upstreamRes);
    if (peeked.error) {
      transientError = { status: 502, message: peeked.error };
      const next = selectFailover(
        catalog, current, currentUpstream, tried, security, failoverAllowed,
        502, requestStartedAt, log, peeked.error,
      );
      if (!next) break;
      tried.add(next.id);
      current = next;
      currentUpstream = catalog.upstreamBySource(current.source)!;
      continue;
    }
    upstreamRes = peeked.response;

    const forcedStream = !sanitizedBody.stream && transformedBody.stream === true;
    let finalResponse = upstreamRes;
    if (responsesWire) {
      finalResponse = await toChatResponse(
        upstreamRes,
        current.id,
        sanitizedBody.stream === true || forcedStream,
      );
      if (forcedStream) {
        finalResponse = await streamToChatResponse(finalResponse, current.id, callerTools);
      }
    } else if (forcedStream) {
      finalResponse = await streamToChatResponse(upstreamRes, current.id, callerTools);
    }

    // A zen request that made it through means the daily bucket has room
    // again: reopen the probe lane so the watchers resume on their own.
    //
    // Scoped to this model's row: the upstream meters each row separately, so
    // a success on "bi" says nothing about "ne".
    if (currentUpstream.id === "zen") noteZenQuotaRecovered(current.id);

    return {
      response: finalResponse,
      model: current,
      upstream: currentUpstream,
      relay: servingRelay,
    };
  }

  return transientError ?? { status: 502, message: "no upstream candidates left" };
}

async function handleChat(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  catalog: RuntimeCatalog,
  log: Logger,
  security: SecurityConfig,
  signal: AbortSignal,
): Promise<void> {
  let bodyText: string;
  try {
    bodyText = await readBody(req);
  } catch {
    sendJson(res, 413, { error: { message: "request body too large" } });
    return;
  }

  let body: unknown;
  try {
    body = JSON.parse(bodyText);
  } catch {
    sendJson(res, 400, { error: { message: "invalid JSON body" } });
    return;
  }

  const parsed = parseChatTurn(body);
  if (!parsed.ok) {
    sendJson(res, 400, { error: { message: parsed.error } });
    return;
  }

  const requestStartedAt = Date.now();
  const sanitizedBody = sanitizeChatBody(
    body as Record<string, unknown>,
    catalog.resolve(parsed.value.model)?.compat.supportsDeveloperRole ?? false,
  ) as Record<string, unknown>;
  if (parsed.value.stream) {
    sanitizedBody.stream_options = { include_usage: true };
  }

  const callerTools = callerToolsFromBody(body);
  const result = await runChatForward(
    req,
    catalog,
    log,
    security,
    parsed.value.model,
    sanitizedBody,
    signal,
  );
  if ("status" in result) {
    activity.record({
      kind: "chat",
      model: parsed.value.model,
      requestedModel: parsed.value.model,
      upstream: catalog.resolve(parsed.value.model)?.source ?? "unknown",
      relay: result.relay,
      inputTokens: 0,
      outputTokens: 0,
      durationMs: Date.now() - requestStartedAt,
      status: "error",
      statusCode: result.status,
    });
    sendJson(res, result.status, {
      error: { message: result.message, type: result.type ?? "upstream_error", status: result.status },
      ...(result.secretTypes ? { secret_types: result.secretTypes } : {}),
    }, result.retryAfter !== undefined ? { "retry-after": String(result.retryAfter) } : {});
    return;
  }

  const { response: upstreamRes, model: current, upstream: currentUpstream } = result;
  const model = catalog.resolve(parsed.value.model) ?? current;
  log.info("chat -> upstream", {
    model: current.id,
    upstream: currentUpstream.id,
    stream: parsed.value.stream,
  });

  const contentType = upstreamRes.headers.get("content-type") ?? "application/json";
  res.writeHead(upstreamRes.status, { "content-type": contentType, ...corsFor(res) });

  if (!parsed.value.stream) {
    // non-stream: buffer once to read usage, then forward the exact bytes
    const text = await upstreamRes.text();
    let outText = text;
    try {
      const json = JSON.parse(text);
      const msg = json?.choices?.[0]?.message;
      if (msg && typeof msg.content === "string" && msg.content.length === 0 && !msg.tool_calls?.length) {
        const rt = extractReasoningText(msg);
        if (rt) {
          // patch only the message object, passing every other field through byte-exact
          json.choices[0].message = { ...msg, content: rt };
          outText = JSON.stringify(json);
        }
      }
      const usage = extractUsage(json);
      if (usage) {
        const fields: Record<string, unknown> = {
          model: current.id,
          upstream: currentUpstream.id,
          durationMs: Date.now() - requestStartedAt,
          ...usage,
        };
        if (current.id !== model.id) fields.failoverFrom = model.id;
        log.info("chat done", fields);
        activity.record({
          kind: "chat",
          model: current.id,
          requestedModel: model.id,
          upstream: currentUpstream.id,
          relay: result.relay,
          inputTokens: usage.inputTokens,
          outputTokens: usage.outputTokens,
          durationMs: Date.now() - requestStartedAt,
          status: "ok",
          ...(current.id !== model.id ? { failoverFrom: model.id } : {}),
        });
      }
    } catch {
      // usage is informational only; the plain response still goes out
    }
    res.end(outText);
    return;
  }

  if (upstreamRes.body) {
    const src = Readable.fromWeb(
      upstreamRes.body as import("node:stream/web").ReadableStream,
    );
    const usageTx = logUsageTransform(current.id, currentUpstream.id, log, requestStartedAt, model.id, activity, "chat", result.relay);
    const filterTx = filterInjectedToolCallsTransform(callerTools);
    let closing = false;
    // a mid-stream upstream failure ends the client response, not the daemon; client abort releases the upstream
    const failStream = () => {
      if (closing) return;
      closing = true;
      if (signal.aborted) {
        try {
          res.destroy();
        } catch {
          // response already gone
        }
        return;
      }
      log.warn("upstream stream interrupted", {
        model: current.id,
        upstream: currentUpstream.id,
        status: 502,
      });
      // Close the client stream cleanly instead of resetting it. Destroying the response
      // turned an upstream reset into a broken socket on the caller's side, which strict
      // translators report as a failed stream with no verdict. Ending the chain lets the
      // terminal transform synthesise finish_reason, so a cut answer arrives as a
      // truncation the client can continue from.
      try {
        src.unpipe(filterTx);
        src.destroy();
        if (!filterTx.destroyed) filterTx.end();
      } catch {
        try {
          res.destroy();
        } catch {
          // response already gone
        }
      }
    };
    src.on("error", failStream);
    usageTx.on("error", failStream);
    res.on("error", () => src.destroy());
    signal.addEventListener("abort", () => src.destroy());
    filterTx.on("error", failStream);
    // Filter BEFORE the fold: upstreams inject tool calls the caller never declared, and
    // this filter drops them. Folding first meant `sawToolCalls` latched on those phantom
    // calls (the fold declined) and the filter then removed them, so a reasoning-only answer
    // reached the client as an empty `content` with finish_reason=stop.
    src.pipe(filterTx).pipe(reasoningToContentTransform()).pipe(usageTx).pipe(res);
  } else {
    res.end();
  }
}

// codex responses wire -> openai chat -> forward -> translate back to responses output
async function handleResponses(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  catalog: RuntimeCatalog,
  log: Logger,
  security: SecurityConfig,
  signal: AbortSignal,
): Promise<void> {
  let bodyText: string;
  try {
    bodyText = await readBody(req);
  } catch {
    sendJson(res, 413, { error: { message: "request body too large" } });
    return;
  }

  let body: unknown;
  try {
    body = JSON.parse(bodyText);
  } catch {
    sendJson(res, 400, { error: { message: "invalid JSON body" } });
    return;
  }

  const parsed = parseResponsesTurn(body);
  if (!parsed.ok) {
    sendJson(res, 400, { error: { message: parsed.error } });
    return;
  }

  const requestStartedAt = Date.now();
  const target = catalog.resolve(parsed.value.model);
  const supportsDev = target?.compat.supportsDeveloperRole ?? false;

  const chatMessages: any[] = [];
  if (parsed.value.system) {
    chatMessages.push({ role: "system", content: parsed.value.system });
  }
  for (const m of parsed.value.messages) {
    chatMessages.push({
      role: m.role,
      content: typeof m.content === "string"
        ? m.content
        : m.content.map((b) => (b.type === "text" ? b : { type: "image_url", image_url: { url: b.url } })),
      ...(m.toolCallId ? { tool_call_id: m.toolCallId } : {}),
      ...(m.role === "assistant" ? { reasoning_content: m.thinking ?? "" } : {}),
      ...(m.toolCalls
        ? { tool_calls: m.toolCalls.map((tc) => ({
            id: tc.id,
            type: "function",
            function: { name: tc.name, arguments: tc.arguments },
          })) }
        : {}),
    });
  }

  const sanitizedBody: Record<string, unknown> = sanitizeChatBody(
    {
      model: parsed.value.model,
      messages: chatMessages,
      ...(parsed.value.tools
        ? {
            tools: parsed.value.tools.map((t) => ({
              type: "function",
              function: { name: t.name, description: t.description, parameters: t.parameters },
            })),
          }
        : {}),
      ...(parsed.value.maxTokens ? { max_tokens: parsed.value.maxTokens } : {}),
      ...(parsed.value.reasoningEffort ? { reasoning_effort: parsed.value.reasoningEffort } : {}),
      stream: parsed.value.stream,
    },
    supportsDev,
  ) as Record<string, unknown>;
  if (parsed.value.stream) {
    sanitizedBody.stream_options = { include_usage: true };
  }

  const callerTools = callerToolsFromBody(sanitizedBody);
  const result = await runChatForward(
    req,
    catalog,
    log,
    security,
    parsed.value.model,
    sanitizedBody,
    signal,
  );
  if ("status" in result) {
    activity.record({
      kind: "responses",
      model: parsed.value.model,
      requestedModel: parsed.value.model,
      upstream: catalog.resolve(parsed.value.model)?.source ?? "unknown",
      relay: result.relay,
      inputTokens: 0,
      outputTokens: 0,
      durationMs: Date.now() - requestStartedAt,
      status: "error",
      statusCode: result.status,
    });
    sendJson(res, result.status, {
      error: { message: result.message, type: result.type ?? "upstream_error", status: result.status },
      ...(result.secretTypes ? { secret_types: result.secretTypes } : {}),
    }, result.retryAfter !== undefined ? { "retry-after": String(result.retryAfter) } : {});
    return;
  }

  const { response: upstreamRes, model: current, upstream: currentUpstream } = result;
  const resolved = catalog.resolve(parsed.value.model) ?? current;
  log.info("responses -> upstream", {
    model: current.id,
    upstream: currentUpstream.id,
    stream: parsed.value.stream,
  });

  if (!parsed.value.stream) {
    const text = await upstreamRes.text();
    let json: any;
    try {
      json = JSON.parse(text);
    } catch {
      sendJson(res, 502, { error: { message: "invalid upstream response" } });
      return;
    }
    const usage = extractUsage(json);
    if (usage) {
      const fields: Record<string, unknown> = {
        model: current.id,
        upstream: currentUpstream.id,
        durationMs: Date.now() - requestStartedAt,
        ...usage,
      };
      if (current.id !== resolved.id) fields.failoverFrom = resolved.id;
      log.info("responses done", fields);
      activity.record({
        kind: "responses",
        model: current.id,
        requestedModel: resolved.id,
        upstream: currentUpstream.id,
        relay: result.relay,
        inputTokens: usage.inputTokens,
        outputTokens: usage.outputTokens,
        durationMs: Date.now() - requestStartedAt,
        status: "ok",
        ...(current.id !== resolved.id ? { failoverFrom: resolved.id } : {}),
      });
    }
    const out = renderResponse(json, current.id);
    res.writeHead(upstreamRes.status, { "content-type": "application/json", ...corsFor(res) });
    res.end(JSON.stringify(out));
    return;
  }

  res.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
    "connection": "keep-alive",
    ...corsFor(res),
  });
  const encoder = new ResponsesStreamEncoder(callerTools);
  let first = true;
  let failed = false;
  let streamUsage: TokenUsage | null = null;
  try {
    if (upstreamRes.body) {
      for await (const frame of readSseStream(
        upstreamRes.body as unknown as import("node:stream/web").ReadableStream,
      )) {
        if (signal.aborted) break;
        if (frame.data === "[DONE]") continue;
        let json: any;
        try { json = JSON.parse(frame.data); } catch { continue; }
        if (first) {
          first = false;
          for (const ev of encoder.open(current.id)) res.write(ev);
        }
        const streamError = sseErrorMessage(json);
        if (streamError) {
          failed = true;
          log.warn("upstream stream error", { model: current.id, upstream: currentUpstream.id, upstreamError: streamError });
          for (const ev of encoder.fail(streamError)) res.write(ev);
          break;
        }
        const usage = extractUsage(json);
        if (usage) streamUsage = usage;
        for (const ev of encoder.push(json)) res.write(ev);
      }
    }
  } catch {
    if (!signal.aborted) {
      log.warn("upstream stream interrupted", {
        model: current.id,
        upstream: currentUpstream.id,
        status: 502,
      });
    }
  }
  if (!failed) for (const ev of encoder.close()) res.write(ev);
  if (streamUsage) {
    const fields: Record<string, unknown> = {
      model: current.id,
      upstream: currentUpstream.id,
      durationMs: Date.now() - requestStartedAt,
      ...streamUsage,
    };
    if (current.id !== resolved.id) fields.failoverFrom = resolved.id;
    log.info("responses done", fields);
    activity.record({
      kind: "responses",
      model: current.id,
      requestedModel: resolved.id,
      upstream: currentUpstream.id,
      relay: result.relay,
      inputTokens: streamUsage.inputTokens,
      outputTokens: streamUsage.outputTokens,
      durationMs: Date.now() - requestStartedAt,
      status: "ok",
      ...(current.id !== resolved.id ? { failoverFrom: resolved.id } : {}),
    });
  }
  res.end();
}

// anthropic messages in -> translate to openai chat -> forward -> translate back
async function handleAnthropic(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  catalog: RuntimeCatalog,
  log: Logger,
  security: SecurityConfig,
  signal: AbortSignal,
): Promise<void> {
  let bodyText: string;
  try {
    bodyText = await readBody(req);
  } catch {
    sendAnthropicError(res, 413, "request body too large");
    return;
  }

  let body: unknown;
  try {
    body = JSON.parse(bodyText);
  } catch {
    sendAnthropicError(res, 400, "invalid JSON body");
    return;
  }

  const parsed = parseAnthropicRequest(body);
  if (!parsed.ok) {
    sendAnthropicError(res, 400, parsed.error);
    return;
  }

  const model = catalog.resolve(parsed.value.model);
  if (!model) {
    sendAnthropicError(res, 400, `unknown model: ${parsed.value.model}`);
    return;
  }

  const upstream = catalog.upstreamBySource(model.source);
  if (!upstream) {
    sendAnthropicError(res, 502, `no upstream for source: ${model.source}`);
    return;
  }

  const requestStartedAt = Date.now();
  const chatBody = parsed.value.chatBody as Record<string, unknown>;
  chatBody.model = model.id;
  // cap max_tokens to the model's limit so a stale client value never reaches the upstream
  if (typeof chatBody.max_tokens === "number" && chatBody.max_tokens > model.maxTokens) {
    chatBody.max_tokens = model.maxTokens;
  }

  const callerTools = callerToolsFromBody(chatBody);
  const result = await runChatForward(
    req,
    catalog,
    log,
    security,
    parsed.value.model,
    chatBody,
    signal,
  );
  if ("status" in result) {
    activity.record({
      kind: "anthropic",
      model: parsed.value.model,
      requestedModel: parsed.value.model,
      upstream: model?.source ?? "unknown",
      relay: result.relay,
      inputTokens: 0,
      outputTokens: 0,
      durationMs: Date.now() - requestStartedAt,
      status: "error",
      statusCode: result.status,
    });
    sendAnthropicError(res, result.status, result.message, result.secretTypes);
    return;
  }

  const { response: upstreamRes, model: current, upstream: currentUpstream } = result;
  log.info("anthropic -> upstream", {
    model: current.id,
    upstream: currentUpstream.id,
    stream: parsed.value.stream,
  });

  if (!parsed.value.stream) {
    const text = await upstreamRes.text();
    let json: any;
    try {
      json = JSON.parse(text);
    } catch {
      sendAnthropicError(res, 502, "invalid upstream response");
      return;
    }
    const message = openAiCompletionToAnthropicMessage(json, current.id);
    const usage = extractUsage(json);
    if (usage) {
      const fields: Record<string, unknown> = {
        model: current.id,
        upstream: currentUpstream.id,
        durationMs: Date.now() - requestStartedAt,
        ...usage,
      };
      if (current.id !== model.id) fields.failoverFrom = model.id;
      log.info("anthropic done", fields);
      activity.record({
        kind: "anthropic",
        model: current.id,
        requestedModel: model.id,
        upstream: currentUpstream.id,
        relay: result.relay,
        inputTokens: usage.inputTokens,
        outputTokens: usage.outputTokens,
        durationMs: Date.now() - requestStartedAt,
        status: "ok",
        ...(current.id !== model.id ? { failoverFrom: model.id } : {}),
      });
    }
    sendJson(res, upstreamRes.status === 200 ? 200 : upstreamRes.status, message);
    return;
  }

  res.writeHead(upstreamRes.status, {
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
    "connection": "keep-alive",
    ...corsFor(res),
  });
  const encoder = new AnthropicStreamEncoder(callerTools);
  let streamUsage: TokenUsage | null = null;
  let streamClosed = false;
  try {
    if (upstreamRes.body) {
      for await (const frame of readSseStream(
        upstreamRes.body as unknown as import("node:stream/web").ReadableStream,
      )) {
        if (signal.aborted) break;
        if (frame.data === "[DONE]") {
          streamClosed = true;
          for (const ev of encoder.close()) res.write(ev);
          break;
        }
        let json: any;
        try { json = JSON.parse(frame.data); } catch { continue; }
        const streamError = sseErrorMessage(json);
        if (streamError) {
          streamClosed = true;
          log.warn("upstream stream error", { model: current.id, upstream: currentUpstream.id, upstreamError: streamError });
          for (const ev of encoder.fail(streamError)) res.write(ev);
          break;
        }
        const usage = extractUsage(json);
        if (usage) streamUsage = usage;
        for (const ev of encoder.push(json, current.id)) res.write(ev);
      }
    }
  } catch {
    if (!signal.aborted) {
      log.warn("upstream stream interrupted", {
        model: current.id,
        upstream: currentUpstream.id,
        status: 502,
      });
    }
  }
  // always emit the closing Anthropic events (some upstreams omit [DONE]) so clients do not hang
  if (!streamClosed) {
    for (const ev of encoder.close()) res.write(ev);
  }
  if (streamUsage) {
    const fields: Record<string, unknown> = {
      model: current.id,
      upstream: currentUpstream.id,
      durationMs: Date.now() - requestStartedAt,
      ...streamUsage,
    };
    if (current.id !== model.id) fields.failoverFrom = model.id;
    log.info("anthropic done", fields);
    activity.record({
      kind: "anthropic",
      model: current.id,
      requestedModel: model.id,
      upstream: currentUpstream.id,
      relay: result.relay,
      inputTokens: streamUsage.inputTokens,
      outputTokens: streamUsage.outputTokens,
      durationMs: Date.now() - requestStartedAt,
      status: "ok",
      ...(current.id !== model.id ? { failoverFrom: model.id } : {}),
    });
  }
  res.end();
}

export function anthropicErrorType(status: number): string {
  if (status === 401) return "authentication_error";
  if (status === 403) return "permission_error";
  if (status === 404) return "not_found_error";
  if (status === 413) return "request_too_large";
  if (status === 429) return "rate_limit_error";
  if (status === 503 || status === 529) return "overloaded_error";
  if (status >= 500) return "api_error";
  return "invalid_request_error";
}

function sendAnthropicError(
  res: http.ServerResponse,
  status: number,
  message: string,
  secretTypes?: SecretType[],
): void {
  sendJson(res, status, {
    type: "error",
    error: { type: anthropicErrorType(status), message },
    ...(secretTypes ? { secret_types: secretTypes } : {}),
  });
}

export function createServer(opts: ServerOptions): http.Server {
  const { catalog, rateLimiter, port, log, startedAt } = opts;
  const security = opts.security ?? DEFAULT_SECURITY_CONFIG;
  activity = opts.activity ?? new ActivityStore();
  const adminOptions: AdminOptions = {
    bind: "127.0.0.1",
    refreshIntervalMs: 0,
    ...opts.admin,
    security,
    catalog,
    port,
  };

  const visibleRelayState = () => {
    const relay = loadRelayState();
    return {
      ...relay,
      enabled: isRelayAllowed(security) ? relay.enabled : false,
      securityMode: security.mode,
      locked: !isRelayAllowed(security),
    };
  };

  return http.createServer((req, res) => {
    (res as CorsAwareResponse).bansosCors = corsHeadersForOrigin(req.headers.origin);
    const clientSignal = clientDisconnectSignal(res);
    const ip = req.socket.remoteAddress ?? "unknown";
    const method = req.method ?? "";

    // bansos-opt: loopback exempt (single-user daemon; UI polling + chat shared the
    // 300/min bucket and starved every other endpoint)
    const loopbackClient = ip === "127.0.0.1" || ip === "::1" || ip.startsWith("::ffff:127.");
    if (!loopbackClient && !rateLimiter.check(ip)) {
      log.warn("rate limit exceeded", { ip });
      sendJson(res, 429, { error: { message: "rate limit exceeded" } });
      return;
    }

    if (!ALLOWED_METHODS.has(method)) {
      sendJson(res, 405, { error: { message: "method not allowed" } });
      return;
    }

    if (method === "OPTIONS") {
      const cors = corsFor(res);
      res.writeHead(204, cors);
      res.end();
      return;
    }

    // DNS-rebinding guard: loopback clients must present a loopback (or own-interface) Host
    if (!isHostTrusted(req)) {
      sendJson(res, 403, { error: { message: "forbidden host" } });
      return;
    }

    if (method === "POST" && !isOriginTrusted(req)) {
      sendJson(res, 403, { error: { message: "cross-site request blocked" } });
      return;
    }

    const rawUrl = req.url ?? "/";
    if (!validatePath(rawUrl)) {
      sendJson(res, 403, { error: { message: "forbidden" } });
      return;
    }
    const cleanUrl = rawUrl.split("?")[0] ?? "/";
    const url = cleanUrl.replace(/\/+$/, "");

    if (method === "GET" && (url === "/v1/models" || url === "/models")) {
      const listAll = new URL(rawUrl, "http://127.0.0.1").searchParams.has("all");
      sendJson(res, 200, {
        object: "list",
        data: (listAll ? catalog.models : catalog.servableModels).map((m) => ({
          id: m.id,
          object: "model",
          created: 0,
          owned_by: m.source,
          source: m.source,
          name: m.name,
          context_window: m.contextWindow,
          context_length: m.contextWindow,
          max_tokens: m.maxTokens,
          maxTokens: m.maxTokens,
          reasoning: m.reasoning,
          input: m.input,
          supports_tools: m.supportsTools !== false,
        })),
      });
      return;
    }

    if (url === "/healthz") {
      const relay = visibleRelayState();
      sendJson(res, 200, {
        status: "ok",
        uptimeSeconds: Math.floor((Date.now() - startedAt) / 1000),
        modelCount: catalog.servableModels.length,
        relay: { enabled: relay.enabled, url: relay.url },
        security: {
          mode: security.mode,
          allowedUpstreams: security.allowedUpstreams,
          allowCrossProviderFailover: security.allowCrossProviderFailover,
        },
      });
      return;
    }

    if (url === "/bansos/status") {
      const relay = visibleRelayState();
      const payload: StatusPayload = {
        status: "ok",
        uptimeSeconds: Math.floor((Date.now() - startedAt) / 1000),
        port,
        modelCount: catalog.servableModels.length,
        models: catalog.servableModels.map((m) => m.id),
        providers: providerCounts(catalog, security),
        relay: { enabled: relay.enabled, url: relay.url },
        ...(zenMirror
          ? { zenMirror: Object.fromEntries(zenMirror.mirrors) }
          : {}),
        ...(zenStage
          ? { zenStage: { upstream: zenStage.upstream.id, models: zenStage.ids() } }
          : {}),
        security: {
          mode: security.mode,
          dlp: security.dlp,
          allowedUpstreams: security.allowedUpstreams,
          allowCrossProviderFailover: security.allowCrossProviderFailover,
        },
      };
      sendJson(res, 200, payload);
      return;
    }

    if (method === "POST" && url === "/bansos/refresh") {
      void catalog
        .refresh()
        .then((report) => {
          sendJson(res, 200, {
            refreshed: true,
            modelCount: catalog.servableModels.length,
            alive: report.alive,
          });
        })
        .catch((err: unknown) => {
          sendJson(res, 500, { error: { message: `refresh failed: ${String(err)}` } });
        });
      return;
    }


    if (method === "GET" && (url === "/bansos/adapters")) {
      const statuses = harnessStatuses();
      sendJson(
        res,
        200,
        ADAPTERS.map((a) => ({
          id: a.id,
          name: a.name,
          wire: a.wire,
          modelMode: a.modelMode,
          configPaths: a.configPaths,
          status: statuses.find((h) => h.id === a.id)?.status ?? "missing",
        })),
      );
      return;
    }

    if (method === "GET" && url === "/bansos/adapters/render") {
      const parsedUrl = new URL(rawUrl, "http://127.0.0.1");
      const id = parsedUrl.searchParams.get("id");
      const model = parsedUrl.searchParams.get("model") || undefined;
      if (!id) {
        sendJson(res, 400, { error: { message: "missing adapter id query parameter" } });
        return;
      }
      const adapter = findAdapter(id);
      if (!adapter) {
        sendJson(res, 404, { error: { message: `adapter "${id}" not found` } });
        return;
      }
      const reqHost = req.headers.host || `127.0.0.1:${port}`;
      const isHttps = Boolean((req.socket as import("node:tls").TLSSocket)?.encrypted);
      const proto = isHttps ? "https" : "http";
      const ctx = buildSetupContext(`${proto}://${reqHost}/v1`, catalog.servableModels, {
        model,
        models: parsedUrl.searchParams.getAll("models"),
        upstreams: parsedUrl.searchParams.getAll("upstream"),
      });
      if ("error" in ctx) {
        sendJson(res, 400, { error: { message: ctx.error } });
        return;
      }
      const config = adapter.render(ctx);
      sendJson(res, 200, {
        id: adapter.id,
        name: adapter.name,
        wire: adapter.wire,
        config,
      });
      return;
    }

    if (method === "GET" && url === "/bansos/relay") {
      // retry carries the in-request failover state: `preferred` set while it
      // differs from `url` means the active relay is NOT the one serving traffic
      // (zen meters per relay, so a spent relay 429s and the request is served by
      // another). The heal job reads this — probing through the daemon returns 200
      // either way, so a plain probe can no longer see a dead active relay.
      sendJson(res, 200, { ...visibleRelayState(), retry: relayRetryState() });
      return;
    }

    if (method === "GET" && url === "/bansos/usage") {
      activity.checkDiskFreshness(); // bansos-opt3: the CLI may have written the file
      const parsedUrl = new URL(rawUrl, "http://127.0.0.1");
      const window = (parsedUrl.searchParams.get("window") || "all") as import("./activity").TimeWindow;
      sendJson(res, 200, activity.getUsage(window));
      return;
    }

    if (method === "GET" && url === "/bansos/events") {
      activity.checkDiskFreshness(); // bansos-opt3: the CLI may have written the file
      const parsedUrl = new URL(rawUrl, "http://127.0.0.1");
      const limitParam = Number(parsedUrl.searchParams.get("limit"));
      const window = (parsedUrl.searchParams.get("window") || "all") as import("./activity").TimeWindow;
      const limit = Number.isFinite(limitParam) && limitParam > 0 ? Math.min(limitParam, 500) : 100;
      sendJson(res, 200, { events: activity.getEvents(limit, window) });
      return;
    }

    if (method === "POST" && url === "/bansos/relay/probe") {
      if (!isRelayAllowed(security)) {
        sendJson(res, 403, {
          error: { message: "relay is disabled by strict security mode", type: "security_policy_error" },
        });
        return;
      }
      void runRequest(
        readBody(req).then(async (bodyText) => {
          let targetUrl = "";
          if (bodyText) {
            try {
              const parsed = JSON.parse(bodyText) as { url?: string };
              targetUrl = parsed.url || "";
            } catch {
              sendJson(res, 400, { error: { message: "invalid json body" } });
              return;
            }
          }
          if (!targetUrl) {
            const current = loadRelayState();
            targetUrl = current.url;
          }
          if (!targetUrl) {
            sendJson(res, 400, { error: { message: "no url specified or active" } });
            return;
          }

          const policy = probeTargetAllowed(targetUrl, loadRelayState());
          if (!policy.allowed) {
            sendJson(res, 403, {
              error: { message: `relay probe blocked: ${policy.reason}`, type: "security_policy_error" },
            });
            return;
          }

          const start = performance.now();
          try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 7000);
            const upstreamRes = await fetch(targetUrl, {
              method: "GET",
              signal: controller.signal,
            });
            clearTimeout(timeoutId);
            const ms = Math.round(performance.now() - start);
            sendJson(res, 200, {
              ok: upstreamRes.status < 500,
              status: upstreamRes.status,
              latencyMs: ms,
            });
          } catch (err) {
            const ms = Math.round(performance.now() - start);
            sendJson(res, 200, {
              ok: false,
              latencyMs: ms,
              error: err instanceof Error ? err.message : "Unreachable",
            });
          }
        }),
        res,
        log,
        "relay probe",
      );
      return;
    }

    if (ADMIN_PATHS.has(url)) {
      void runRequest(
        handleAdmin(req, method, url, rawUrl, () => readBody(req, 1024 * 1024), adminOptions).then((result) => {
          sendJson(res, result.status, result.body);
          const after = result.after;
          if (after) {
            if (res.writableFinished) setImmediate(after);
            else res.once("finish", after);
          }
        }),
        res,
        log,
        "settings",
      );
      return;
    }

    if (method === "POST" && url === "/bansos/relay") {
      if (!isRelayAllowed(security)) {
        sendJson(res, 403, {
          error: { message: "relay mutation is disabled by strict security mode", type: "security_policy_error" },
        });
        return;
      }
      void runRequest(
        readBody(req).then((bodyText) => {
          let body: Record<string, unknown> = {};
          if (bodyText) {
            try {
              body = JSON.parse(bodyText) as Record<string, unknown>;
            } catch {
              sendJson(res, 400, { error: { message: "invalid json body" } });
              return;
            }
          }
          const updated = applyRelayMutation(loadRelayState(), body);
          if ("error" in updated) {
            sendJson(res, 400, { error: { message: updated.error } });
            return;
          }
          saveRelayState(updated);
          sendJson(res, 200, updated);
        }),
        res,
        log,
        "relay mutation",
      );
      return;
    }

    if (
      method === "GET" &&
      (url === "" ||
        url === "/index.html" ||
        cleanUrl.startsWith("/assets/") ||
        url === "/favicon.ico" ||
        url === "/favicon.svg" ||
        url === "/favicon.png" ||
        url === "/apple-touch-icon.png" ||
        url === "/manifest.json" ||
        url === "/robots.txt")
    ) {
      serveStaticUi(res, cleanUrl);
      return;
    }

    if (method === "POST" && (url === "/v1/responses" || url === "/responses")) {
      runRequest(handleResponses(req, res, catalog, log, security, clientSignal), res, log, "responses");
      return;
    }

    if (method === "POST" && (url === "/v1/chat/completions" || url === "/chat/completions")) {
      runRequest(handleChat(req, res, catalog, log, security, clientSignal), res, log, "chat");
      return;
    }

    if (method === "POST" && (url === "/v1/messages" || url === "/messages")) {
      runRequest(handleAnthropic(req, res, catalog, log, security, clientSignal), res, log, "anthropic");
      return;
    }

    const notFound = () => {
      const accept = req.headers.accept ?? "";
      if (accept.includes("text/html")) {
        const notFoundHtml = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Page Not Found | Bansos Router</title>
</head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;background:#111113;color:#f4f4f6;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0;padding:1.5rem;box-sizing:border-box;">
  <div style="max-width:400px;width:100%;text-align:center;background:#16161a;border:1px solid #23232a;border-radius:1rem;padding:2.5rem 2rem;box-shadow:0 20px 25px -5px rgba(0,0,0,0.5);">
    <div style="display:inline-flex;align-items:center;justify-content:center;width:3rem;height:3rem;border-radius:0.75rem;background:#202028;border:1px solid #2c2c36;margin-bottom:1.25rem;">
      <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#eab308" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="12" y1="8" x2="12" y2="12"></line><line x1="12" y1="16" x2="12.01" y2="16"></line></svg>
    </div>
    <h1 style="font-size:1.125rem;font-weight:700;margin:0 0 0.5rem 0;color:#ffffff;letter-spacing:-0.025em;">Page Not Found</h1>
    <p style="font-size:0.8125rem;color:#9393a0;margin:0 0 1.5rem 0;line-height:1.5;">This page does not exist.</p>
    <a href="/" style="display:inline-flex;align-items:center;justify-content:center;gap:0.5rem;background:#2b64e0;color:#ffffff;font-weight:600;font-size:0.8125rem;padding:0.625rem 1.25rem;border-radius:0.5rem;text-decoration:none;transition:background 0.15s ease;cursor:pointer;">
      <span>← Back to Dashboard</span>
    </a>
  </div>
</body>
</html>`;
        res.writeHead(404, {
          "content-type": "text/html; charset=utf-8",
          "content-length": Buffer.byteLength(notFoundHtml),
          ...corsFor(res),
        });
        res.end(notFoundHtml);
        return;
      }
      sendJson(res, 404, { error: { message: "not found" } });
    };

    notFound();
  });
}
