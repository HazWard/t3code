/**
 * t3code.ts — T3 Code's Pi extension.
 *
 * Loaded into every T3-spawned Pi RPC session via `pi --mode rpc -e
 * <this file>`. It stays inert everywhere else: the factory returns
 * without registering anything unless `T3CODE_PI_MANAGED=1` is present in
 * the Pi process environment (only the Pi adapter sets it).
 *
 * Three jobs:
 *   1. MCP bridge — Pi has no MCP support, so this file implements a
 *      minimal Streamable-HTTP MCP client against the session's `t3-code`
 *      MCP endpoint and registers each advertised tool via
 *      `pi.registerTool()`. Tools the session may not use fail server-side
 *      with a typed tool error instead of being filtered here.
 *   2. Permission gate — `tool_call` interception routes mutating tool
 *      calls through `ctx.ui.confirm` per the thread runtime mode, which
 *      in RPC mode surfaces as an `extension_ui_request` the adapter
 *      translates into T3 approvals.
 *   3. T3 context — `before_agent_start` appends the T3 runtime identity
 *      and preview/device/PR tool doctrine to the system prompt.
 *
 * Coupling note: the `T3CODE_*` environment contract below is duplicated
 * in `../Layers/PiAdapter.ts` (which sets it). This file must stay
 * self-contained — Pi loads it standalone, so it cannot import repo
 * modules. Keep both sides in sync.
 *
 * The bridge is hand-rolled rather than delegated to a generic MCP
 * extension: every mutating call has to be gated here and translated into
 * a T3 approval, which a broker would bypass or double-prompt. Revisit if
 * pi gains native MCP support.
 *
 * @module provider/pi/t3code
 */

// @effect-diagnostics globalFetch:off - Pi extensions run in plain Node with no Effect runtime; fetch is the only transport.
// @effect-diagnostics globalTimers:off - the per-call failsafe deadline is a plain timer this file owns and clears.

const MANAGED_ENV = "T3CODE_PI_MANAGED";
const MCP_ENDPOINT_ENV = "T3CODE_MCP_ENDPOINT";
const MCP_AUTH_ENV = "T3CODE_MCP_AUTH";
const RUNTIME_MODE_ENV = "T3CODE_RUNTIME_MODE";

const MCP_PROTOCOL_VERSION = "2025-06-18";

/** Failsafe budget for a single MCP call, so a hung server cannot wedge a turn. */
const CALL_TIMEOUT_MS = 300_000;

/** Shorter budget for teardown: shutdown must never wait on a dead socket. */
const SESSION_SHUTDOWN_TIMEOUT_MS = 5_000;

/**
 * Scope note: this is a deliberately partial MCP client. T3's server never
 * sends or accepts sampling, elicitation, roots, resource/prompt listing,
 * `list_changed` notifications, `logging/setLevel`, paginated `tools/list`
 * cursors, or OAuth. It also cannot push unsolicited notifications: the
 * transport is plain POST/response, with no standing SSE subscription. None of
 * that is implemented here on purpose — it would be dead code that reads like
 * capability the bridge does not have.
 */

/** Tools that never mutate anything and skip the approval prompt. */
const READ_ONLY_TOOLS: ReadonlySet<string> = new Set([
  "read",
  "grep",
  "find",
  "ls",
  "preview_status",
  "preview_snapshot",
  "device_list",
  "list_thread_pull_requests",
]);

/** Built-in tools that change files; `auto-accept-edits` auto-approves them. */
const FILE_TOOLS: ReadonlySet<string> = new Set(["edit", "write"]);

/**
 * Approval gate from the thread's runtime mode. `full-access` and `auto`
 * approve everything; `auto-accept-edits` still asks before commands and
 * other actions; anything else (including an unset mode, for non-T3 use)
 * asks before every mutating call. Read-only tools are never gated.
 */
function runtimeModeRequiresApproval(runtimeMode: string, toolName: string): boolean {
  if (READ_ONLY_TOOLS.has(toolName)) return false;
  if (runtimeMode === "full-access" || runtimeMode === "auto") return false;
  if (runtimeMode === "auto-accept-edits") {
    return !FILE_TOOLS.has(toolName);
  }
  return true;
}

interface PiToolContent {
  readonly type: string;
  readonly text?: string;
  readonly data?: string;
  readonly mimeType?: string;
}

interface PiToolResult {
  readonly content: ReadonlyArray<PiToolContent>;
  readonly details?: Record<string, unknown>;
  readonly isError?: boolean;
}

interface PiToolDefinition {
  readonly name: string;
  readonly label?: string;
  readonly description: string;
  readonly parameters: Record<string, unknown>;
  readonly execute: (
    toolCallId: string,
    params: Record<string, unknown>,
    signal?: AbortSignal,
    onUpdate?: (update: unknown) => void,
    ctx?: PiExtensionContext,
  ) => Promise<PiToolResult>;
}

interface PiUi {
  readonly notify: (message: string, type: string) => void;
  readonly confirm: (title: string, message?: string) => Promise<boolean>;
  readonly select: (title: string, options: ReadonlyArray<string>) => Promise<string | undefined>;
}

interface PiExtensionContext {
  readonly ui: PiUi;
}

interface PiToolCallEvent {
  readonly toolName: string;
  readonly toolCallId: string;
  readonly input: Record<string, unknown>;
}

interface PiBeforeAgentStartEvent {
  readonly prompt: string;
  readonly systemPrompt: string;
}

interface PiExtensionApi {
  readonly on: (event: string, handler: (event: never, ctx: PiExtensionContext) => unknown) => void;
  readonly registerTool: (tool: PiToolDefinition) => void;
}

interface McpToolDescription {
  readonly name: string;
  readonly description?: string;
  readonly inputSchema: Record<string, unknown>;
}

function readEnv(name: string): string {
  const value = (globalThis as unknown as { process?: { env?: Record<string, string> } }).process
    ?.env?.[name];
  return (value ?? "").trim();
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Transport state: an MCP session id only exists once `initialize` has landed. */
type McpSession =
  | { readonly state: "unconnected" }
  | { readonly state: "ready"; readonly sessionId: string };

/** HTTP-level MCP failure; `status` lets a 404 be retried exactly once. */
class McpHttpError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "McpHttpError";
    this.status = status;
  }
}

/**
 * Concatenates a frame's `data:` lines per the SSE spec. `event:` and `id:` are
 * ignored: plain POST/response carries one JSON-RPC message per frame and no
 * stream is ever resumed.
 */
function readSseFrameData(frame: string): string | undefined {
  const data: string[] = [];
  for (const line of frame.split("\n")) {
    if (line.startsWith("data:")) data.push(line.slice("data:".length).replace(/^ /, ""));
  }
  return data.length > 0 ? data.join("\n") : undefined;
}

/**
 * Returns the first frame that parses as a JSON-RPC message, falling back to
 * the last parseable value. Empty frames and `[DONE]` sentinels are skipped.
 */
function parseSseFrames(body: string): unknown {
  let last: unknown;
  for (const frame of body.replace(/\r\n/g, "\n").split("\n\n")) {
    const raw = readSseFrameData(frame);
    const payload = raw?.trim();
    if (!payload || payload === "[DONE]") continue;
    let parsed: unknown;
    try {
      parsed = JSON.parse(payload) as unknown;
    } catch {
      continue;
    }
    if (
      isRecord(parsed) &&
      ("jsonrpc" in parsed || "id" in parsed || "result" in parsed || "error" in parsed)
    ) {
      return parsed;
    }
    last = parsed;
  }
  return last;
}

/**
 * Parses a Streamable-HTTP MCP response. T3's server replies with a bare JSON
 * body (`RpcSerialization.layerJsonRpc` uses `application/json` with no
 * framing), so that is the hot path; the SSE framing below is a defensive
 * fallback for a spec-shaped proxy.
 */
function parseMcpResponseBody(body: string): unknown {
  const trimmed = body.trim();
  if (!trimmed) return undefined;
  try {
    return JSON.parse(trimmed) as unknown;
  } catch {
    return parseSseFrames(trimmed);
  }
}

function mcpContentToPiToolContent(content: unknown): PiToolContent[] {
  if (!Array.isArray(content)) return [{ type: "text", text: JSON.stringify(content) }];
  const out: PiToolContent[] = [];
  for (const block of content) {
    if (!isRecord(block)) {
      out.push({ type: "text", text: JSON.stringify(block) });
      continue;
    }
    if (block.type === "text" && typeof block.text === "string") {
      out.push({ type: "text", text: block.text });
    } else if (
      block.type === "image" &&
      typeof block.data === "string" &&
      typeof block.mimeType === "string"
    ) {
      out.push({ type: "image", data: block.data, mimeType: block.mimeType });
    } else {
      out.push({ type: "text", text: JSON.stringify(block) });
    }
  }
  return out.length > 0 ? out : [{ type: "text", text: "(empty result)" }];
}

function describeToolInput(input: Record<string, unknown>): string {
  const entries = Object.entries(input);
  if (entries.length === 0) return "no arguments";
  return entries
    .map(([key, value]) => {
      const rendered = typeof value === "string" ? value : JSON.stringify(value);
      const clipped = rendered.length > 160 ? `${rendered.slice(0, 160)}…` : rendered;
      return `${key}=${clipped}`;
    })
    .join(" ");
}

export default function t3CodeExtension(pi: PiExtensionApi): void {
  if (readEnv(MANAGED_ENV) !== "1") return;

  const endpoint = readEnv(MCP_ENDPOINT_ENV);
  const authHeader = readEnv(MCP_AUTH_ENV);
  const runtimeMode = readEnv(RUNTIME_MODE_ENV).toLowerCase();

  /** Per-call transport scope: pi's cancellation crossed with a failsafe timer. */
  interface AbortScope {
    readonly signal: AbortSignal;
    readonly timedOut: () => boolean;
    readonly cleanup: () => void;
  }

  const makeAbortScope = (
    signal: AbortSignal | undefined,
    timeoutMs: number = CALL_TIMEOUT_MS,
  ): AbortScope => {
    const controller = new AbortController();
    let timedOut = false;
    const forwardAbort = () => controller.abort();
    if (signal) {
      if (signal.aborted) controller.abort();
      else signal.addEventListener("abort", forwardAbort, { once: true });
    }
    const timer = setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, timeoutMs);
    return {
      signal: controller.signal,
      timedOut: () => timedOut,
      cleanup: () => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", forwardAbort);
      },
    };
  };

  let nextRequestId = 1;
  let session: McpSession = { state: "unconnected" };
  let connecting: Promise<string> | undefined;
  let toolsRegistered = false;
  /** `execute` carries its own context; the `session_start` context is the fallback. */
  let sessionCtx: PiExtensionContext | undefined;

  const notify = (ctx: PiExtensionContext | undefined, message: string, type: string): void => {
    (ctx ?? sessionCtx)?.ui.notify(message, type);
  };

  const mcpHeaders = (withSession: boolean): Record<string, string> => ({
    "content-type": "application/json",
    accept: "application/json, text/event-stream",
    // The transport rejects post-initialize requests without the negotiated
    // version header.
    "mcp-protocol-version": MCP_PROTOCOL_VERSION,
    ...(authHeader ? { authorization: authHeader } : {}),
    ...(withSession && session.state === "ready" ? { "mcp-session-id": session.sessionId } : {}),
  });

  interface McpPostOptions {
    readonly signal?: AbortSignal | undefined;
    readonly ctx?: PiExtensionContext | undefined;
    /** Notifications carry no id and no response payload. */
    readonly notification?: boolean;
    /**
     * Send the session id when one is held. `initialize` passes false: after a
     * 404 the dead id is still in hand, and the server answers any request
     * carrying an unknown `mcp-session-id` with another 404, so re-initializing
     * with it would fail against itself.
     */
    readonly withSession?: boolean;
  }

  const postMcp = async (
    method: string,
    params: Record<string, unknown> | undefined,
    options: McpPostOptions = {},
  ): Promise<unknown> => {
    if (!endpoint) throw new Error("T3 MCP endpoint is not configured.");
    const message = options.notification
      ? { jsonrpc: "2.0", method, ...(params ? { params } : {}) }
      : { jsonrpc: "2.0", id: nextRequestId++, method, params: params ?? {} };
    const response = await fetch(endpoint, {
      method: "POST",
      headers: mcpHeaders(options.withSession !== false),
      body: JSON.stringify(message),
      ...(options.signal ? { signal: options.signal } : {}),
    });

    if (response.status === 404) {
      throw new McpHttpError(404, `T3 MCP request ${method} failed with HTTP 404.`);
    }
    if (response.status === 401) {
      notify(
        options.ctx,
        "The T3 MCP credential is no longer valid, so T3 Code tools cannot run. Restart the pi session to re-authenticate.",
        "warning",
      );
      throw new McpHttpError(401, `T3 MCP request ${method} failed with HTTP 401.`);
    }
    if (!response.ok) {
      throw new McpHttpError(
        response.status,
        `T3 MCP request ${method} failed with HTTP ${response.status}.`,
      );
    }

    const returnedSession = response.headers.get("mcp-session-id");
    if (returnedSession && returnedSession.trim()) {
      session = { state: "ready", sessionId: returnedSession.trim() };
    }
    if (options.notification) return undefined;

    const parsed = parseMcpResponseBody(await response.text());
    if (!isRecord(parsed)) return undefined;
    if (isRecord(parsed.error)) {
      const message =
        typeof parsed.error.message === "string" ? parsed.error.message : "unknown MCP error";
      throw new Error(`T3 MCP request ${method} failed: ${message}`);
    }
    return parsed.result;
  };

  const initializeSession = async (): Promise<string> => {
    const scope = makeAbortScope(undefined);
    try {
      await postMcp(
        "initialize",
        {
          protocolVersion: MCP_PROTOCOL_VERSION,
          capabilities: {},
          clientInfo: { name: "t3-code-pi-bridge", version: "1" },
        },
        { signal: scope.signal, withSession: false },
      );
      await postMcp("notifications/initialized", undefined, {
        signal: scope.signal,
        notification: true,
      });
      if (session.state !== "ready") throw new Error("T3 MCP server returned no session id.");
      return session.sessionId;
    } finally {
      scope.cleanup();
    }
  };

  /**
   * `unconnected` → `ready(sessionId)`. Concurrent tool calls share one
   * handshake: pi runs sibling calls in parallel, and each would otherwise open
   * its own server session.
   */
  const ensureSession = (): Promise<string> => {
    if (session.state === "ready") return Promise.resolve(session.sessionId);
    connecting ??= initializeSession().finally(() => {
      connecting = undefined;
    });
    return connecting;
  };

  /**
   * One request, with a single re-handshake when the server no longer knows our
   * session. A restarted T3 server drops its in-memory session map, so without
   * this the bridge would 404 for the rest of the pi process's life. A second
   * failure is real and never retried.
   */
  const mcpRequest = async (
    method: string,
    params: Record<string, unknown> | undefined,
    signal?: AbortSignal,
    ctx?: PiExtensionContext,
  ): Promise<unknown> => {
    try {
      await ensureSession();
      return await postMcp(method, params, { signal, ctx });
    } catch (error) {
      if (!(error instanceof McpHttpError) || error.status !== 404 || signal?.aborted === true) {
        throw error;
      }
      session = { state: "unconnected" };
      await ensureSession();
      return await postMcp(method, params, { signal, ctx });
    }
  };

  const callT3Tool = async (
    toolName: string,
    args: Record<string, unknown>,
    signal: AbortSignal | undefined,
    ctx: PiExtensionContext | undefined,
  ): Promise<PiToolResult> => {
    const scope = makeAbortScope(signal);
    try {
      const result = await mcpRequest(
        "tools/call",
        { name: toolName, arguments: args },
        scope.signal,
        ctx,
      );
      if (!isRecord(result)) return { content: [{ type: "text", text: "(empty result)" }] };
      return {
        content: mcpContentToPiToolContent(result.content),
        ...(isRecord(result.details) ? { details: result.details } : {}),
        ...(result.isError === true ? { isError: true } : {}),
      };
    } catch (error) {
      // pi only marks a tool call failed when `execute` throws, so an aborted
      // or timed-out call must surface as a throw rather than a returned flag
      // that pi would render as a successful call.
      if (scope.timedOut()) {
        throw new Error(`T3 tool call timed out after ${CALL_TIMEOUT_MS} ms.`, { cause: error });
      }
      if (signal?.aborted === true) {
        throw new Error("T3 tool call was cancelled.", { cause: error });
      }
      throw error instanceof Error ? error : new Error(String(error));
    } finally {
      scope.cleanup();
    }
  };

  pi.on("session_shutdown", async () => {
    const current = session.state === "ready" ? session.sessionId : undefined;
    session = { state: "unconnected" };
    if (!current || !endpoint) return;
    const scope = makeAbortScope(undefined, SESSION_SHUTDOWN_TIMEOUT_MS);
    try {
      // Best effort: the server may already be gone, and teardown must neither
      // throw nor hang on a dead socket. The server answers 204 on a live
      // session, 400 without an id, and 404 for an unknown one; none of that
      // matters here.
      await fetch(endpoint, {
        method: "DELETE",
        headers: {
          "mcp-protocol-version": MCP_PROTOCOL_VERSION,
          ...(authHeader ? { authorization: authHeader } : {}),
          "mcp-session-id": current,
        },
        signal: scope.signal,
      });
    } catch {
      // Swallowed on purpose.
    } finally {
      scope.cleanup();
    }
  });

  pi.on("session_start", async (_event, ctx) => {
    sessionCtx = ctx;
    if (!endpoint || toolsRegistered) return;
    try {
      await ensureSession();
      const listed = await mcpRequest("tools/list", {});
      const tools: McpToolDescription[] = [];
      if (isRecord(listed) && Array.isArray(listed.tools)) {
        for (const entry of listed.tools) {
          if (!isRecord(entry) || typeof entry.name !== "string" || !entry.name) continue;
          tools.push({
            name: entry.name,
            ...(typeof entry.description === "string" && entry.description
              ? { description: entry.description }
              : {}),
            inputSchema: isRecord(entry.inputSchema) ? entry.inputSchema : { type: "object" },
          });
        }
      }
      for (const tool of tools) {
        if (typeof tool.name !== "string" || !tool.name) continue;
        const toolName = tool.name;
        pi.registerTool({
          name: toolName,
          label: toolName,
          description:
            typeof tool.description === "string" && tool.description
              ? tool.description
              : `T3 Code tool ${toolName}`,
          parameters: isRecord(tool.inputSchema) ? tool.inputSchema : { type: "object" },
          execute: async (_toolCallId, params, signal, _onUpdate, toolCtx) =>
            callT3Tool(toolName, params, signal, toolCtx),
        });
      }
      toolsRegistered = true;
    } catch (error) {
      ctx.ui.notify(
        `T3 bridge unavailable: ${error instanceof Error ? error.message : String(error)}`,
        "warning",
      );
    }
  });

  pi.on("tool_call", async (rawEvent, ctx) => {
    const event = rawEvent as unknown as PiToolCallEvent;
    if (!event || typeof event.toolName !== "string") return undefined;
    if (!runtimeModeRequiresApproval(runtimeMode, event.toolName)) return undefined;
    const input = isRecord(event.input) ? event.input : {};
    const confirmed = await ctx.ui.confirm(
      `Allow ${event.toolName}?`,
      `T3 Code approval: ${event.toolName} ${describeToolInput(input)}`,
    );
    if (confirmed) return undefined;
    return { block: true as const, reason: `Denied by T3 Code approval for ${event.toolName}.` };
  });

  pi.on("before_agent_start", async (rawEvent) => {
    const event = rawEvent as unknown as PiBeforeAgentStartEvent;
    if (!event || typeof event.systemPrompt !== "string") return undefined;
    return {
      systemPrompt: `${event.systemPrompt}\n\n<t3_code>You are running inside T3 Code through the pi harness. A T3 bridge extension provides collaborative tools: use the preview_* tools for browser navigation, inspection, interaction, screenshots, and recordings, the device_* tools for simulators and emulators, and link_pull_request to register every pull request you create or work on for this thread. Tool calls that modify state ask the user for approval first; proceed once approved.</t3_code>`,
    };
  });
}
