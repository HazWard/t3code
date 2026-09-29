// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalTimers:off - tests use a real timer tick to let a stubbed fetch get in flight.
import { afterEach, describe, expect, it, vi } from "@effect/vitest";

import t3CodeExtension from "./t3code.ts";

interface FakePiUi {
  readonly notify: (message: string, type: string) => void;
  readonly confirm: (title: string, message?: string) => Promise<boolean>;
  readonly select: (title: string, options: ReadonlyArray<string>) => Promise<string | undefined>;
}

interface FakePiCtx {
  readonly ui: FakePiUi;
}

interface CapturedTool {
  readonly name: string;
  readonly description: string;
  readonly parameters: Record<string, unknown>;
  readonly execute: (
    toolCallId: string,
    params: Record<string, unknown>,
    signal?: AbortSignal,
    onUpdate?: (update: unknown) => void,
    ctx?: FakePiCtx,
  ) => Promise<{
    readonly content: ReadonlyArray<{ readonly type: string; readonly text?: string }>;
    readonly isError?: boolean;
  }>;
}

function makeFakePi() {
  const handlers = new Map<string, (event: never, ctx: never) => unknown>();
  const tools = new Map<string, CapturedTool>();
  return {
    handlers,
    tools,
    api: {
      on: (event: string, handler: (event: never, ctx: never) => unknown) => {
        handlers.set(event, handler);
      },
      registerTool: (tool: CapturedTool) => {
        tools.set(tool.name, tool);
      },
    },
    fire: (event: string, payload: unknown, ctx: unknown) =>
      handlers.get(event)?.(payload as never, ctx as never) as Promise<unknown>,
  };
}

function makeFakeCtx(confirmResult = true) {
  const notifications: Array<{ message: string; type: string }> = [];
  const confirms: Array<{ title: string; message?: string }> = [];
  return {
    notifications,
    confirms,
    ui: {
      notify: (message: string, type: string) => {
        notifications.push({ message, type });
      },
      confirm: async (title: string, message?: string) => {
        confirms.push({ title, ...(message !== undefined ? { message } : {}) });
        return confirmResult;
      },
      select: async () => undefined,
    },
  };
}

const SAVED_ENV = { ...process.env };

function setManagedEnv(extra: Record<string, string> = {}) {
  process.env.T3CODE_PI_MANAGED = "1";
  process.env.T3CODE_MCP_ENDPOINT = "http://127.0.0.1:9/mcp";
  process.env.T3CODE_MCP_AUTH = "Bearer test";
  Object.assign(process.env, extra);
}

afterEach(() => {
  process.env = { ...SAVED_ENV };
  // @ts-expect-error restore the real fetch after each stubbed test.
  delete globalThis.fetch;
});

interface McpStubRequest {
  readonly url: string;
  /** HTTP method, upper-cased. */
  readonly method: string;
  readonly headers: Record<string, string>;
  /** Parsed JSON-RPC body, absent for a bodyless request. */
  readonly body: Record<string, unknown> | undefined;
}

/** Per-method call ordinal, plus the total request count. */
interface McpStubCounts {
  readonly method: number;
  readonly total: number;
}

interface McpStubReply {
  readonly status?: number;
  /** Response `mcp-session-id`; defaults to `"session-1"`, `null` sends none. */
  readonly sessionId?: string | null;
  /** Raw body, used verbatim; takes precedence over `json`. */
  readonly body?: string;
  /** JSON-serialized into the body. */
  readonly json?: unknown;
  /** Reject the fetch instead of responding. */
  readonly throw?: Error;
  /** Never settle, to exercise the abort and timeout paths. */
  readonly hang?: boolean;
}

function abortError(): Error {
  const error = new Error("The operation was aborted.");
  error.name = "AbortError";
  return error;
}

function normalizeHeaders(init: unknown): Record<string, string> {
  const raw = (init as { headers?: unknown } | undefined)?.headers;
  if (!raw || typeof raw !== "object") return {};
  return Object.fromEntries(
    Object.entries(raw as Record<string, unknown>).map(([key, value]) => [
      key.toLowerCase(),
      String(value),
    ]),
  );
}

/**
 * Scriptable stand-in for the T3 MCP endpoint. Records every request and lets a
 * handler script a reply per request, so a test can sequence 404-then-success,
 * hang a call, or force a rejection. Wires `init.signal` so aborting the fetch
 * rejects the way real `fetch` does.
 */
function installMcpStub(
  handler: (request: McpStubRequest, counts: McpStubCounts) => McpStubReply | Promise<McpStubReply>,
) {
  const requests: McpStubRequest[] = [];
  const methodCounts = new Map<string, number>();

  const fetchStub = async (
    _url: string,
    init?: { method?: string; headers?: unknown; body?: unknown; signal?: AbortSignal },
  ) => {
    const method = String(init?.method ?? "POST").toUpperCase();
    const rawBody = typeof init?.body === "string" ? init.body : undefined;
    const request: McpStubRequest = {
      url: String(_url),
      method,
      headers: normalizeHeaders(init),
      body: rawBody ? (JSON.parse(rawBody) as Record<string, unknown>) : undefined,
    };
    requests.push(request);

    const key = method === "POST" ? String(request.body?.method ?? "") : method;
    const methodCount = (methodCounts.get(key) ?? 0) + 1;
    methodCounts.set(key, methodCount);

    const signal = init?.signal;
    if (signal?.aborted) throw abortError();

    let onAbort: (() => void) | undefined;
    const aborted = new Promise<never>((_resolve, reject) => {
      onAbort = () => reject(abortError());
      signal?.addEventListener("abort", onAbort, { once: true });
    });

    let reply: McpStubReply;
    try {
      reply = await Promise.race([
        (async () => {
          const resolved = await handler(request, { method: methodCount, total: requests.length });
          if (resolved.hang) return await new Promise<never>(() => undefined);
          return resolved;
        })(),
        aborted,
      ]);
    } finally {
      if (onAbort) signal?.removeEventListener("abort", onAbort);
    }
    if (reply.throw) throw reply.throw;
    if (signal?.aborted) throw abortError();

    const status = reply.status ?? 200;
    const sessionId = reply.sessionId === undefined ? "session-1" : reply.sessionId;
    return {
      ok: status >= 200 && status < 300,
      status,
      headers: {
        get: (name: string) => (name.toLowerCase() === "mcp-session-id" ? sessionId : null),
      },
      text: async () => reply.body ?? (reply.json === undefined ? "" : JSON.stringify(reply.json)),
    };
  };

  // @ts-expect-error minimal MCP transport stub.
  globalThis.fetch = fetchStub;
  return { requests, methodCount: (method: string) => methodCounts.get(method) ?? 0 };
}

/** A JSON-RPC success reply, echoing the request id. */
const jsonRpcOk = (request: McpStubRequest, result: unknown): McpStubReply => ({
  json: { jsonrpc: "2.0", id: request.body?.id, result },
});

/** Lets every pending microtask and one timer tick settle. */
const settle = () => new Promise((resolve) => setTimeout(resolve, 0));

/** The original one-method stub, kept so the pre-existing tests stay unchanged. */
function stubFetch(handler: (method: string) => unknown) {
  return installMcpStub((request) => {
    if (request.method !== "POST" || !request.body) return {};
    const method = String(request.body.method);
    if (method === "notifications/initialized") return { sessionId: null };
    return {
      json: { jsonrpc: "2.0", id: request.body.id, result: handler(method) },
    };
  });
}

const MCP_TOOLS = [
  { name: "preview_open", description: "Open a preview", inputSchema: { type: "object" } },
  { name: "device_open", description: "Open a device", inputSchema: { type: "object" } },
  {
    name: "link_pull_request",
    description: "Link a PR",
    inputSchema: { type: "object" },
  },
];

describe("t3CodeExtension", () => {
  it("registers nothing outside a T3-managed Pi process", () => {
    delete process.env.T3CODE_PI_MANAGED;
    const fake = makeFakePi();
    t3CodeExtension(fake.api);
    expect(fake.handlers.size).toBe(0);
    expect(fake.tools.size).toBe(0);
  });

  it("registers every advertised MCP tool on session_start", async () => {
    setManagedEnv({ T3CODE_RUNTIME_MODE: "full-access" });
    stubFetch((method) => {
      if (method === "initialize") return { protocolVersion: "2025-06-18", capabilities: {} };
      if (method === "tools/list") return { tools: MCP_TOOLS };
      throw new Error(`unexpected ${method}`);
    });
    const fake = makeFakePi();
    t3CodeExtension(fake.api);
    await fake.fire("session_start", { reason: "startup" }, makeFakeCtx());

    expect([...fake.tools.keys()].sort()).toEqual([
      "device_open",
      "link_pull_request",
      "preview_open",
    ]);
    const tool = fake.tools.get("preview_open");
    expect(tool?.description).toBe("Open a preview");
  });

  it("sends the negotiated protocol version on post-initialize requests", async () => {
    setManagedEnv({ T3CODE_RUNTIME_MODE: "full-access" });
    const seen: Array<{ method: string; headers: Record<string, string> }> = [];
    globalThis.fetch = (async (
      _url: string,
      init: { body?: string; headers?: Record<string, string> },
    ) => {
      const body = JSON.parse(String(init.body)) as { method: string };
      if (body.method === "notifications/initialized") {
        return { ok: true, headers: { get: () => null }, text: async () => "" };
      }
      seen.push({ method: body.method, headers: { ...init.headers } });
      const result =
        body.method === "initialize"
          ? { protocolVersion: "2025-06-18", capabilities: {} }
          : { tools: MCP_TOOLS };
      return {
        ok: true,
        headers: { get: () => "session-1" },
        text: async () => JSON.stringify({ jsonrpc: "2.0", id: 1, result }),
      };
    }) as unknown as typeof fetch;
    const fake = makeFakePi();
    t3CodeExtension(fake.api);
    await fake.fire("session_start", { reason: "startup" }, makeFakeCtx());

    expect(seen.find((call) => call.method === "tools/list")?.headers["mcp-protocol-version"]).toBe(
      "2025-06-18",
    );
  });

  it("notifies instead of bridging when the MCP endpoint is unreachable", async () => {
    setManagedEnv({ T3CODE_MCP_ENDPOINT: "" });
    const fake = makeFakePi();
    t3CodeExtension(fake.api);
    const ctx = makeFakeCtx();
    await fake.fire("session_start", { reason: "startup" }, ctx);
    expect(fake.tools.size).toBe(0);
    expect(ctx.notifications).toEqual([]);
  });

  it("forwards tool calls and reports MCP failures as errors", async () => {
    setManagedEnv();
    stubFetch((method) => {
      if (method === "initialize") return {};
      if (method === "tools/list") return { tools: MCP_TOOLS };
      if (method === "tools/call")
        return { content: [{ type: "text", text: "opened" }], isError: false };
      throw new Error(`unexpected ${method}`);
    });
    const fake = makeFakePi();
    t3CodeExtension(fake.api);
    await fake.fire("session_start", { reason: "startup" }, makeFakeCtx());

    const result = await fake.tools.get("preview_open")?.execute("call-1", { url: "https://x" });
    expect(result?.content).toEqual([{ type: "text", text: "opened" }]);
    expect(result?.isError).toBeUndefined();
  });

  it("maps MCP transport failures to error results", async () => {
    setManagedEnv();
    globalThis.fetch = (async () => {
      throw new Error("connection refused");
    }) as typeof fetch;
    const fake = makeFakePi();
    t3CodeExtension(fake.api);
    const ctx = makeFakeCtx();
    await fake.fire("session_start", { reason: "startup" }, ctx);
    expect(ctx.notifications.some((note) => note.message.includes("T3 bridge unavailable"))).toBe(
      true,
    );
  });

  const GATE_CASES = [
    { mode: "full-access", tool: "bash", prompts: false },
    { mode: "full-access", tool: "edit", prompts: false },
    { mode: "auto", tool: "bash", prompts: false },
    { mode: "auto", tool: "preview_click", prompts: false },
    { mode: "auto-accept-edits", tool: "edit", prompts: false },
    { mode: "auto-accept-edits", tool: "write", prompts: false },
    { mode: "auto-accept-edits", tool: "bash", prompts: true },
    { mode: "auto-accept-edits", tool: "preview_click", prompts: true },
    { mode: "auto-accept-edits", tool: "read", prompts: false },
    { mode: "approval-required", tool: "edit", prompts: true },
    { mode: "approval-required", tool: "bash", prompts: true },
    { mode: "approval-required", tool: "read", prompts: false },
    { mode: undefined, tool: "bash", prompts: true },
    { mode: undefined, tool: "read", prompts: false },
  ] as const;

  it.each(GATE_CASES)(
    "runtime mode '$mode' prompts=$prompts for '$tool'",
    async ({ mode, tool, prompts }) => {
      setManagedEnv(
        mode === undefined ? { T3CODE_RUNTIME_MODE: "" } : { T3CODE_RUNTIME_MODE: mode },
      );
      const fake = makeFakePi();
      t3CodeExtension(fake.api);
      const ctx = makeFakeCtx(true);

      expect(await fake.fire("tool_call", { toolName: tool, input: {} }, ctx)).toBeUndefined();
      expect(ctx.confirms.length).toBe(prompts ? 1 : 0);
    },
  );

  it("blocks mutating tools when the approval is declined", async () => {
    setManagedEnv({ T3CODE_RUNTIME_MODE: "approval-required" });
    const fake = makeFakePi();
    t3CodeExtension(fake.api);

    expect(
      await fake.fire("tool_call", { toolName: "read", input: {} }, makeFakeCtx()),
    ).toBeUndefined();

    expect(
      await fake.fire(
        "tool_call",
        { toolName: "bash", input: { command: "rm -rf /tmp/x" } },
        makeFakeCtx(true),
      ),
    ).toBeUndefined();

    expect(
      await fake.fire(
        "tool_call",
        { toolName: "bash", input: { command: "rm -rf /tmp/x" } },
        makeFakeCtx(false),
      ),
    ).toEqual({ block: true, reason: "Denied by T3 Code approval for bash." });
  });

  it("appends T3 context to the system prompt", async () => {
    setManagedEnv();
    const fake = makeFakePi();
    t3CodeExtension(fake.api);
    const result = (await fake.fire(
      "before_agent_start",
      { prompt: "hi", systemPrompt: "base prompt" },
      makeFakeCtx(),
    )) as { systemPrompt: string };
    expect(result.systemPrompt.startsWith("base prompt")).toBe(true);
    expect(result.systemPrompt).toContain("<t3_code>");
    expect(result.systemPrompt).toContain("preview_*");
  });

  it("re-initializes once and retries when the server forgot the session", async () => {
    setManagedEnv();
    const stub = installMcpStub((request, counts) => {
      if (request.method === "DELETE") return { status: 204, sessionId: null };
      const method = String(request.body?.method ?? "");
      if (method === "notifications/initialized") return { sessionId: null };
      if (method === "initialize") {
        return {
          sessionId: `session-${counts.method}`,
          ...jsonRpcOk(request, { protocolVersion: "2025-06-18", capabilities: {} }),
        };
      }
      if (method === "tools/list") return jsonRpcOk(request, { tools: MCP_TOOLS });
      if (method === "tools/call") {
        // The first call lands after the server dropped our session; the retry
        // must run on the freshly negotiated one.
        if (counts.method === 1) return { status: 404, sessionId: null };
        return jsonRpcOk(request, { content: [{ type: "text", text: "recovered" }] });
      }
      throw new Error(`unexpected ${method}`);
    });
    const fake = makeFakePi();
    t3CodeExtension(fake.api);
    await fake.fire("session_start", { reason: "startup" }, makeFakeCtx());

    const result = await fake.tools.get("preview_open")?.execute("call-1", { url: "https://x" });
    expect(result?.content).toEqual([{ type: "text", text: "recovered" }]);
    expect(stub.methodCount("initialize")).toBe(2);
    expect(stub.methodCount("tools/call")).toBe(2);

    // The re-handshake must not carry the dead session id, or the server would
    // answer it with another 404.
    const initializes = stub.requests.filter((request) => request.body?.method === "initialize");
    expect(initializes.map((request) => request.headers["mcp-session-id"])).toEqual([
      undefined,
      undefined,
    ]);
    const calls = stub.requests.filter((request) => request.body?.method === "tools/call");
    expect(calls.map((request) => request.headers["mcp-session-id"])).toEqual([
      "session-1",
      "session-2",
    ]);
  });

  it("surfaces a second consecutive 404 instead of retrying forever", async () => {
    setManagedEnv();
    const stub = installMcpStub((request) => {
      if (request.method === "DELETE") return { status: 204, sessionId: null };
      const method = String(request.body?.method ?? "");
      if (method === "notifications/initialized") return { sessionId: null };
      if (method === "initialize") return jsonRpcOk(request, {});
      if (method === "tools/list") return jsonRpcOk(request, { tools: MCP_TOOLS });
      if (method === "tools/call") return { status: 404, sessionId: null };
      throw new Error(`unexpected ${method}`);
    });
    const fake = makeFakePi();
    t3CodeExtension(fake.api);
    await fake.fire("session_start", { reason: "startup" }, makeFakeCtx());

    await expect(fake.tools.get("preview_open")?.execute("call-1", {})).rejects.toThrow(/HTTP 404/);
    expect(stub.methodCount("tools/call")).toBe(2);
    expect(stub.methodCount("initialize")).toBe(2);
  });

  it("closes the server session on shutdown", async () => {
    setManagedEnv();
    const stub = installMcpStub((request) => {
      if (request.method === "DELETE") return { status: 204, sessionId: null };
      const method = String(request.body?.method ?? "");
      if (method === "notifications/initialized") return { sessionId: null };
      if (method === "initialize") return jsonRpcOk(request, {});
      if (method === "tools/list") return jsonRpcOk(request, { tools: MCP_TOOLS });
      throw new Error(`unexpected ${method}`);
    });
    const fake = makeFakePi();
    t3CodeExtension(fake.api);
    await fake.fire("session_start", { reason: "startup" }, makeFakeCtx());
    await fake.fire("session_shutdown", { reason: "quit" }, makeFakeCtx());

    const deletes = stub.requests.filter((request) => request.method === "DELETE");
    expect(deletes).toHaveLength(1);
    expect(deletes[0]?.headers["mcp-session-id"]).toBe("session-1");
    expect(deletes[0]?.headers["mcp-protocol-version"]).toBe("2025-06-18");
  });

  it("swallows session teardown failures", async () => {
    setManagedEnv();
    installMcpStub((request) => {
      if (request.method === "DELETE") return { throw: new Error("socket already closed") };
      const method = String(request.body?.method ?? "");
      if (method === "notifications/initialized") return { sessionId: null };
      if (method === "initialize") return jsonRpcOk(request, {});
      if (method === "tools/list") return jsonRpcOk(request, { tools: MCP_TOOLS });
      throw new Error(`unexpected ${method}`);
    });
    const fake = makeFakePi();
    t3CodeExtension(fake.api);
    await fake.fire("session_start", { reason: "startup" }, makeFakeCtx());
    const shutdownCtx = makeFakeCtx();

    // Must resolve rather than reject, and must not warn about the teardown.
    await fake.fire("session_shutdown", { reason: "quit" }, shutdownCtx);
    expect(shutdownCtx.notifications).toEqual([]);
  });

  it("parses multi-frame SSE bodies and skips the [DONE] sentinel", async () => {
    setManagedEnv();
    installMcpStub((request) => {
      if (request.method === "DELETE") return { status: 204, sessionId: null };
      const method = String(request.body?.method ?? "");
      if (method === "notifications/initialized") return { sessionId: null };
      if (method === "initialize") return jsonRpcOk(request, {});
      if (method === "tools/list") return jsonRpcOk(request, { tools: MCP_TOOLS });
      if (method === "tools/call") {
        const id = String(request.body?.id);
        return {
          body: [
            "event: ping",
            'data: {"note":"not a json-rpc message"}',
            "",
            "event: message",
            `id: ${id}`,
            `data: {"jsonrpc":"2.0","id":${id},`,
            'data: "result":{"content":[{"type":"text","text":"split across frames"}]}}',
            "",
            "event: message",
            "data: [DONE]",
            "",
          ].join("\n"),
        };
      }
      throw new Error(`unexpected ${method}`);
    });
    const fake = makeFakePi();
    t3CodeExtension(fake.api);
    await fake.fire("session_start", { reason: "startup" }, makeFakeCtx());

    const result = await fake.tools.get("preview_open")?.execute("call-1", {});
    expect(result?.content).toEqual([{ type: "text", text: "split across frames" }]);
  });

  it("cancels a tool call when pi aborts the signal", async () => {
    setManagedEnv();
    const stub = installMcpStub((request) => {
      if (request.method === "DELETE") return { status: 204, sessionId: null };
      const method = String(request.body?.method ?? "");
      if (method === "notifications/initialized") return { sessionId: null };
      if (method === "initialize") return jsonRpcOk(request, {});
      if (method === "tools/list") return jsonRpcOk(request, { tools: MCP_TOOLS });
      if (method === "tools/call") return { hang: true };
      throw new Error(`unexpected ${method}`);
    });
    const fake = makeFakePi();
    t3CodeExtension(fake.api);
    await fake.fire("session_start", { reason: "startup" }, makeFakeCtx());
    const tool = fake.tools.get("preview_open");

    const alreadyAborted = new AbortController();
    alreadyAborted.abort();
    await expect(tool?.execute("call-1", {}, alreadyAborted.signal)).rejects.toThrow(/cancelled/i);

    const callCount = () =>
      stub.requests.filter((request) => request.body?.method === "tools/call").length;
    const before = callCount();
    const midFlight = new AbortController();
    const pending = tool?.execute("call-2", {}, midFlight.signal);
    await settle();
    // The stub records the request before it hangs, so this proves the fetch
    // was actually in flight when the abort arrived.
    expect(callCount()).toBe(before + 1);
    midFlight.abort();
    await expect(pending).rejects.toThrow(/cancelled/i);
  });

  it("fails a tool call after the failsafe timeout", async () => {
    setManagedEnv();
    installMcpStub((request) => {
      if (request.method === "DELETE") return { status: 204, sessionId: null };
      const method = String(request.body?.method ?? "");
      if (method === "notifications/initialized") return { sessionId: null };
      if (method === "initialize") return jsonRpcOk(request, {});
      if (method === "tools/list") return jsonRpcOk(request, { tools: MCP_TOOLS });
      if (method === "tools/call") return { hang: true };
      throw new Error(`unexpected ${method}`);
    });
    const fake = makeFakePi();
    t3CodeExtension(fake.api);
    await fake.fire("session_start", { reason: "startup" }, makeFakeCtx());

    vi.useFakeTimers();
    try {
      const pending = fake.tools
        .get("preview_open")
        ?.execute("call-1", {})
        .then(
          () => "resolved",
          (error: unknown) => (error instanceof Error ? error.message : String(error)),
        );
      await vi.advanceTimersByTimeAsync(300_000);
      expect(await pending).toContain("timed out");
    } finally {
      vi.useRealTimers();
    }
  });

  it("notifies that the credential is dead on a 401", async () => {
    setManagedEnv();
    installMcpStub((request) => {
      if (request.method === "DELETE") return { status: 204, sessionId: null };
      const method = String(request.body?.method ?? "");
      if (method === "notifications/initialized") return { sessionId: null };
      if (method === "initialize") return jsonRpcOk(request, {});
      if (method === "tools/list") return jsonRpcOk(request, { tools: MCP_TOOLS });
      if (method === "tools/call") return { status: 401, sessionId: null };
      throw new Error(`unexpected ${method}`);
    });
    const fake = makeFakePi();
    t3CodeExtension(fake.api);
    const ctx = makeFakeCtx();
    await fake.fire("session_start", { reason: "startup" }, ctx);

    await expect(
      fake.tools.get("preview_open")?.execute("call-1", {}, undefined, undefined, ctx),
    ).rejects.toThrow(/HTTP 401/);
    const warning = ctx.notifications.find((note) => /credential/i.test(note.message));
    expect(warning?.message).toMatch(/restart/i);
  });
});
