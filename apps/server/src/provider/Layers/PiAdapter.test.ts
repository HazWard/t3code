// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import {
  ApprovalRequestId,
  PiSettings,
  ProviderInstanceId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";

import { ServerConfig } from "../../config.ts";
import { writeFakeCli } from "../../testUtils/fakeCli.ts";
import { makePiAdapter } from "./PiAdapter.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);

const testCrypto = Crypto.make({
  randomBytes: (size) => new Uint8Array(size).fill(7),
  digest: (_algorithm, data) => Effect.succeed(data),
});

/**
 * Scripted stand-in for `pi --mode rpc`. Handles `get_state`, `set_model`,
 * `abort`, `compact`, and `prompt`; every inbound command line is appended
 * to `PI_CAPTURE_PATH` for assertions. `PI_FAKE_MODE=approval` emits one
 * `extension_ui_request` confirm per prompt and waits for the matching
 * `extension_ui_response` before settling the turn.
 */
const FAKE_PI_RPC_SOURCE = [
  "import { appendFileSync } from 'node:fs';",
  "import { createInterface } from 'node:readline';",
  "const capturePath = process.env.PI_CAPTURE_PATH ?? '';",
  "const approvalMode = process.env.PI_FAKE_MODE === 'approval';",
  "const notifyMode = process.env.PI_FAKE_MODE === 'notify';",
  "const log = (line) => { if (capturePath) appendFileSync(capturePath, line + '\\n'); };",
  "log(JSON.stringify({ type: 'argv', argv: process.argv.slice(2) }));",
  "log(JSON.stringify({ type: 'spawnEnv', runtimeMode: process.env.T3CODE_RUNTIME_MODE ?? null, mcpCapabilities: process.env.T3CODE_MCP_CAPABILITIES ?? null }));",
  "const emit = (value) => process.stdout.write(JSON.stringify(value) + '\\n');",
  "const respond = (id, command, data) => emit({ id, type: 'response', command, success: true, data: data ?? {} });",
  "let pendingApproval = null;",
  "const settleTurn = () => {",
  "  emit({ type: 'agent_end', messages: [], willRetry: false });",
  "  emit({ type: 'agent_settled' });",
  "};",
  "const rl = createInterface({ input: process.stdin });",
  "rl.on('line', (line) => {",
  "  log(line);",
  "  let msg;",
  "  try { msg = JSON.parse(line); } catch { return; }",
  "  if (msg.type === 'get_state') {",
  "    respond(msg.id, 'get_state', { sessionId: 'pi-fake-session', model: { id: 'gpt-5.6-luna', name: 'GPT-5.6 Luna', provider: 'opencode-go' } });",
  "    if (notifyMode) emit({ type: 'extension_ui_request', id: 'ui-notify-1', method: 'notify', message: 'T3 MCP credential is no longer valid', notifyType: 'warning' });",
  "  } else if (msg.type === 'set_model') {",
  "    respond(msg.id, 'set_model', { provider: msg.provider, modelId: msg.modelId });",
  "  } else if (msg.type === 'abort' || msg.type === 'compact') {",
  "    respond(msg.id, msg.type, {});",
  "  } else if (msg.type === 'set_thinking_level') {",
  "    if (process.env.PI_FAKE_THINKING === 'unsupported') {",
  "      emit({ id: msg.id, type: 'response', command: 'set_thinking_level', success: false, error: { message: 'thinking levels not supported' } });",
  "    } else {",
  "      respond(msg.id, 'set_thinking_level', {});",
  "    }",
  "  } else if (msg.type === 'prompt') {",
  "    respond(msg.id, 'prompt', {});",
  "    if (process.env.PI_FAKE_ERROR === '1') {",
  "      emit({ type: 'agent_start' });",
  "      emit({ type: 'message_end', message: { role: 'assistant', content: [], stopReason: 'error', errorMessage: 'OpenAI API error (429): monthly usage limit reached.', usage: { input: 1, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 1 } } });",
  "      settleTurn();",
  "    } else {",
  "    emit({ type: 'agent_start' });",
  "    emit({ type: 'turn_start' });",
  "    emit({ type: 'message_start', message: { role: 'assistant', content: [] } });",
  "    emit({ type: 'message_update', usage: { input: 12, output: 5, cacheRead: 2, cacheWrite: 0, totalTokens: 17 }, assistantMessageEvent: { type: 'text_start', contentIndex: 0 } });",
  "    emit({ type: 'message_update', usage: { input: 12, output: 5, cacheRead: 2, cacheWrite: 0, totalTokens: 17 }, assistantMessageEvent: { type: 'text_delta', contentIndex: 0, delta: 'hello' } });",
  "    emit({ type: 'tool_execution_start', toolCallId: 'call-1', toolName: 'bash', args: { command: 'echo hi' } });",
  "    emit({ type: 'tool_execution_end', toolCallId: 'call-1', toolName: 'bash', result: { content: [{ type: 'text', text: 'hi' }] }, isError: false });",
  "    if (approvalMode && !pendingApproval) {",
  "      pendingApproval = 'ui-1';",
  "      emit({ type: 'extension_ui_request', id: 'ui-1', method: 'confirm', title: 'Allow bash?', message: 'Run echo hi' });",
  "    } else {",
  "      emit({ type: 'message_end', message: { role: 'assistant', content: [{ type: 'text', text: 'hello' }], usage: { input: 12, output: 5, cacheRead: 2, cacheWrite: 0, totalTokens: 17 } } });",
  "      settleTurn();",
  "    }",
  "    }",
  "  } else if (msg.type === 'extension_ui_response') {",
  "    if (pendingApproval && msg.id === pendingApproval) {",
  "      pendingApproval = null;",
  "      emit({ type: 'message_end', message: { role: 'assistant', content: [{ type: 'text', text: 'hello' }] } });",
  "      settleTurn();",
  "    }",
  "  } else {",
  "    respond(msg.id, msg.type ?? 'unknown', {});",
  "  }",
  "});",
  "process.stdin.on('end', () => process.exit(0));",
  "",
].join("\n");

const piAdapterTestLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-pi-adapter-test-",
}).pipe(
  Layer.provideMerge(NodeServices.layer),
  Layer.provideMerge(Layer.succeed(Crypto.Crypto, testCrypto)),
);

const makeTestAdapter = (binaryPath: string, options?: Parameters<typeof makePiAdapter>[1]) =>
  makePiAdapter(decodePiSettings({ enabled: true, binaryPath }), {
    extensionPath: "/test/t3code.ts",
    ...options,
  }).pipe(Effect.orDie);

async function readJsonLines(filePath: string) {
  const raw = await NodeFSP.readFile(filePath, "utf8");
  return raw
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

interface EventCollector {
  readonly events: Array<ProviderRuntimeEvent>;
  readonly stop: Effect.Effect<void>;
}

function collectEvents(adapter: {
  readonly streamEvents: Stream.Stream<ProviderRuntimeEvent>;
}): Effect.Effect<EventCollector> {
  return Effect.gen(function* () {
    const events: Array<ProviderRuntimeEvent> = [];
    const fiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
      Effect.sync(() => {
        events.push(event);
      }),
    ).pipe(Effect.forkChild);
    return {
      events,
      stop: Fiber.interrupt(fiber),
    };
  });
}

function waitForEvent(
  events: ReadonlyArray<ProviderRuntimeEvent>,
  predicate: (event: ProviderRuntimeEvent) => boolean,
  attempts = 200,
): Effect.Effect<ProviderRuntimeEvent> {
  const attempt = (remaining: number): Effect.Effect<ProviderRuntimeEvent> =>
    Effect.gen(function* () {
      const found = events.find(predicate);
      if (found) return found;
      if (remaining <= 0) {
        return yield* Effect.die(new Error("Timed out waiting for Pi runtime event."));
      }
      yield* Effect.sleep("25 millis");
      return yield* attempt(remaining - 1);
    });
  return attempt(attempts);
}

const writeFakePiRpc = (
  mode: "basic" | "approval" | "notify",
  capturePath: string,
  errorTurn = false,
  extraEnv: Record<string, string> = {},
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-pi-rpc-" });
    return writeFakeCli({
      directory: dir,
      name: "pi",
      env: {
        PI_FAKE_MODE: mode,
        PI_CAPTURE_PATH: capturePath,
        ...(errorTurn ? { PI_FAKE_ERROR: "1" } : {}),
        ...extraEnv,
      },
      source: FAKE_PI_RPC_SOURCE,
    });
  });

const makeCapturePath = () =>
  Effect.promise(async () => {
    const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "pi-capture-"));
    return NodePath.join(dir, "commands.ndjson");
  });

function requireTurnCompleted(
  event: ProviderRuntimeEvent,
): asserts event is Extract<ProviderRuntimeEvent, { type: "turn.completed" }> {
  assert.equal(event.type, "turn.completed");
}

function requireRequestOpened(
  event: ProviderRuntimeEvent,
): asserts event is Extract<ProviderRuntimeEvent, { type: "request.opened" }> {
  assert.equal(event.type, "request.opened");
}

function requireRequestResolved(
  event: ProviderRuntimeEvent,
): asserts event is Extract<ProviderRuntimeEvent, { type: "request.resolved" }> {
  assert.equal(event.type, "request.resolved");
}

function requireSessionConfigured(
  event: ProviderRuntimeEvent,
): asserts event is Extract<ProviderRuntimeEvent, { type: "session.configured" }> {
  assert.equal(event.type, "session.configured");
}

function requireContentDelta(
  event: ProviderRuntimeEvent,
): asserts event is Extract<ProviderRuntimeEvent, { type: "content.delta" }> {
  assert.equal(event.type, "content.delta");
}

function requireItemCompleted(
  event: ProviderRuntimeEvent,
): asserts event is Extract<ProviderRuntimeEvent, { type: "item.completed" }> {
  assert.equal(event.type, "item.completed");
}

function requireRuntimeWarning(
  event: ProviderRuntimeEvent,
): asserts event is Extract<ProviderRuntimeEvent, { type: "runtime.warning" }> {
  assert.equal(event.type, "runtime.warning");
}

it.layer(piAdapterTestLayer)("PiAdapterLive", (it) => {
  it.effect("surfaces a pi notify as a runtime warning before any turn exists", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("pi-notify-warning");
      const capturePath = yield* makeCapturePath();
      const binaryPath = yield* writeFakePiRpc("notify", capturePath);
      const adapter = yield* makeTestAdapter(binaryPath);
      const collector = yield* collectEvents(adapter);

      yield* adapter.startSession({
        threadId,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      const warning = yield* waitForEvent(
        collector.events,
        (event) => event.type === "runtime.warning",
      );
      requireRuntimeWarning(warning);
      assert.equal(warning.payload.message, "T3 MCP credential is no longer valid");
      // The fake emits from the `get_state` handshake, so no turn is active: a
      // notify must still surface rather than being gated on an active turn.
      assert.isUndefined(warning.turnId);

      yield* collector.stop;
      yield* adapter.stopSession(threadId);
    }).pipe(TestClock.withLive),
  );

  it.effect("starts a session and completes a scripted turn", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("pi-basic-turn");
      const capturePath = yield* makeCapturePath();
      const binaryPath = yield* writeFakePiRpc("basic", capturePath);
      const adapter = yield* makeTestAdapter(binaryPath);
      const collector = yield* collectEvents(adapter);

      const session = yield* adapter.startSession({
        threadId,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      assert.equal(session.provider, "pi");
      assert.equal(session.status, "ready");
      assert.equal(session.model, "opencode-go/gpt-5.6-luna");
      assert.isTrue(yield* adapter.hasSession(threadId));

      const started = yield* adapter.sendTurn({ threadId, input: "Say hello" });
      const completed = yield* waitForEvent(
        collector.events,
        (event) => event.type === "turn.completed" && event.turnId === started.turnId,
      );
      requireTurnCompleted(completed);
      assert.equal(completed.payload.state, "completed");
      assert.deepStrictEqual(completed.payload.tokenUsage, {
        usageScope: "main_agent",
        usageStatus: "complete",
        inputTokens: 12,
        outputTokens: 5,
        cachedInputTokens: 2,
        hasSubagents: false,
      });

      const deltas = collector.events.filter((event) => event.type === "content.delta");
      for (const delta of deltas) requireContentDelta(delta);
      assert.deepStrictEqual(
        deltas.map((event) => event.payload.delta),
        ["hello"],
      );
      const toolCompleted = collector.events.find(
        (event) =>
          event.type === "item.completed" &&
          event.payload.itemType === "command_execution" &&
          event.payload.title === "bash",
      );
      assert.isDefined(toolCompleted);
      requireItemCompleted(toolCompleted!);
      assert.equal(toolCompleted.payload.status, "completed");

      const lines = yield* Effect.promise(() => readJsonLines(capturePath));
      const prompt = lines.find((line) => line.type === "prompt") as
        | { message?: string }
        | undefined;
      assert.equal(prompt?.message, "Say hello");

      const spawnEnv = lines.find((line) => line.type === "spawnEnv") as
        | { runtimeMode?: unknown; mcpCapabilities?: unknown }
        | undefined;
      assert.equal(spawnEnv?.runtimeMode, "full-access");
      assert.isNull(spawnEnv?.mcpCapabilities);

      yield* collector.stop;
      yield* adapter.stopSession(threadId);
      assert.isFalse(yield* adapter.hasSession(threadId));
    }).pipe(TestClock.withLive),
  );

  it.effect("omits --session-dir when Pi's own session directory is configured", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("pi-native-session-dir");
      const capturePath = yield* makeCapturePath();
      const binaryPath = yield* writeFakePiRpc("basic", capturePath);
      const adapter = yield* makeTestAdapter(binaryPath, { usePiSessionDirectory: true });

      const session = yield* adapter.startSession({
        threadId,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      assert.equal(session.status, "ready");

      const lines = yield* Effect.promise(() => readJsonLines(capturePath));
      const argv = lines.find((line) => line.type === "argv") as
        | { argv?: Array<string> }
        | undefined;
      assert.isDefined(argv);
      assert.notInclude(argv?.argv ?? [], "--session-dir");
      assert.include(argv?.argv ?? [], "--session-id");

      yield* adapter.stopSession(threadId);
      assert.isFalse(yield* adapter.hasSession(threadId));
    }).pipe(TestClock.withLive),
  );

  it.effect("applies the thinking level after a model switch, once", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("pi-thinking-level");
      const capturePath = yield* makeCapturePath();
      const binaryPath = yield* writeFakePiRpc("basic", capturePath);
      const adapter = yield* makeTestAdapter(binaryPath);
      const collector = yield* collectEvents(adapter);
      const selection = (model: string) => ({
        instanceId: ProviderInstanceId.make("pi"),
        model,
        options: [{ id: "reasoningEffort", value: "high" }],
      });

      yield* adapter.startSession({ threadId, cwd: process.cwd(), runtimeMode: "full-access" });
      const started = yield* adapter.sendTurn({
        threadId,
        input: "Say hello",
        modelSelection: selection("anthropic/claude-sonnet-4"),
      });
      const completed = yield* waitForEvent(
        collector.events,
        (event) => event.type === "turn.completed" && event.turnId === started.turnId,
      );
      requireTurnCompleted(completed);
      assert.equal(completed.payload.state, "completed");

      const lines = yield* Effect.promise(() => readJsonLines(capturePath));
      const setModelIndex = lines.findIndex((line) => line.type === "set_model");
      const thinkingIndex = lines.findIndex((line) => line.type === "set_thinking_level");
      assert.isAbove(setModelIndex, -1);
      assert.isAbove(thinkingIndex, setModelIndex);
      assert.equal(lines[thinkingIndex]?.level, "high");

      const second = yield* adapter.sendTurn({
        threadId,
        input: "Again",
        modelSelection: selection("anthropic/claude-sonnet-4"),
      });
      yield* waitForEvent(
        collector.events,
        (event) => event.type === "turn.completed" && event.turnId === second.turnId,
      );
      const later = yield* Effect.promise(() => readJsonLines(capturePath));
      assert.equal(later.filter((line) => line.type === "set_thinking_level").length, 1);

      yield* collector.stop;
      yield* adapter.stopSession(threadId);
    }).pipe(TestClock.withLive),
  );

  it.effect("swallows unsupported thinking levels without failing the turn", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("pi-thinking-unsupported");
      const capturePath = yield* makeCapturePath();
      const binaryPath = yield* writeFakePiRpc("basic", capturePath, false, {
        PI_FAKE_THINKING: "unsupported",
      });
      const adapter = yield* makeTestAdapter(binaryPath);
      const collector = yield* collectEvents(adapter);

      yield* adapter.startSession({ threadId, cwd: process.cwd(), runtimeMode: "full-access" });
      const started = yield* adapter.sendTurn({
        threadId,
        input: "Say hello",
        modelSelection: {
          instanceId: ProviderInstanceId.make("pi"),
          model: "anthropic/claude-sonnet-4",
          options: [{ id: "reasoningEffort", value: "high" }],
        },
      });
      const completed = yield* waitForEvent(
        collector.events,
        (event) => event.type === "turn.completed" && event.turnId === started.turnId,
      );
      requireTurnCompleted(completed);
      assert.equal(completed.payload.state, "completed");

      const lines = yield* Effect.promise(() => readJsonLines(capturePath));
      assert.equal(lines.filter((line) => line.type === "set_thinking_level").length, 1);

      yield* collector.stop;
      yield* adapter.stopSession(threadId);
    }).pipe(TestClock.withLive),
  );

  it.effect("surfaces provider errors as failed turns with visible text", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("pi-error-turn");
      const capturePath = yield* makeCapturePath();
      const binaryPath = yield* writeFakePiRpc("basic", capturePath, true);
      const adapter = yield* makeTestAdapter(binaryPath);
      const collector = yield* collectEvents(adapter);

      yield* adapter.startSession({ threadId, cwd: process.cwd(), runtimeMode: "full-access" });
      const started = yield* adapter.sendTurn({ threadId, input: "Say hello" });
      const completed = yield* waitForEvent(
        collector.events,
        (event) => event.type === "turn.completed" && event.turnId === started.turnId,
      );
      requireTurnCompleted(completed);
      assert.equal(completed.payload.state, "failed");
      const errorMessage = (completed.payload as { errorMessage?: unknown }).errorMessage;
      assert.isTrue(typeof errorMessage === "string" && errorMessage.includes("429"));

      const deltas = collector.events.filter((event) => event.type === "content.delta");
      for (const delta of deltas) requireContentDelta(delta);
      assert.isTrue(deltas.some((event) => (event.payload.delta as string).includes("429")));

      yield* collector.stop;
      yield* adapter.stopSession(threadId);
    }).pipe(TestClock.withLive),
  );

  it.effect("routes extension approvals through request.opened and answers on decision", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("pi-approval-turn");
      const capturePath = yield* makeCapturePath();
      const binaryPath = yield* writeFakePiRpc("approval", capturePath);
      const adapter = yield* makeTestAdapter(binaryPath);
      const collector = yield* collectEvents(adapter);

      yield* adapter.startSession({ threadId, cwd: process.cwd(), runtimeMode: "full-access" });
      const started = yield* adapter.sendTurn({ threadId, input: "Run it" });
      const opened = yield* waitForEvent(
        collector.events,
        (event) => event.type === "request.opened",
      );
      requireRequestOpened(opened);
      assert.equal(opened.payload.requestType, "command_execution_approval");
      assert.isTrue((opened.payload.detail ?? "").includes("Allow bash?"));

      assert.isDefined(opened.requestId);
      yield* adapter.respondToRequest(
        threadId,
        ApprovalRequestId.make(opened.requestId as string),
        "accept",
      );
      const resolved = yield* waitForEvent(
        collector.events,
        (event) => event.type === "request.resolved",
      );
      requireRequestResolved(resolved);
      assert.equal(resolved.requestId, opened.requestId);
      yield* waitForEvent(
        collector.events,
        (event) => event.type === "turn.completed" && event.turnId === started.turnId,
      );

      const lines = yield* Effect.promise(() => readJsonLines(capturePath));
      const uiResponse = lines.find((line) => line.type === "extension_ui_response") as
        | { confirmed?: boolean; id?: string }
        | undefined;
      assert.equal(uiResponse?.id, "ui-1");
      assert.equal(uiResponse?.confirmed, true);

      yield* collector.stop;
      yield* adapter.stopSession(threadId);
    }).pipe(TestClock.withLive),
  );

  it.effect("switches the session model before a turn with a new selection", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("pi-model-switch");
      const capturePath = yield* makeCapturePath();
      const binaryPath = yield* writeFakePiRpc("basic", capturePath);
      const adapter = yield* makeTestAdapter(binaryPath);
      const collector = yield* collectEvents(adapter);

      yield* adapter.startSession({ threadId, cwd: process.cwd(), runtimeMode: "full-access" });
      const started = yield* adapter.sendTurn({
        threadId,
        input: "Use another model",
        modelSelection: {
          instanceId: ProviderInstanceId.make("pi"),
          model: "anthropic/claude-sonnet-4-20250514",
        },
      });
      yield* waitForEvent(
        collector.events,
        (event) => event.type === "turn.completed" && event.turnId === started.turnId,
      );

      const configured = collector.events.find((event) => event.type === "session.configured");
      assert.isDefined(configured);
      requireSessionConfigured(configured!);
      assert.deepStrictEqual(configured.payload.config, {
        model: "anthropic/claude-sonnet-4-20250514",
      });
      const lines = yield* Effect.promise(() => readJsonLines(capturePath));
      const setModel = lines.find((line) => line.type === "set_model") as
        | { provider?: string; modelId?: string }
        | undefined;
      assert.equal(setModel?.provider, "anthropic");
      assert.equal(setModel?.modelId, "claude-sonnet-4-20250514");

      yield* collector.stop;
      yield* adapter.stopSession(threadId);
    }).pipe(TestClock.withLive),
  );

  it.effect("forwards interrupt and compaction commands", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("pi-commands");
      const capturePath = yield* makeCapturePath();
      const binaryPath = yield* writeFakePiRpc("basic", capturePath);
      const adapter = yield* makeTestAdapter(binaryPath);

      yield* adapter.startSession({ threadId, cwd: process.cwd(), runtimeMode: "full-access" });
      yield* adapter.interruptTurn(threadId);
      assert.equal(adapter.compaction?.type, "native");
      if (adapter.compaction?.type === "native") {
        yield* adapter.compaction.start(threadId);
      }

      const lines = yield* Effect.promise(() => readJsonLines(capturePath));
      assert.isTrue(lines.some((line) => line.type === "abort"));
      assert.isTrue(lines.some((line) => line.type === "compact"));

      yield* adapter.stopSession(threadId);
    }).pipe(TestClock.withLive),
  );
});
