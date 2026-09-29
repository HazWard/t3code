/**
 * PiAdapter — `ProviderAdapter` for the Pi coding agent over its RPC mode.
 *
 * Each T3 thread owns one persistent `pi --mode rpc` process (JSONL over
 * piped stdio). The adapter correlates commands by `id`, translates the
 * streamed Pi events into canonical `ProviderRuntimeEvent`s, and routes
 * the T3 bridge extension's `extension_ui_request` dialogs into T3
 * approval requests answered by `respondToRequest` /
 * `respondToUserInput`.
 *
 * Turn lifecycle: `sendTurn` returns once Pi accepts the prompt; the turn
 * completes when Pi emits `agent_end` with `willRetry: false` (or
 * `agent_settled` as a backstop).
 *
 * @module provider/Layers/PiAdapter
 */
import {
  ApprovalRequestId,
  EventId,
  type PiSettings,
  type ProviderRuntimeEvent,
  type ProviderSession,
  ProviderDriverKind,
  ProviderInstanceId,
  type ModelSelection,
  RuntimeItemId,
  RuntimeRequestId,
  type ThreadId,
  TurnId,
  type TurnTokenUsage,
} from "@t3tools/contracts";
import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as Ndjson from "effect/unstable/encoding/Ndjson";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";
import {
  buildPiRpcArgs,
  buildPiUiResponse,
  encodePiResumeCursor,
  normalizePiTurnUsage,
  parsePiResumeCursor,
  type PendingPiUiRequest,
  piApprovalRequestType,
  PI_MANAGED_ENV,
  PI_MCP_AUTH_ENV,
  PI_MCP_ENDPOINT_ENV,
  PI_RUNTIME_MODE_ENV,
  piToolItemType,
  resolvePiModelTarget,
} from "../pi/PiProtocol.ts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";

const PROVIDER = ProviderDriverKind.make("pi");
const PI_COMMAND_TIMEOUT_MS = 30_000;
const PI_ABORT_TIMEOUT_MS = 10_000;
const PI_COMPACT_TIMEOUT_MS = 120_000;
const PI_MAX_IMAGE_BYTES = 4_000_000;

export interface PiAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
  readonly instanceId?: ProviderInstanceId;
  /** Absolute path to the T3 Pi extension (`pi/t3code.ts`). Required. */
  readonly extensionPath?: string;
  /** Base directory for `pi --session-dir`. Defaults to `<stateDir>/pi-sessions`. */
  readonly sessionBaseDir?: string;
  /**
   * Skip `pi --session-dir` so sessions live in pi's own directory
   * (`~/.pi/agent`), shared with standalone pi runs. Defaults to false
   * (T3-managed storage under `sessionBaseDir`).
   */
  readonly usePiSessionDirectory?: boolean;
}

interface PiPendingCommand {
  readonly deferred: Deferred.Deferred<unknown, ProviderAdapterError>;
}

export type PiAdapterShape = ProviderAdapterShape<ProviderAdapterError>;

interface PiPendingUiRequest extends PendingPiUiRequest {
  readonly piId: string;
}

interface PiTurnRecord {
  readonly id: TurnId;
  readonly items: Array<unknown>;
}

interface PiSessionContext {
  readonly threadId: ThreadId;
  session: ProviderSession;
  readonly teardown: () => Effect.Effect<void>;
  readonly writeLine: (line: string) => Effect.Effect<void, ProviderAdapterProcessError>;
  readonly pendingCommands: Map<string, PiPendingCommand>;
  readonly pendingUiRequests: Map<ApprovalRequestId, PiPendingUiRequest>;
  activeTurnId: TurnId | undefined;
  assistantItemId: RuntimeItemId | undefined;
  turnHadTextDelta: boolean;
  lastUsage: unknown;
  /** Latest assistant stop reason / error text, settled into the turn outcome. */
  lastStopReason: string | undefined;
  lastErrorMessage: string | undefined;
  /** Set by interruptTurn so the next settlement reads as interrupted. */
  interruptArmed: boolean;
  streaming: boolean;
  turns: Array<PiTurnRecord>;
  piSessionId: string | undefined;
  currentModel: string | undefined;
  /** Pi thinking level currently applied in the session, from `get_state`/`set_thinking_level`. */
  currentThinkingLevel: string | undefined;
  stopped: boolean;
  commandSeq: number;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function sanitizePiSessionId(threadId: ThreadId): string {
  const sanitized = (threadId as string).replace(/[^A-Za-z0-9_-]/g, "-");
  return sanitized || "t3-pi-session";
}

function buildTurnTokenUsage(usage: unknown): TurnTokenUsage | undefined {
  const normalized = normalizePiTurnUsage(usage);
  if (normalized.inputTokens === undefined && normalized.outputTokens === undefined) {
    return undefined;
  }
  if (normalized.inputTokens !== undefined && normalized.outputTokens !== undefined) {
    return {
      usageScope: "main_agent",
      usageStatus: "complete",
      inputTokens: normalized.inputTokens,
      outputTokens: normalized.outputTokens,
      ...(normalized.cachedInputTokens !== undefined
        ? { cachedInputTokens: normalized.cachedInputTokens }
        : {}),
      hasSubagents: false,
    };
  }
  return {
    usageScope: "main_agent",
    usageStatus: "partial",
    ...(normalized.inputTokens !== undefined ? { inputTokens: normalized.inputTokens } : {}),
    ...(normalized.outputTokens !== undefined ? { outputTokens: normalized.outputTokens } : {}),
    ...(normalized.cachedInputTokens !== undefined
      ? { cachedInputTokens: normalized.cachedInputTokens }
      : {}),
    hasSubagents: false,
  };
}

function extractAssistantText(message: Record<string, unknown>): string | undefined {
  const content = message.content;
  if (typeof content === "string" && content.trim()) return content;
  if (!Array.isArray(content)) return undefined;
  const parts: Array<string> = [];
  for (const block of content) {
    if (!isRecord(block)) continue;
    if (block.type === "text" && typeof block.text === "string" && block.text) {
      parts.push(block.text);
    }
  }
  const joined = parts.join("");
  return joined.trim() ? joined : undefined;
}

function extractApprovalToolName(title: string): string {
  const match = title.match(/^Allow\s+([A-Za-z0-9_.-]+)\?/);
  return match?.[1] ?? "unknown";
}

/** Clip provider error text for turn payloads; the full body stays in native logs. */
function clipPiErrorText(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  if (!trimmed) return undefined;
  return trimmed.length > 2000 ? `${trimmed.slice(0, 2000)}…` : trimmed;
}

export const makePiAdapter = (
  piSettings: PiSettings,
  options?: PiAdapterLiveOptions,
): Effect.Effect<
  ProviderAdapterShape<ProviderAdapterError>,
  never,
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | Path.Path
  | Scope.Scope
  | ServerConfig
> =>
  Effect.gen(function* () {
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const crypto = yield* Crypto.Crypto;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const serverConfig = yield* ServerConfig;
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("pi");
    const baseEnvironment = options?.environment ?? process.env;

    const managedNativeEventLogger =
      options?.nativeEventLogger === undefined && options?.nativeEventLogPath !== undefined
        ? yield* makeEventNdjsonLogger(options.nativeEventLogPath, { stream: "native" }).pipe(
            Effect.orElseSucceed(() => undefined),
          )
        : undefined;
    const eventLogger = options?.nativeEventLogger ?? managedNativeEventLogger;

    const sessions = new Map<ThreadId, PiSessionContext>();
    const threadLocksRef = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());
    const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate Pi runtime identifier.",
            cause,
          }),
      ),
    );
    const nextEventId = Effect.map(randomUUIDv4, (id) => EventId.make(id));
    const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });

    const offerRuntimeEvent = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);

    const getThreadSemaphore = (threadId: string) =>
      SynchronizedRef.modifyEffect(threadLocksRef, (current) => {
        const existing = current.get(threadId);
        if (existing) return Effect.succeed([existing, current] as const);
        return Semaphore.make(1).pipe(
          Effect.map((semaphore) => {
            const next = new Map(current);
            next.set(threadId, semaphore);
            return [semaphore, next] as const;
          }),
        );
      });

    const withThreadLock = <A, E, R>(threadId: string, effect: Effect.Effect<A, E, R>) =>
      Effect.flatMap(getThreadSemaphore(threadId), (semaphore) => semaphore.withPermit(effect));

    const logNative = (threadId: ThreadId, method: string, payload: unknown) =>
      Effect.gen(function* () {
        if (!eventLogger) return;
        const observedAt = yield* nowIso;
        yield* eventLogger.write(
          {
            observedAt,
            event: {
              id: yield* randomUUIDv4,
              kind: "notification",
              provider: PROVIDER,
              createdAt: observedAt,
              method,
              threadId,
              payload,
            },
          },
          threadId,
        );
      });

    const requireSession = (
      threadId: ThreadId,
    ): Effect.Effect<PiSessionContext, ProviderAdapterSessionNotFoundError> => {
      const ctx = sessions.get(threadId);
      if (!ctx || ctx.stopped) {
        return Effect.fail(
          new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }),
        );
      }
      return Effect.succeed(ctx);
    };

    const completeTurn = (ctx: PiSessionContext): Effect.Effect<void, ProviderAdapterError> =>
      Effect.gen(function* () {
        const turnId = ctx.activeTurnId;
        if (!turnId) return;
        ctx.activeTurnId = undefined;
        ctx.assistantItemId = undefined;
        ctx.streaming = false;
        // Provider failures (e.g. upstream 429/quota) arrive as an assistant
        // message with stopReason "error" plus errorMessage, followed by a
        // normal agent_end — settle those as failed so the client surfaces
        // them instead of rendering an empty completed turn.
        const interrupted = ctx.interruptArmed;
        ctx.interruptArmed = false;
        const failed = !interrupted && ctx.lastStopReason === "error";
        const errorMessage = failed ? clipPiErrorText(ctx.lastErrorMessage) : undefined;
        if (errorMessage && !ctx.turnHadTextDelta) {
          yield* offerRuntimeEvent({
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            threadId: ctx.threadId,
            turnId,
            type: "content.delta",
            payload: { streamKind: "assistant_text", delta: errorMessage },
          });
          ctx.turnHadTextDelta = true;
        }
        const tokenUsage = buildTurnTokenUsage(ctx.lastUsage);
        ctx.turns = [...ctx.turns, { id: turnId, items: [] }];
        ctx.session = { ...ctx.session, status: "ready", activeTurnId: undefined };
        yield* offerRuntimeEvent({
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: ctx.threadId,
          turnId,
          type: "turn.completed",
          payload: {
            state: interrupted ? "interrupted" : failed ? "failed" : "completed",
            ...(tokenUsage ? { tokenUsage } : {}),
            ...(errorMessage ? { errorMessage } : {}),
          },
        });
      });

    const failPendingCommands = (ctx: PiSessionContext, detail: string) =>
      Effect.forEach(
        Array.from(ctx.pendingCommands.values()),
        (pending) =>
          Deferred.fail(
            pending.deferred,
            new ProviderAdapterProcessError({
              provider: PROVIDER,
              threadId: ctx.threadId,
              detail,
            }),
          ).pipe(Effect.ignore),
        { discard: true },
      );

    const dispatchRpcMessage = (
      ctx: PiSessionContext,
      message: unknown,
    ): Effect.Effect<void, ProviderAdapterError> =>
      Effect.gen(function* () {
        if (!isRecord(message)) return;
        const type = nonEmptyString(message.type);
        if (!type) return;
        yield* logNative(ctx.threadId, `pi.rpc.${type}`, message);

        if (type === "response") {
          const id = nonEmptyString(message.id);
          if (!id) return;
          const pending = ctx.pendingCommands.get(id);
          if (!pending) return;
          ctx.pendingCommands.delete(id);
          if (message.success === true) {
            yield* Deferred.succeed(pending.deferred, message.data);
          } else {
            yield* Deferred.fail(
              pending.deferred,
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "pi.rpc.response",
                detail: nonEmptyString(message.error) ?? "Pi command failed.",
              }),
            );
          }
          return;
        }

        if (type === "agent_start") {
          ctx.streaming = true;
          return;
        }
        if (type === "agent_end") {
          ctx.streaming = false;
          const willRetry = isRecord(message) && message.willRetry === true;
          if (!willRetry) {
            yield* completeTurn(ctx);
          }
          return;
        }
        if (type === "agent_settled") {
          ctx.streaming = false;
          yield* completeTurn(ctx);
          return;
        }
        if (type === "turn_start" || type === "turn_end") return;

        if (type === "message_start" && isRecord(message.message)) {
          const role = nonEmptyString(message.message.role);
          if (role === "assistant" && ctx.activeTurnId) {
            const itemId = RuntimeItemId.make(`pi-assistant-${yield* randomUUIDv4}`);
            ctx.assistantItemId = itemId;
            yield* offerRuntimeEvent({
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              providerInstanceId: boundInstanceId,
              threadId: ctx.threadId,
              turnId: ctx.activeTurnId,
              itemId,
              type: "item.started",
              payload: { itemType: "assistant_message", status: "inProgress" },
            });
          }
          return;
        }

        if (type === "message_update") {
          if (isRecord(message.usage)) ctx.lastUsage = message.usage;
          const deltaEvent = message.assistantMessageEvent;
          if (!isRecord(deltaEvent) || !ctx.activeTurnId) return;
          const deltaType = nonEmptyString(deltaEvent.type);
          if (
            (deltaType === "text_delta" || deltaType === "thinking_delta") &&
            typeof deltaEvent.delta === "string"
          ) {
            if (deltaType === "text_delta") ctx.turnHadTextDelta = true;
            yield* offerRuntimeEvent({
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              providerInstanceId: boundInstanceId,
              threadId: ctx.threadId,
              turnId: ctx.activeTurnId,
              ...(ctx.assistantItemId ? { itemId: ctx.assistantItemId } : {}),
              type: "content.delta",
              payload: {
                streamKind: deltaType === "text_delta" ? "assistant_text" : "reasoning_text",
                delta: deltaEvent.delta,
              },
            });
          } else if (deltaType === "toolcall_start" && ctx.activeTurnId) {
            const toolName = nonEmptyString(deltaEvent.toolName) ?? "unknown";
            const toolCallId = nonEmptyString(deltaEvent.id) ?? (yield* randomUUIDv4);
            yield* offerRuntimeEvent({
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              providerInstanceId: boundInstanceId,
              threadId: ctx.threadId,
              turnId: ctx.activeTurnId,
              itemId: RuntimeItemId.make(toolCallId),
              type: "item.started",
              payload: {
                itemType: piToolItemType(toolName),
                status: "inProgress",
                title: toolName,
              },
            });
          }
          return;
        }

        if (type === "message_end" && isRecord(message.message)) {
          const role = nonEmptyString(message.message.role);
          if (role === "assistant" && ctx.activeTurnId) {
            const stopReason = nonEmptyString(message.message.stopReason);
            if (stopReason) ctx.lastStopReason = stopReason;
            const errorMessage = nonEmptyString(message.message.errorMessage);
            if (errorMessage) ctx.lastErrorMessage = errorMessage;
            if (!ctx.turnHadTextDelta) {
              const text = extractAssistantText(message.message);
              if (text) {
                yield* offerRuntimeEvent({
                  ...(yield* makeEventStamp()),
                  provider: PROVIDER,
                  providerInstanceId: boundInstanceId,
                  threadId: ctx.threadId,
                  turnId: ctx.activeTurnId,
                  ...(ctx.assistantItemId ? { itemId: ctx.assistantItemId } : {}),
                  type: "content.delta",
                  payload: { streamKind: "assistant_text", delta: text },
                });
              }
            }
            if (isRecord(message.message.usage)) ctx.lastUsage = message.message.usage;
            if (ctx.assistantItemId) {
              yield* offerRuntimeEvent({
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                providerInstanceId: boundInstanceId,
                threadId: ctx.threadId,
                turnId: ctx.activeTurnId,
                itemId: ctx.assistantItemId,
                type: "item.completed",
                payload: { itemType: "assistant_message", status: "completed" },
              });
              ctx.assistantItemId = undefined;
            }
          }
          return;
        }

        if (
          (type === "tool_execution_start" ||
            type === "tool_execution_update" ||
            type === "tool_execution_end") &&
          ctx.activeTurnId
        ) {
          const toolName = nonEmptyString(message.toolName) ?? "unknown";
          const toolCallId = nonEmptyString(message.toolCallId) ?? (yield* randomUUIDv4);
          const itemId = RuntimeItemId.make(toolCallId);
          if (type === "tool_execution_start") {
            yield* offerRuntimeEvent({
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              providerInstanceId: boundInstanceId,
              threadId: ctx.threadId,
              turnId: ctx.activeTurnId,
              itemId,
              type: "item.started",
              payload: {
                itemType: piToolItemType(toolName),
                status: "inProgress",
                title: toolName,
              },
            });
          } else if (type === "tool_execution_update") {
            yield* offerRuntimeEvent({
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              providerInstanceId: boundInstanceId,
              threadId: ctx.threadId,
              turnId: ctx.activeTurnId,
              itemId,
              type: "item.updated",
              payload: {
                itemType: piToolItemType(toolName),
                status: "inProgress",
                title: toolName,
              },
            });
          } else {
            const isError = message.isError === true;
            yield* offerRuntimeEvent({
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              providerInstanceId: boundInstanceId,
              threadId: ctx.threadId,
              turnId: ctx.activeTurnId,
              itemId,
              type: "item.completed",
              payload: {
                itemType: piToolItemType(toolName),
                status: isError ? "failed" : "completed",
                title: toolName,
              },
            });
          }
          return;
        }

        if (type === "extension_ui_request") {
          const method = nonEmptyString(message.method) ?? "";
          if (method === "notify") {
            // `notify` is fire-and-forget and carries the bridge's own warnings
            // (dead MCP credential, unavailable bridge). Those can arrive before
            // any turn exists, so this branch must not be gated on an active
            // turn the way the dialog methods below are.
            const text = nonEmptyString(message.message);
            const notifyType = nonEmptyString(message.notifyType) ?? "info";
            // `info` has no home in the runtime-event contract; drop it rather
            // than mislabel it as a warning.
            if (text && notifyType !== "info") {
              yield* offerRuntimeEvent({
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                providerInstanceId: boundInstanceId,
                threadId: ctx.threadId,
                ...(ctx.activeTurnId ? { turnId: ctx.activeTurnId } : {}),
                type: "runtime.warning",
                payload: { message: text },
              });
            }
            return;
          }
          if (
            ctx.activeTurnId &&
            (method === "select" ||
              method === "confirm" ||
              method === "input" ||
              method === "editor")
          ) {
            const piId = nonEmptyString(message.id);
            if (!piId) return;
            const title = nonEmptyString(message.title) ?? `Pi approval (${method})`;
            const requestId = ApprovalRequestId.make(yield* randomUUIDv4);
            const rawOptions = Array.isArray(message.options)
              ? message.options.filter(
                  (entry): entry is string => typeof entry === "string" && !!entry.trim(),
                )
              : undefined;
            ctx.pendingUiRequests.set(requestId, {
              method,
              ...(rawOptions ? { options: rawOptions } : {}),
              piId,
            });
            yield* offerRuntimeEvent({
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              providerInstanceId: boundInstanceId,
              threadId: ctx.threadId,
              turnId: ctx.activeTurnId,
              requestId: RuntimeRequestId.make(requestId),
              type: "request.opened",
              payload: {
                requestType: piApprovalRequestType(extractApprovalToolName(title)),
                detail: title,
              },
            });
          }
        }
      });

    const sendCommand = (
      ctx: PiSessionContext,
      command: Record<string, unknown>,
      timeoutMs: number,
    ): Effect.Effect<unknown, ProviderAdapterError> =>
      Effect.gen(function* () {
        if (ctx.stopped) {
          return yield* new ProviderAdapterProcessError({
            provider: PROVIDER,
            threadId: ctx.threadId,
            detail: "Pi session is stopped.",
          });
        }
        ctx.commandSeq += 1;
        const id = `t3-pi-${ctx.commandSeq}`;
        const deferred = yield* Deferred.make<unknown, ProviderAdapterError>();
        ctx.pendingCommands.set(id, { deferred });
        const written = yield* ctx
          // @effect-diagnostics-next-line preferSchemaOverJson:off - Pi RPC framing is newline-delimited JSON by protocol.
          .writeLine(`${JSON.stringify({ ...command, id })}\n`)
          .pipe(Effect.option);
        if (Option.isNone(written)) {
          ctx.pendingCommands.delete(id);
          return yield* new ProviderAdapterProcessError({
            provider: PROVIDER,
            threadId: ctx.threadId,
            detail: "Failed to write Pi command: process input closed.",
          });
        }
        return yield* Deferred.await(deferred).pipe(
          Effect.timeoutOption(timeoutMs),
          Effect.flatMap(
            Option.match({
              onNone: () =>
                Effect.fail(
                  new ProviderAdapterRequestError({
                    provider: PROVIDER,
                    method: typeof command.type === "string" ? command.type : "pi.rpc",
                    detail: "Pi command timed out.",
                  }),
                ),
              onSome: (value) => Effect.succeed(value),
            }),
          ),
          Effect.ensuring(
            Effect.sync(() => {
              ctx.pendingCommands.delete(id);
            }),
          ),
        );
      });

    /**
     * Apply a requested `reasoningEffort` selection as Pi's thinking level.
     * Call after any `set_model` so a model switch cannot clobber the level;
     * unsupported models or unknown values are swallowed (logged natively)
     * and never fail the turn.
     */
    const applyPiThinkingLevel = (
      ctx: PiSessionContext,
      modelSelection: ModelSelection | undefined,
    ): Effect.Effect<void, ProviderAdapterError> =>
      Effect.gen(function* () {
        const requested = getModelSelectionStringOptionValue(modelSelection, "reasoningEffort");
        if (!requested || requested === ctx.currentThinkingLevel) return;
        const applied = yield* sendCommand(
          ctx,
          { type: "set_thinking_level", level: requested },
          PI_COMMAND_TIMEOUT_MS,
        ).pipe(
          Effect.option,
          Effect.map((option) => option._tag === "Some"),
        );
        if (!applied) {
          yield* logNative(ctx.threadId, "pi.rpc.set_thinking_level.unsupported", {
            level: requested,
          });
          return;
        }
        ctx.currentThinkingLevel = requested;
        yield* offerRuntimeEvent({
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: ctx.threadId,
          type: "session.configured",
          payload: {
            config: {
              ...(ctx.currentModel ? { model: ctx.currentModel } : {}),
              options: [{ id: "reasoningEffort", value: requested }],
            },
          },
        });
      });

    const stopSessionInternal = (ctx: PiSessionContext) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        ctx.stopped = true;
        sessions.delete(ctx.threadId);
        yield* failPendingCommands(ctx, "Pi session stopped.");
        yield* ctx.teardown().pipe(Effect.ignore);
      });

    const stopAllInternal = () =>
      Effect.forEach(Array.from(sessions.values()), (ctx) => stopSessionInternal(ctx), {
        discard: true,
      });

    yield* Effect.addFinalizer(() => stopAllInternal().pipe(Effect.ignore));

    const startSession: PiAdapterShape["startSession"] = (input) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          if (input.provider && input.provider !== PROVIDER) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
            });
          }
          if (!input.cwd?.trim()) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "cwd is required and must be non-empty.",
            });
          }
          const extensionPath = options?.extensionPath;
          if (!extensionPath) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "Pi T3 extension path is not configured.",
            });
          }
          const cwd = path.resolve(input.cwd.trim());
          const previous = sessions.get(input.threadId);
          if (previous && !previous.stopped) {
            yield* stopSessionInternal(previous);
          }

          const sessionScope = yield* Scope.make();
          const teardownSessionScope = () =>
            Scope.close(sessionScope, Exit.void).pipe(Effect.ignore);

          const mcpSession = McpProviderSession.readMcpProviderSession(input.threadId);
          // Pi-native storage shares ~/.pi/agent with standalone pi runs;
          // otherwise sessions stay isolated under T3's managed state dir.
          const usePiSessionDirectory = options?.usePiSessionDirectory === true;
          const sessionDir = usePiSessionDirectory
            ? undefined
            : path.join(
                options?.sessionBaseDir ?? path.join(serverConfig.stateDir, "pi-sessions"),
                sanitizePiSessionId(input.threadId),
              );
          if (sessionDir) {
            yield* fileSystem.makeDirectory(sessionDir, { recursive: true }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterProcessError({
                    provider: PROVIDER,
                    threadId: input.threadId,
                    detail: `Failed to prepare Pi session directory '${sessionDir}'.`,
                    cause,
                  }),
              ),
            );
          }

          const resumed = parsePiResumeCursor(input.resumeCursor);
          const sessionId = resumed?.piSessionId ?? sanitizePiSessionId(input.threadId);

          const turnModelSelection =
            input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
          const initialTarget = resolvePiModelTarget(turnModelSelection?.model);
          const settingsProvider = piSettings.defaultProvider.trim() || undefined;
          const settingsModel = resolvePiModelTarget(piSettings.defaultModel.trim() || undefined);
          const spawnProvider = initialTarget.provider ?? settingsProvider;
          const spawnModelId = initialTarget.modelId ?? settingsModel.modelId;
          const spawnModel =
            spawnProvider && spawnModelId
              ? `${spawnProvider}/${spawnModelId}`
              : (spawnModelId ?? settingsModel.provider);
          const environment = McpProviderSession.withAgentDeviceEnvironment(
            {
              ...baseEnvironment,
              [PI_MANAGED_ENV]: "1",
              // Approval gating in the T3 extension reads the thread runtime
              // mode, not the MCP capability set (which the server enforces
              // itself). Always set: the gate applies with or without MCP.
              [PI_RUNTIME_MODE_ENV]: input.runtimeMode,
              ...(mcpSession
                ? {
                    [PI_MCP_ENDPOINT_ENV]: mcpSession.endpoint,
                    [PI_MCP_AUTH_ENV]: mcpSession.authorizationHeader,
                  }
                : {}),
            },
            mcpSession,
          );

          const binary = piSettings.binaryPath || "pi";
          const rpcArgs = buildPiRpcArgs({
            ...(sessionDir ? { sessionDir } : {}),
            sessionId,
            extensionPath,
            ...(spawnProvider ? { provider: spawnProvider } : {}),
            ...(spawnModel ? { modelId: spawnModel } : {}),
            launchArgs: piSettings.launchArgs,
          });
          const spawnCommand = yield* resolveSpawnCommand(binary, rpcArgs, {
            env: environment,
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: "Failed to resolve Pi spawn command.",
                  cause,
                }),
            ),
          );

          const child = yield* childProcessSpawner
            .spawn(
              ChildProcess.make(spawnCommand.command, spawnCommand.args, {
                cwd,
                env: environment,
                shell: spawnCommand.shell,
                stdin: { stream: "pipe", endOnDone: false },
                stdout: "pipe",
                stderr: "pipe",
              }),
            )
            .pipe(
              // The child outlives startSession: its lifetime is the session
              // scope, released by teardown (kill + close) on stop.
              Effect.provideService(Scope.Scope, sessionScope),
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterProcessError({
                    provider: PROVIDER,
                    threadId: input.threadId,
                    detail: "Failed to spawn Pi RPC process.",
                    cause,
                  }),
              ),
            );

          const writeQueue = yield* Queue.unbounded<string>();
          const writeLine = (line: string) =>
            Queue.offer(writeQueue, line).pipe(
              Effect.asVoid,
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterProcessError({
                    provider: PROVIDER,
                    threadId: input.threadId,
                    detail: "Failed to queue Pi command.",
                    cause,
                  }),
              ),
            );
          yield* Queue.take(writeQueue).pipe(
            Effect.flatMap((line) => Stream.run(Stream.encodeText(Stream.make(line)), child.stdin)),
            Effect.forever,
            Effect.forkIn(sessionScope),
          );

          const createdAt = yield* nowIso;
          const ctx: PiSessionContext = {
            threadId: input.threadId,
            session: {
              provider: PROVIDER,
              providerInstanceId: boundInstanceId,
              status: "connecting",
              runtimeMode: input.runtimeMode,
              cwd,
              ...(turnModelSelection?.model ? { model: turnModelSelection.model } : {}),
              threadId: input.threadId,
              activeTurnId: undefined,
              createdAt,
              updatedAt: createdAt,
            },
            teardown: () =>
              Effect.gen(function* () {
                yield* child.kill().pipe(Effect.ignore);
                yield* teardownSessionScope();
              }),
            writeLine,
            pendingCommands: new Map(),
            pendingUiRequests: new Map(),
            activeTurnId: undefined,
            assistantItemId: undefined,
            turnHadTextDelta: false,
            lastUsage: undefined,
            lastStopReason: undefined,
            lastErrorMessage: undefined,
            interruptArmed: false,
            streaming: false,
            turns: [],
            piSessionId: undefined,
            currentModel: undefined,
            currentThinkingLevel: undefined,
            stopped: false,
            commandSeq: 0,
          };
          sessions.set(input.threadId, ctx);

          yield* child.stderr.pipe(Stream.runDrain, Effect.ignore, Effect.forkIn(sessionScope));
          yield* child.stdout.pipe(
            Stream.pipeThroughChannel(Ndjson.decode({ ignoreEmptyLines: true })),
            Stream.runForEach((message) =>
              dispatchRpcMessage(ctx, message).pipe(
                Effect.catchCause((cause) => Effect.logDebug("Pi RPC dispatch failed.", { cause })),
              ),
            ),
            Effect.ignore,
            Effect.forkIn(sessionScope),
          );
          yield* child.exitCode.pipe(
            Effect.flatMap((code) =>
              Effect.gen(function* () {
                if (ctx.stopped) return;
                ctx.stopped = true;
                yield* failPendingCommands(ctx, `Pi process exited with code ${Number(code)}.`);
                yield* offerRuntimeEvent({
                  ...(yield* makeEventStamp()),
                  provider: PROVIDER,
                  providerInstanceId: boundInstanceId,
                  threadId: ctx.threadId,
                  ...(ctx.activeTurnId ? { turnId: ctx.activeTurnId } : {}),
                  type: "session.exited",
                  payload: {
                    reason: `Pi process exited with code ${Number(code)}.`,
                    exitKind: "error" as const,
                  },
                });
              }),
            ),
            Effect.ignore,
            Effect.forkIn(sessionScope),
          );

          const initialState = (yield* sendCommand(
            ctx,
            { type: "get_state" },
            PI_COMMAND_TIMEOUT_MS,
          ).pipe(Effect.orElseSucceed(() => undefined))) as Record<string, unknown> | undefined;
          if (initialState && isRecord(initialState)) {
            const reportedSessionId = nonEmptyString(initialState.sessionId);
            if (reportedSessionId) ctx.piSessionId = reportedSessionId;
            if (isRecord(initialState.model)) {
              const provider = nonEmptyString(initialState.model.provider);
              const id = nonEmptyString(initialState.model.id);
              if (provider && id) ctx.currentModel = `${provider}/${id}`;
            }
            const thinkingLevel = nonEmptyString(initialState.thinkingLevel);
            if (thinkingLevel) ctx.currentThinkingLevel = thinkingLevel;
          }

          yield* applyPiThinkingLevel(ctx, turnModelSelection);

          const updatedAt = yield* nowIso;
          ctx.session = {
            ...ctx.session,
            status: "ready",
            ...(ctx.currentModel ? { model: ctx.currentModel } : {}),
            ...(ctx.piSessionId ? { resumeCursor: encodePiResumeCursor(ctx.piSessionId) } : {}),
            updatedAt,
          };
          return ctx.session;
        }),
      );

    const sendTurn: PiAdapterShape["sendTurn"] = (input) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(input.threadId);
          const text = input.input?.trim();
          const imageParts = yield* Effect.forEach(
            (input.attachments ?? []).filter((attachment) => attachment.type === "image"),
            (attachment) =>
              Effect.gen(function* () {
                const attachmentPath = resolveAttachmentPath({
                  attachmentsDir: serverConfig.attachmentsDir,
                  attachment,
                });
                if (!attachmentPath) {
                  return yield* new ProviderAdapterRequestError({
                    provider: PROVIDER,
                    method: "prompt",
                    detail: `Invalid attachment id '${attachment.id}'.`,
                  });
                }
                const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProviderAdapterRequestError({
                        provider: PROVIDER,
                        method: "prompt",
                        detail: `Failed to read attachment '${attachment.id}'.`,
                        cause,
                      }),
                  ),
                );
                if (bytes.byteLength > PI_MAX_IMAGE_BYTES) {
                  return yield* new ProviderAdapterRequestError({
                    provider: PROVIDER,
                    method: "prompt",
                    detail: `Attachment '${attachment.id}' exceeds the Pi image size limit.`,
                  });
                }
                return {
                  type: "image",
                  data: Buffer.from(bytes).toString("base64"),
                  mimeType: attachment.mimeType,
                };
              }),
          );
          if (!text && imageParts.length === 0) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue: "Turn requires non-empty text or image attachments.",
            });
          }

          const turnModelSelection =
            input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
          if (turnModelSelection?.model) {
            const target = resolvePiModelTarget(turnModelSelection.model);
            const slug =
              target.provider && target.modelId
                ? `${target.provider}/${target.modelId}`
                : undefined;
            if (slug && slug !== ctx.currentModel) {
              const switched = yield* sendCommand(
                ctx,
                { type: "set_model", provider: target.provider, modelId: target.modelId },
                PI_COMMAND_TIMEOUT_MS,
              ).pipe(
                Effect.option,
                Effect.map((option) => option._tag === "Some"),
              );
              if (switched) {
                ctx.currentModel = slug;
                ctx.session = { ...ctx.session, model: slug };
                yield* offerRuntimeEvent({
                  ...(yield* makeEventStamp()),
                  provider: PROVIDER,
                  providerInstanceId: boundInstanceId,
                  threadId: ctx.threadId,
                  type: "session.configured",
                  payload: { config: { model: slug } },
                });
              }
            }
          }
          // After any model switch so a new model cannot clobber the level.
          yield* applyPiThinkingLevel(ctx, turnModelSelection);

          const turnId = TurnId.make(yield* randomUUIDv4);
          ctx.activeTurnId = turnId;
          ctx.assistantItemId = undefined;
          ctx.turnHadTextDelta = false;
          ctx.lastUsage = undefined;
          ctx.lastStopReason = undefined;
          ctx.lastErrorMessage = undefined;
          ctx.interruptArmed = false;
          ctx.session = {
            ...ctx.session,
            status: "running",
            activeTurnId: turnId,
            updatedAt: yield* nowIso,
          };
          yield* offerRuntimeEvent({
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            threadId: ctx.threadId,
            turnId,
            type: "turn.started",
            payload: {},
          });

          yield* sendCommand(
            ctx,
            {
              type: "prompt",
              message: text ?? "(see attached images)",
              ...(imageParts.length > 0 ? { images: imageParts } : {}),
              ...(ctx.streaming ? { streamingBehavior: "steer" } : {}),
            },
            PI_COMMAND_TIMEOUT_MS,
          );
          ctx.streaming = true;
          return { threadId: input.threadId, turnId };
        }),
      );

    const interruptTurn: PiAdapterShape["interruptTurn"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        ctx.interruptArmed = true;
        yield* sendCommand(ctx, { type: "abort" }, PI_ABORT_TIMEOUT_MS).pipe(Effect.ignore);
      });

    const answerUiRequest = (
      threadId: ThreadId,
      requestId: ApprovalRequestId,
      answer: (pending: PiPendingUiRequest) => Record<string, unknown> | undefined,
    ) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const pending = ctx.pendingUiRequests.get(requestId);
        if (!pending) return;
        ctx.pendingUiRequests.delete(requestId);
        const response = answer(pending);
        if (response) {
          yield* ctx
            // @effect-diagnostics-next-line preferSchemaOverJson:off - Pi RPC framing is newline-delimited JSON by protocol.
            .writeLine(`${JSON.stringify(response)}\n`)
            .pipe(Effect.ignore);
        }
        yield* offerRuntimeEvent({
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: ctx.threadId,
          ...(ctx.activeTurnId ? { turnId: ctx.activeTurnId } : {}),
          requestId: RuntimeRequestId.make(requestId),
          type: "request.resolved",
          payload: { requestType: "dynamic_tool_call" },
        });
      });

    const respondToRequest: PiAdapterShape["respondToRequest"] = (threadId, requestId, decision) =>
      answerUiRequest(threadId, requestId, (pending) =>
        buildPiUiResponse(pending.piId, pending, decision),
      );

    const respondToUserInput: PiAdapterShape["respondToUserInput"] = (
      threadId,
      requestId,
      answers,
    ) =>
      answerUiRequest(threadId, requestId, (pending) => {
        if (pending.method === "confirm") {
          const first = Object.values(answers).find((value) => typeof value === "boolean");
          return buildPiUiResponse(pending.piId, pending, first === false ? "decline" : "accept");
        }
        const firstText = Object.values(answers).find(
          (value): value is string => typeof value === "string" && !!value.trim(),
        );
        if (firstText === undefined) {
          return buildPiUiResponse(pending.piId, pending, "cancel");
        }
        return { type: "extension_ui_response", id: pending.piId, value: firstText };
      });

    const stopSession: PiAdapterShape["stopSession"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = sessions.get(threadId);
        if (!ctx) return;
        yield* stopSessionInternal(ctx);
      });

    return {
      provider: PROVIDER,
      capabilities: {
        sessionModelSwitch: "in-session",
        supportsConversationRollback: false,
      },
      startSession,
      sendTurn,
      compaction: {
        type: "native",
        start: (threadId) =>
          Effect.gen(function* () {
            const ctx = yield* requireSession(threadId);
            yield* sendCommand(ctx, { type: "compact" }, PI_COMPACT_TIMEOUT_MS);
          }),
      },
      interruptTurn: (threadId) => interruptTurn(threadId),
      respondToRequest: (threadId, requestId, decision) =>
        respondToRequest(threadId, requestId, decision),
      respondToUserInput: (threadId, requestId, answers) =>
        respondToUserInput(threadId, requestId, answers),
      stopSession,
      listSessions: () =>
        Effect.succeed(
          Array.from(sessions.values())
            .filter((ctx) => !ctx.stopped)
            .map((ctx) => ctx.session),
        ),
      hasSession: (threadId) =>
        Effect.succeed(sessions.get(threadId) !== undefined && !sessions.get(threadId)?.stopped),
      readThread: (threadId) =>
        Effect.gen(function* () {
          const ctx = yield* requireSession(threadId);
          return {
            threadId,
            turns: ctx.turns.map((turn) => ({ id: turn.id, items: turn.items })),
          };
        }),
      rollbackThread: (threadId) =>
        Effect.gen(function* () {
          const ctx = yield* requireSession(threadId);
          return {
            threadId,
            turns: ctx.turns.map((turn) => ({ id: turn.id, items: turn.items })),
          };
        }),
      stopAll: () => stopAllInternal(),
      streamEvents: Stream.fromPubSub(runtimeEventPubSub),
    } satisfies ProviderAdapterShape<ProviderAdapterError>;
  });
