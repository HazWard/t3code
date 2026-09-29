/**
 * PiProtocol — pure Pi RPC protocol helpers shared by the Pi adapter,
 * provider probe, and text generation.
 *
 * Everything here is synchronous and side-effect free so it can be unit
 * tested without spawning `pi`. Process management lives in
 * `../Layers/PiAdapter.ts`; the T3-side Pi extension contract it speaks
 * to lives in `./t3code.ts`.
 *
 * Model slug convention: Pi models are namespaced by their upstream
 * provider as `<provider>/<modelId>` (e.g. `opencode-go/gpt-5.6-luna`),
 * matching Pi's own `--model provider/id` vocabulary. The product slug
 * `pi-default` (see `PI_DEFAULT_MODEL` in contracts) means "whatever the
 * Pi session is already using" and is never sent to Pi.
 *
 * @module provider/pi/PiProtocol
 */
import {
  PI_DEFAULT_MODEL,
  type ProviderOptionDescriptor,
  type ServerProviderAuth,
} from "@t3tools/contracts";
import { tokenizeCliArgs } from "@t3tools/shared/cliArgs";

/** Environment contract set by the adapter on every Pi RPC process. Mirrored in `./t3code.ts`. */
export const PI_MANAGED_ENV = "T3CODE_PI_MANAGED";
export const PI_MCP_ENDPOINT_ENV = "T3CODE_MCP_ENDPOINT";
export const PI_MCP_AUTH_ENV = "T3CODE_MCP_AUTH";
export const PI_RUNTIME_MODE_ENV = "T3CODE_RUNTIME_MODE";

export const PI_RESUME_VERSION = 1 as const;

export interface PiModelTarget {
  readonly provider?: string | undefined;
  readonly modelId?: string | undefined;
}

/**
 * Resolve a T3 model slug to Pi `--provider`/`--model` (spawn) or
 * `set_model` (in-session) arguments. `pi-default`, empty, and bare slugs
 * without a `/` keep the session's current model.
 */
export function resolvePiModelTarget(model: string | undefined): PiModelTarget {
  const trimmed = model?.trim() ?? "";
  if (!trimmed || trimmed === PI_DEFAULT_MODEL) return {};
  const separator = trimmed.indexOf("/");
  if (separator <= 0 || separator === trimmed.length - 1) return {};
  return {
    provider: trimmed.slice(0, separator).trim(),
    modelId: trimmed.slice(separator + 1).trim(),
  };
}

export function piModelSlug(provider: string, modelId: string): string {
  return `${provider.trim()}/${modelId.trim()}`;
}

export interface PiRpcModel {
  readonly id: string;
  readonly name: string;
  readonly provider: string;
  readonly reasoning?: boolean | undefined;
  /** Pi thinking level → provider-native value; entries may be null (unsupported). */
  readonly thinkingLevelMap?: Record<string, unknown> | undefined;
}

/** Pi thinking levels in display order. */
export const PI_THINKING_LEVELS = [
  "off",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
] as const;

const PI_DEFAULT_THINKING_LEVELS: ReadonlySet<string> = new Set([
  "off",
  "minimal",
  "low",
  "medium",
  "high",
]);

/** Display labels mirroring the Codex/OpenCode reasoning conventions. */
const PI_THINKING_LEVEL_LABELS: Readonly<Record<string, string>> = {
  off: "Off",
  minimal: "Minimal",
  low: "Low",
  medium: "Medium",
  high: "High",
  xhigh: "Extra High",
  max: "Max",
};

/**
 * Reasoning option descriptor for a catalog model, or undefined for a
 * model the Pi catalog reports as reasoning-less (`get_state` returns
 * `["off"]` levels for those). With a `thinkingLevelMap`, only levels the
 * model maps to non-null provider values are offered. `defaultLevel` (pi's
 * session default from `get_state.thinkingLevel`) marks the matching
 * option so the composer trigger shows the active level instead of a bare
 * chevron; levels outside the supported set are ignored.
 */
export function piThinkingLevelDescriptor(
  model: PiRpcModel,
  defaultLevel?: string | undefined,
): ProviderOptionDescriptor | undefined {
  if (model.reasoning !== true) return undefined;
  const supported = model.thinkingLevelMap
    ? PI_THINKING_LEVELS.filter((level) => model.thinkingLevelMap?.[level] != null)
    : PI_THINKING_LEVELS.filter((level) => PI_DEFAULT_THINKING_LEVELS.has(level));
  if (supported.length === 0) return undefined;
  return {
    id: "reasoningEffort",
    label: "Reasoning",
    type: "select",
    options: supported.map((id) => ({
      id,
      label: PI_THINKING_LEVEL_LABELS[id] ?? id,
      ...(id === defaultLevel ? { isDefault: true } : {}),
    })),
  } satisfies ProviderOptionDescriptor;
}

export function piRpcModelToSlug(model: PiRpcModel): string | undefined {
  const provider = model.provider.trim();
  const id = model.id.trim();
  if (!provider || !id) return undefined;
  return piModelSlug(provider, id);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

/**
 * Parse `pi auth check --json` output. Ready means the upstream provider
 * has credentials; anything else is unauthenticated (Pi exits 0 either
 * way, so the JSON body is the only signal).
 */
export function parsePiAuthCheck(jsonText: string, provider: string): ServerProviderAuth {
  let parsed: unknown;
  try {
    parsed = JSON.parse(jsonText) as unknown;
  } catch {
    return { status: "unknown" };
  }
  if (!isRecord(parsed)) return { status: "unknown" };
  if (parsed.status === "ready") {
    const authType = nonEmptyString(parsed.authType);
    return {
      status: "authenticated",
      ...(authType ? { type: authType } : {}),
      label: `Pi ${provider}`,
    };
  }
  if (parsed.status === "not_ready") {
    return { status: "unauthenticated" };
  }
  return { status: "unknown" };
}

export interface PiTurnUsage {
  readonly inputTokens?: number | undefined;
  readonly outputTokens?: number | undefined;
  readonly cachedInputTokens?: number | undefined;
  readonly reasoningTokens?: number | undefined;
}

/**
 * Normalize a Pi `usage` block (`message_update.usage` / assistant
 * `usage`) to main-agent turn usage. Pi reports cumulative provider
 * usage; Pi has no subagents, so everything is main-agent scoped.
 */
export function normalizePiTurnUsage(usage: unknown): PiTurnUsage {
  if (!isRecord(usage)) return {};
  const integer = (value: unknown): number | undefined =>
    typeof value === "number" && Number.isInteger(value) && value >= 0 ? value : undefined;
  return {
    ...(integer(usage.input) !== undefined ? { inputTokens: integer(usage.input) } : {}),
    ...(integer(usage.output) !== undefined ? { outputTokens: integer(usage.output) } : {}),
    ...(integer(usage.cacheRead) !== undefined
      ? { cachedInputTokens: integer(usage.cacheRead) }
      : {}),
  };
}

const PI_APPROVAL_REQUEST_TYPES = ["command_execution_approval", "dynamic_tool_call"] as const;

/** Request type for a `tool_call` gate prompt, by tool. */
export function piApprovalRequestType(
  toolName: string,
): (typeof PI_APPROVAL_REQUEST_TYPES)[number] {
  return toolName === "bash" || toolName === "powershell"
    ? "command_execution_approval"
    : "dynamic_tool_call";
}

export type PiCanonicalItemType =
  | "command_execution"
  | "file_change"
  | "mcp_tool_call"
  | "dynamic_tool_call";

/** Timeline item type for a Pi tool call, by tool. */
export function piToolItemType(toolName: string): PiCanonicalItemType {
  if (toolName === "bash" || toolName === "powershell") return "command_execution";
  if (toolName === "edit" || toolName === "write") return "file_change";
  if (
    toolName.startsWith("preview_") ||
    toolName.startsWith("device_") ||
    toolName.startsWith("link_") ||
    toolName.startsWith("unlink_") ||
    toolName.startsWith("list_thread_")
  ) {
    return "mcp_tool_call";
  }
  return "dynamic_tool_call";
}

export interface PiResumeCursor {
  readonly schemaVersion: typeof PI_RESUME_VERSION;
  readonly piSessionId: string;
}

export function encodePiResumeCursor(piSessionId: string): PiResumeCursor {
  return { schemaVersion: PI_RESUME_VERSION, piSessionId };
}

export function parsePiResumeCursor(raw: unknown): PiResumeCursor | undefined {
  if (!isRecord(raw)) return undefined;
  if (raw.schemaVersion !== PI_RESUME_VERSION) return undefined;
  const piSessionId = nonEmptyString(raw.piSessionId);
  if (!piSessionId) return undefined;
  return { schemaVersion: PI_RESUME_VERSION, piSessionId };
}

export interface PiRpcArgsInput {
  /**
   * Directory for `pi --session-dir`. When omitted the flag is left out and
   * pi uses its own default (`~/.pi/agent`).
   */
  readonly sessionDir?: string | undefined;
  readonly sessionId: string;
  readonly extensionPath: string;
  readonly provider?: string | undefined;
  readonly modelId?: string | undefined;
  readonly launchArgs?: string | undefined;
}

/** argv for `pi --mode rpc` for one T3 thread. */
export function buildPiRpcArgs(input: PiRpcArgsInput): ReadonlyArray<string> {
  const args = [
    "--mode",
    "rpc",
    ...(input.sessionDir ? ["--session-dir", input.sessionDir] : []),
    "--session-id",
    input.sessionId,
    "--extension",
    input.extensionPath,
  ];
  if (input.provider) args.push("--provider", input.provider);
  if (input.modelId) args.push("--model", input.modelId);
  return [...args, ...tokenizeCliArgs(input.launchArgs)];
}

export interface PiRpcCommandDescription {
  readonly name: string;
  readonly description?: string | undefined;
}

/** Map Pi `get_commands` entries to T3 slash commands (compact first). */
export function piCommandsToSlashCommands(
  commands: ReadonlyArray<PiRpcCommandDescription>,
): Array<{ name: string; description?: string }> {
  const seen = new Set<string>(["compact"]);
  const out: Array<{ name: string; description?: string }> = [
    { name: "compact", description: "Summarize the conversation and reduce context usage" },
  ];
  for (const command of commands) {
    const name = command.name.trim().replace(/^\/+/, "");
    if (!name || seen.has(name)) continue;
    seen.add(name);
    out.push({
      name,
      ...(command.description?.trim() ? { description: command.description.trim() } : {}),
    });
  }
  return out;
}

export type PiUiDialogMethod = "select" | "confirm" | "input" | "editor";

export interface PendingPiUiRequest {
  readonly method: string;
  readonly options?: ReadonlyArray<string> | undefined;
}

const isUiDialogMethod = (method: string): method is PiUiDialogMethod =>
  method === "select" || method === "confirm" || method === "input" || method === "editor";

/**
 * Build the `extension_ui_response` answering a Pi dialog request from a
 * T3 approval decision. Accept (including session/all variants) maps to
 * the affirmative answer, anything else cancels (the extension treats
 * cancellation as denial).
 */
export function buildPiUiResponse(
  requestId: string,
  pending: PendingPiUiRequest,
  decision: "accept" | "acceptForSession" | "acceptAlways" | "decline" | "cancel",
): Record<string, unknown> {
  const base = { type: "extension_ui_response", id: requestId };
  if (decision !== "accept" && decision !== "acceptForSession" && decision !== "acceptAlways") {
    return { ...base, cancelled: true };
  }
  if (!isUiDialogMethod(pending.method)) return { ...base, cancelled: true };
  if (pending.method === "confirm") return { ...base, confirmed: true };
  const firstOption = pending.options?.[0];
  return { ...base, value: firstOption ?? true };
}
