/**
 * PiProvider — snapshot + probe for the Pi coding agent driver.
 *
 * Mirrors the Grok provider module: a version probe (`pi --version`), an
 * auth probe (`pi auth check --provider <default> --json`; auth itself
 * stays Pi-side), and a structured catalog probe over a throwaway
 * `pi --mode rpc --no-session` process (`get_state` for the session
 * default model, `get_available_models` for the catalog, `get_commands`
 * for slash commands). The RPC round trip never sends a prompt, so
 * probing costs nothing.
 *
 * @module provider/Layers/PiProvider
 */
import {
  type CustomModelSetting,
  type ModelCapabilities,
  type PiSettings,
  type ServerProvider,
  type ServerProviderAuth,
  type ServerProviderModel,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Stream from "effect/Stream";
import * as Ndjson from "effect/unstable/encoding/Ndjson";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { HttpClient } from "effect/unstable/http";
import { causeErrorTag } from "@t3tools/shared/observability";

import {
  AUTH_PROBE_TIMEOUT_MS,
  buildServerProvider,
  COMPACT_SLASH_COMMAND,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  type ProviderMaintenanceCapabilities,
} from "../providerMaintenance.ts";
import {
  parsePiAuthCheck,
  piCommandsToSlashCommands,
  piRpcModelToSlug,
  piThinkingLevelDescriptor,
  resolvePiModelTarget,
  type PiRpcModel,
} from "../pi/PiProtocol.ts";

const PI_PRESENTATION = {
  displayName: "Pi",
  supportsConversationRollback: false,
  badgeLabel: "Early Access",
  showInteractionModeToggle: false,
} as const;

const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const VERSION_PROBE_TIMEOUT_MS = 4_000;
const PI_RPC_PROBE_TIMEOUT_MS = 20_000;
const PI_RPC_COMMAND_TIMEOUT_MS = 15_000;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function piModelsFromSettings(
  customModels: ReadonlyArray<CustomModelSetting> | undefined,
  builtInModels: ReadonlyArray<ServerProviderModel> = [],
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(builtInModels, customModels ?? [], EMPTY_CAPABILITIES);
}

export function buildInitialPiProviderSnapshot(
  piSettings: PiSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = piModelsFromSettings(piSettings.customModels);

    if (!piSettings.enabled) {
      return buildServerProvider({
        presentation: PI_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Pi is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking Pi CLI availability...",
      },
    });
  });
}

const runPiCliCommand = (
  piSettings: PiSettings,
  args: ReadonlyArray<string>,
  environment: NodeJS.ProcessEnv,
  cwd?: string,
) =>
  Effect.gen(function* () {
    const command = piSettings.binaryPath || "pi";
    const spawnCommand = yield* resolveSpawnCommand(command, args, { env: environment });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        ...(cwd ? { cwd } : {}),
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });

interface PiRpcProbeResult {
  readonly stateModel: { readonly provider: string; readonly id: string } | undefined;
  /** Pi's session-default thinking level from `get_state` (fresh `--no-session` probe). */
  readonly stateThinkingLevel: string | undefined;
  readonly models: ReadonlyArray<PiRpcModel>;
  readonly commands: ReadonlyArray<{ readonly name: string; readonly description?: string }>;
}

/**
 * One-shot structured probe: spawn `pi --mode rpc --no-session`, issue
 * `get_state` / `get_available_models` / `get_commands`, correlate by
 * command id, then tear the process down. No prompt is ever sent.
 */
const probePiRpcCatalog = (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv,
  cwd?: string,
): Effect.Effect<Option.Option<PiRpcProbeResult>, never, ChildProcessSpawner.ChildProcessSpawner> =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const command = piSettings.binaryPath || "pi";
    const spawnCommand = yield* resolveSpawnCommand(command, ["--mode", "rpc", "--no-session"], {
      env: environment,
    }).pipe(Effect.orElseSucceed(() => undefined));
    if (!spawnCommand) return Option.none<PiRpcProbeResult>();

    return yield* Effect.scoped(
      Effect.gen(function* () {
        const child = yield* spawner
          .spawn(
            ChildProcess.make(spawnCommand.command, spawnCommand.args, {
              ...(cwd ? { cwd } : {}),
              env: environment,
              shell: spawnCommand.shell,
              stdin: { stream: "pipe", endOnDone: false },
              stdout: "pipe",
              stderr: "pipe",
            }),
          )
          .pipe(Effect.orElseSucceed(() => undefined));
        if (!child) return Option.none<PiRpcProbeResult>();

        yield* child.stderr.pipe(Stream.runDrain, Effect.ignore, Effect.forkScoped);
        const wanted = ["pi-probe-state", "pi-probe-models", "pi-probe-commands"] as const;
        const seen = new Map<string, unknown>();
        const allSeen = yield* Deferred.make<void>();
        yield* child.stdout.pipe(
          Stream.pipeThroughChannel(Ndjson.decode({ ignoreEmptyLines: true })),
          Stream.runForEach((value) =>
            Effect.gen(function* () {
              if (!isRecord(value) || value.type !== "response") return;
              const id = nonEmptyString(value.id);
              if (!id || !(wanted as ReadonlyArray<string>).includes(id) || seen.has(id)) {
                return;
              }
              seen.set(id, value.success === true ? value.data : undefined);
              if (seen.size >= wanted.length) {
                yield* Deferred.succeed(allSeen, undefined);
              }
            }),
          ),
          Effect.ignore,
          Effect.forkScoped,
        );

        const payload =
          `{"id":"pi-probe-state","type":"get_state"}\n` +
          `{"id":"pi-probe-models","type":"get_available_models"}\n` +
          `{"id":"pi-probe-commands","type":"get_commands"}\n`;
        yield* Stream.run(Stream.encodeText(Stream.make(payload)), child.stdin).pipe(Effect.ignore);

        // Settle when every response arrives or the process exits, whichever
        // comes first. Event-driven (no polling) so frozen test clocks are
        // fine; the timeout is a backstop for live runs only.
        yield* Effect.raceFirst(Deferred.await(allSeen), child.exitCode.pipe(Effect.asVoid)).pipe(
          Effect.timeoutOption(PI_RPC_COMMAND_TIMEOUT_MS),
          Effect.ignore,
        );
        yield* child.kill().pipe(Effect.ignore);

        if (seen.size === 0) return Option.none<PiRpcProbeResult>();
        return Option.some(extractPiRpcProbeResult(seen));
      }),
    );
  }).pipe(
    Effect.timeoutOption(PI_RPC_PROBE_TIMEOUT_MS),
    Effect.map((option) => Option.flatten(option)),
    Effect.orElseSucceed(() => Option.none<PiRpcProbeResult>()),
  );

function extractPiRpcProbeResult(seen: ReadonlyMap<string, unknown>): PiRpcProbeResult {
  const stateData = seen.get("pi-probe-state");
  let stateModel: PiRpcProbeResult["stateModel"];
  let stateThinkingLevel: string | undefined;
  if (isRecord(stateData)) {
    if (isRecord(stateData.model)) {
      const provider = nonEmptyString(stateData.model.provider);
      const id = nonEmptyString(stateData.model.id);
      if (provider && id) stateModel = { provider, id };
    }
    stateThinkingLevel = nonEmptyString(stateData.thinkingLevel);
  }

  const modelsData = seen.get("pi-probe-models");
  const mutableModels: Array<PiRpcModel> = [];
  if (isRecord(modelsData) && Array.isArray(modelsData.models)) {
    for (const entry of modelsData.models) {
      if (!isRecord(entry)) continue;
      const provider = nonEmptyString(entry.provider);
      const id = nonEmptyString(entry.id);
      if (!provider || !id) continue;
      mutableModels.push({
        provider,
        id,
        name: nonEmptyString(entry.name) ?? id,
        ...(entry.reasoning === true ? { reasoning: true as const } : {}),
        ...(isRecord(entry.thinkingLevelMap)
          ? { thinkingLevelMap: { ...entry.thinkingLevelMap } }
          : {}),
      });
    }
  }
  const models: PiRpcProbeResult["models"] = mutableModels;

  const commandsData = seen.get("pi-probe-commands");
  const mutableCommands: Array<PiRpcProbeResult["commands"][number]> = [];
  if (isRecord(commandsData) && Array.isArray(commandsData.commands)) {
    for (const entry of commandsData.commands) {
      if (!isRecord(entry)) continue;
      const name = nonEmptyString(entry.name);
      if (!name) continue;
      const description = nonEmptyString(entry.description);
      if (description === undefined) {
        mutableCommands.push({ name });
      } else {
        mutableCommands.push({ name, description });
      }
    }
  }
  const commands: PiRpcProbeResult["commands"] = mutableCommands;

  return { stateModel, stateThinkingLevel, models, commands };
}

export const checkPiProviderStatus = Effect.fn("checkPiProviderStatus")(function* (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
  cwd?: string,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = piModelsFromSettings(piSettings.customModels);

  if (!piSettings.enabled) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Pi is disabled in T3 Code settings.",
      },
    });
  }

  const versionResult = yield* runPiCliCommand(piSettings, ["--version"], environment, cwd).pipe(
    Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionResult)) {
    const error = versionResult.failure;
    yield* Effect.logWarning("Pi CLI health check failed.", {
      errorTag: error._tag,
    });
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(error)
          ? "Pi CLI (`pi`) is not installed or not on PATH."
          : "Failed to execute Pi CLI health check.",
      },
    });
  }

  if (Option.isNone(versionResult.success)) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "Pi CLI is installed but timed out while running `pi --version`.",
      },
    });
  }

  const versionOutput = versionResult.success.value;
  const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
  if (versionOutput.code !== 0) {
    yield* Effect.logWarning("Pi CLI version probe exited with a non-zero status.", {
      exitCode: versionOutput.code,
    });
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: "Pi CLI is installed but failed to run.",
      },
    });
  }

  // Auth stays Pi-side. When a default upstream provider is configured,
  // report its credential state; otherwise auth is unknown until chat.
  const defaultProvider = piSettings.defaultProvider.trim();
  let auth: ServerProviderAuth = { status: "unknown" };
  if (defaultProvider) {
    const authResult = yield* runPiCliCommand(
      piSettings,
      ["auth", "check", "--provider", defaultProvider, "--json"],
      environment,
      cwd,
    ).pipe(Effect.timeoutOption(AUTH_PROBE_TIMEOUT_MS), Effect.exit);
    if (Exit.isSuccess(authResult) && Option.isSome(authResult.value)) {
      const output = authResult.value.value;
      auth =
        output.code === 0
          ? parsePiAuthCheck(output.stdout, defaultProvider)
          : { status: "unknown" };
    }
  }

  const catalogExit = yield* probePiRpcCatalog(piSettings, environment, cwd).pipe(Effect.exit);
  const catalog =
    Exit.isSuccess(catalogExit) && Option.isSome(catalogExit.value)
      ? Option.getOrUndefined(catalogExit.value)
      : undefined;
  if (!catalog) {
    yield* Effect.logWarning("Pi RPC catalog probe failed or timed out.");
  }

  const configuredDefault = resolvePiModelTarget(piSettings.defaultModel);
  const discoveredModels: ServerProviderModel[] = [];
  if (catalog) {
    const seen = new Set<string>();
    for (const model of catalog.models) {
      const slug = piRpcModelToSlug(model);
      if (!slug || seen.has(slug)) continue;
      seen.add(slug);
      const isDefault =
        (catalog.stateModel !== undefined &&
          catalog.stateModel.provider === model.provider &&
          catalog.stateModel.id === model.id) ||
        (configuredDefault.provider === model.provider && configuredDefault.modelId === model.id);
      const thinkingDescriptor = piThinkingLevelDescriptor(model, catalog.stateThinkingLevel);
      discoveredModels.push({
        slug,
        name: model.name,
        subProvider: model.provider,
        isCustom: false,
        ...(isDefault ? { isDefault: true } : {}),
        capabilities: thinkingDescriptor
          ? createModelCapabilities({ optionDescriptors: [thinkingDescriptor] })
          : EMPTY_CAPABILITIES,
      });
    }
  }
  const models =
    discoveredModels.length > 0
      ? piModelsFromSettings(piSettings.customModels, discoveredModels)
      : fallbackModels;

  if (auth.status === "unauthenticated") {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models,
      slashCommands: catalog
        ? piCommandsToSlashCommands(catalog.commands)
        : [COMPACT_SLASH_COMMAND],
      probe: {
        installed: true,
        version,
        status: "error",
        auth,
        message: `Pi CLI is installed but '${defaultProvider}' has no credentials. Run \`pi auth\` to sign in.`,
      },
    });
  }

  return buildServerProvider({
    presentation: PI_PRESENTATION,
    enabled: piSettings.enabled,
    checkedAt,
    models,
    slashCommands: catalog ? piCommandsToSlashCommands(catalog.commands) : [COMPACT_SLASH_COMMAND],
    probe: {
      installed: true,
      version,
      status: catalog ? "ready" : "warning",
      auth,
      ...(catalog ? {} : { message: "Pi CLI is installed but the model catalog probe failed." }),
    },
  });
});

export const enrichPiSnapshot = (input: {
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  readonly enableProviderUpdateChecks?: boolean;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
  readonly httpClient: HttpClient.HttpClient;
}): Effect.Effect<void> => {
  const { snapshot, publishSnapshot } = input;

  return enrichProviderSnapshotWithVersionAdvisory(snapshot, input.maintenanceCapabilities, {
    enableProviderUpdateChecks: input.enableProviderUpdateChecks,
  }).pipe(
    Effect.provideService(HttpClient.HttpClient, input.httpClient),
    Effect.flatMap((enrichedSnapshot) => publishSnapshot(enrichedSnapshot)),
    Effect.catchCause((cause) =>
      Effect.logWarning("Pi version advisory enrichment failed", {
        errorTag: causeErrorTag(cause),
      }),
    ),
    Effect.asVoid,
  );
};
