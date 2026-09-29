// @effect-diagnostics nodeBuiltinImport:off
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import { PiSettings } from "@t3tools/contracts";

import { writeFakeCli } from "../../testUtils/fakeCli.ts";
import { buildInitialPiProviderSnapshot, checkPiProviderStatus } from "./PiProvider.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);

/** Stand-in for the Pi CLI: `--version`, `auth check`, and a scripted `--mode rpc` loop. */
const writeFakePiCli = () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-pi-probe-" });
    return writeFakeCli({
      directory: dir,
      name: "pi",
      source: [
        "import { createInterface } from 'node:readline';",
        "const args = process.argv.slice(2);",
        "if (args[0] === '--version') {",
        "  process.stdout.write('0.85.1\\n');",
        "  process.exit(0);",
        "}",
        "if (args[0] === 'auth') {",
        "  const at = args.indexOf('--provider');",
        "  const provider = at >= 0 ? args[at + 1] : '';",
        "  const ready = provider === 'opencode-go';",
        "  process.stdout.write(JSON.stringify(ready",
        "    ? { status: 'ready', provider, authType: 'api_key' }",
        "    : { status: 'not_ready', provider, reason: 'credentials_not_configured' }) + '\\n');",
        "  process.exit(0);",
        "}",
        "if (args[0] !== '--mode') process.exit(11);",
        "const respond = (id, command, data) => {",
        "  process.stdout.write(JSON.stringify({ id, type: 'response', command, success: true, data }) + '\\n');",
        "};",
        "const rl = createInterface({ input: process.stdin });",
        "rl.on('line', (line) => {",
        "  let msg;",
        "  try { msg = JSON.parse(line); } catch { return; }",
        "  if (msg.type === 'get_state') {",
        "    respond(msg.id, 'get_state', { sessionId: 'pi-probe-1', thinkingLevel: 'high', model: { id: 'gpt-5.6-luna', name: 'GPT-5.6 Luna', provider: 'opencode-go' } });",
        "  } else if (msg.type === 'get_available_models') {",
        "    respond(msg.id, 'get_available_models', { models: [",
        "      { id: 'gpt-5.6-luna', name: 'GPT-5.6 Luna', provider: 'opencode-go', reasoning: true, thinkingLevelMap: { off: 'off', minimal: 'minimal', low: 'low', medium: 'medium', high: 'high', xhigh: null, max: null } },",
        "      { id: 'claude-sonnet-4-20250514', name: 'Claude Sonnet 4', provider: 'anthropic' },",
        "    ] });",
        "  } else if (msg.type === 'get_commands') {",
        "    respond(msg.id, 'get_commands', { commands: [",
        "      { name: 'review', description: 'Review code', source: 'extension' },",
        "    ] });",
        "  } else {",
        "    respond(msg.id, msg.type ?? 'unknown', {});",
        "  }",
        "});",
        "process.stdin.on('end', () => process.exit(0));",
        "",
      ].join("\n"),
    });
  });

describe("buildInitialPiProviderSnapshot", () => {
  it.effect("returns a disabled snapshot when Pi is opt-in and off", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialPiProviderSnapshot(decodePiSettings({}));
      expect(snapshot.enabled).toBe(false);
      expect(snapshot.status).toBe("disabled");
      expect(snapshot.displayName).toBe("Pi");
      expect(snapshot.badgeLabel).toBe("Early Access");
    }),
  );

  it.effect("returns a pending snapshot when enabled", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialPiProviderSnapshot(decodePiSettings({ enabled: true }));
      expect(snapshot.enabled).toBe(true);
      expect(snapshot.installed).toBe(true);
      expect(snapshot.status).toBe("warning");
      expect(snapshot.version).toBeNull();
      expect(snapshot.message).toContain("Checking Pi");
      expect(snapshot.supportsConversationRollback).toBe(false);
    }),
  );
});

it.layer(NodeServices.layer)("checkPiProviderStatus", (it) => {
  it.effect("reports the binary as missing when the binary path does not resolve", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkPiProviderStatus(
        decodePiSettings({
          enabled: true,
          binaryPath: "/definitely/not/installed/pi-binary",
        }),
      );
      expect(snapshot.enabled).toBe(true);
      expect(snapshot.installed).toBe(false);
      expect(snapshot.status).toBe("error");
      expect(snapshot.message).toMatch(/not installed|not on PATH|Failed to execute/);
    }),
  );

  it.effect("reports ready with RPC-discovered models and commands", () =>
    Effect.gen(function* () {
      const snapshot = yield* Effect.scoped(
        Effect.gen(function* () {
          const piPath = yield* writeFakePiCli();
          return yield* checkPiProviderStatus(
            decodePiSettings({ enabled: true, binaryPath: piPath }),
          );
        }),
      );
      expect(snapshot.enabled).toBe(true);
      expect(snapshot.installed).toBe(true);
      expect(snapshot.version).toBe("0.85.1");
      expect(snapshot.status).toBe("ready");
      expect(snapshot.auth).toEqual({ status: "unknown" });
      expect(snapshot.models.map((model) => model.slug)).toEqual([
        "opencode-go/gpt-5.6-luna",
        "anthropic/claude-sonnet-4-20250514",
      ]);
      expect(snapshot.models[0]?.subProvider).toBe("opencode-go");
      expect(snapshot.models[0]?.isDefault).toBe(true);
      expect(
        snapshot.models[0]?.capabilities?.optionDescriptors?.map((option) => option.id),
      ).toEqual(["reasoningEffort"]);
      const lunaDescriptor = snapshot.models[0]?.capabilities?.optionDescriptors?.[0];
      const lunaOptions = lunaDescriptor?.type === "select" ? lunaDescriptor.options : [];
      expect(lunaOptions.map((option) => option.id)).toEqual([
        "off",
        "minimal",
        "low",
        "medium",
        "high",
      ]);
      expect(lunaOptions.find((option) => option.id === "high")?.isDefault).toBe(true);
      expect(lunaOptions.filter((option) => option.isDefault).length).toBe(1);
      expect(snapshot.models[1]?.capabilities?.optionDescriptors).toEqual([]);
      expect(snapshot.slashCommands.map((command) => command.name)).toEqual(["compact", "review"]);
    }),
  );

  it.effect("reports authenticated when the default provider has Pi credentials", () =>
    Effect.gen(function* () {
      const snapshot = yield* Effect.scoped(
        Effect.gen(function* () {
          const piPath = yield* writeFakePiCli();
          return yield* checkPiProviderStatus(
            decodePiSettings({
              enabled: true,
              binaryPath: piPath,
              defaultProvider: "opencode-go",
            }),
          );
        }),
      );
      expect(snapshot.status).toBe("ready");
      expect(snapshot.auth).toEqual({
        status: "authenticated",
        type: "api_key",
        label: "Pi opencode-go",
      });
    }),
  );

  it.effect("reports unauthenticated when the default provider lacks Pi credentials", () =>
    Effect.gen(function* () {
      const snapshot = yield* Effect.scoped(
        Effect.gen(function* () {
          const piPath = yield* writeFakePiCli();
          return yield* checkPiProviderStatus(
            decodePiSettings({
              enabled: true,
              binaryPath: piPath,
              defaultProvider: "mistral",
            }),
          );
        }),
      );
      expect(snapshot.status).toBe("error");
      expect(snapshot.auth).toEqual({ status: "unauthenticated" });
      expect(snapshot.message).toContain("mistral");
    }),
  );

  it.effect("appends custom models after discovered ones", () =>
    Effect.gen(function* () {
      const snapshot = yield* Effect.scoped(
        Effect.gen(function* () {
          const piPath = yield* writeFakePiCli();
          return yield* checkPiProviderStatus(
            decodePiSettings({
              enabled: true,
              binaryPath: piPath,
              customModels: ["my-org/custom-model"],
            }),
          );
        }),
      );
      expect(snapshot.models.map((model) => model.slug)).toEqual([
        "opencode-go/gpt-5.6-luna",
        "anthropic/claude-sonnet-4-20250514",
        "my-org/custom-model",
      ]);
      expect(snapshot.models[2]?.isCustom).toBe(true);
    }),
  );
});
