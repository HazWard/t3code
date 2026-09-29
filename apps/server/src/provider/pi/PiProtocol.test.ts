// @effect-diagnostics nodeBuiltinImport:off
import { describe, expect, it } from "@effect/vitest";

import { PI_DEFAULT_MODEL } from "@t3tools/contracts";
import {
  buildPiRpcArgs,
  buildPiUiResponse,
  encodePiResumeCursor,
  normalizePiTurnUsage,
  parsePiAuthCheck,
  parsePiResumeCursor,
  piApprovalRequestType,
  piCommandsToSlashCommands,
  piRpcModelToSlug,
  piThinkingLevelDescriptor,
  piToolItemType,
  resolvePiModelTarget,
} from "./PiProtocol.ts";

describe("resolvePiModelTarget", () => {
  it("keeps the session model for pi-default, empty, and bare slugs", () => {
    expect(resolvePiModelTarget(PI_DEFAULT_MODEL)).toEqual({});
    expect(resolvePiModelTarget(undefined)).toEqual({});
    expect(resolvePiModelTarget("")).toEqual({});
    expect(resolvePiModelTarget("gpt-5.6-luna")).toEqual({});
  });

  it("splits provider/model slugs", () => {
    expect(resolvePiModelTarget("opencode-go/gpt-5.6-luna")).toEqual({
      provider: "opencode-go",
      modelId: "gpt-5.6-luna",
    });
    expect(resolvePiModelTarget("anthropic/claude-sonnet-4-20250514")).toEqual({
      provider: "anthropic",
      modelId: "claude-sonnet-4-20250514",
    });
  });

  it("rejects malformed slugs", () => {
    expect(resolvePiModelTarget("/model")).toEqual({});
    expect(resolvePiModelTarget("provider/")).toEqual({});
  });

  it("round-trips through piRpcModelToSlug", () => {
    expect(piRpcModelToSlug({ provider: "opencode-go", id: "x", name: "X" })).toBe("opencode-go/x");
    expect(piRpcModelToSlug({ provider: "", id: "x", name: "X" })).toBeUndefined();
    expect(piRpcModelToSlug({ provider: "p", id: " ", name: "X" })).toBeUndefined();
  });
});

describe("parsePiAuthCheck", () => {
  it("maps ready to an authenticated Pi credential", () => {
    expect(
      parsePiAuthCheck(
        JSON.stringify({ status: "ready", provider: "opencode-go", authType: "api_key" }),
        "opencode-go",
      ),
    ).toEqual({ status: "authenticated", type: "api_key", label: "Pi opencode-go" });
  });

  it("maps not_ready to unauthenticated", () => {
    expect(
      parsePiAuthCheck(
        JSON.stringify({
          status: "not_ready",
          provider: "mistral",
          reason: "credentials_not_configured",
        }),
        "mistral",
      ),
    ).toEqual({ status: "unauthenticated" });
  });

  it("maps garbage to unknown", () => {
    expect(parsePiAuthCheck("not json", "x")).toEqual({ status: "unknown" });
    expect(parsePiAuthCheck("{}", "x")).toEqual({ status: "unknown" });
  });
});

describe("normalizePiTurnUsage", () => {
  it("picks the known counters", () => {
    expect(normalizePiTurnUsage({ input: 10, output: 4, cacheRead: 6, totalTokens: 20 })).toEqual({
      inputTokens: 10,
      outputTokens: 4,
      cachedInputTokens: 6,
    });
  });

  it("drops non-integer counters and non-records", () => {
    expect(normalizePiTurnUsage({ input: 1.5, output: "4" })).toEqual({});
    expect(normalizePiTurnUsage(null)).toEqual({});
  });
});

describe("tool/request mapping", () => {
  it("maps lifecycle item types by tool", () => {
    expect(piToolItemType("bash")).toBe("command_execution");
    expect(piToolItemType("powershell")).toBe("command_execution");
    expect(piToolItemType("edit")).toBe("file_change");
    expect(piToolItemType("write")).toBe("file_change");
    expect(piToolItemType("preview_navigate")).toBe("mcp_tool_call");
    expect(piToolItemType("device_open")).toBe("mcp_tool_call");
    expect(piToolItemType("link_pull_request")).toBe("mcp_tool_call");
    expect(piToolItemType("read")).toBe("dynamic_tool_call");
    expect(piToolItemType("some-extension-tool")).toBe("dynamic_tool_call");
  });

  it("uses command approval for shells and dynamic approval otherwise", () => {
    expect(piApprovalRequestType("bash")).toBe("command_execution_approval");
    expect(piApprovalRequestType("write")).toBe("dynamic_tool_call");
  });
});

describe("resume cursor", () => {
  it("round-trips and rejects foreign payloads", () => {
    const encoded = encodePiResumeCursor("session-123");
    expect(parsePiResumeCursor(encoded)).toEqual({
      schemaVersion: 1,
      piSessionId: "session-123",
    });
    expect(parsePiResumeCursor({ schemaVersion: 99, piSessionId: "x" })).toBeUndefined();
    expect(parsePiResumeCursor(null)).toBeUndefined();
  });
});

describe("buildPiRpcArgs", () => {
  it("pins session isolation, the T3 extension, and model flags", () => {
    expect(
      buildPiRpcArgs({
        sessionDir: "/state/pi-sessions/thread-1",
        sessionId: "thread-1",
        extensionPath: "/srv/pi/t3code.ts",
        provider: "opencode-go",
        modelId: "opencode-go/gpt-5.6-luna",
        launchArgs: "--thinking high",
      }),
    ).toEqual([
      "--mode",
      "rpc",
      "--session-dir",
      "/state/pi-sessions/thread-1",
      "--session-id",
      "thread-1",
      "--extension",
      "/srv/pi/t3code.ts",
      "--provider",
      "opencode-go",
      "--model",
      "opencode-go/gpt-5.6-luna",
      "--thinking",
      "high",
    ]);
  });

  it("omits model flags when Pi defaults apply", () => {
    expect(
      buildPiRpcArgs({
        sessionDir: "/s",
        sessionId: "id",
        extensionPath: "/e.ts",
        launchArgs: "",
      }),
    ).toEqual([
      "--mode",
      "rpc",
      "--session-dir",
      "/s",
      "--session-id",
      "id",
      "--extension",
      "/e.ts",
    ]);
  });

  it("omits --session-dir when Pi's own session directory applies", () => {
    expect(buildPiRpcArgs({ sessionId: "id", extensionPath: "/e.ts" })).toEqual([
      "--mode",
      "rpc",
      "--session-id",
      "id",
      "--extension",
      "/e.ts",
    ]);
  });
});

describe("piThinkingLevelDescriptor", () => {
  it("returns undefined for models without reasoning", () => {
    expect(piThinkingLevelDescriptor({ id: "x", name: "X", provider: "p" })).toBeUndefined();
  });

  it("offers the standard levels when reasoning and no map", () => {
    const descriptor = piThinkingLevelDescriptor({
      id: "x",
      name: "X",
      provider: "p",
      reasoning: true,
    });
    expect(descriptor?.id).toBe("reasoningEffort");
    const options = descriptor?.type === "select" ? descriptor.options : [];
    expect(options.map((option) => option.id)).toEqual(["off", "minimal", "low", "medium", "high"]);
    expect(options.map((option) => option.label)).toEqual([
      "Off",
      "Minimal",
      "Low",
      "Medium",
      "High",
    ]);
  });

  it("filters map entries with null provider values", () => {
    const descriptor = piThinkingLevelDescriptor({
      id: "x",
      name: "X",
      provider: "p",
      reasoning: true,
      thinkingLevelMap: { off: "off", low: "low", xhigh: null, max: null },
    });
    const options = descriptor?.type === "select" ? descriptor.options : [];
    expect(options.map((option) => option.id)).toEqual(["off", "low"]);
  });

  it("returns undefined when the map supports no levels", () => {
    expect(
      piThinkingLevelDescriptor({
        id: "x",
        name: "X",
        provider: "p",
        reasoning: true,
        thinkingLevelMap: { xhigh: null },
      }),
    ).toBeUndefined();
  });

  it("marks the pi default level so the trigger shows it", () => {
    const descriptor = piThinkingLevelDescriptor(
      {
        id: "x",
        name: "X",
        provider: "p",
        reasoning: true,
        thinkingLevelMap: { low: "low", high: "high", max: null },
      },
      "high",
    );
    const options = descriptor?.type === "select" ? descriptor.options : [];
    expect(options.map((option) => option.id)).toEqual(["low", "high"]);
    expect(options.find((option) => option.id === "high")?.label).toBe("High");
    expect(options.find((option) => option.id === "high")?.isDefault).toBe(true);
    expect(options.find((option) => option.id === "low")?.isDefault).toBeUndefined();
  });

  it("ignores a default level outside the supported set", () => {
    const descriptor = piThinkingLevelDescriptor(
      {
        id: "x",
        name: "X",
        provider: "p",
        reasoning: true,
      },
      "bogus",
    );
    const options = descriptor?.type === "select" ? descriptor.options : [];
    expect(options.filter((option) => option.isDefault)).toEqual([]);
  });
});

describe("piCommandsToSlashCommands", () => {
  it("keeps compact first and dedupes", () => {
    expect(
      piCommandsToSlashCommands([
        { name: "compact", description: "native" },
        { name: "/review", description: "Review code" },
        { name: "review" },
        { name: "  " },
      ]),
    ).toEqual([
      { name: "compact", description: "Summarize the conversation and reduce context usage" },
      { name: "review", description: "Review code" },
    ]);
  });
});

describe("buildPiUiResponse", () => {
  it("confirms confirm dialogs and picks the first select option", () => {
    expect(buildPiUiResponse("id-1", { method: "confirm" }, "accept")).toEqual({
      type: "extension_ui_response",
      id: "id-1",
      confirmed: true,
    });
    expect(
      buildPiUiResponse(
        "id-2",
        { method: "select", options: ["Allow", "Block"] },
        "acceptForSession",
      ),
    ).toEqual({ type: "extension_ui_response", id: "id-2", value: "Allow" });
  });

  it("cancels on decline, cancel, and unknown methods", () => {
    expect(buildPiUiResponse("id-1", { method: "confirm" }, "decline")).toEqual({
      type: "extension_ui_response",
      id: "id-1",
      cancelled: true,
    });
    expect(buildPiUiResponse("id-1", { method: "confirm" }, "cancel")).toEqual({
      type: "extension_ui_response",
      id: "id-1",
      cancelled: true,
    });
    expect(buildPiUiResponse("id-1", { method: "notify" }, "accept")).toEqual({
      type: "extension_ui_response",
      id: "id-1",
      cancelled: true,
    });
  });
});
