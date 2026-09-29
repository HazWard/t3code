// @effect-diagnostics nodeBuiltinImport:off
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { HttpClient } from "effect/unstable/http";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { NoOpProviderEventLoggers, ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { PiDriver } from "./PiDriver.ts";

const testCrypto = Crypto.make({
  randomBytes: (size) => new Uint8Array(size).fill(3),
  digest: (_algorithm, data) => Effect.succeed(data),
});

const testLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-pi-driver-test-",
}).pipe(
  Layer.provideMerge(NodeServices.layer),
  Layer.provideMerge(ServerSettingsService.layerTest()),
  Layer.provideMerge(
    Layer.mock(BackgroundPolicy.BackgroundPolicy)({
      shouldRunScopeWork: () => Effect.succeed(false),
    }),
  ),
  Layer.provideMerge(Layer.succeed(ProviderEventLoggers, NoOpProviderEventLoggers)),
  Layer.provideMerge(Layer.succeed(Crypto.Crypto, testCrypto)),
  Layer.provideMerge(
    Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make(() => Effect.die("Disabled Pi must not make an HTTP request")),
    ),
  ),
);

it.layer(testLayer)("PiDriver", (it) => {
  it.effect("ships disabled by default with the pi binary", () => {
    const defaults = PiDriver.defaultConfig();
    expect(PiDriver.driverKind).toBe("pi");
    expect(PiDriver.metadata.displayName).toBe("Pi");
    expect(PiDriver.metadata.supportsMultipleInstances).toBe(true);
    expect(defaults.enabled).toBe(false);
    expect(defaults.binaryPath).toBe("pi");
    return Effect.void;
  });

  it.effect("creates a disabled instance without probing", () =>
    Effect.gen(function* () {
      const instance = yield* PiDriver.create({
        instanceId: ProviderInstanceId.make("pi"),
        displayName: undefined,
        environment: [],
        enabled: false,
        config: PiDriver.defaultConfig(),
      });
      expect(instance.driverKind).toBe("pi");
      expect(instance.instanceId).toBe("pi");
      expect(instance.adapter.provider).toBe("pi");
      expect(instance.adapter.capabilities.sessionModelSwitch).toBe("in-session");
      expect(instance.textGeneration).toBeDefined();

      const snapshot = yield* instance.snapshot.getSnapshot;
      expect(snapshot.enabled).toBe(false);
      expect(snapshot.status).toBe("disabled");
      expect(snapshot.displayName).toBe("Pi");
    }),
  );
});
