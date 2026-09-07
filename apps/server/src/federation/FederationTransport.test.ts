import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { EnvironmentId, type TailcatNodeKey } from "@t3tools/contracts";
import * as NetService from "@t3tools/shared/Net";
import * as TailcatRuntime from "@t3tools/tailcat/runtime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as TestClock from "effect/testing/TestClock";
import { HttpClient } from "effect/unstable/http";

import * as ServerConfig from "../config.ts";
import * as FederationTransport from "./FederationTransport.ts";

const NODE_KEY: TailcatNodeKey =
  "nodekey:9ab555a4a588b75d2054adb683db82461bb6c707d43e8ba39439f8eb1e821503";

it.effect("reuses a fresh forward until idle expiry and reopens it on demand", () =>
  Effect.gen(function* () {
    yield* TestClock.adjust("1 hour");
    const closed = yield* Queue.unbounded<number>();
    const nextPort = yield* Ref.make(40_000);
    const runtime = Layer.mock(TailcatRuntime.TailcatRuntime)({
      generateClientIdentity: () => Effect.succeed({ nodeKey: NODE_KEY }),
      forward: (options) =>
        Effect.gen(function* () {
          const running = yield* Ref.make(true);
          const stop = Ref.set(running, false).pipe(
            Effect.andThen(Queue.offer(closed, options.localPort)),
            Effect.asVoid,
          );
          yield* Effect.addFinalizer(() => stop);
          return {
            pid: options.localPort,
            address: options.address,
            remotePort: options.remotePort,
            localPort: options.localPort,
            httpBaseUrl: `http://127.0.0.1:${options.localPort}`,
            wsBaseUrl: `ws://127.0.0.1:${options.localPort}`,
            exit: Effect.never,
            isRunning: Ref.get(running),
            recentOutput: Effect.succeed([]),
            stop,
          };
        }),
    });
    const transport = yield* FederationTransport.make.pipe(
      Effect.provide(
        Layer.mergeAll(
          runtime,
          Layer.mock(NetService.NetService)({
            reserveLoopbackPort: () => Ref.updateAndGet(nextPort, (port) => port + 1),
          }),
          Layer.succeed(
            HttpClient.HttpClient,
            HttpClient.make(() => Effect.die("Mock forwards must not make HTTP requests")),
          ),
          ServerConfig.layerTest(process.cwd(), { prefix: "t3-federation-transport-test-" }),
        ),
      ),
    );
    const peer = {
      peerId: EnvironmentId.make("environment-peer"),
      transport: {
        tailcat: {
          address:
            "tco2FwWCB-p3FjjOrzlCPp0w8aT3p9xDZ1nNaXWX_dASxDCFT_MmFrWCDRnh2-iykbZ7W4Fl0g3nBpwTnR3iXVCKKCk4pps47ndGFpGQEu",
          port: 3773,
        },
      },
    };
    const first = yield* transport.endpointFor(peer);
    assert.deepEqual(yield* transport.endpointFor(peer), first);
    assert.equal(yield* Ref.get(nextPort), first.localPort);

    yield* TestClock.adjust("9 minutes");
    assert.isTrue(yield* transport.isActive(peer.peerId));
    yield* TestClock.adjust("2 minutes");
    assert.equal(yield* Queue.take(closed), first.localPort);
    assert.isFalse(yield* transport.isActive(peer.peerId));

    const reopened = yield* transport.endpointFor(peer);
    assert.notEqual(reopened.localPort, first.localPort);
    assert.isTrue(yield* transport.isActive(peer.peerId));
    yield* transport.drop(peer.peerId);
    assert.equal(yield* Queue.take(closed), reopened.localPort);
    assert.isFalse(yield* transport.isActive(peer.peerId));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
