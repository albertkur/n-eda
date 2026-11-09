# Known Issues

Defects found while documenting n-eda 7.0.3, split into what is still open and what has since been fixed.

The **Open** items describe current behavior — the rest of the docs are written to match them, so treat this
file as the authority when something surprises you.

---

# Open

## 1. Distributed observer is broken under all proxy consumers

**Severity: high — silent data loss. Scope: AWS Lambda / RPC / gRPC proxying only.**

All three out-of-process handlers look up observer registrations by the bare event name:

```typescript
// aws-lambda-event-handler.ts:90, rpc-event-handler.ts:88, grpc-event-handler.ts:87
const eventRegistration = isObservedEvent
    ? this._manager!.observerEventMap.get(event.name)
    : this._manager!.eventMap.get(event.name);
```

But `observerEventMap` is keyed by the **observation key** — `` `.${observer}.${observable}.${event}` ``
(`eda-manager.ts:192`, `event-registration.ts:125`). The lookup is therefore always `undefined`, the
handler returns without dispatching, and the event is marked processed.

The correct pattern is in the in-process consumer, `consumer.ts:261-266`:

```typescript
const observationKey = EventRegistration.generateObservationKey(
    distributedObserverEvent.observerTypeName,
    distributedObserverEvent.observedEvent.refType,
    distributedObserverEvent.observedEvent.name);
eventRegistration = this._manager.observerEventMap.get(observationKey);
```

**Workaround:** use the in-process Redis consumer for observer handlers. That path is covered by
`test/observer.test.ts` and works.

---

## 2. Observer fan-out is skipped for pure publishers

**Severity: high — silent, and the failure looks like a configuration problem.**

`RedisEventBus.publish` returns early when partition bucketing produced nothing:

```typescript
// redis-event-bus.ts:116-117
if (partitionEvents.size === 0)
    return;      // never reaches the observer fan-out block at :128
```

Bucketing skips any event whose name is absent from **the publishing process's own** `eventMap`, and
`eventMap` holds only `@event` handlers — `@observedEvent` handlers are indexed in `observerEventMap`
instead. So the drop happens both cross-service (the Order service publishes `OrderShippedEvent` and has no
handler for it, because the Customer service is the one observing) and in a single process (an observer
handler alone never puts the event in `eventMap`).

**Workaround:** call `.forcePublish()` on the observable's topic. `test/utils/observer-test-utils.ts` does
exactly this, and removing it makes `test/observer.test.ts` fail with zero notifications — a reliable
reproduction.

Deliberately not fixed: the apparent one-line control-flow change has wider implications than it looks.

---

## 3. `EdaContext.topic` throws under proxy consumers

**Severity: medium. Scope: AWS Lambda / RPC / gRPC proxying only.**

`DefaultEdaContext.topic` is only populated by `RedisEventSubMgr.onEventReceived`
(`redis-event-sub-mgr.ts:156-164`). The `onEventReceived` overrides in `AwsLambdaEventHandler`,
`RpcEventHandler`, and `GrpcEventHandler` are empty no-ops, so the scoped context is never set and the
getter fails its `given(...).ensure(t => t._topic != null, "topic not set")`.

**Impact:** a handler that injects `"EdaContext"` works in-process and throws under any proxy.

---

## 4. `dispose()` disposes a container it does not own

**Severity: low, but surprising.**

`EdaManager` tracks `_ownsContainer` and correctly skips `container.bootstrap()` for an externally supplied
container — but `dispose()` (`eda-manager.ts:447-459`) disposes it unconditionally. Sharing a container
between `EdaManager` and the rest of an app means disposing the manager takes the app's container with it.

Deliberately not fixed: correcting the ownership check would break anyone currently relying on the manager
to tear their container down. Documented instead, on `EdaManager.dispose`.

---

## 5. `mapToPartition` is case-sensitive but duplicate detection is not

**Severity: low.**

`registerTopics` rejects duplicates case-insensitively (`topic.name.toLowerCase()`), but `_topicMap` is
keyed by the exact name (`eda-manager.ts:380`), as is the whole publish path. A topic registered as `"foo"`
cannot be resolved by `mapToPartition("Foo", ...)`.

The confusing part — a generic, unnamed assertion failure — **has been fixed**; the assertion now explains
that names are matched case-sensitively and only resolve after bootstrap. The casing asymmetry itself
remains by design.

---

## 6. `AwsLambdaProxyProcessor` throws a `TypeError` on an empty payload

**Severity: low. Scope: AWS Lambda proxying only.**

`aws-lambda-proxy-processor.ts:35-44` sets `result = null` when `response.Payload` is falsy, then reads
`result.eventName` unconditionally. An empty Lambda response yields a `TypeError` instead of a useful
diagnostic.

---

## 7. Inconsistent failure contract across proxy handlers

**Severity: low — a trap for anyone implementing a custom endpoint. Scope: proxying only.**

- `AwsLambdaEventHandler.process` and `RpcEventHandler.process` **return** `{ statusCode: 500, error }`.
- `GrpcEventHandler.process` **throws**.

`RpcEventHandler` also returns HTTP 200 on failure with the error in the body
(`rpc-event-handler.ts:56-61`), so `RpcProxyProcessor` detects failure only by echo-checking
`body.eventName` and `body.eventId`. A custom RPC server that does not echo both fields exactly burns all
10 retries on every event.

---

## 8. `GrpcDetails.isSecure` is accepted and ignored

**Severity: medium as a security expectation gap. Scope: gRPC / RPC transports only.**

The TLS branch is commented out. Both transports use insecure credentials
(`Grpc.credentials.createInsecure()` / `Grpc.ServerCredentials.createInsecure()`), and HTTP RPC is plain
`http://` with no authentication on `/process`. Full event payloads cross the wire unencrypted.
Passing `isSecure: true` changes nothing.

---

## 9. `.proto` files resolve from `src/` even when running from `dist/`

**Severity: medium — a packaging landmine. Scope: gRPC only.**

```typescript
// grpc-server.ts:188-193, grpc-client-factory.ts:44-49
const basePath = dirname.endsWith(`dist${Path.sep}redis-implementation`)
    ? Path.resolve(dirname, "..", "..", "src", "redis-implementation")
    : dirname;
```

`dist/redis-implementation/` contains no `.proto` files, and `package.json` has no `files` field — so gRPC
works only because the published tarball happens to ship `src/`. Any consumer that prunes `src/` breaks
gRPC at runtime. The `endsWith` check is also brittle for non-`dist` layouts.

---

## 10. Dead code and stale references

**Severity: cosmetic, but misleading to readers.**

- `src/in-memory-implementation/*` — both files commented out top to bottom; exports commented out of
  `src/index.ts`. There is no in-memory implementation.
- `src/event-handler-tracer.ts` — entirely comments. `registerConsumerTracer` is commented out of
  `EdaManager`, and `test/utils/eda-test-utils.ts` references a `registerEventHandlerTracer` that never
  existed under that name.
- `src/redis-implementation/consumer-profiler.ts`, `profiling-consumer.ts` — not wired up. `enableMetrics()`
  is live, but it drives `MetricsReporter`, not these files.
- `src/redis-implementation/default-scheduler.ts` — `@deprecated`; `Broker` always uses `OptimizedScheduler`.
- Wildcard event-name matching was removed but its parsing remains commented in
  `event-registration.ts:103-116`.

All five inactive files now carry an `// INACTIVE` header explaining their status. Deleting them is a
separate cleanup, not a bug fix.

---

# Fixed

## `bootstrap()` had no gRPC mutual-exclusion guards

`eda-manager.ts` cross-checked subscriber/Lambda, subscriber/RPC, and Lambda/RPC, but nothing involving
`_isGrpcConsumer` — so a manager could be configured as both an event subscriber and a gRPC consumer and
would bootstrap happily.

All three missing pairs are now enforced, completing the matrix. Covered by `test/guards.test.ts`, which
also pins the pre-existing guards.

## `mapToPartition` failed with an unnamed assertion

The topic check was `.ensure(t => this._topicMap.has(t))` with no message, so both a casing mismatch and a
pre-bootstrap call produced `InvalidArgumentException: Argument 'topic' is invalid.` It now names both
causes. Covered by `test/eda.test.ts`.

The underlying case-sensitivity is unchanged and remains open as item #5 above.

## `package.json` declared the wrong license

`"license": "ISC"` contradicted both the `LICENSE` file and the README, which are MIT. Now `"MIT"`.

## `yarn test` could not fail

```json
"test": "... node --test ... || true && yarn teardown-redis-server"
```

The `|| true` guaranteed exit 0 so teardown always ran — which also meant a failing suite looked green. The
script now captures the test exit code, runs teardown, then re-raises it.

`setup-redis-server` was also made idempotent (`docker rm -f test-redis` first). Previously it aborted
outright when a `test-redis` container already existed, and — more subtly — a reused container carried over
consumer dedupe state. Because `test/eda.test.ts` publishes deterministic event ids, those ids were
suppressed as duplicates on a second run and the suite failed with `0 !== 20000`. A fresh container per run
removes that whole class of flake.
