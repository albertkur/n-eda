# n-eda

## Overview

n-eda is a TypeScript framework for building event-driven applications on Node.js. It gives you a
partitioned, ordered, at-least-once event log on top of Redis, a dependency-injected event handler
model, and optional transports that let a consumer live in a separate process — over HTTP RPC, gRPC,
or AWS Lambda.

The design goal is ordered processing at scale: events are hashed onto partitions by a partition key,
and n-eda guarantees that events sharing a partition key are handled one at a time, in order, across
your entire fleet.

## Features

- **Partitioned, ordered processing** — events sharing a partition key are processed sequentially and in order; different keys process concurrently
- **At-least-once delivery** with per-partition read offsets and a rolling dedupe window
- **Redis-backed distributed log** — no broker to operate beyond Redis itself
- **Dependency injection throughout** — every handler resolves from a per-event child DI scope (`@nivinjoseph/n-ject`)
- **Consumer groups** — independent read offsets let multiple services consume the same topic
- **Out-of-process consumers** — proxy event processing to AWS Lambda, HTTP RPC, or gRPC
- **Distributed observer pattern** — subscribe to events from a *specific instance* of a remote entity
- **Convention-based handler discovery** — `discoverEventHandlers()` replaces hand-maintained registration lists
- **OpenTelemetry tracing and metrics** built in — producer → receive → process spans with W3C context
  propagation, plus handler latency, outcome, event-age, and partition-lag instruments

## Installation

```bash
# Using npm
npm install @nivinjoseph/n-eda

# Using yarn
yarn add @nivinjoseph/n-eda
```

### Requirements & setup

- **Node.js >= 24.10** (see `engines` in package.json).
- **Redis** — required. There is no in-memory implementation; `RedisEventBus` and `RedisEventSubMgr`
  are the only shipped implementations of `EventBus`/`EventSubMgr`.
- **ESM only** — the package is published with `"type": "module"`; there is no CommonJS build. Relative
  imports in your own code need explicit `.js` extensions.
- **Standard (ES2023+) decorators** — `@event`, `@observedEvent`, `@observable`, and `@observer` are
  stage-3 class decorators that write to `context.metadata`. Compile with TypeScript 5+ standard
  decorators and include `"ESNext.Decorators"` in `lib`. Do **not** enable `experimentalDecorators`;
  this package does not use `reflect-metadata`.
- **`Symbol.metadata` is polyfilled on import** — `src/index.ts` runs `Symbol.metadata ??= Symbol("Symbol.metadata")`
  as a load-time side effect. Decorator metadata does not work unless the package barrel has been imported.

A minimal `tsconfig.json`:

```jsonc
{
    "compilerOptions": {
        "target": "ES2023",
        "lib": ["ES2023", "ESNext.Decorators"],
        "module": "NodeNext",
        "moduleResolution": "NodeNext",
        "strict": true
        // note: no "experimentalDecorators", no "emitDecoratorMetadata"
    }
}
```

### Ecosystem

n-eda sits on a family of sibling packages. You will write code against most of them:

- `@nivinjoseph/n-util` — `Serializable` and the `@serialize` decorator (**the event wire format**),
  plus `Duration` (topic TTLs), `Disposable`, `DisposableWrapper`, and `Delay`.
- `@nivinjoseph/n-ject` — the DI container. `EdaManager` *is* a container wrapper: `Container`,
  `ComponentInstaller`, `Registry`, and the `@inject` decorator on your handlers.
- `@nivinjoseph/n-log` — a `Logger` must be registered under the DI key `"Logger"`; several n-eda
  internals resolve it at runtime. `ConsoleLogger` is the usual choice.
- `@nivinjoseph/n-defensive` — the `given(...)` fluent assertions. Every runtime contract in n-eda is
  expressed with it, so n-eda's error messages are `given(...)` messages.
- `@nivinjoseph/n-exception` — `ApplicationException`, `ObjectDisposedException`, `ArgumentException`
  are what n-eda throws.
- `@nivinjoseph/n-ext` — prototype extensions (`getTypeName()`, `orderBy`, `where`, `contains`),
  loaded as a side effect of importing this package.
- `@nivinjoseph/n-config` — reads `config.json`/env; n-eda uses it for the `env` key in shutdown drains.
- `@nivinjoseph/n-svc` — only needed if you deploy `RpcServer` or `GrpcServer` as a standalone process.
- `ioredis` — you construct the `Redis` client yourself and register it under the DI key `"EdaRedisClient"`.

## Application Organization

```
src/
├── events/
│   ├── user-created-event.ts
│   └── order-placed-event.ts
├── event-handlers/                 # the directory discoverEventHandlers() scans
│   ├── user-created-event-handler.ts
│   └── order-placed-event-handler.ts
├── common-component-installer.ts   # registers "Logger", "EdaRedisClient", your services
└── main.ts                         # builds and bootstraps the EdaManager
```

## Quick Start

The four pieces below are the whole minimum. All of them are required — omitting the installer is the
single most common mistake, because `RedisEventBus` cannot resolve its Redis client without it.

### 1. Define an event

An event is an n-util `Serializable` that implements `EdaEvent`. The `@serialize` decorators are
load-bearing: they define the wire format, and the consumer reconstructs the event with n-util's
`Deserializer`.

```typescript
import { given } from "@nivinjoseph/n-defensive";
import { Serializable, serialize } from "@nivinjoseph/n-util";
import { EdaEvent } from "@nivinjoseph/n-eda";

@serialize("MyApp")                              // namespace — REQUIRED on the class
export class UserCreatedEvent extends Serializable implements EdaEvent
{
    private readonly _id: string;
    private readonly _userId: string;


    @serialize
    public get id(): string { return this._id; }

    @serialize                                   // must be serialized for eda purposes
    public get name(): string { return (<Object>UserCreatedEvent).getTypeName(); }

    @serialize
    public get userId(): string { return this._userId; }

    // NOT serialized — derived on both sides
    public get partitionKey(): string { return this._userId; }

    public get refId(): string { return this._userId; }
    public get refType(): string { return "User"; }


    public constructor(data: { id: string; userId: string; })
    {
        super(data);

        const { id, userId } = data;

        given(id, "id").ensureHasValue().ensureIsString();
        this._id = id;

        given(userId, "userId").ensureHasValue().ensureIsString();
        this._userId = userId;
    }
}
```

`partitionKey` is the ordering key. Events with the same `partitionKey` are processed in order, one
at a time. Choose it to match your consistency boundary — an aggregate id is usually right.

`refId` and `refType` identify the *instance* that emitted the event and are only meaningful for the
[distributed observer](#distributed-observer). They are still required by the interface; if you do not
use observers, return any stable constant.

### 2. Define a handler

`EdaEventHandler<T>` is an **interface** — use `implements`, never `extends`.

```typescript
import { given } from "@nivinjoseph/n-defensive";
import { inject } from "@nivinjoseph/n-ject";
import { Logger } from "@nivinjoseph/n-log";
import { EdaEventHandler, event } from "@nivinjoseph/n-eda";
import { UserCreatedEvent } from "../events/user-created-event.js";

@event(UserCreatedEvent)                         // n-eda decorator goes on top
@inject("Logger")                                // n-ject decorator below it
export class UserCreatedEventHandler implements EdaEventHandler<UserCreatedEvent>
{
    private readonly _logger: Logger;


    public constructor(logger: Logger)
    {
        given(logger, "logger").ensureHasValue().ensureIsObject();
        this._logger = logger;
    }


    public async handle(event: UserCreatedEvent): Promise<void>
    {
        given(event, "event").ensureHasValue().ensureIsObject().ensureIsType(UserCreatedEvent);

        await this._logger.logInfo(`User created: ${event.userId}`);
    }
}
```

### 3. Register your dependencies

n-eda resolves `"EdaRedisClient"` and `"Logger"` from the container. You supply both through a
`ComponentInstaller`.

```typescript
import { given } from "@nivinjoseph/n-defensive";
import { ComponentInstaller, Registry } from "@nivinjoseph/n-ject";
import { ConsoleLogger } from "@nivinjoseph/n-log";
import { Delay, DisposableWrapper } from "@nivinjoseph/n-util";
import { Redis } from "ioredis";

export class CommonComponentInstaller implements ComponentInstaller
{
    public async install(registry: Registry): Promise<void>
    {
        given(registry, "registry").ensureHasValue().ensureIsObject();

        const edaRedisClient = new Redis();

        // Registering a Disposable is how the Redis connection gets closed on
        // edaManager.dispose() — n-eda never closes the client it is handed.
        const edaRedisClientDisposable = new DisposableWrapper(async () =>
        {
            await Delay.seconds(5);
            await new Promise<void>((resolve) =>
            {
                edaRedisClient.quit(() => resolve()).catch(e => console.error(e));
            });
        });

        registry
            .registerInstance("Logger", new ConsoleLogger())
            .registerInstance("EdaRedisClient", edaRedisClient)
            .registerInstance("EdaRedisClientDisposable", edaRedisClientDisposable);
    }
}
```

### 4. Build, bootstrap, and consume

```typescript
import { Duration } from "@nivinjoseph/n-util";        // note: Duration is n-util, not n-eda
import { EdaManager, EventBus, RedisEventBus, RedisEventSubMgr, Topic } from "@nivinjoseph/n-eda";
import { CommonComponentInstaller } from "./common-component-installer.js";
import { UserCreatedEvent } from "./events/user-created-event.js";
import { UserCreatedEventHandler } from "./event-handlers/user-created-event-handler.js";

// .subscribe() is REQUIRED to consume — topics are publish-only by default.
const userTopic = new Topic("user", Duration.fromHours(1), 25).subscribe();

const edaManager = new EdaManager();
edaManager
    .useInstaller(new CommonComponentInstaller())
    .useConsumerName("user-service")
    .registerTopics(userTopic)
    .registerEventHandlers(UserCreatedEventHandler)
    .registerEventBus(RedisEventBus)
    .registerEventSubscriptionManager(RedisEventSubMgr, "user-service-group");

await edaManager.bootstrap();                          // async since 7.0.0

// Do NOT await this — it is the consume loop and never resolves until dispose().
edaManager.beginConsumption().catch(e => console.error(e));

// Publishing
const eventBus = edaManager.serviceLocator.resolve<EventBus>("EventBus");
await eventBus.publish("user", new UserCreatedEvent({ id: "evt-1", userId: "usr-1" }));

// Shutdown
await edaManager.dispose();
```

## Core Concepts

### Events

Every event implements `EdaEvent`, which requires five getters:

| Member | Serialized? | Purpose |
|---|---|---|
| `id` | **yes** | Unique event id. Used for deduplication. |
| `name` | **yes** | The event type name. Used to route to a handler. |
| `partitionKey` | no | Ordering and partition assignment key. |
| `refId` | no | Id of the emitting instance (distributed observer). |
| `refType` | no | Type name of the emitting instance (distributed observer). |

The wire format is n-util's `serialize()` output, which includes a `$typename` discriminator built
from the `@serialize("Namespace")` argument and the class name. The consumer calls
`Deserializer.deserialize`, which **throws for an unregistered type** — so the consuming process must
import the event class, and the class name is a persisted identity. Renaming an event class breaks
deserialization of every event already in flight.

### Event Handlers

Handlers are registered **scoped** in the DI container, keyed by their class name. Three consequences:

1. A fresh handler instance is constructed for every event.
2. **Handler class names must be globally unique** across your application.
3. Exactly **one handler class per event type** — registering two throws
   `Multiple event handlers detected for event '<name>'`.

Handlers may publish downstream events by injecting `"EventBus"`.

### Topics and partitioning

```typescript
const topic = new Topic("orders", Duration.fromHours(4), 100);
```

All three constructor arguments are required. `numPartitions` must be > 0 and fixes the concurrency
ceiling for that topic — **changing it later rehashes every key** and breaks ordering across the change.

> **TTL rounds to whole minutes.** `Topic` stores `ttlDuration.toMinutes(true)`. A sub-minute duration
> such as `Duration.fromSeconds(20)` rounds to `0` and makes Redis reject the write with
> `ERR invalid expire time`. Use a minute or more.

Topics are configured by chaining:

| Method | Effect |
|---|---|
| `.subscribe()` | **Consume this topic.** Without it the topic is publish-only and no consumer is created. |
| `.forcePublish()` | Publish events even when this process has no registered handler for them. |
| `.configurePartitionAffinity("0-49")` | Own only partitions 0–49 (inclusive). Required for horizontal scaling. |
| `.flushConsume()` | Drain and discard without dispatching. For recovering from a backlog you intend to abandon. |
| `.disable()` | Create neither producers nor consumers for this topic. |

Partition assignment is `MurmurHash.x86.hash32(partitionKey) % numPartitions`, exposed as
`edaManager.mapToPartition(topic, event)`.

By default the partition key is `event.partitionKey`. Override globally with:

```typescript
edaManager.usePartitionKeyMapper((event) => event.refId);   // callable ONCE
```

> **Topics are publish-only by default.** `Topic._publishOnly` starts `true`. A service that registers
> topics and handlers but forgets `.subscribe()` will start cleanly, publish correctly, and never
> consume anything.

### EventBus

`EventBus` is how you publish. Resolve it from the container, or `@inject("EventBus")` into a handler.

```typescript
const eventBus = edaManager.serviceLocator.resolve<EventBus>("EventBus");
await eventBus.publish("orders", event1, event2, event3);   // variadic
```

The topic string must exactly match a registered `Topic.name` (case-sensitive).

> **Events with no locally-registered handler are silently dropped at publish time.** `RedisEventBus`
> only writes events whose `name` is in this manager's `eventMap`. A dedicated publisher service — one
> that emits events it never consumes — publishes nothing at all until you call `.forcePublish()` on
> the topic. This is the sharpest edge in the library.

### EventSubMgr and consumer groups

```typescript
edaManager.registerEventSubscriptionManager(RedisEventSubMgr, "orders-service");
```

The `consumerGroupId` scopes the read offset. Two services with **different** group ids each get a full,
independent copy of the stream. Two processes with the **same** group id share one offset.

> **There is no rebalancing.** n-eda has no group coordination, ownership lease, or heartbeat. Every
> process that subscribes to a topic creates a consumer for *every* partition unless you tell it
> otherwise. Two replicas in the same group without disjoint affinity will both read the same window
> and both dispatch — duplicate processing.

Scale out with **manual, disjoint** ranges:

```typescript
// replica A
new Topic("orders", Duration.fromHours(4), 100).subscribe().configurePartitionAffinity("0-49");
// replica B
new Topic("orders", Duration.fromHours(4), 100).subscribe().configurePartitionAffinity("50-99");
```

### Delivery semantics

Delivery is **at-least-once**, with a rolling dedupe window of the last **1000 event ids** per
(topic, partition, consumer group). Design handlers to be idempotent.

A failing handler is retried **10 times** with an escalating delay of `(5 + n) * n` seconds —
`6, 14, 24, 36, 50, 66, 84, 104, 126` — roughly **8.5 minutes** in total. After the last attempt the
event is logged, marked processed, and **dropped**. There is no dead-letter queue. While a poison
event is retrying it holds up its batch window on that partition.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full failure-mode table.

### EdaContext

Handlers can learn which topic delivered the current event:

```typescript
import { EdaContext } from "@nivinjoseph/n-eda";

@event(UserCreatedEvent)
@inject("EdaContext")
export class UserCreatedEventHandler implements EdaEventHandler<UserCreatedEvent>
{
    private readonly _edaContext: EdaContext;

    public constructor(edaContext: EdaContext) { this._edaContext = edaContext; }

    public async handle(event: UserCreatedEvent): Promise<void>
    {
        console.log(this._edaContext.topic);
    }
}
```

`"EdaContext"` is registered scoped by the `EdaManager` constructor — you never register it yourself.

> **Only populated on the in-process Redis consumer.** Under the AWS Lambda, RPC, and gRPC consumers
> the context is never set, and reading `.topic` throws `topic not set`.

### Handler discovery

Instead of maintaining a registration list by hand:

```typescript
import { discoverEventHandlers } from "@nivinjoseph/n-eda";

edaManager.registerEventHandlers(
    ...await discoverEventHandlers(new URL("./event-handlers", import.meta.url)));
```

The convention: a file named `<name>-event-handler.js` (as emitted) exporting a class named
`<Pascal>EventHandler` that has `@event(...)` or `@observedEvent(...)` applied and a `handle` method.
Prefix a class name with `_` to opt out. The scan is recursive and sorted, so registration order is
deterministic.

It throws if the directory is missing, if a matching file exports no handler, if a discovered class
name does not end in `EventHandler`, or if two distinct classes resolve to the same name.

> Discovery scans **emitted `.js`**, so always pass `new URL("./event-handlers", import.meta.url)` —
> a path relative to the compiled module — never a path derived from `cwd`.

### Distributed observer

The observer pattern lets a service subscribe to events from **one specific instance** of a remote
entity — "notify me when *this particular* order ships" — rather than to a whole event type.

Enable it with a dedicated topic, on both the observable and the observer side:

```typescript
edaManager.enableDistributedObserver(new Topic("observer", Duration.fromHours(1), 25).subscribe());
```

Observer handlers carry **three** decorators and implement `ObserverEdaEventHandler`, whose `handle`
takes a second `observerId` argument:

```typescript
import { ObserverEdaEventHandler, observable, observedEvent, observer } from "@nivinjoseph/n-eda";

@observedEvent(OrderShippedEvent)    // the event to watch
@observable(Order)                   // the type emitting it
@observer(Customer)                  // the type doing the watching
export class CustomerOrderShippedHandler implements ObserverEdaEventHandler<OrderShippedEvent>
{
    public async handle(event: OrderShippedEvent, observerId: string): Promise<void>
    {
        // observerId is the Customer id that subscribed
    }
}
```

Subscribe and unsubscribe through the event bus:

```typescript
await eventBus.subscribeToObservables(Customer, customerId, [
    { observableType: Order, observableId: orderId, observableEventType: OrderShippedEvent }
]);

await eventBus.unsubscribeFromObservables(Customer, customerId, [
    { observableType: Order, observableId: orderId, observableEventType: OrderShippedEvent }
]);
```

For this to work, the observable's event must set `refType` to the observable type's name (`"Order"`)
and `refId` to that instance's id. Subscribing without a matching registered handler throws
`No handler registered for observation key '...'`.

> The observer fan-out runs inside `publish()` *after* the normal partition-mapping step, and
> `publish()` returns early when that step produced nothing. A pure-publisher service will not notify
> remote observers unless the topic is `.forcePublish()`ed.

### Out-of-process consumers

A consumer can run in a different process from the Redis consume loop. The reading process *proxies*
each event out; the executing process *acts as* the consumer. These are mutually exclusive with
registering an `EventSubMgr` in the same manager.

```typescript
// Reading side — forwards events out
edaManager.proxyToRpc({ host: "localhost", port: 8080 });
edaManager.proxyToGrpc({ host: "localhost", port: 50051, connectionPoolSize: 50 });
edaManager.proxyToAwsLambda({
    region: "us-east-1",
    funcName: "my-consumer",
    credentials: { accessKeyId: "...", accessKeySecret: "..." }
});

// Executing side — receives and dispatches to handlers
edaManager.actAsRpcConsumer(new RpcEventHandler());
```

`RpcServer` and `GrpcServer` host the executing side:

```typescript
new RpcServer(8080, null, container)
    .registerEventHandler(new RpcEventHandler())
    .registerStartupScript(MyStartupScript)      // implements ApplicationScript
    .bootstrap();                                // synchronous
```

> RPC and gRPC transports are **plaintext and unauthenticated**. `GrpcDetails.isSecure` is accepted
> and ignored. Run them only on a trusted network.

### OpenTelemetry

n-eda emits three spans per event under the tracer name `"n-eda"`, with W3C trace context propagated
through the event payload:

| Span | Kind | Emitted by |
|---|---|---|
| `event.<EventName> publish` | `PRODUCER` | the producer, on publish |
| `event.<EventName> receive` | `INTERNAL` | the consumer, spanning queue wait + all retries |
| `event.<EventName> process` | `CONSUMER` | the processor, spanning every attempt and the retry delays between them |

Attributes follow the `messaging.*` semantic conventions. No setup is required beyond having an
OpenTelemetry SDK configured in your process.

n-eda also records metrics under the meter name `"n-eda"`:

| Instrument | Type | What it answers |
|---|---|---|
| `neda.event.process.duration` | histogram, seconds | Handler latency per attempt; failed attempts carry `error.type` |
| `neda.event.processed` | counter | Final outcomes; `neda.outcome="failed"` counts events dropped after their retries |
| `neda.event.age` | histogram, seconds | How long an event waited between publish and consumer pickup |
| `neda.partition.lag` | observable gauge, batches | Lag per partition |
| `neda.partition.lag_peak` | observable gauge, batches | The highest lag since the previous collection, even if it drained before then |
| `neda.partition.sample_age` | observable gauge, seconds | How stale that lag figure is; climbs while a partition is stuck |
| `neda.partition.written` / `neda.partition.consumed` | observable counters, batches | Batches written to and consumed from each partition since the process first read it; their rates are the production and consumption rates |
| `neda.batch.lost` | counter, batches | Batches whose payload never arrived or expired: lost events. Alert on any increase |
| `neda.event.skipped` | counter | Events a consumer passed over, by `neda.skip.reason`: `duplicate`, `no_handler`, `partition_key_error` |
| `neda.event.sent` | counter | Events published, per type, from the publishing process; failed writes carry `error.type` |
| `neda.event.dropped` | counter | Events `publish()` discarded because this process has no handler for them and the topic isn't forced |

They need no n-eda opt-in, but `bootstrap()` takes the meter **once**: register your `MeterProvider`
before calling it, or every recording is a no-op. The producer stamps each event with `$publishedAt`
(epoch ms) for the age metric; older consumers ignore it. Attributes, cardinality, and how to read these
metrics are covered in [docs/observability.md](docs/observability.md#opentelemetry-metrics).

**Upgrading with Datadog:** the setup documented up to 7.0.5 generated a log-based metric named
`neda.partition.lag`. The gauge above now uses that name and flows without opt-in, so delete the log-based
metric before deploying the upgrade ([details](docs/observability.md#datadog)).

> On failure, n-eda records the **full serialized event payload** into span attributes and log lines.
> If your events carry personal data, account for that in your telemetry pipeline.

## Examples

[`test/utils/eda-test-utils.ts`](test/utils/eda-test-utils.ts) is a complete, working setup —
events, handlers, installer, and a fully configured `EdaManager` — and
[`test/eda.test.ts`](test/eda.test.ts) exercises it end to end against a real Redis, asserting
per-partition ordering across 10,000 events.

For the distributed observer, [`test/utils/observer-test-utils.ts`](test/utils/observer-test-utils.ts) and
[`test/observer.test.ts`](test/observer.test.ts) show the three-decorator handler, `subscribeToObservables`,
and the full fan-out → notify → unwrap → handler path.

**When this README and the tests disagree, trust the tests.**

Run them with:

```bash
yarn setup-redis-server   # docker: redis:7.0 on localhost:6379
yarn test
```

## API Reference

### `EdaManager`

The root object. Wraps an n-ject `Container`, owns configuration, and drives the lifecycle.

```typescript
public constructor(container?: Container)
```

Pass a `Container` to share one with the rest of your app; **you** must then bootstrap it. Omit it and
`EdaManager` creates and bootstraps its own. Either way the constructor registers `"EdaContext"` scoped.

**Statics**

- `EdaManager.eventBusKey` → `"EventBus"`
- `EdaManager.eventSubMgrKey` → `"EventSubMgr"`

**Configuration** — all return `this`, all throw `invoking method after bootstrap` if called after `bootstrap()`:

- `useInstaller(installer: ComponentInstaller): this` — install your DI registrations.
- `useConsumerName(name: string): this` — label for this consumer. Defaults to `"UNNAMED"`.
- `registerTopics(...topics: Array<Topic>): this` — variadic, accumulates across calls. Throws on a
  case-insensitive duplicate name.
- `usePartitionKeyMapper(func: (event: EdaEvent) => string): this` — override the partition key.
  **Callable once**; defaults to `event.partitionKey`.
- `enableMetrics(metricsInterval?: Duration): this` — opt in to per-partition lag/throughput logging
  (see [docs/observability.md](docs/observability.md)); **no metrics lines are emitted without this
  call**. The optional interval defaults to one minute; must be positive and at most 2^31-1 ms
  (~24.8 days). One log line per topic-partition per interval, so widen it to trade dashboard
  resolution for log cost.
- `registerEventHandlers(...eventHandlerClasses): this` — throws if a class carries no `@event`/
  `@observedEvent`, if two handlers claim the same event type or observation key, or if two handler
  classes share a name.
- `registerEventBus(eventBus: EventBus | ClassDefinition<EventBus>): this` — **required**. Callable once.
  A class is registered singleton; an instance is registered as-is.
- `registerEventSubscriptionManager(eventSubMgr, consumerGroupId: string): this` — callable once.
  `consumerGroupId` is required and has no default.
- `cleanUpKeys(): this` — delete event payload keys from Redis after this consumer reads them.
  **Only safe when exactly one consumer group reads the topic.**
- `enableDistributedObserver(topic: Topic): this` — register the observer-notification topic. You may
  not publish to it directly.
- `proxyToAwsLambda(d: LambdaDetails)` / `proxyToRpc(d: RpcDetails)` / `proxyToGrpc(d: GrpcDetails)` —
  forward event processing out of process.
- `actAsAwsLambdaConsumer(h: AwsLambdaEventHandler)` / `actAsRpcConsumer(h: RpcEventHandler)` /
  `actAsGrpcConsumer(h: GrpcEventHandler)` — receive proxied events.

**Lifecycle**

- `bootstrap(): Promise<void>` — **async.** Requires at least one topic and a registered event bus.
  Rejects if called twice, if the manager is both an event subscriber and a Lambda or RPC consumer, or
  if it is both a Lambda and an RPC consumer. Installs the default partition key mapper, builds the
  topic map, bootstraps the container (only if it owns it), takes the `"n-eda"` meter from the global
  OpenTelemetry `MeterProvider` (register yours first), then `initialize(this)` on the event bus, sub
  manager, and any proxy handler.
- `beginConsumption(): Promise<void>` — requires `bootstrap()` and a registered `EventSubMgr`.
  **Does not resolve** until `dispose()`. Do not `await` it in a startup path.
- `mapToPartition(topic: string, event: EdaEvent): number` — requires bootstrap. Note the topic map is
  keyed by exact name, so casing must match the registration.
- `dispose(): Promise<void>` — idempotent. Disposes the subscription manager, then the container.
  Disposes the container even if it was supplied externally.

**Getters** — `containerRegistry`, `serviceLocator`, `topics`, `distributedObserverTopic`, `eventMap`,
`observerEventMap`, `consumerName`, `consumerGroupId`, `cleanKeys`, `partitionKeyMapper`, `metricsEnabled`, `metricsInterval`,
`awsLambdaDetails`, `awsLambdaProxyEnabled`, `isAwsLambdaConsumer`, `rpcDetails`, `rpcProxyEnabled`,
`isRpcConsumer`, `grpcDetails`, `grpcProxyEnabled`, `isGrpcConsumer`, and `otelMetrics` (internal: the
OpenTelemetry instruments the consume path records into; a no-op instance before `bootstrap()`).

### `EdaEvent`

```typescript
export interface EdaEvent extends Serializable
{
    get id(): string;
    get name(): string;
    get partitionKey(): string;
    get refId(): string;
    get refType(): string;
}
```

Implementations must extend n-util's `Serializable` and carry `@serialize("Namespace")`. `id` and
`name` must each carry a property-level `@serialize`.

### `EdaEventHandler<TEvent>`

```typescript
export interface EdaEventHandler<TEvent extends EdaEvent>
{
    handle(event: TEvent): Promise<void>;
}
```

An **interface** — `implements` it. Pair with `@event(EventClass)`.

### `event(eventType)`

```typescript
export function event<TEvent extends EdaEvent, This extends EdaEventHandler<TEvent>>(
    eventType: EdaEventClass<TEvent>): EventHandlerEventDecorator<TEvent, This>
```

Class decorator binding a handler to an event type. Throws if applied to a non-class or to a class
without a `handle` method. Place it above `@inject(...)`.

### `Topic`

```typescript
public constructor(name: string, ttlDuration: Duration, numPartitions: number)
```

Getters: `name`, `ttlMinutes`, `numPartitions`, `publishOnly`, `partitionAffinity`, `isDisabled`,
`isForce`, `isFlush`.

Chainable: `subscribe()`, `forcePublish()`, `flushConsume()`,
`configurePartitionAffinity(\`${number}-${number}\`)`, `disable()`.

`configurePartitionAffinity` bounds are **inclusive** and must fall within `[0, numPartitions - 1]`;
it throws `ArgumentException` otherwise.

### `EventBus`

```typescript
export interface EventBus extends Disposable
{
    initialize(manager: EdaManager): void;
    publish(topic: string, ...events: ReadonlyArray<EdaEvent>): Promise<void>;
    subscribeToObservables(observerType: Function, observerId: string,
        watches: ReadonlyArray<ObservableWatch>): Promise<void>;
    unsubscribeFromObservables(observerType: Function, observerId: string,
        watches: ReadonlyArray<ObservableWatch>): Promise<void>;
}
```

Registered under `"EventBus"`. `initialize` is called by `bootstrap()`; you never call it.

### `ObservableWatch`

```typescript
export type ObservableWatch = {
    observableType: Function | string;
    observableId: string;
    observableEventType: Function | string;
};
```

`observableType` must name the same type as the published event's `refType`, and `observableId` its `refId`.

### `EventSubMgr`

```typescript
export interface EventSubMgr extends Disposable
{
    initialize(manager: EdaManager): void;
    consume(): Promise<void>;
}
```

`consume()` does not resolve until disposal.

### `RedisEventBus` / `RedisEventSubMgr`

The shipped implementations. Both are `@inject`ed with `"EdaRedisClient"` (`RedisEventSubMgr` also
takes `"Logger"`), so both require those DI registrations. Register the classes, not instances, and
let the container construct them.

### `EdaContext`

```typescript
export interface EdaContext { get topic(): string; }
```

Resolve with `@inject("EdaContext")`. Throws `topic not set` when read outside the in-process
Redis consumer.

### `EventRegistration`

The reflected result of applying the decorators to a handler class. Built internally by
`registerEventHandlers` and exposed through `edaManager.eventMap` / `observerEventMap`.

Getters: `eventHandlerType`, `eventHandlerTypeName`, `eventType`, `eventTypeName`, `isObservedEvent`,
and — only when `isObservedEvent` — `observableType`, `observableTypeName`, `observerType`,
`observerTypeName`, `observationKey`.

```typescript
public static generateObservationKey(observerTypeName: string, observableTypeName: string,
    observableEventTypeName: string): string      // => `.${observer}.${observable}.${event}`
```

### `ObserverEdaEventHandler<TEvent>`

```typescript
export interface ObserverEdaEventHandler<TEvent extends EdaEvent>
{
    handle(event: TEvent, observerId: string): Promise<void>;
}
```

### `observedEvent(eventType)` / `observable(type)` / `observer(type)`

Class decorators for observer handlers. **All three are required together**, and a class carrying them
must not also carry `@event`. `observable`/`observer` take the domain classes; only their type names
are used.

### `discoverEventHandlers(directoryUrl)`

```typescript
export function discoverEventHandlers(directoryUrl: URL): Promise<Array<EventHandlerClass>>
```

Convention-based handler discovery. See [Handler discovery](#handler-discovery).

### `LambdaDetails` / `RpcDetails` / `GrpcDetails`

```typescript
export interface LambdaDetails
{
    readonly region: string;
    readonly funcName: string;
    credentials: { readonly accessKeyId: string; readonly accessKeySecret: string; };
}

export interface RpcDetails { readonly host: string; readonly port: number; }

export interface GrpcDetails
{
    readonly host: string;
    readonly port: number;
    readonly isSecure?: boolean;          // accepted but currently ignored
    readonly connectionPoolSize?: number; // defaults to 50
}
```

### `AwsLambdaEventHandler` / `RpcEventHandler` / `GrpcEventHandler`

Receive proxied events in the executing process. Each exposes `initialize(manager)` (called by
`bootstrap()`) and `process(...)`, and a `protected onEventReceived(scope, topic, event)` hook you may
override.

`AwsLambdaEventHandler.process` and `RpcEventHandler.process` **return** `{ statusCode: 500, error }`
on failure; `GrpcEventHandler.process` **throws**. A custom RPC server must echo `eventName` and
`eventId` back exactly, or the proxy treats every event as failed.

### `RpcServer` / `GrpcServer`

```typescript
public constructor(port: number, host: string | null, container: Container, logger?: Logger | null)

public registerEventHandler(eventHandler: RpcEventHandler): this
public registerStartupScript(applicationScriptClass: ClassHierarchy<ApplicationScript>): this
public registerShutdownScript(applicationScriptClass: ClassHierarchy<ApplicationScript>): this
public registerDisposeAction(disposeAction: () => Promise<void>): this
public bootstrap(): void                          // synchronous
```

`host` is nullable but positionally required. `registerEventHandler` is mandatory and must precede
`bootstrap()`.

### `ApplicationScript`

```typescript
export interface ApplicationScript { run(): Promise<void>; }
```

Startup/shutdown hooks for `RpcServer`/`GrpcServer`.

### `NedaClearTrackedKeysEvent`

An internal system event. Publishing it to a topic fans it out to every partition and clears each
consumer's dedupe tracking list. Use it to force reprocessing after a deliberate offset rewind.

## Best Practices

1. **Make handlers idempotent.** Delivery is at-least-once and the dedupe window is only 1000 events
   per partition per group.
2. **Choose `partitionKey` as your consistency boundary.** It is the unit of ordering and of
   concurrency; an aggregate id is usually the right answer.
3. **Set `numPartitions` generously up front.** It caps concurrency for the topic and cannot be changed
   without rehashing every key.
4. **Always call `.subscribe()`** on topics this service consumes, and `.forcePublish()` on topics it
   only publishes to.
5. **Never `await beginConsumption()`.** Fire it and keep going.
6. **Give every consumer group a distinct `consumerGroupId`**, and when scaling a group past one
   replica, assign disjoint `configurePartitionAffinity` ranges.
7. **Run Redis with `maxmemory-policy noeviction`.** The write-index, read-index, and tracked-keys keys
   carry no TTL; evicting them silently stops or rewinds consumption.
8. **Use `cleanUpKeys()` only with a single consumer group** on the topic — it deletes payloads other
   groups still need.
9. **Keep event classes stable.** The class name is a persisted identity; renaming one breaks every
   in-flight event of that type.
10. **Register a `Disposable` alongside your Redis client.** n-eda does not close the client it is given.
11. **Log or alert on retry exhaustion.** There is no dead-letter queue; the only record of a dropped
    event is a log line.

## Related

- [ARCHITECTURE.md](ARCHITECTURE.md) — Redis key layout, the consume loop, delivery guarantees, tuning constants
- [llms.txt](llms.txt) — condensed API guide and the rules the type system does not enforce
- [docs/observability.md](docs/observability.md) — the OpenTelemetry metrics, the partition metrics log contract, and Datadog setup
- [docs/known-issues.md](docs/known-issues.md) — known defects and their workarounds

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For support, please open an issue in the [GitHub repository](https://github.com/nivinjoseph/n-eda/issues).
