import { Container, Registry, ServiceLocator, ComponentInstaller } from "@nivinjoseph/n-ject";
import { EventBus } from "./event-bus.js";
import { EventSubMgr } from "./event-sub-mgr.js";
import { ClassDefinition, Disposable, Duration } from "@nivinjoseph/n-util";
import { EventRegistration } from "./event-registration.js";
import { Topic } from "./topic.js";
import { EdaEvent } from "./eda-event.js";
import { EdaEventHandler } from "./eda-event-handler.js";
import { AwsLambdaEventHandler } from "./redis-implementation/aws-lambda-event-handler.js";
import { LambdaDetails } from "./lambda-details.js";
import { RpcDetails } from "./rpc-details.js";
import { RpcEventHandler } from "./redis-implementation/rpc-event-handler.js";
import { GrpcEventHandler } from "./redis-implementation/grpc-event-handler.js";
import { GrpcDetails } from "./grpc-details.js";
import { ObserverEdaEventHandler } from "./observer-eda-event-handler.js";
import { EdaMetrics } from "./metrics/eda-metrics.js";
/**
 * The root object of an n-eda application: a dependency-injection container wrapper that owns topic and
 * handler configuration, and drives the publish/consume lifecycle.
 *
 * Lifecycle, in order:
 * 1. `new EdaManager(container?)` — also registers `"EdaContext"` scoped.
 * 2. Configuration — every `use*` / `register*` / `proxyTo*` / `actAs*` method, in any order among
 *    themselves, but **all before** `bootstrap()`. Each returns `this` for chaining.
 * 3. `await bootstrap()` — requires at least one topic and a registered event bus.
 * 4. `beginConsumption()` — only if an `EventSubMgr` was registered. **Do not await it.**
 * 5. `await dispose()`.
 *
 * Contract: the container must be able to resolve `"EdaRedisClient"` (an `ioredis` `Redis`) and `"Logger"`
 * (an n-log `Logger`) — supply both through a `ComponentInstaller` passed to
 * {@link EdaManager.useInstaller}. The DI keys n-eda reserves are `"EventBus"`, `"EventSubMgr"`,
 * `"EdaContext"`, and every registered handler's class name.
 *
 * @example
 * ```typescript
 * const topic = new Topic("user", Duration.fromHours(1), 25).subscribe();
 *
 * const edaManager = new EdaManager();
 * edaManager
 *     .useInstaller(new CommonComponentInstaller())
 *     .registerTopics(topic)
 *     .registerEventHandlers(UserCreatedEventHandler)
 *     .registerEventBus(RedisEventBus)
 *     .registerEventSubscriptionManager(RedisEventSubMgr, "user-service-group");
 *
 * await edaManager.bootstrap();
 * edaManager.beginConsumption().catch(e => console.error(e));   // never awaited
 * ```
 */
export declare class EdaManager implements Disposable {
    private readonly _container;
    private readonly _ownsContainer;
    private readonly _topics;
    private readonly _topicMap;
    private readonly _eventMap;
    private readonly _observerEventMap;
    private _metricsEnabled;
    private _metricsInterval;
    private _otelMetrics;
    private _partitionKeyMapper;
    private _eventBusRegistered;
    private _eventSubMgrRegistered;
    private _evtSubMgr;
    private _consumerName;
    private _consumerGroupId;
    private _cleanKeys;
    private _distributedObserverTopic;
    private _awsLambdaDetails;
    private _isAwsLambdaConsumer;
    private _awsLambdaEventHandler;
    private _rpcDetails;
    private _isRpcConsumer;
    private _rpcEventHandler;
    private _grpcDetails;
    private _isGrpcConsumer;
    private _grpcEventHandler;
    private _isDisposed;
    private _disposePromise;
    private _isBootstrapped;
    /** DI key the event bus is registered under: `"EventBus"`. */
    static get eventBusKey(): string;
    /** DI key the subscription manager is registered under: `"EventSubMgr"`. */
    static get eventSubMgrKey(): string;
    /** The underlying container as a `Registry`, for registering additional components. */
    get containerRegistry(): Registry;
    /**
     * The underlying container as a `ServiceLocator`. This is how you get the event bus out:
     * `serviceLocator.resolve<EventBus>("EventBus")`.
     */
    get serviceLocator(): ServiceLocator;
    /** All registered topics, including the distributed observer topic if one was enabled. */
    get topics(): ReadonlyArray<Topic>;
    /** The topic dedicated to distributed observer notifications, or `null` if the feature is off. */
    get distributedObserverTopic(): Topic | null;
    /**
     * Standard handler registrations, keyed by **event type name**. `RedisEventBus.publish` consults this to
     * decide whether an event is publishable at all — an event absent from this map is dropped unless the
     * topic was configured with `.forcePublish()`.
     */
    get eventMap(): ReadonlyMap<string, EventRegistration>;
    /**
     * Observer handler registrations, keyed by **observation key** (`` `.${observer}.${observable}.${event}` ``),
     * not by event name. Use `EventRegistration.generateObservationKey(...)` to build a lookup key.
     */
    get observerEventMap(): ReadonlyMap<string, EventRegistration>;
    /** This consumer's label, or `"UNNAMED"` if {@link EdaManager.useConsumerName} was never called. */
    get consumerName(): string;
    /** The consumer group id, or `null` if no subscription manager was registered. Scopes the read offset. */
    get consumerGroupId(): string | null;
    /** Whether consumers delete event payload keys after reading them. See {@link EdaManager.cleanUpKeys}. */
    get cleanKeys(): boolean;
    /** Lambda connection details, or `null` if not proxying to Lambda. */
    get awsLambdaDetails(): LambdaDetails | null;
    /** Whether this process forwards event processing to a Lambda function. */
    get awsLambdaProxyEnabled(): boolean;
    /** Whether this process *is* the Lambda executing proxied events. */
    get isAwsLambdaConsumer(): boolean;
    /** RPC connection details, or `null` if not proxying over RPC. */
    get rpcDetails(): RpcDetails | null;
    /** Whether this process forwards event processing over HTTP RPC. */
    get rpcProxyEnabled(): boolean;
    /** Whether this process *is* the RPC consumer executing proxied events. */
    get isRpcConsumer(): boolean;
    /** gRPC connection details, or `null` if not proxying over gRPC. */
    get grpcDetails(): GrpcDetails | null;
    /** Whether this process forwards event processing over gRPC. */
    get grpcProxyEnabled(): boolean;
    /** Whether this process *is* the gRPC consumer executing proxied events. */
    get isGrpcConsumer(): boolean;
    /**
     * The function used to derive an event's partition key. `null` until `bootstrap()` installs the default
     * (`event => event.partitionKey`) unless {@link EdaManager.usePartitionKeyMapper} supplied one.
     */
    get partitionKeyMapper(): (event: EdaEvent) => string;
    /** Whether per-partition metrics logging is on. `false` unless {@link EdaManager.enableMetrics} was called. */
    get metricsEnabled(): boolean;
    /**
     * How often the `MetricsReporter` logs per-partition lag and throughput when metrics are enabled.
     * Defaults to one minute; see {@link EdaManager.enableMetrics}.
     */
    get metricsInterval(): Duration;
    /**
     * The OpenTelemetry instruments n-eda records into. Created by `bootstrap()`; a no-op instance before it.
     *
     * Note: internal — the consume path records through it. These metrics need no opt-in on the n-eda side;
     * they are live whenever the host registered a `MeterProvider` before `bootstrap()`. See
     * `docs/observability.md`.
     */
    get otelMetrics(): EdaMetrics;
    /**
     * Creates a manager, optionally sharing an existing DI container.
     *
     * Contract: omit `container` and the manager creates one and bootstraps it during
     * {@link EdaManager.bootstrap}. Supply one and **you** are responsible for bootstrapping it — but note
     * that {@link EdaManager.dispose} will dispose it either way.
     *
     * Either way the constructor registers `DefaultEdaContext` scoped under `"EdaContext"`.
     *
     * @param container - an existing n-ject `Container` to adopt, or omit to create one
     * @throws if `container` is supplied but is not a `Container`
     */
    constructor(container?: Container);
    /**
     * Installs your application's DI registrations.
     *
     * RULE: n-eda cannot start without this in practice. `RedisEventBus` is `@inject("EdaRedisClient")` and
     * several internals resolve `"Logger"` at runtime, so an installer registering both is mandatory.
     * n-eda never closes the Redis client it is handed — register a `Disposable` alongside it so
     * {@link EdaManager.dispose} tears the connection down.
     *
     * @param installer - the component installer
     * @returns this manager, for chaining
     * @throws if `installer` is missing or not an object, or if called after `bootstrap()`
     */
    useInstaller(installer: ComponentInstaller): this;
    /**
     * Sets a human-readable label for this consumer, used in diagnostics. Defaults to `"UNNAMED"`.
     *
     * Note: this is not the consumer *group* id — that is the second argument to
     * {@link EdaManager.registerEventSubscriptionManager} and is what actually scopes read offsets. The value
     * here is not trimmed.
     *
     * @param name - the consumer label
     * @returns this manager, for chaining
     * @throws if `name` is missing or not a string, or if called after `bootstrap()`
     */
    useConsumerName(name: string): this;
    /**
     * Registers topics with this manager. Variadic and cumulative — may be called more than once.
     *
     * RULE: at least one topic is required by `bootstrap()`. Remember that topics are publish-only unless
     * `.subscribe()` was chained onto them.
     *
     * Note: duplicate detection is case-**insensitive**, but the internal topic map built at bootstrap is
     * keyed by exact name, so casing must match between registration and `publish`/`mapToPartition`.
     *
     * @param topics - the topics to register
     * @returns this manager, for chaining
     * @throws `ApplicationException` if a topic name duplicates one already registered
     * @throws if called after `bootstrap()`
     */
    registerTopics(...topics: Array<Topic>): this;
    /**
     * Opts this process in to per-partition metrics logging — one flat JSON line per topic-partition per
     * interval, shaped for log-pipeline dashboards (see `docs/observability.md`). Without this call, no
     * metrics lines are emitted.
     *
     * RULE: ingest volume is `topics × partitions` lines per interval. Widen the interval on a service
     * owning many partitions to trade dashboard resolution for log cost.
     *
     * Note: consumers report to their `Broker` at half this interval, so on a healthy consume loop every
     * logged line is at most one report old. A loop blocked mid-batch (e.g. a handler stuck in its retry
     * ladder) pauses reporting for that partition — which the logged `sampleAgeMs` field exposes.
     *
     * @param metricsInterval - how long to wait between reporting ticks; greater than zero, at most
     * ~24.8 days (Node's 2^31-1 ms timer maximum — beyond it `setInterval` clamps to 1ms and would flood
     * the logs). Defaults to one minute when omitted.
     * @returns this manager, for chaining
     * @throws if `metricsInterval` is given but not positive or over the timer maximum, or if called after
     * `bootstrap()`
     */
    enableMetrics(metricsInterval?: Duration): this;
    /**
     * Overrides how an event's partition key is derived. Unset, `bootstrap()` installs
     * `event => event.partitionKey`.
     *
     * RULE: callable **once** — a second call throws. The key determines both the Redis partition
     * (`hash32(key) % numPartitions`) and the process-wide ordering lock, so it is the unit of both
     * concurrency and consistency.
     *
     * Note: the value is trimmed for partition assignment but used untrimmed as the scheduler's ordering
     * key, so a mapper returning padded strings yields two different keys. Return trimmed values.
     *
     * @param func - maps an event to its partition key
     * @returns this manager, for chaining
     * @throws if `func` is missing or not a function, if a mapper is already set, or if called after
     * `bootstrap()`
     */
    usePartitionKeyMapper(func: (event: EdaEvent) => string): this;
    /**
     * Registers event handler classes. Each is reflected through `EventRegistration`, indexed by event type
     * name (or observation key, for observer handlers), and registered **scoped** in the container under its
     * own class name.
     *
     * RULE: three uniqueness constraints follow from that, all enforced here — one handler class per event
     * type, one per observation key, and globally unique handler class names (they are DI keys).
     *
     * @param eventHandlerClasses - decorated handler classes; pair with `discoverEventHandlers` to avoid
     * maintaining this list by hand
     * @returns this manager, for chaining
     * @throws if a class carries neither `@event` nor `@observedEvent`, carries both, or is an observer
     * handler missing `@observable`/`@observer`
     * @throws `ApplicationException` on a duplicate event type or observation key
     * @throws if two handler classes share a name, or if called after `bootstrap()`
     *
     * @example
     * ```typescript
     * edaManager.registerEventHandlers(
     *     ...await discoverEventHandlers(new URL("./event-handlers", import.meta.url)));
     * ```
     */
    registerEventHandlers(...eventHandlerClasses: Array<ClassDefinition<EdaEventHandler<EdaEvent>> | ClassDefinition<ObserverEdaEventHandler<EdaEvent>>>): this;
    /**
     * Registers the event bus under `"EventBus"`. **Mandatory** — `bootstrap()` fails without it.
     *
     * Contract: pass the **class** (`RedisEventBus`) so the container constructs it and injects its
     * dependencies; passing an instance registers that instance as-is and bypasses injection.
     *
     * @param eventBus - the event bus class or a pre-built instance
     * @returns this manager, for chaining
     * @throws if `eventBus` is missing, if a bus is already registered, or if called after `bootstrap()`
     */
    registerEventBus(eventBus: EventBus | ClassDefinition<EventBus>): this;
    /**
     * Registers the subscription manager under `"EventSubMgr"`, making this process a consumer. Optional —
     * omit it for a publish-only service.
     *
     * Contract: `consumerGroupId` scopes the Redis read offset. Distinct ids each receive an independent copy
     * of the stream; processes sharing an id share one offset — and because there is no rebalancing, sharing
     * an id without disjoint `configurePartitionAffinity` ranges causes duplicate processing.
     *
     * Note: mutually exclusive with acting as an AWS Lambda or RPC consumer; `bootstrap()` enforces that.
     *
     * @param eventSubMgr - the subscription manager class (preferred) or instance
     * @param consumerGroupId - the consumer group id; required, trimmed, no default
     * @returns this manager, for chaining
     * @throws if either argument is missing, if a subscription manager is already registered, or if called
     * after `bootstrap()`
     */
    registerEventSubscriptionManager(eventSubMgr: EventSubMgr | ClassDefinition<EventSubMgr>, consumerGroupId: string): this;
    /**
     * Makes consumers delete event payload keys from Redis once they have been read, instead of waiting for
     * the topic TTL to expire them.
     *
     * RULE: **destructive across consumer groups.** The delete happens as soon as *this* group's read index
     * advances, so a second consumer group on the same topic loses data it has not read yet. Enable this only
     * when exactly one consumer group consumes the topic.
     *
     * @returns this manager, for chaining
     * @throws if called after `bootstrap()`
     */
    cleanUpKeys(): this;
    /**
     * Forwards event processing to an AWS Lambda function instead of running handlers in this process. This
     * process still runs the Redis consume loop.
     *
     * Note: no "already set" guard — calling twice silently overwrites.
     *
     * @param lambdaDetails - region, function name, and credentials
     * @returns this manager, for chaining
     * @throws if `lambdaDetails` is missing or not an object, or if called after `bootstrap()`
     */
    proxyToAwsLambda(lambdaDetails: LambdaDetails): this;
    /**
     * Marks this process as the Lambda that executes proxied events. Use inside the Lambda itself, paired
     * with a producing process that called {@link EdaManager.proxyToAwsLambda}.
     *
     * Note: mutually exclusive with registering an `EventSubMgr` or acting as an RPC consumer.
     * Also note that `EdaContext.topic` is unavailable on this path.
     *
     * @param handler - the handler whose `process` the Lambda entry point delegates to
     * @returns this manager, for chaining
     * @throws if `handler` is missing or not an `AwsLambdaEventHandler`, or if called after `bootstrap()`
     */
    actAsAwsLambdaConsumer(handler: AwsLambdaEventHandler): this;
    /**
     * Forwards event processing to an HTTP RPC consumer instead of running handlers in this process.
     *
     * Note: the transport is plaintext and unauthenticated; calls time out after 60 seconds. No "already
     * set" guard — calling twice silently overwrites.
     *
     * @param rpcDetails - host and port of the RPC consumer
     * @returns this manager, for chaining
     * @throws if `rpcDetails` is missing or not an object, or if called after `bootstrap()`
     */
    proxyToRpc(rpcDetails: RpcDetails): this;
    /**
     * Marks this process as the RPC consumer that executes proxied events. Pair with an `RpcServer` to host
     * the endpoint.
     *
     * Note: mutually exclusive with registering an `EventSubMgr` or acting as a Lambda consumer.
     * `EdaContext.topic` is unavailable on this path.
     *
     * @param handler - the handler the RPC server dispatches to
     * @returns this manager, for chaining
     * @throws if `handler` is missing or not an `RpcEventHandler`, or if called after `bootstrap()`
     */
    actAsRpcConsumer(handler: RpcEventHandler): this;
    /**
     * Forwards event processing to a gRPC consumer instead of running handlers in this process. The `.proto`
     * definitions ship with this package.
     *
     * Note: connections are insecure — `GrpcDetails.isSecure` is ignored — and there is no per-call deadline.
     * No "already set" guard; calling twice silently overwrites.
     *
     * @param grpcDetails - host, port, and optional pool size
     * @returns this manager, for chaining
     * @throws if `grpcDetails` is missing or not an object, or if called after `bootstrap()`
     */
    proxyToGrpc(grpcDetails: GrpcDetails): this;
    /**
     * Marks this process as the gRPC consumer that executes proxied events. Pair with a `GrpcServer` to host
     * the endpoint.
     *
     * Note: mutually exclusive with registering an `EventSubMgr` or acting as a Lambda or RPC consumer;
     * `bootstrap()` enforces that. `EdaContext.topic` is unavailable on this path.
     *
     * @param handler - the handler the gRPC server dispatches to
     * @returns this manager, for chaining
     * @throws if `handler` is missing or not a `GrpcEventHandler`, or if called after `bootstrap()`
     */
    actAsGrpcConsumer(handler: GrpcEventHandler): this;
    /**
     * Turns on the distributed observer pattern, dedicating a topic to observer notifications.
     *
     * Contract: the topic is registered like any other (so it must not duplicate an existing name) and is
     * additionally recorded as the observer topic. Both the observable-side and observer-side processes must
     * call this. You may **not** publish to this topic directly — `RedisEventBus.publish` rejects it.
     *
     * @param topic - the dedicated notification topic; `.subscribe()` it on the observer side
     * @returns this manager, for chaining
     * @throws if `topic` is missing or not a `Topic`, or if called after `bootstrap()`
     * @throws `ApplicationException` if a topic with that name is already registered
     */
    enableDistributedObserver(topic: Topic): this;
    /**
     * Finalizes configuration and brings the manager up. **Async since 7.0.0 — always `await` it.**
     *
     * In order: installs the default partition key mapper if none was set; builds the topic lookup map;
     * bootstraps the container (only if this manager created it); takes the `"n-eda"` meter from the global
     * OpenTelemetry `MeterProvider`; then calls `initialize(this)` on the event bus, the subscription manager
     * if registered, and any proxy consumer handler.
     *
     * Note: register your `MeterProvider` before calling this. The meter taken here is kept for the
     * manager's lifetime, and if no provider is registered yet it is a no-op meter. That case is logged once,
     * at info level, through the container's `"Logger"`.
     *
     * After this returns, every configuration method throws.
     *
     * @throws `ObjectDisposedException` if already disposed
     * @throws if called twice, if no topic was registered, or if no event bus was registered
     * @throws if the consumer roles conflict — the event subscriber, AWS Lambda consumer, RPC consumer, and
     * gRPC consumer roles are mutually exclusive, and every pair is checked
     */
    bootstrap(): Promise<void>;
    /**
     * Starts the consume loop, creating one consumer per owned partition of every subscribed topic.
     *
     * RULE: the returned promise **does not resolve** until {@link EdaManager.dispose}. Never `await` this in
     * a startup path — call it and move on, attaching a `.catch(...)` so failures are not swallowed.
     *
     * @throws `ObjectDisposedException` if already disposed
     * @throws if not bootstrapped, or if no `EventSubMgr` was registered
     *
     * @example
     * ```typescript
     * await edaManager.bootstrap();
     * edaManager.beginConsumption().catch(e => console.error(e));   // not awaited
     * ```
     */
    beginConsumption(): Promise<void>;
    /**
     * Computes which partition of a topic an event belongs to:
     * `murmurhash3 x86 hash32(partitionKeyMapper(event).trim()) % topic.numPartitions`.
     *
     * Note: the topic lookup is by **exact** name and the map only exists after `bootstrap()`, so this always
     * fails beforehand — and fails for a case mismatch even though duplicate detection is case-insensitive.
     *
     * @param topic - a registered topic name
     * @param event - the event to place
     * @returns the zero-based partition number
     * @throws `ObjectDisposedException` if already disposed
     * @throws if the topic is not registered, the event is missing, or the manager is not bootstrapped
     */
    mapToPartition(topic: string, event: EdaEvent): number;
    /**
     * Shuts the manager down: stops the consume loop (which is what finally resolves
     * {@link EdaManager.beginConsumption}), then disposes the container — and with it anything `Disposable`
     * you registered, such as the Redis client.
     *
     * Idempotent: the first call's promise is memoized and returned to every subsequent caller. Safe to call
     * before `bootstrap()`.
     *
     * Note: the container is disposed **even when it was supplied externally**. If you share a container with
     * the rest of your application, disposing the manager takes that container down too.
     *
     * @returns a promise that resolves once teardown is complete
     */
    dispose(): Promise<void>;
}
//# sourceMappingURL=eda-manager.d.ts.map