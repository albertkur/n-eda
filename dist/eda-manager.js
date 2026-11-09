import { given } from "@nivinjoseph/n-defensive";
import { Container } from "@nivinjoseph/n-ject";
import { ApplicationException, ObjectDisposedException } from "@nivinjoseph/n-exception";
import { Duration } from "@nivinjoseph/n-util";
import { EventRegistration } from "./event-registration.js";
import { Topic } from "./topic.js";
import MurmurHash from "murmurhash3js";
import { AwsLambdaEventHandler } from "./redis-implementation/aws-lambda-event-handler.js";
import { RpcEventHandler } from "./redis-implementation/rpc-event-handler.js";
import { GrpcEventHandler } from "./redis-implementation/grpc-event-handler.js";
import { DefaultEdaContext } from "./eda-context.js";
import * as otelApi from "@opentelemetry/api";
import { EdaMetrics } from "./metrics/eda-metrics.js";
// import { ConsumerTracer } from "./event-handler-tracer";
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
// public
export class EdaManager {
    _container;
    _ownsContainer;
    _topics;
    _topicMap;
    _eventMap;
    _observerEventMap;
    // private readonly _wildKeys: Array<string>;
    _metricsEnabled = false;
    _metricsInterval = Duration.fromMinutes(1);
    // Replaced in bootstrap(); no-op until then, so nothing can record into a meter taken too early.
    _otelMetrics = new EdaMetrics(otelApi.createNoopMeter(), null);
    _partitionKeyMapper = null;
    _eventBusRegistered = false;
    _eventSubMgrRegistered = false;
    _evtSubMgr = null;
    _consumerName = "UNNAMED";
    _consumerGroupId = null;
    _cleanKeys = false;
    _distributedObserverTopic = null;
    // private _consumerTracer: ConsumerTracer | null = null;
    _awsLambdaDetails = null;
    _isAwsLambdaConsumer = false;
    _awsLambdaEventHandler = null;
    _rpcDetails = null;
    _isRpcConsumer = false;
    _rpcEventHandler = null;
    _grpcDetails = null;
    _isGrpcConsumer = false;
    _grpcEventHandler = null;
    _isDisposed = false;
    _disposePromise = null;
    _isBootstrapped = false;
    /** DI key the event bus is registered under: `"EventBus"`. */
    static get eventBusKey() { return "EventBus"; }
    /** DI key the subscription manager is registered under: `"EventSubMgr"`. */
    static get eventSubMgrKey() { return "EventSubMgr"; }
    /** The underlying container as a `Registry`, for registering additional components. */
    get containerRegistry() { return this._container; }
    /**
     * The underlying container as a `ServiceLocator`. This is how you get the event bus out:
     * `serviceLocator.resolve<EventBus>("EventBus")`.
     */
    get serviceLocator() { return this._container; }
    /** All registered topics, including the distributed observer topic if one was enabled. */
    get topics() { return this._topics; }
    /** The topic dedicated to distributed observer notifications, or `null` if the feature is off. */
    get distributedObserverTopic() { return this._distributedObserverTopic; }
    /**
     * Standard handler registrations, keyed by **event type name**. `RedisEventBus.publish` consults this to
     * decide whether an event is publishable at all — an event absent from this map is dropped unless the
     * topic was configured with `.forcePublish()`.
     */
    get eventMap() { return this._eventMap; }
    /**
     * Observer handler registrations, keyed by **observation key** (`` `.${observer}.${observable}.${event}` ``),
     * not by event name. Use `EventRegistration.generateObservationKey(...)` to build a lookup key.
     */
    get observerEventMap() { return this._observerEventMap; }
    /** This consumer's label, or `"UNNAMED"` if {@link EdaManager.useConsumerName} was never called. */
    get consumerName() { return this._consumerName; }
    /** The consumer group id, or `null` if no subscription manager was registered. Scopes the read offset. */
    get consumerGroupId() { return this._consumerGroupId; }
    /** Whether consumers delete event payload keys after reading them. See {@link EdaManager.cleanUpKeys}. */
    get cleanKeys() { return this._cleanKeys; }
    // public get consumerTracer(): ConsumerTracer | null { return this._consumerTracer; }
    /** Lambda connection details, or `null` if not proxying to Lambda. */
    get awsLambdaDetails() { return this._awsLambdaDetails; }
    /** Whether this process forwards event processing to a Lambda function. */
    get awsLambdaProxyEnabled() { return this._awsLambdaDetails != null; }
    /** Whether this process *is* the Lambda executing proxied events. */
    get isAwsLambdaConsumer() { return this._isAwsLambdaConsumer; }
    /** RPC connection details, or `null` if not proxying over RPC. */
    get rpcDetails() { return this._rpcDetails; }
    /** Whether this process forwards event processing over HTTP RPC. */
    get rpcProxyEnabled() { return this._rpcDetails != null; }
    /** Whether this process *is* the RPC consumer executing proxied events. */
    get isRpcConsumer() { return this._isRpcConsumer; }
    /** gRPC connection details, or `null` if not proxying over gRPC. */
    get grpcDetails() { return this._grpcDetails; }
    /** Whether this process forwards event processing over gRPC. */
    get grpcProxyEnabled() { return this._grpcDetails != null; }
    /** Whether this process *is* the gRPC consumer executing proxied events. */
    get isGrpcConsumer() { return this._isGrpcConsumer; }
    /**
     * The function used to derive an event's partition key. `null` until `bootstrap()` installs the default
     * (`event => event.partitionKey`) unless {@link EdaManager.usePartitionKeyMapper} supplied one.
     */
    get partitionKeyMapper() { return this._partitionKeyMapper; }
    /** Whether per-partition metrics logging is on. `false` unless {@link EdaManager.enableMetrics} was called. */
    get metricsEnabled() { return this._metricsEnabled; }
    /**
     * How often the `MetricsReporter` logs per-partition lag and throughput when metrics are enabled.
     * Defaults to one minute; see {@link EdaManager.enableMetrics}.
     */
    get metricsInterval() { return this._metricsInterval; }
    /**
     * The OpenTelemetry instruments n-eda records into. Created by `bootstrap()`; a no-op instance before it.
     *
     * Note: internal — the consume path records through it. These metrics need no opt-in on the n-eda side;
     * they are live whenever the host registered a `MeterProvider` before `bootstrap()`. See
     * `docs/observability.md`.
     */
    get otelMetrics() { return this._otelMetrics; }
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
    constructor(container) {
        given(container, "container").ensureIsObject().ensureIsType(Container);
        if (container == null) {
            this._container = new Container();
            this._ownsContainer = true;
        }
        else {
            this._container = container;
            this._ownsContainer = false;
        }
        this._topics = new Array();
        this._topicMap = new Map();
        this._eventMap = new Map();
        this._observerEventMap = new Map();
        // this._wildKeys = new Array<string>();
        this._container.registerScoped("EdaContext", DefaultEdaContext);
    }
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
    useInstaller(installer) {
        given(installer, "installer").ensureHasValue().ensureIsObject();
        given(this, "this").ensure(t => !t._isBootstrapped, "invoking method after bootstrap");
        this._container.install(installer);
        return this;
    }
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
    useConsumerName(name) {
        given(name, "name").ensureHasValue().ensureIsString();
        given(this, "this").ensure(t => !t._isBootstrapped, "invoking method after bootstrap");
        this._consumerName = name;
        return this;
    }
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
    registerTopics(...topics) {
        given(topics, "topics").ensureHasValue().ensureIsArray();
        given(this, "this").ensure(t => !t._isBootstrapped, "invoking method after bootstrap");
        for (const topic of topics) {
            const name = topic.name.toLowerCase();
            if (this._topics.some(t => t.name.toLowerCase() === name))
                throw new ApplicationException(`Multiple topics with the name '${name}' detected.`);
            this._topics.push(topic);
        }
        return this;
    }
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
    enableMetrics(metricsInterval) {
        given(metricsInterval, "metricsInterval")
            .ensure(t => t.toMilliSeconds() > 0, "must be greater than zero")
            .ensure(t => t.toMilliSeconds() <= 2147483647, "must not exceed 2^31-1 ms (~24.8 days), Node's timer maximum");
        given(this, "this").ensure(t => !t._isBootstrapped, "invoking method after bootstrap");
        this._metricsEnabled = true;
        if (metricsInterval != null)
            this._metricsInterval = metricsInterval;
        return this;
    }
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
    usePartitionKeyMapper(func) {
        given(func, "func").ensureHasValue().ensureIsFunction();
        given(this, "this")
            .ensure(t => !t._partitionKeyMapper, "partition key mapper already set")
            .ensure(t => !t._isBootstrapped, "invoking method after bootstrap");
        this._partitionKeyMapper = func;
        return this;
    }
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
    registerEventHandlers(...eventHandlerClasses) {
        given(eventHandlerClasses, "eventHandlerClasses").ensureHasValue().ensureIsArray();
        given(this, "this").ensure(t => !t._isBootstrapped, "invoking method after bootstrap");
        for (const eventHandler of eventHandlerClasses) {
            const eventRegistration = new EventRegistration(eventHandler);
            if (eventRegistration.isObservedEvent) {
                // observable, observedEvent, observer, observedEventHandler
                // Need to enforce: that for one observable.observedEvent.observer combination, there is on only one handler
                if (this._observerEventMap.has(eventRegistration.observationKey))
                    throw new ApplicationException(`Multiple observer event handlers detected for observer key '${eventRegistration.observationKey}'.`);
                this._observerEventMap.set(eventRegistration.observationKey, eventRegistration);
            }
            else {
                // this enforces that you cannot have 2 handler classes for the same event
                if (this._eventMap.has(eventRegistration.eventTypeName))
                    throw new ApplicationException(`Multiple event handlers detected for event '${eventRegistration.eventTypeName}'.`);
                this._eventMap.set(eventRegistration.eventTypeName, eventRegistration);
            }
            // this enforces that you cannot have 2 handler classes with the same name
            this._container.registerScoped(eventRegistration.eventHandlerTypeName, eventRegistration.eventHandlerType);
        }
        return this;
    }
    // public registerConsumerTracer(tracer: ConsumerTracer): this
    // {
    //     given(tracer, "tracer").ensureHasValue().ensureIsFunction();
    //     given(this, "this")
    //         .ensure(t => !t._isBootstrapped, "invoking method after bootstrap")
    //         .ensure(t => !t._consumerTracer, "consumer tracer already set");
    //     this._consumerTracer = tracer;
    //     return this;
    // }
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
    registerEventBus(eventBus) {
        given(eventBus, "eventBus").ensureHasValue();
        given(this, "this")
            .ensure(t => !t._isBootstrapped, "invoking method after bootstrap")
            .ensure(t => !t._eventBusRegistered, "event bus already registered");
        if (typeof eventBus === "function")
            this._container.registerSingleton(EdaManager.eventBusKey, eventBus);
        else
            this._container.registerInstance(EdaManager.eventBusKey, eventBus);
        this._eventBusRegistered = true;
        return this;
    }
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
    registerEventSubscriptionManager(eventSubMgr, consumerGroupId) {
        given(eventSubMgr, "eventSubMgr").ensureHasValue();
        given(consumerGroupId, "consumerGroupId").ensureHasValue().ensureIsString();
        given(this, "this")
            .ensure(t => !t._isBootstrapped, "invoking method after bootstrap")
            .ensure(t => !t._eventSubMgrRegistered, "event subscription manager already registered");
        if (typeof eventSubMgr === "function")
            this._container.registerSingleton(EdaManager.eventSubMgrKey, eventSubMgr);
        else
            this._container.registerInstance(EdaManager.eventSubMgrKey, eventSubMgr);
        this._consumerGroupId = consumerGroupId.trim();
        this._eventSubMgrRegistered = true;
        return this;
    }
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
    cleanUpKeys() {
        given(this, "this")
            .ensure(t => !t._isBootstrapped, "invoking method after bootstrap");
        this._cleanKeys = true;
        return this;
    }
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
    proxyToAwsLambda(lambdaDetails) {
        given(lambdaDetails, "lambdaDetails").ensureHasValue().ensureIsObject();
        given(this, "this")
            .ensure(t => !t._isBootstrapped, "invoking method after bootstrap");
        this._awsLambdaDetails = lambdaDetails;
        return this;
    }
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
    actAsAwsLambdaConsumer(handler) {
        given(handler, "handler").ensureHasValue().ensureIsObject().ensureIsInstanceOf(AwsLambdaEventHandler);
        given(this, "this")
            .ensure(t => !t._isBootstrapped, "invoking method after bootstrap");
        this._awsLambdaEventHandler = handler;
        this._isAwsLambdaConsumer = true;
        return this;
    }
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
    proxyToRpc(rpcDetails) {
        given(rpcDetails, "rpcDetails").ensureHasValue().ensureIsObject();
        given(this, "this")
            .ensure(t => !t._isBootstrapped, "invoking method after bootstrap");
        this._rpcDetails = rpcDetails;
        return this;
    }
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
    actAsRpcConsumer(handler) {
        given(handler, "handler").ensureHasValue().ensureIsObject().ensureIsInstanceOf(RpcEventHandler);
        given(this, "this")
            .ensure(t => !t._isBootstrapped, "invoking method after bootstrap");
        this._rpcEventHandler = handler;
        this._isRpcConsumer = true;
        return this;
    }
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
    proxyToGrpc(grpcDetails) {
        given(grpcDetails, "grpcDetails").ensureHasValue().ensureIsObject();
        given(this, "this")
            .ensure(t => !t._isBootstrapped, "invoking method after bootstrap");
        this._grpcDetails = grpcDetails;
        return this;
    }
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
    actAsGrpcConsumer(handler) {
        given(handler, "handler").ensureHasValue().ensureIsObject().ensureIsInstanceOf(GrpcEventHandler);
        given(this, "this")
            .ensure(t => !t._isBootstrapped, "invoking method after bootstrap");
        this._grpcEventHandler = handler;
        this._isGrpcConsumer = true;
        return this;
    }
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
    enableDistributedObserver(topic) {
        given(topic, "topic").ensureHasValue().ensureIsType(Topic);
        given(this, "this").ensure(t => !t._isBootstrapped, "invoking method after bootstrap");
        const name = topic.name.toLowerCase();
        if (this._topics.some(t => t.name.toLowerCase() === name))
            throw new ApplicationException(`Multiple topics with the name '${name}' detected when registering distributed observer topic.`);
        this._topics.push(topic);
        this._distributedObserverTopic = topic;
        return this;
    }
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
    async bootstrap() {
        if (this._isDisposed)
            throw new ObjectDisposedException(this);
        given(this, "this")
            .ensure(t => !t._isBootstrapped, "bootstrapping more than once")
            .ensure(t => t._topics.length > 0, "no topics registered")
            // .ensure(t => !!t._partitionKeyMapper, "no partition key mapper set")
            .ensure(t => t._eventBusRegistered, "no event bus registered")
            .ensure(t => !(t._eventSubMgrRegistered && t._isAwsLambdaConsumer), "cannot be both event subscriber and lambda consumer")
            .ensure(t => !(t._eventSubMgrRegistered && t._isRpcConsumer), "cannot be both event subscriber and rpc consumer")
            .ensure(t => !(t._isAwsLambdaConsumer && t._isRpcConsumer), "cannot be both lambda consumer and rpc consumer")
            .ensure(t => !(t._eventSubMgrRegistered && t._isGrpcConsumer), "cannot be both event subscriber and grpc consumer")
            .ensure(t => !(t._isAwsLambdaConsumer && t._isGrpcConsumer), "cannot be both lambda consumer and grpc consumer")
            .ensure(t => !(t._isRpcConsumer && t._isGrpcConsumer), "cannot be both rpc consumer and grpc consumer");
        // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
        if (this._partitionKeyMapper == null)
            this._partitionKeyMapper = (edaEvent) => edaEvent.partitionKey;
        this._topics.map(t => this._topicMap.set(t.name, t));
        if (this._ownsContainer)
            await this._container.bootstrap();
        // As late as possible: the metrics API hands out a no-op meter, permanently, if no MeterProvider is
        // registered yet — and a host may register one from code that runs ahead of bootstrap.
        const meter = otelApi.metrics.getMeter("n-eda");
        this._otelMetrics = new EdaMetrics(meter, this._consumerGroupId);
        // The API hands out one shared no-op meter, so identity tells a dead meter apart. Info, not a warning:
        // a host that does not use OpenTelemetry metrics would otherwise be warned on every start.
        if (meter === otelApi.createNoopMeter())
            await this._container.resolve("Logger").logInfo("n-eda: OpenTelemetry metrics are off for "
                + "this manager's lifetime. No MeterProvider was registered before bootstrap(), or the registered "
                + "one comes from an @opentelemetry/api version incompatible with n-eda's.");
        this._container.resolve(EdaManager.eventBusKey).initialize(this);
        if (this._eventSubMgrRegistered)
            this._container.resolve(EdaManager.eventSubMgrKey).initialize(this);
        if (this._isAwsLambdaConsumer)
            this._awsLambdaEventHandler.initialize(this);
        if (this._isRpcConsumer)
            this._rpcEventHandler.initialize(this);
        if (this._isGrpcConsumer)
            this._grpcEventHandler.initialize(this);
        this._isBootstrapped = true;
    }
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
    async beginConsumption() {
        if (this._isDisposed)
            throw new ObjectDisposedException(this);
        given(this, "this")
            .ensure(t => t._isBootstrapped, "not bootstrapped")
            .ensure(t => t._eventSubMgrRegistered, "no EventSubMgr registered");
        this._evtSubMgr = this.serviceLocator.resolve(EdaManager.eventSubMgrKey);
        await this._evtSubMgr.consume();
    }
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
    mapToPartition(topic, event) {
        given(topic, "topic").ensureHasValue().ensureIsString()
            .ensure(t => this._topicMap.has(t), "must be a registered topic name, matched case-sensitively, and is only resolvable after bootstrap");
        given(event, "event").ensureHasValue().ensureIsObject();
        if (this._isDisposed)
            throw new ObjectDisposedException(this);
        given(this, "this")
            .ensure(t => t._isBootstrapped, "not bootstrapped");
        const partitionKey = this._partitionKeyMapper(event).trim();
        return MurmurHash.x86.hash32(partitionKey) % this._topicMap.get(topic).numPartitions;
    }
    // public getEventRegistration(event: EdaEvent): EventRegistration | false
    // {
    //     let eventRegistration: EventRegistration | null = null;
    //     if (this._eventMap.has(event.name))
    //         eventRegistration = this._eventMap.get(event.name) as EventRegistration;
    //     else
    //     {
    //         const wildKey = this._wildKeys.find(t => event.name.startsWith(t));
    //         if (wildKey)
    //             eventRegistration = this._eventMap.get(wildKey) as EventRegistration;
    //     }
    //     return eventRegistration || false;
    // }
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
    dispose() {
        if (!this._isDisposed) {
            this._isDisposed = true;
            if (this._evtSubMgr != null)
                this._disposePromise = this._evtSubMgr.dispose().then(() => this._container.dispose());
            else
                this._disposePromise = this._container.dispose();
        }
        return this._disposePromise;
    }
}
//# sourceMappingURL=eda-manager.js.map