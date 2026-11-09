import { EdaManager } from "../eda-manager.js";
import { EventSubMgr } from "../event-sub-mgr.js";
// import * as Redis from "redis";
import { given } from "@nivinjoseph/n-defensive";
import { ObjectDisposedException } from "@nivinjoseph/n-exception";
import { ServiceLocator, inject } from "@nivinjoseph/n-ject";
import { Logger } from "@nivinjoseph/n-log";
import { Delay } from "@nivinjoseph/n-util";
import { Redis } from "ioredis";
import { EdaEvent } from "../eda-event.js";
import { AwsLambdaProxyProcessor } from "./aws-lambda-proxy-processor.js";
import { Broker } from "./broker.js";
import { Consumer } from "./consumer.js";
import { DefaultProcessor } from "./default-processor.js";
import { GrpcClientFactory } from "./grpc-client-factory.js";
import { GrpcProxyProcessor } from "./grpc-proxy-processor.js";
import { MetricsReporter } from "./metrics-reporter.js";
import { Monitor } from "./monitor.js";
import { Processor } from "./processor.js";
import { RpcProxyProcessor } from "./rpc-proxy-processor.js";
import { DefaultEdaContext } from "../eda-context.js";
// import { ConsumerProfiler } from "./consumer-profiler";
// import { ProfilingConsumer } from "./profiling-consumer";

// public
/**
 * The Redis-backed `EventSubMgr` — the only shipped implementation, and the process that actually runs
 * handlers in-process.
 *
 * Contract: register the **class** with
 * `EdaManager.registerEventSubscriptionManager(RedisEventSubMgr, consumerGroupId)`. It is
 * `@inject("EdaRedisClient", "Logger")`, so both DI keys must be registered by your `ComponentInstaller`.
 *
 * On `consume()` it creates, for every topic that is neither disabled nor publish-only, one `Consumer` and
 * one `Processor` per owned partition — all partitions unless `Topic.configurePartitionAffinity` narrowed
 * the range — plus a `Broker` per topic, process-wide a `Monitor`, and (only when
 * `EdaManager.enableMetrics()` was called) a `MetricsReporter`. It also registers the OpenTelemetry partition
 * instruments, regardless of `enableMetrics()`.
 *
 * Note: this is the only path that populates `EdaContext`. If every registered topic is publish-only or
 * disabled while a subscription manager is registered, `consume()` throws, because the `Monitor` requires a
 * non-empty consumer list.
 */
@inject("EdaRedisClient", "Logger")
export class RedisEventSubMgr implements EventSubMgr
{
    private readonly _client: Redis;
    private readonly _logger: Logger;
    private readonly _brokers = new Array<Broker>();
    // Assigned only once consume() has run, so dispose() has to tolerate them being absent.
    private _monitor: Monitor | null = null;
    private _metricsReporter: MetricsReporter | null = null;
    private _unobservePartitions: (() => void) | null = null;

    private _isDisposing = false;
    private _isDisposed = false;
    private _disposePromise: Promise<void> | null = null;
    private _manager: EdaManager = null as any;
    private _isConsuming = false;


    /**
     * @param redisClient - injected from the DI key `"EdaRedisClient"`
     * @param logger - injected from the DI key `"Logger"`
     */
    public constructor(redisClient: Redis, logger: Logger)
    {
        given(redisClient, "redisClient").ensureHasValue().ensureIsObject();
        this._client = redisClient;

        given(logger, "logger").ensureHasValue().ensureIsObject();
        this._logger = logger;
    }


    /**
     * Binds the subscription manager to its manager. Called by `EdaManager.bootstrap()` — never call it
     * yourself.
     *
     * @param manager - the bootstrapping manager
     * @throws `ObjectDisposedException` if already disposed
     * @throws if the manager is missing or not an `EdaManager`, or if already initialized
     */
    public initialize(manager: EdaManager): void
    {
        given(manager, "manager").ensureHasValue().ensureIsObject().ensureIsType(EdaManager);

        if (this._isDisposed)
            throw new ObjectDisposedException(this);

        given(this, "this").ensure(t => !t._manager, "already initialized");

        this._manager = manager;
        // if (this._manager.metricsEnabled)
        //     ConsumerProfiler.initialize();
    }

    public async consume(): Promise<void>
    {
        if (this._isDisposed)
            throw new ObjectDisposedException(this);

        given(this, "this").ensure(t => !!t._manager, "not initialized");

        if (!this._isConsuming)
        {
            this._isConsuming = true;

            try
            {
                const monitorConsumers = new Array<Consumer>();

                this._manager.topics.forEach(topic =>
                {
                    if (topic.isDisabled || topic.publishOnly)
                        return;

                    let partitions = topic.partitionAffinity ? [...topic.partitionAffinity] : null;
                    if (partitions == null)
                    {
                        partitions = new Array<number>();
                        for (let partition = 0; partition < topic.numPartitions; partition++)
                            partitions.push(partition);
                    }

                    const consumers = partitions
                        .map(partition =>
                            new Consumer(this._client, this._manager, topic.name, partition, topic.isFlush));

                    let processors: Array<Processor>;
                    if (this._manager.awsLambdaProxyEnabled)
                        processors = consumers.map(_ => new AwsLambdaProxyProcessor(this._manager));
                    else if (this._manager.rpcProxyEnabled)
                        processors = consumers.map(_ => new RpcProxyProcessor(this._manager));
                    else if (this._manager.grpcProxyEnabled)
                    {
                        const grpcClientFactory = new GrpcClientFactory(this._manager);
                        processors = consumers.map(_ => new GrpcProxyProcessor(this._manager, grpcClientFactory));
                    }
                    else
                        processors = consumers.map(_ => new DefaultProcessor(this._manager, this.onEventReceived.bind(this)));

                    const broker = new Broker(topic, consumers, processors);
                    this._brokers.push(broker);

                    monitorConsumers.push(...consumers);
                    // const monitor = new Monitor(this._client, consumers, this._logger);
                    // this._monitors.push(monitor);
                });

                // Assigned to the field before starting, so that a failed start still leaves the Monitor's
                // duplicated connection reachable for the unwind below (and for dispose()).
                this._monitor = new Monitor(this._client, monitorConsumers, this._logger);
                await this._monitor.start();

                // dispose() may have landed while start() was awaited; it saw _metricsReporter as null then,
                // so a reporter created now would install a timer nothing ever clears. Stop instead.
                if (this._isDisposing)
                    return;

                // Not gated on enableMetrics(): the instruments read the consumers' own readings, not the log
                // reporter's samples, and are no-ops unless the host registered a MeterProvider.
                this._unobservePartitions = this._manager.otelMetrics.observePartitions();

                // Metrics are opt-in via EdaManager.enableMetrics(); without it no reporter exists and no
                // metrics lines are emitted. consumerGroupId is non-null here: registerEventSubscriptionManager
                // requires it, and consume() only runs when a sub-mgr was registered.
                if (this._manager.metricsEnabled)
                {
                    this._metricsReporter = new MetricsReporter(
                        this._brokers, this._logger, this._manager.consumerGroupId!,
                        this._manager.consumerName, this._manager.metricsInterval);
                    this._metricsReporter.start();
                }

                this._brokers.forEach(t => t.initialize());
            }
            catch (error)
            {
                // Unwind fully so a caller that catches and retries consume() after a transient failure gets
                // a real second attempt — otherwise the _isConsuming guard above would skip setup and leave
                // the process sitting in the wait loop below, looking healthy while consuming nothing.
                this._unobservePartitions?.();
                this._unobservePartitions = null;

                await Promise.all([
                    this._monitor?.dispose(),
                    this._metricsReporter?.dispose(),
                    ...this._brokers.map(t => t.dispose())
                ]).catch(e => console.error(e));

                this._monitor = null;
                this._metricsReporter = null;
                this._brokers.length = 0;
                this._isConsuming = false;

                throw error;
            }
        }

        while (!this._isDisposed)
        {
            await Delay.seconds(5);
        }
    }

    public dispose(): Promise<void>
    {
        if (!this._isDisposing)
        {
            this._isDisposing = true;
            console.warn("Disposing EventSubMgr");

            // The meter outlives this manager; a callback left registered would keep reporting dead brokers.
            this._unobservePartitions?.();
            this._unobservePartitions = null;

            this._disposePromise = Promise.all([
                // ...this._monitors.map(t => t.dispose()),
                // Optional: both are only assigned once consume() has run, so disposing a manager that never
                // started must not throw here.
                this._monitor?.dispose(),
                this._metricsReporter?.dispose(),
                ...this._brokers.map(t => t.dispose())
            ])
                .catch(e => console.error(e))
                .finally(() =>
                {
                    this._isDisposed = true;
                    console.warn("EventSubMgr disposed");
                }) as unknown as Promise<void>;

            // if (this._manager.metricsEnabled)
            // {
            //     await Delay.seconds(3);

            //     ConsumerProfiler.aggregate(this._manager.consumerName, this._consumers.map(t => (<ProfilingConsumer>t).profiler));
            // }
        }

        return this._disposePromise!;
    }


    /**
     * Called once per event, immediately after the per-event DI child scope is created and before the handler
     * is resolved. This implementation resolves the scoped `DefaultEdaContext` and stamps the topic onto it.
     *
     * Override to attach additional per-event ambient state to the scope.
     *
     * @param scope - the child scope for this delivery, disposed once `handle()` returns
     * @param topic - the topic that delivered the event
     * @param event - the deserialized event
     */
    protected onEventReceived(scope: ServiceLocator, topic: string, event: EdaEvent): void
    {
        given(scope, "scope").ensureHasValue().ensureIsObject();
        given(topic, "topic").ensureHasValue().ensureIsString();
        given(event, "event").ensureHasValue().ensureIsObject();
        
        const edaContext = scope.resolve<DefaultEdaContext>("EdaContext");
        edaContext.topic = topic;
    }
}