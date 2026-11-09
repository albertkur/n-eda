import { EdaManager } from "../eda-manager.js";
import { EventSubMgr } from "../event-sub-mgr.js";
import { ServiceLocator } from "@nivinjoseph/n-ject";
import { Logger } from "@nivinjoseph/n-log";
import { Redis } from "ioredis";
import { EdaEvent } from "../eda-event.js";
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
export declare class RedisEventSubMgr implements EventSubMgr {
    private readonly _client;
    private readonly _logger;
    private readonly _brokers;
    private _monitor;
    private _metricsReporter;
    private _unobservePartitions;
    private _isDisposing;
    private _isDisposed;
    private _disposePromise;
    private _manager;
    private _isConsuming;
    /**
     * @param redisClient - injected from the DI key `"EdaRedisClient"`
     * @param logger - injected from the DI key `"Logger"`
     */
    constructor(redisClient: Redis, logger: Logger);
    /**
     * Binds the subscription manager to its manager. Called by `EdaManager.bootstrap()` — never call it
     * yourself.
     *
     * @param manager - the bootstrapping manager
     * @throws `ObjectDisposedException` if already disposed
     * @throws if the manager is missing or not an `EdaManager`, or if already initialized
     */
    initialize(manager: EdaManager): void;
    consume(): Promise<void>;
    dispose(): Promise<void>;
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
    protected onEventReceived(scope: ServiceLocator, topic: string, event: EdaEvent): void;
}
//# sourceMappingURL=redis-event-sub-mgr.d.ts.map