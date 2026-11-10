import { Disposable } from "@nivinjoseph/n-util";
import { Redis } from "ioredis";
import { EdaManager } from "../eda-manager.js";
import { Broker } from "./broker.js";
/**
 * Reads one partition of one topic for one consumer group, and routes what it finds to the broker.
 *
 * The loop: `MGET` the write and read indexes; if caught up, sleep on a cancellable delay that the `Monitor`
 * interrupts on a pub/sub doorbell; otherwise `MGET` a window of at most 50 slots, inflate and deserialize
 * each batch, skip ids already in the dedupe set, route the rest concurrently, then advance the read index.
 *
 * Contract: the read index advances only after every routed event settles, so a crash mid-batch replays that
 * batch. Deduplication is a rolling window of the last 1000 event ids per (topic, partition, group),
 * persisted as a Redis list and reloaded at startup. Events whose handler registration is missing are marked
 * processed and skipped — deliberate, so a rolling deployment does not stall.
 *
 * Note: a slot whose payload has not landed yet is retried up to 200 times (≈42 s) — this absorbs the race
 * between the producer's `INCR` and its `SETEX`, which are separate round-trips.
 *
 * Metrics: every pass records both indexes for the partition instruments; each event picked up records its
 * `neda.event.age`; each event skipped, and each slot given up on, counts in `neda.event.skipped` and
 * `neda.batch.lost`. See `docs/observability.md`.
 */
export declare class Consumer implements Disposable {
    private readonly _edaPrefix;
    private readonly _nedaClearTrackedKeysEventName;
    private readonly _nedaDistributedObserverNotifyEventName;
    private readonly _client;
    private readonly _manager;
    private readonly _logger;
    private readonly _topic;
    private readonly _partition;
    private readonly _cleanKeys;
    private readonly _flush;
    private readonly _otelMetrics;
    private _isDisposed;
    private readonly _maxTrackedSize;
    private readonly _keepTrackedSize;
    private _trackedKeysArray;
    private _trackedKeysSet;
    private _keysToTrack;
    private _consumePromise;
    private _broker;
    private _delayCanceller;
    private _lastReportTime;
    private readonly _reportIntervalMs;
    private get _writeIndexKey();
    private get _readIndexKey();
    private get _trackedKeysKey();
    private get _fullId();
    get id(): string;
    constructor(client: Redis, manager: EdaManager, topic: string, partition: number, flush?: boolean);
    registerBroker(broker: Broker): void;
    consume(): void;
    dispose(): Promise<void>;
    awaken(): void;
    private _beginConsume;
    private _attemptRoute;
    private _fetchPartitionWriteAndConsumerPartitionReadIndexes;
    private _incrementConsumerPartitionReadIndex;
    private _retrieveEvent;
    private _batchRetrieveEvents;
    private _clearAllEventTracking;
    private _track;
    private _saveTrackedKeys;
    private _purgeTrackedKeys;
    private _loadTrackedKeys;
    private _decompressEvents;
    private _removeKeys;
}
//# sourceMappingURL=consumer.d.ts.map