import * as otelApi from "@opentelemetry/api";
/**
 * Why a consumer passed over an event without handling it, as `neda.skip.reason` on `neda.event.skipped`:
 * - `duplicate` — already in the consumer's dedupe window, as on a replay;
 * - `no_handler` — this process has no handler for its type, typically mid rolling deploy;
 * - `partition_key_error` — the manager's `partitionKeyMapper` threw on it.
 */
export type SkipReason = "duplicate" | "no_handler" | "partition_key_error";
/**
 * The OpenTelemetry instruments n-eda records into, created once per manager from the meter it is given.
 *
 * Contract: `EdaManager.bootstrap()` creates the live instance from `metrics.getMeter("n-eda")`. The API
 * returns a no-op meter, permanently, when no `MeterProvider` is registered at that moment, so every
 * recording below costs next to nothing unless the host registered one first (for example via a preload).
 *
 * RULE: attributes must stay bounded. Topic, consumer group, event type, outcome, skip reason, error class, and
 * partition are each a small closed set; event ids and partition keys are not, and must never become attributes — every
 * distinct value is a new time series for the backend to store and bill.
 *
 * RULE: recording never throws. Every caller sits on a delivery path — inside the consume loop, the route, or
 * the retry ladder — where a thrown error would drop an event, stall a partition, or re-run a handler that
 * already succeeded. The SDK guards none of its record path, so a host's custom provider or attributes
 * processor could throw; each recording method catches that and reports it through `diag` instead.
 *
 * RULE: assumes a single metrics reader. `neda.partition.lag_peak` resets at every collection, so a second
 * reader would split the peaks between them, each seeing only those since the other's last collection.
 *
 * Note: internal; not exported from the barrel. The metric contract is documented in `docs/observability.md`.
 */
export declare class EdaMetrics {
    private readonly _meter;
    private readonly _consumerGroupId;
    private readonly _processDuration;
    private readonly _processed;
    private readonly _eventAge;
    private readonly _partitionLag;
    private readonly _partitionLagPeak;
    private readonly _partitionSampleAge;
    private readonly _partitionWritten;
    private readonly _partitionConsumed;
    private readonly _batchesLost;
    private readonly _eventsSkipped;
    private readonly _messagesSent;
    private readonly _eventsDropped;
    private readonly _partitionReadings;
    /**
     * @param meter - the meter to create the instruments from
     * @param consumerGroupId - stamped on every recording as `messaging.consumer.group.name`; `null` (a
     * publish-only process) omits the attribute
     */
    constructor(meter: otelApi.Meter, consumerGroupId: string | null);
    /**
     * Records a successful processing attempt: its duration, and the event's final `success` outcome.
     *
     * @param topic - the topic that delivered the event
     * @param eventType - the handled event's type name (for an observer event, the observed event's)
     * @param durationSeconds - how long the attempt took
     */
    recordAttemptSuccess(topic: string, eventType: string, durationSeconds: number): void;
    /**
     * Records a failed processing attempt: its duration, tagged with the error's class as `error.type`.
     *
     * Note: not a final outcome — the attempt may be retried. See {@link EdaMetrics.recordRetriesExhausted}.
     *
     * @param topic - the topic that delivered the event
     * @param eventType - the handled event's type name
     * @param durationSeconds - how long the attempt took
     * @param error - what the attempt threw
     */
    recordAttemptFailure(topic: string, eventType: string, durationSeconds: number, error: unknown): void;
    /**
     * Records an event's final `failed` outcome: every retry was exhausted, and the event is dropped.
     *
     * @param topic - the topic that delivered the event
     * @param eventType - the dropped event's type name
     */
    recordRetriesExhausted(topic: string, eventType: string): void;
    /**
     * Records how long an event waited between publish and consumer pickup.
     *
     * Note: once per delivery, not once per event. An event replayed after a shutdown interrupted it, or after
     * a crash, is picked up, and measured, again.
     *
     * @param topic - the topic that delivered the event
     * @param eventType - the event's type name
     * @param ageSeconds - pickup time minus publish time. A negative age (the producer's clock ahead of the
     * consumer's) is recorded as 0: histograms reject negative values outright.
     */
    recordEventAge(topic: string, eventType: string, ageSeconds: number): void;
    /**
     * Records a publish batch the consumer gave up on: its slot was allocated but its payload never appeared,
     * so its events are lost.
     *
     * @param topic - the partition's topic
     * @param partition - the partition the batch was in
     */
    recordBatchLost(topic: string, partition: number): void;
    /**
     * Records an event the consumer passed over without handling it. See {@link SkipReason}.
     *
     * @param topic - the topic that delivered the event
     * @param eventType - the event's type name (for an observer event, the observed event's)
     * @param reason - why it was skipped
     */
    recordEventSkipped(topic: string, eventType: string, reason: SkipReason): void;
    /**
     * Records the events of one publish batch, written to their partition.
     *
     * Note: no consumer group, unlike the consumer-side instruments — sending is not done on behalf of one.
     *
     * @param topic - the topic written to
     * @param eventTypes - each event's type name (for an observer notification, the observed event's)
     */
    recordEventsSent(topic: string, eventTypes: ReadonlyArray<string>): void;
    /**
     * Records the events of one publish batch whose write failed after its retries, tagged with the error's
     * class as `error.type`.
     *
     * @param topic - the topic written to
     * @param eventTypes - each event's type name
     * @param error - what the write threw
     */
    recordEventsSendFailed(topic: string, eventTypes: ReadonlyArray<string>, error: unknown): void;
    /**
     * Records an event `publish()` discarded unsent: this process has no handler for its type, and the topic
     * was not configured with `forcePublish()`.
     *
     * @param topic - the topic it was published to
     * @param eventType - the event's type name
     */
    recordEventDropped(topic: string, eventType: string): void;
    /**
     * Records one reading of a partition's indexes, as taken on every pass of its consume loop. Every partition
     * instrument reads from these:
     * - `neda.partition.written` and `neda.partition.consumed` are the latest write and read index;
     * - `neda.partition.lag` is the latest lag, and `neda.partition.lag_peak` the highest since the previous
     * collection — what the lag gauge cannot show, a spike that subsides between two collections;
     * - `neda.partition.sample_age` is the time since the latest reading.
     *
     * Note: in-memory only — the instruments read these out at collection, so this never calls into the SDK.
     *
     * @param topic - the partition's topic
     * @param partition - the partition read
     * @param writeIndex - the partition's write index: batches ever written to it
     * @param readIndex - this consumer group's read index: batches it has consumed
     */
    recordPartitionReading(topic: string, partition: number, writeIndex: number, readIndex: number): void;
    /**
     * Reports every partition's readings through the partition gauges and counters each time the metrics SDK
     * collects. A partition appears once its consume loop has taken its first reading.
     *
     * Each collection resets a partition's peak to its latest reading — not to 0, so a quiet interval reports
     * the lag it sat at rather than a false 0 — which is why only one metrics reader may collect.
     *
     * @returns a function that unregisters the callback. Call it on disposal: the meter outlives the consumers.
     */
    observePartitions(): () => void;
    private _addPerEventType;
    private _producerAttributes;
    private _eventAttributes;
    private _partitionAttributes;
    private _baseAttributes;
}
//# sourceMappingURL=eda-metrics.d.ts.map