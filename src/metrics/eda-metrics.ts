import { given } from "@nivinjoseph/n-defensive";
import * as otelApi from "@opentelemetry/api";
import {
    ATTR_ERROR_TYPE,
    ATTR_MESSAGING_CONSUMER_GROUP_NAME,
    ATTR_MESSAGING_DESTINATION_NAME,
    ATTR_MESSAGING_DESTINATION_PARTITION_ID,
    ATTR_MESSAGING_SYSTEM,
    ERROR_TYPE_VALUE_OTHER
} from "@opentelemetry/semantic-conventions/incubating";


/** n-eda's own attributes. Namespaced, as OpenTelemetry asks of anything outside the semantic conventions. */
const eventTypeAttribute = "neda.event.type";
const outcomeAttribute = "neda.outcome";
const skipReasonAttribute = "neda.skip.reason";

/**
 * Why a consumer passed over an event without handling it, as `neda.skip.reason` on `neda.event.skipped`:
 * - `duplicate` — already in the consumer's dedupe window, as on a replay;
 * - `no_handler` — this process has no handler for its type, typically mid rolling deploy;
 * - `partition_key_error` — the manager's `partitionKeyMapper` threw on it.
 */
export type SkipReason = "duplicate" | "no_handler" | "partition_key_error";

// Bucket advice, in seconds. The SDK's default boundaries (0, 5, 10, ... 10000) are sized for milliseconds;
// in seconds nearly every handler would land in the first bucket.
// Processing extends the messaging convention's recommended set to a minute, for slow proxied handlers.
const processDurationBuckets = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10, 30, 60];
// Age reaches 10 minutes so an event held behind a key's retry ladder (~8.5 minutes) lands in a real bucket
// rather than in the overflow.
const eventAgeBuckets = [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600];


/**
 * The OpenTelemetry instruments n-eda records into, created once per manager from the meter it is given.
 *
 * Contract: `EdaManager.bootstrap()` creates the live instance from `metrics.getMeter("n-eda")`. The API
 * returns a no-op meter, permanently, when no `MeterProvider` is registered at that moment, so every
 * recording below costs next to nothing unless the host registered one first (for example via a preload).
 *
 * RULE: attributes must stay bounded. Topic, consumer group, event type, outcome, skip reason, error class, and
 * partition are each a small closed set; event ids and partition keys are not, and must never become
 * attributes — every distinct value is a new time series for the backend to store and bill.
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
export class EdaMetrics
{
    private readonly _meter: otelApi.Meter;
    private readonly _consumerGroupId: string | null;
    private readonly _processDuration: otelApi.Histogram;
    private readonly _processed: otelApi.Counter;
    private readonly _eventAge: otelApi.Histogram;
    private readonly _partitionLag: otelApi.ObservableGauge;
    private readonly _partitionLagPeak: otelApi.ObservableGauge;
    private readonly _partitionSampleAge: otelApi.ObservableGauge;
    private readonly _partitionWritten: otelApi.ObservableCounter;
    private readonly _partitionConsumed: otelApi.ObservableCounter;
    private readonly _batchesLost: otelApi.Counter;
    private readonly _eventsSkipped: otelApi.Counter;
    private readonly _eventsSent: otelApi.Counter;
    private readonly _eventsDropped: otelApi.Counter;
    // topic -> partition -> the most recent index reading, when it was taken, and the highest lag since the last
    // collection. The partition instruments read only these, never the log reporter's samples.
    private readonly _partitionReadings = new Map<string, Map<number, PartitionReadings>>();


    /**
     * @param meter - the meter to create the instruments from
     * @param consumerGroupId - stamped on every recording as `messaging.consumer.group.name`; `null` (a
     * publish-only process) omits the attribute
     */
    public constructor(meter: otelApi.Meter, consumerGroupId: string | null)
    {
        given(meter, "meter").ensureHasValue().ensureIsObject();
        this._meter = meter;

        given(consumerGroupId as string, "consumerGroupId").ensureIsString();
        this._consumerGroupId = consumerGroupId;

        this._processDuration = meter.createHistogram("neda.event.process.duration", {
            description: "Duration of one attempt to process an event, excluding the delays between retries.",
            unit: "s",
            advice: { explicitBucketBoundaries: processDurationBuckets }
        });

        this._processed = meter.createCounter("neda.event.processed", {
            description: "Events that reached a final outcome: handled, or failed after exhausting every retry.",
            unit: "{event}"
        });

        this._eventAge = meter.createHistogram("neda.event.age", {
            description: "Time between an event being published and a consumer picking it up.",
            unit: "s",
            advice: { explicitBucketBoundaries: eventAgeBuckets }
        });

        this._partitionLag = meter.createObservableGauge("neda.partition.lag", {
            description: "Publish batches this consumer group has yet to read from the partition.",
            unit: "{batch}",
            valueType: otelApi.ValueType.INT
        });

        // A gauge rather than a histogram's max: a histogram's max is the interval's peak only under delta
        // temporality, and Prometheus-compatible backends drop it altogether. A gauge reads the same either way.
        this._partitionLagPeak = meter.createObservableGauge("neda.partition.lag_peak", {
            description: "The highest lag the partition's consume loop read since the previous collection.",
            unit: "{batch}",
            valueType: otelApi.ValueType.INT
        });

        this._partitionSampleAge = meter.createObservableGauge("neda.partition.sample_age", {
            description: "Time since the partition's consume loop last read its lag. Grows while the loop is blocked.",
            unit: "s"
        });

        // Counters rather than rate gauges: the backend derives production and consumption rates from them, at any
        // window and across resets, where a precomputed rate would fix both in n-eda.
        this._partitionWritten = meter.createObservableCounter("neda.partition.written", {
            description: "Publish batches written to the partition: its write index.",
            unit: "{batch}",
            valueType: otelApi.ValueType.INT
        });

        this._partitionConsumed = meter.createObservableCounter("neda.partition.consumed", {
            description: "Publish batches this consumer group has consumed from the partition: its read index.",
            unit: "{batch}",
            valueType: otelApi.ValueType.INT
        });

        this._batchesLost = meter.createCounter("neda.batch.lost", {
            description: "Publish batches skipped because their payload was still missing after every read attempt.",
            unit: "{batch}",
            valueType: otelApi.ValueType.INT
        });

        this._eventsSkipped = meter.createCounter("neda.event.skipped", {
            description: "Events a consumer passed over without handling, by neda.skip.reason.",
            unit: "{event}",
            valueType: otelApi.ValueType.INT
        });

        this._eventsSent = meter.createCounter("neda.event.sent", {
            description: "Events the producer attempted to write to a partition; failed writes carry error.type.",
            unit: "{event}",
            valueType: otelApi.ValueType.INT
        });

        this._eventsDropped = meter.createCounter("neda.event.dropped", {
            description: "Events publish() discarded because this process has no handler for them and the topic "
                + "is not forced.",
            unit: "{event}",
            valueType: otelApi.ValueType.INT
        });
    }


    /**
     * Records a successful processing attempt: its duration, and the event's final `success` outcome.
     *
     * @param topic - the topic that delivered the event
     * @param eventType - the handled event's type name (for an observer event, the observed event's)
     * @param durationSeconds - how long the attempt took
     */
    public recordAttemptSuccess(topic: string, eventType: string, durationSeconds: number): void
    {
        try
        {
            const attributes = this._eventAttributes(topic, eventType);

            this._processDuration.record(durationSeconds, attributes);
            this._processed.add(1, { ...attributes, [outcomeAttribute]: "success" });
        }
        catch (error)
        {
            reportRecordingFailure("a successful attempt", error);
        }
    }

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
    public recordAttemptFailure(topic: string, eventType: string, durationSeconds: number, error: unknown): void
    {
        try
        {
            this._processDuration.record(durationSeconds, {
                ...this._eventAttributes(topic, eventType),
                [ATTR_ERROR_TYPE]: errorTypeOf(error)
            });
        }
        catch (recordingError)
        {
            reportRecordingFailure("a failed attempt", recordingError);
        }
    }

    /**
     * Records an event's final `failed` outcome: every retry was exhausted, and the event is dropped.
     *
     * @param topic - the topic that delivered the event
     * @param eventType - the dropped event's type name
     */
    public recordRetriesExhausted(topic: string, eventType: string): void
    {
        try
        {
            this._processed.add(1, { ...this._eventAttributes(topic, eventType), [outcomeAttribute]: "failed" });
        }
        catch (error)
        {
            reportRecordingFailure("an exhausted retry ladder", error);
        }
    }

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
    public recordEventAge(topic: string, eventType: string, ageSeconds: number): void
    {
        try
        {
            this._eventAge.record(Math.max(0, ageSeconds), this._eventAttributes(topic, eventType));
        }
        catch (error)
        {
            reportRecordingFailure("an event age", error);
        }
    }

    /**
     * Records a publish batch the consumer gave up on: its slot was allocated but its payload never appeared,
     * so its events are lost.
     *
     * @param topic - the partition's topic
     * @param partition - the partition the batch was in
     */
    public recordBatchLost(topic: string, partition: number): void
    {
        try
        {
            this._batchesLost.add(1, this._partitionAttributes(topic, partition));
        }
        catch (error)
        {
            reportRecordingFailure("a lost batch", error);
        }
    }

    /**
     * Records an event the consumer passed over without handling it. See {@link SkipReason}.
     *
     * @param topic - the topic that delivered the event
     * @param eventType - the event's type name (for an observer event, the observed event's)
     * @param reason - why it was skipped
     */
    public recordEventSkipped(topic: string, eventType: string, reason: SkipReason): void
    {
        try
        {
            this._eventsSkipped.add(1, { ...this._eventAttributes(topic, eventType), [skipReasonAttribute]: reason });
        }
        catch (error)
        {
            reportRecordingFailure("a skipped event", error);
        }
    }

    /**
     * Records the events of one publish batch, written to their partition.
     *
     * Note: no consumer group, unlike the consumer-side instruments — sending is not done on behalf of one.
     *
     * @param topic - the topic written to
     * @param eventTypes - each event's type name (for an observer notification, the observed event's)
     */
    public recordEventsSent(topic: string, eventTypes: ReadonlyArray<string>): void
    {
        try
        {
            this._addPerEventType(this._eventsSent, topic, eventTypes, {});
        }
        catch (error)
        {
            reportRecordingFailure("sent events", error);
        }
    }

    /**
     * Records the events of one publish batch whose write failed after its retries, tagged with the error's
     * class as `error.type`.
     *
     * @param topic - the topic written to
     * @param eventTypes - each event's type name
     * @param error - what the write threw
     */
    public recordEventsSendFailed(topic: string, eventTypes: ReadonlyArray<string>, error: unknown): void
    {
        try
        {
            this._addPerEventType(this._eventsSent, topic, eventTypes, { [ATTR_ERROR_TYPE]: errorTypeOf(error) });
        }
        catch (recordingError)
        {
            reportRecordingFailure("failed sends", recordingError);
        }
    }

    /**
     * Records an event `publish()` discarded unsent: this process has no handler for its type, and the topic
     * was not configured with `forcePublish()`.
     *
     * @param topic - the topic it was published to
     * @param eventType - the event's type name
     */
    public recordEventDropped(topic: string, eventType: string): void
    {
        try
        {
            this._eventsDropped.add(1, this._producerAttributes(topic, eventType));
        }
        catch (error)
        {
            reportRecordingFailure("a dropped event", error);
        }
    }

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
    public recordPartitionReading(topic: string, partition: number, writeIndex: number, readIndex: number): void
    {
        // Negative after a Redis flush resets the write index below the read index; 0, as `Broker.report` has it.
        const lag = Math.max(0, writeIndex - readIndex);
        const readAt = Date.now();

        let partitions = this._partitionReadings.get(topic);
        if (partitions == null)
        {
            partitions = new Map<number, PartitionReadings>();
            this._partitionReadings.set(topic, partitions);
        }

        const readings = partitions.get(partition);
        if (readings == null)
            partitions.set(partition, { writeIndex, readIndex, lag, readAt, peakLag: lag });
        else
        {
            readings.writeIndex = writeIndex;
            readings.readIndex = readIndex;
            readings.lag = lag;
            readings.readAt = readAt;
            readings.peakLag = Math.max(readings.peakLag, lag);
        }
    }

    /**
     * Reports every partition's readings through the partition gauges and counters each time the metrics SDK
     * collects. A partition appears once its consume loop has taken its first reading.
     *
     * Each collection resets a partition's peak to its latest reading — not to 0, so a quiet interval reports
     * the lag it sat at rather than a false 0 — which is why only one metrics reader may collect.
     *
     * @returns a function that unregisters the callback. Call it on disposal: the meter outlives the consumers.
     */
    public observePartitions(): () => void
    {
        const observables = [
            this._partitionLag, this._partitionLagPeak, this._partitionSampleAge,
            this._partitionWritten, this._partitionConsumed
        ];
        // Errors thrown in here are caught by the SDK's collection, so the callback itself needs no guard.
        const callback: otelApi.BatchObservableCallback = (observer) =>
        {
            const now = Date.now();

            this._partitionReadings.forEach((partitions, topic) => partitions.forEach((readings, partition) =>
            {
                const attributes = this._partitionAttributes(topic, partition);

                observer.observe(this._partitionLag, readings.lag, attributes);
                observer.observe(this._partitionLagPeak, readings.peakLag, attributes);
                // Clamped: a backwards clock step would otherwise report a frozen reading as fresher than a
                // live one.
                observer.observe(this._partitionSampleAge, Math.max(0, now - readings.readAt) / 1000, attributes);
                // An index that drops (a Redis flush) is a counter reset, which the SDK and backends handle.
                observer.observe(this._partitionWritten, readings.writeIndex, attributes);
                observer.observe(this._partitionConsumed, readings.readIndex, attributes);

                readings.peakLag = readings.lag;
            }));
        };

        this._meter.addBatchObservableCallback(callback, observables);

        return () => this._meter.removeBatchObservableCallback(callback, observables);
    }

    private _addPerEventType(counter: otelApi.Counter, topic: string, eventTypes: ReadonlyArray<string>,
        attributes: otelApi.Attributes): void
    {
        const counts = new Map<string, number>();
        eventTypes.forEach(t => counts.set(t, (counts.get(t) ?? 0) + 1));

        counts.forEach((count, eventType) =>
            counter.add(count, {
                ...this._producerAttributes(topic, eventType),
                ...attributes
            }));
    }

    private _producerAttributes(topic: string, eventType: string): otelApi.Attributes
    {
        return {
            [ATTR_MESSAGING_SYSTEM]: "n-eda",
            [ATTR_MESSAGING_DESTINATION_NAME]: topic,
            [eventTypeAttribute]: eventType
        };
    }

    private _eventAttributes(topic: string, eventType: string): otelApi.Attributes
    {
        return { ...this._baseAttributes(topic), [eventTypeAttribute]: eventType };
    }

    private _partitionAttributes(topic: string, partition: number): otelApi.Attributes
    {
        return { ...this._baseAttributes(topic), [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: partition.toString() };
    }

    private _baseAttributes(topic: string): otelApi.Attributes
    {
        // Topic only, not the spans' `topic+++partition`: per-event series multiply with every attribute, so
        // only the per-partition instruments add the partition.
        const attributes: otelApi.Attributes = {
            [ATTR_MESSAGING_SYSTEM]: "n-eda",
            [ATTR_MESSAGING_DESTINATION_NAME]: topic
        };
        if (this._consumerGroupId != null)
            attributes[ATTR_MESSAGING_CONSUMER_GROUP_NAME] = this._consumerGroupId;

        return attributes;
    }
}


interface PartitionReadings
{
    writeIndex: number;
    readIndex: number;
    /** `writeIndex - readIndex`, clamped at 0. */
    lag: number;
    /** Epoch ms the latest reading was taken. */
    readAt: number;
    /** The highest lag since the previous collection. */
    peakLag: number;
}


/**
 * Routes a recording failure to the OpenTelemetry diagnostic logger — the channel the SDK uses for its own
 * recording problems, silent unless the host set one up — so that it never reaches the delivery path.
 */
function reportRecordingFailure(recording: string, error: unknown): void
{
    otelApi.diag.warn(`n-eda: failed to record metrics for ${recording}`, error);
}

/**
 * The `error.type` for a thrown value: its class name, which is bounded and what failures are grouped by.
 * A thrown non-object has no class, and reports the convention's `_OTHER`, as does one whose class cannot be
 * read.
 */
function errorTypeOf(error: unknown): string
{
    if (error != null && typeof error === "object")
    {
        let name: unknown;
        try
        {
            // Typed loosely on purpose: an object made with Object.create(null) has no constructor at all.
            name = (error as { constructor?: { name?: unknown; }; }).constructor?.name;
        }
        catch
        {
            // A revoked Proxy, or a throwing `constructor` getter. The attempt's duration still counts.
        }

        if (typeof name === "string" && name.length > 0)
            return name;
    }

    return ERROR_TYPE_VALUE_OTHER;
}
