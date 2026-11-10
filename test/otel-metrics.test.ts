import { ApplicationException } from "@nivinjoseph/n-exception";
import { ComponentInstaller, Registry } from "@nivinjoseph/n-ject";
import { Deferred, Duration } from "@nivinjoseph/n-util";
import * as otelApi from "@opentelemetry/api";
import { AggregationTemporality, DataPointType, Histogram } from "@opentelemetry/sdk-metrics";
import assert from "node:assert";
import test, { after, before, describe } from "node:test";
import { EdaManager, EventBus, EventSubMgr, Topic } from "../src/index.js";
import { EdaMetrics } from "../src/metrics/eda-metrics.js";
import { Processor } from "../src/redis-implementation/processor.js";
import { WorkItem } from "../src/redis-implementation/scheduler.js";
import { TestMetricsPipeline } from "./utils/otel-test-utils.js";
import { silentLogger } from "./utils/test-stubs.js";


/**
 * The OpenTelemetry instruments, checked against a real metrics SDK. None of this needs Redis: `EdaMetrics` is
 * driven directly, and the processor runs against a manager bootstrapped with stub bus and subscription
 * manager instances.
 *
 * Exhausting the retry ladder takes ~8.5 minutes, so the `failed` outcome is covered through
 * `recordRetriesExhausted` directly rather than through a processor.
 */

// Set before any manager bootstraps: bootstrap() takes its meter from the global provider exactly once.
const globalPipeline = new TestMetricsPipeline();
otelApi.metrics.setGlobalMeterProvider(globalPipeline.provider);

/** Fails its first `failures` attempts with an `ApplicationException`, then succeeds. */
class ScriptedProcessor extends Processor
{
    private _failuresLeft: number;


    public constructor(manager: EdaManager, failures: number)
    {
        super(manager);
        this._failuresLeft = failures;
    }


    protected processEvent(_workItem: WorkItem): Promise<void>
    {
        if (this._failuresLeft > 0)
        {
            this._failuresLeft--;
            return Promise.reject(new ApplicationException("scripted failure"));
        }

        return Promise.resolve();
    }
}

function createWorkItem(topic: string): WorkItem
{
    return {
        topic,
        partition: 0,
        eventName: "OrderPlaced",
        eventRegistration: { eventTypeName: "OrderPlaced" },
        event: { id: "evt-1", name: "OrderPlaced", partitionKey: "order-1", serialize: () => ({ id: "evt-1" }) },
        span: otelApi.trace.wrapSpanContext(otelApi.INVALID_SPAN_CONTEXT),
        deferred: new Deferred<void>()
    } as unknown as WorkItem;
}

async function runToCompletion(processor: Processor, workItem: WorkItem): Promise<void>
{
    processor.availability.subscribe(() => { /* not needed: one work item per processor */ });
    processor.doneProcessing.subscribe(() => { /* not needed: completion is the deferred */ });

    processor.process(workItem);
    await workItem.deferred.promise;
}


await describe("OpenTelemetry metrics tests", async () =>
{
    after(async () =>
    {
        await globalPipeline.shutdown();
    });


    await describe("instruments", async () =>
    {
        const sharedAttributes = {
            "messaging.system": "n-eda",
            "messaging.destination.name": "orders",
            "messaging.consumer.group.name": "billing",
            "neda.event.type": "OrderPlaced"
        };

        await test("a successful attempt records its duration and a success outcome", async () =>
        {
            const pipeline = new TestMetricsPipeline();
            new EdaMetrics(pipeline.meter(), "billing").recordAttemptSuccess("orders", "OrderPlaced", 0.2);

            const collected = await pipeline.collect();

            assert.strictEqual(collected.find("neda.event.process.duration")!.descriptor.unit, "s");

            const points = collected.points<Histogram>("neda.event.process.duration");
            assert.strictEqual(points.length, 1);

            const [point] = points;
            assert.deepStrictEqual(point.attributes, sharedAttributes);
            assert.strictEqual(point.value.count, 1);
            assert.strictEqual(point.value.sum, 0.2);
            // Bucket advice reached the SDK — the defaults are millisecond-sized and would misfile seconds.
            assert.strictEqual(point.value.buckets.boundaries[0], 0.005);
            assert.strictEqual(point.value.buckets.boundaries.takeLast(), 60);

            const processed = collected.find("neda.event.processed")!;
            assert.strictEqual(processed.descriptor.unit, "{event}");
            assert.deepStrictEqual(processed.dataPoints.map(t => [t.attributes, t.value]),
                [[{ ...sharedAttributes, "neda.outcome": "success" }, 1]]);

            await pipeline.shutdown();
        });

        await test("a failed attempt records error.type and no outcome", async () =>
        {
            const pipeline = new TestMetricsPipeline();
            const edaMetrics = new EdaMetrics(pipeline.meter(), "billing");
            edaMetrics.recordAttemptFailure("orders", "OrderPlaced", 0.1, new ApplicationException("boom"));
            edaMetrics.recordAttemptFailure("orders", "OrderPlaced", 0.1, "a thrown string");

            const collected = await pipeline.collect();

            assert.strictEqual(collected.histogramCount("neda.event.process.duration",
                { "error.type": "ApplicationException" }), 1);
            // A thrown non-object has no class name to report.
            assert.strictEqual(collected.histogramCount("neda.event.process.duration", { "error.type": "_OTHER" }), 1);
            // An attempt failure may still be retried, so it is not a final outcome.
            assert.strictEqual(collected.count("neda.event.processed"), 0);

            await pipeline.shutdown();
        });

        await test("exhausted retries count as a failed outcome", async () =>
        {
            const pipeline = new TestMetricsPipeline();
            new EdaMetrics(pipeline.meter(), "billing").recordRetriesExhausted("orders", "OrderPlaced");

            const collected = await pipeline.collect();

            assert.deepStrictEqual(collected.points("neda.event.processed").map(t => [t.attributes, t.value]),
                [[{ ...sharedAttributes, "neda.outcome": "failed" }, 1]]);
            assert.strictEqual(collected.find("neda.event.process.duration")?.dataPoints.length ?? 0, 0);

            await pipeline.shutdown();
        });

        await test("event age is in seconds and clamps a negative age to 0", async () =>
        {
            const pipeline = new TestMetricsPipeline();
            const edaMetrics = new EdaMetrics(pipeline.meter(), "billing");
            edaMetrics.recordEventAge("orders", "OrderPlaced", 2.5);
            // The producer's clock ahead of the consumer's. The SDK drops negative histogram values outright,
            // which would hide the event rather than report it as fresh.
            edaMetrics.recordEventAge("orders", "OrderPlaced", -0.4);

            const collected = await pipeline.collect();

            const age = collected.find("neda.event.age")!;
            assert.strictEqual(age.descriptor.unit, "s");

            const [point] = collected.points<Histogram>("neda.event.age");
            assert.deepStrictEqual(point.attributes, sharedAttributes);
            assert.strictEqual(point.value.count, 2);
            assert.strictEqual(point.value.sum, 2.5);
            assert.strictEqual(point.value.min, 0);
            assert.strictEqual(point.value.buckets.boundaries.takeLast(), 600);

            await pipeline.shutdown();
        });

        await test("without a consumer group the attribute is omitted, not empty", async () =>
        {
            const pipeline = new TestMetricsPipeline();
            new EdaMetrics(pipeline.meter(), null).recordEventAge("orders", "OrderPlaced", 1);

            const [point] = (await pipeline.collect()).points("neda.event.age");
            assert.ok(!("messaging.consumer.group.name" in point.attributes));

            await pipeline.shutdown();
        });

        await test("a meter that throws never reaches the caller", () =>
        {
            // Every caller sits on a delivery path, where a throw would drop an event or stall a partition.
            const fail = (): never => { throw new Error("broken meter"); };
            const instrument = { record: fail, add: fail };
            const meter = {
                createHistogram: () => instrument,
                createCounter: () => instrument,
                createObservableGauge: () => ({}),
                createObservableCounter: () => ({})
            } as unknown as otelApi.Meter;

            const edaMetrics = new EdaMetrics(meter, "billing");

            assert.doesNotThrow(() => edaMetrics.recordAttemptSuccess("orders", "OrderPlaced", 0.1));
            assert.doesNotThrow(() => edaMetrics.recordAttemptFailure("orders", "OrderPlaced", 0.1, new Error()));
            assert.doesNotThrow(() => edaMetrics.recordRetriesExhausted("orders", "OrderPlaced"));
            assert.doesNotThrow(() => edaMetrics.recordEventAge("orders", "OrderPlaced", 1));
            assert.doesNotThrow(() => edaMetrics.recordPartitionReading("orders", 0, 105, 100));
            assert.doesNotThrow(() => edaMetrics.recordBatchLost("orders", 0));
            assert.doesNotThrow(() => edaMetrics.recordEventSkipped("orders", "OrderPlaced", "duplicate"));
            assert.doesNotThrow(() => edaMetrics.recordEventsSent("orders", ["OrderPlaced"]));
            assert.doesNotThrow(() => edaMetrics.recordEventsSendFailed("orders", ["OrderPlaced"], new Error()));
            assert.doesNotThrow(() => edaMetrics.recordEventDropped("orders", "OrderPlaced"));
        });

        await test("an error whose class cannot be read still records its attempt", async () =>
        {
            const pipeline = new TestMetricsPipeline();
            const unreadable = Object.defineProperty({}, "constructor", {
                get: (): never => { throw new Error("no class for you"); }
            });

            new EdaMetrics(pipeline.meter(), "billing").recordAttemptFailure("orders", "OrderPlaced", 0.1, unreadable);

            const collected = await pipeline.collect();
            assert.strictEqual(collected.histogramCount("neda.event.process.duration", { "error.type": "_OTHER" }), 1);

            await pipeline.shutdown();
        });
    });

    await describe("losses, skips, and sends", async () =>
    {
        const ordersEvent = {
            "messaging.system": "n-eda",
            "messaging.destination.name": "orders",
            "messaging.consumer.group.name": "billing",
            "neda.event.type": "OrderPlaced"
        };
        const ordersSend = {
            "messaging.system": "n-eda",
            "messaging.destination.name": "orders",
            "neda.event.type": "OrderPlaced"
        };

        await test("a lost batch is counted against its partition", async () =>
        {
            const pipeline = new TestMetricsPipeline();
            new EdaMetrics(pipeline.meter(), "billing").recordBatchLost("orders", 3);

            const collected = await pipeline.collect();
            assert.strictEqual(collected.find("neda.batch.lost")!.descriptor.unit, "{batch}");
            assert.deepStrictEqual(collected.points("neda.batch.lost").map(p => [p.attributes, p.value]), [[{
                "messaging.system": "n-eda",
                "messaging.destination.name": "orders",
                "messaging.consumer.group.name": "billing",
                "messaging.destination.partition.id": "3"
            }, 1]]);

            await pipeline.shutdown();
        });

        await test("skipped events are counted by reason", async () =>
        {
            const pipeline = new TestMetricsPipeline();
            const edaMetrics = new EdaMetrics(pipeline.meter(), "billing");
            edaMetrics.recordEventSkipped("orders", "OrderPlaced", "duplicate");
            edaMetrics.recordEventSkipped("orders", "OrderPlaced", "duplicate");
            edaMetrics.recordEventSkipped("orders", "OrderPlaced", "no_handler");
            edaMetrics.recordEventSkipped("orders", "OrderPlaced", "partition_key_error");

            const collected = await pipeline.collect();
            assert.strictEqual(collected.find("neda.event.skipped")!.descriptor.unit, "{event}");
            assert.deepStrictEqual(
                collected.points("neda.event.skipped", ordersEvent)
                    .map(p => [p.attributes["neda.skip.reason"], p.value])
                    .orderBy(p => `${p[0]}`),
                [["duplicate", 2], ["no_handler", 1], ["partition_key_error", 1]]);

            await pipeline.shutdown();
        });

        await test("sent events are counted per type, without a consumer group, and failures carry error.type",
            async () =>
            {
                const pipeline = new TestMetricsPipeline();
                // A process that also consumes: its group must not leak onto what it sends.
                const edaMetrics = new EdaMetrics(pipeline.meter(), "billing");
                edaMetrics.recordEventsSent("orders", ["OrderPlaced", "OrderShipped", "OrderPlaced"]);
                edaMetrics.recordEventsSendFailed("orders", ["OrderPlaced"], new ApplicationException("down"));

                const collected = await pipeline.collect();
                assert.strictEqual(collected.find("neda.event.sent")!.descriptor.unit, "{event}");

                const sent = collected.points("neda.event.sent");
                assert.deepStrictEqual(
                    sent.map(p => [p.attributes["neda.event.type"], p.attributes["error.type"] ?? null, p.value])
                        .orderBy(p => `${p[0]}-${p[1]}`),
                    [["OrderPlaced", "ApplicationException", 1], ["OrderPlaced", null, 2], ["OrderShipped", null, 1]]);
                sent.forEach(p => assert.ok(!("messaging.consumer.group.name" in p.attributes)));
                assert.deepStrictEqual(
                    collected.points("neda.event.sent", ordersSend)
                        .find(p => p.attributes["error.type"] == null)!.attributes,
                    ordersSend);

                await pipeline.shutdown();
            });

        await test("events dropped at publish are counted, without a consumer group", async () =>
        {
            const pipeline = new TestMetricsPipeline();
            new EdaMetrics(pipeline.meter(), "billing").recordEventDropped("orders", "OrderPlaced");

            const collected = await pipeline.collect();
            assert.strictEqual(collected.find("neda.event.dropped")!.descriptor.unit, "{event}");
            assert.deepStrictEqual(collected.points("neda.event.dropped").map(p => [p.attributes, p.value]), [[{
                "messaging.system": "n-eda",
                "messaging.destination.name": "orders",
                "neda.event.type": "OrderPlaced"
            }, 1]]);

            await pipeline.shutdown();
        });
    });

    await describe("partition gauges", async () =>
    {
        const ordersZero = {
            "messaging.system": "n-eda",
            "messaging.destination.name": "orders",
            "messaging.destination.partition.id": "0",
            "messaging.consumer.group.name": "billing"
        };

        await test("report each partition's latest lag reading, and how long ago it was taken", async (t) =>
        {
            const pipeline = new TestMetricsPipeline();
            const edaMetrics = new EdaMetrics(pipeline.meter(), "billing");
            const unobserve = edaMetrics.observePartitions();

            const clock = t.mock.method(Date, "now", () => 1_000_000);
            edaMetrics.recordPartitionReading("orders", 0, 100, 0);
            edaMetrics.recordPartitionReading("orders", 0, 100, 60);
            edaMetrics.recordPartitionReading("orders", 1, 10, 10);
            edaMetrics.recordPartitionReading("shipments", 0, 7, 2);
            // The reading stops refreshing, as it does while a consume loop is blocked.
            clock.mock.mockImplementation(() => 1_005_000);

            const collected = await pipeline.collect();

            assert.strictEqual(collected.find("neda.partition.lag")!.descriptor.unit, "{batch}");
            assert.deepStrictEqual(
                collected.points("neda.partition.lag")
                    .map(p => [p.attributes["messaging.destination.name"],
                        p.attributes["messaging.destination.partition.id"], p.value])
                    .orderBy(p => `${p[0]}-${p[1]}`),
                [["orders", "0", 40], ["orders", "1", 0], ["shipments", "0", 5]]);
            assert.deepStrictEqual(collected.points("neda.partition.lag", ordersZero)[0].attributes, ordersZero);

            assert.strictEqual(collected.find("neda.partition.sample_age")!.descriptor.unit, "s");
            assert.deepStrictEqual(collected.points("neda.partition.sample_age", ordersZero).map(p => p.value), [5]);

            unobserve();
            await pipeline.shutdown();
        });

        await test("the peak gauge keeps a spike the lag gauge would miss", async () =>
        {
            // Cumulative, as n-strument exports: unlike a histogram's max, the peak gauge is per interval either way.
            const pipeline = new TestMetricsPipeline();
            const edaMetrics = new EdaMetrics(pipeline.meter(), "billing");
            const unobserve = edaMetrics.observePartitions();
            const peaks = async (): Promise<Array<number>> =>
                (await pipeline.collect()).points("neda.partition.lag_peak", ordersZero).map(p => p.value);

            // A spike that has drained again by the time the interval is collected.
            edaMetrics.recordPartitionReading("orders", 0, 100, 100);
            edaMetrics.recordPartitionReading("orders", 0, 220, 100);
            edaMetrics.recordPartitionReading("orders", 0, 223, 220);

            const first = await pipeline.collect();
            assert.strictEqual(first.find("neda.partition.lag_peak")!.descriptor.unit, "{batch}");
            assert.deepStrictEqual(first.points("neda.partition.lag", ordersZero).map(p => p.value), [3]);

            const [spike] = first.points("neda.partition.lag_peak", ordersZero);
            assert.strictEqual(spike.value, 120);
            assert.deepStrictEqual(spike.attributes, ordersZero);

            // The next interval starts over from the latest reading: a quiet one reports the lag it sat at, not 0.
            assert.deepStrictEqual(await peaks(), [3]);

            edaMetrics.recordPartitionReading("orders", 0, 230, 223);
            edaMetrics.recordPartitionReading("orders", 0, 231, 230);
            assert.deepStrictEqual(await peaks(), [7]);
            assert.deepStrictEqual(await peaks(), [1]);

            // A Redis flush resets the write index below the read index.
            edaMetrics.recordPartitionReading("orders", 1, 10, 50);
            const partitionOne = { ...ordersZero, "messaging.destination.partition.id": "1" };
            const flushed = await pipeline.collect();
            assert.deepStrictEqual(flushed.points("neda.partition.lag", partitionOne).map(p => p.value), [0]);
            assert.deepStrictEqual(flushed.points("neda.partition.lag_peak", partitionOne).map(p => p.value), [0]);

            unobserve();
            await pipeline.shutdown();
        });

        await test("the counters carry the write and read index, for the backend to rate", async () =>
        {
            // Cumulative, as n-strument exports: each point is the index itself.
            const pipeline = new TestMetricsPipeline();
            const edaMetrics = new EdaMetrics(pipeline.meter(), "billing");
            const unobserve = edaMetrics.observePartitions();
            const indexes = async (): Promise<Array<Array<number>>> =>
            {
                const collected = await pipeline.collect();
                return ["neda.partition.written", "neda.partition.consumed"]
                    .map(name => collected.points(name, ordersZero).map(p => p.value));
            };

            edaMetrics.recordPartitionReading("orders", 0, 100, 60);
            const first = await pipeline.collect();

            for (const name of ["neda.partition.written", "neda.partition.consumed"])
            {
                const metric = first.find(name)!;
                assert.strictEqual(metric.descriptor.unit, "{batch}");
                // A monotonic sum: what backends treat as a counter, and rate.
                assert.ok(metric.dataPointType === DataPointType.SUM && metric.isMonotonic,
                    `${name} should be a counter`);
                assert.deepStrictEqual(first.points(name, ordersZero)[0].attributes, ordersZero);
            }
            assert.deepStrictEqual(first.points("neda.partition.written", ordersZero).map(p => p.value), [100]);
            assert.deepStrictEqual(first.points("neda.partition.consumed", ordersZero).map(p => p.value), [60]);

            // 30 batches written and 20 consumed in between: what rate() turns into the two rates.
            edaMetrics.recordPartitionReading("orders", 0, 130, 80);
            assert.deepStrictEqual(await indexes(), [[130], [80]]);

            // A Redis flush drops both indexes: a counter reset, carried as the new value, not a negative one.
            edaMetrics.recordPartitionReading("orders", 0, 5, 0);
            assert.deepStrictEqual(await indexes(), [[5], [0]]);

            unobserve();
            await pipeline.shutdown();
        });

        await test("a partition appears once its consume loop has read it", async () =>
        {
            const pipeline = new TestMetricsPipeline();
            const edaMetrics = new EdaMetrics(pipeline.meter(), "billing");
            const unobserve = edaMetrics.observePartitions();

            // Absent, not zero, until the first reading.
            assert.strictEqual((await pipeline.collect()).points("neda.partition.lag").length, 0);

            edaMetrics.recordPartitionReading("orders", 0, 100, 60);
            assert.deepStrictEqual((await pipeline.collect()).points("neda.partition.lag").map(p => p.value), [40]);

            unobserve();
            await pipeline.shutdown();
        });

        await test("stop reporting once unregistered", async () =>
        {
            // Delta: a cumulative reader would keep re-exporting the gauge's last value after removal.
            const pipeline = new TestMetricsPipeline(AggregationTemporality.DELTA);
            const edaMetrics = new EdaMetrics(pipeline.meter(), "billing");
            edaMetrics.recordPartitionReading("orders", 0, 100, 60);

            const unobserve = edaMetrics.observePartitions();
            assert.strictEqual((await pipeline.collect()).points("neda.partition.lag").length, 1);

            unobserve();
            edaMetrics.recordPartitionReading("orders", 0, 200, 60);

            assert.strictEqual((await pipeline.collect()).points("neda.partition.lag").length, 0);

            await pipeline.shutdown();
        });
    });

    await describe("processor", async () =>
    {
        let manager: EdaManager;

        before(async () =>
        {
            const installer: ComponentInstaller = {
                install: (registry: Registry) =>
                {
                    registry.registerInstance("Logger", silentLogger);
                    return Promise.resolve();
                }
            };
            const eventBus = {
                initialize: () => { /* stub */ },
                publish: () => Promise.resolve(),
                subscribeToObservables: () => Promise.resolve(),
                unsubscribeFromObservables: () => Promise.resolve(),
                dispose: () => Promise.resolve()
            } as unknown as EventBus;
            const eventSubMgr = {
                initialize: () => { /* stub */ },
                consume: () => Promise.resolve(),
                dispose: () => Promise.resolve()
            } as unknown as EventSubMgr;

            manager = new EdaManager()
                .useInstaller(installer)
                .registerTopics(new Topic("processor-metrics", Duration.fromMinutes(10), 1))
                .registerEventBus(eventBus)
                .registerEventSubscriptionManager(eventSubMgr, "processor-group");

            await manager.bootstrap();
        });

        after(async () =>
        {
            await manager.dispose();
        });


        await test("a successful attempt is timed and counted once", async () =>
        {
            const processor = new ScriptedProcessor(manager, 0);
            await runToCompletion(processor, createWorkItem("processor-success"));
            await processor.dispose();

            const topic = { "messaging.destination.name": "processor-success" };
            const collected = await globalPipeline.collect();

            const [duration] = collected.points<Histogram>("neda.event.process.duration", topic);
            assert.strictEqual(duration.value.count, 1);
            assert.strictEqual(duration.attributes["error.type"], undefined);
            assert.strictEqual(duration.attributes["neda.event.type"], "OrderPlaced");
            assert.strictEqual(duration.attributes["messaging.consumer.group.name"], "processor-group");

            assert.strictEqual(collected.count("neda.event.processed", { ...topic, "neda.outcome": "success" }), 1);
        });

        await test("a failed then successful attempt times both, and counts one success", async () =>
        {
            // One failure costs the ladder's first delay, 6 seconds.
            const processor = new ScriptedProcessor(manager, 1);
            await runToCompletion(processor, createWorkItem("processor-retry"));
            await processor.dispose();

            const topic = { "messaging.destination.name": "processor-retry" };
            const collected = await globalPipeline.collect();

            const attempts = collected.points<Histogram>("neda.event.process.duration", topic);
            assert.deepStrictEqual(
                attempts.map(t => [t.attributes["error.type"] ?? null, t.value.count]).orderBy(t => `${t[0]}`),
                [["ApplicationException", 1], [null, 1]]);
            // The retry sleep sits between the attempts, not inside either one.
            attempts.forEach(t => assert.ok(t.value.sum! < 1, `attempt took ${t.value.sum}s`));

            assert.strictEqual(collected.count("neda.event.processed", { ...topic, "neda.outcome": "success" }), 1);
            assert.strictEqual(collected.count("neda.event.processed", { ...topic, "neda.outcome": "failed" }), 0);
        });
    });
});
