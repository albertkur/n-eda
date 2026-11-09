import { given } from "@nivinjoseph/n-defensive";
import { ComponentInstaller, Registry } from "@nivinjoseph/n-ject";
import { Delay, Deserializer, DisposableWrapper, Duration, Serializable, serialize } from "@nivinjoseph/n-util";
import * as otelApi from "@opentelemetry/api";
import { Histogram } from "@opentelemetry/sdk-metrics";
import { Redis } from "ioredis";
import assert from "node:assert";
import test, { after, before, describe } from "node:test";
import Zlib from "node:zlib";
import {
    EdaEvent, EdaEventHandler, EdaManager, EventBus, RedisEventBus, RedisEventSubMgr, Topic, event
} from "../src/index.js";
import { Producer } from "../src/redis-implementation/producer.js";
import { CollectedMetrics, TestMetricsPipeline } from "./utils/otel-test-utils.js";
import { silentLogger } from "./utils/test-stubs.js";


/**
 * The `$publishedAt` stamp end to end against a live Redis: the producer writes it, a consumer turns it into
 * `neda.event.age`, and an event without it — as an older producer writes — is processed but goes unmeasured.
 * Also the one place the sent, dropped, and skipped counts and the partition instruments are read off a real
 * publish path and consume loop.
 */

// Set before bootstrap(), which takes its meter from the global provider exactly once.
const pipeline = new TestMetricsPipeline();
otelApi.metrics.setGlobalMeterProvider(pipeline.provider);

// Event ids are unique per run: a reused Redis keeps each group's dedupe window, which would skip repeats.
const runId = Date.now();
const handledIds = new Set<string>();


@serialize("Test")
class AgeTestEvent extends Serializable implements EdaEvent
{
    private readonly _id: string;


    @serialize
    public get id(): string { return this._id; }

    @serialize
    public get name(): string { return (<Object>AgeTestEvent).getTypeName(); }

    public get partitionKey(): string { return this._id; }
    public get refId(): string { return "neda"; }
    public get refType(): string { return "neda"; }


    public constructor(data: { id: string; })
    {
        super(data);

        const { id } = data;

        given(id, "id").ensureHasValue().ensureIsString();
        this._id = id;
    }
}

/** Registered for deserialization, but with no handler anywhere: publish drops it, a consumer skips it. */
@serialize("Test")
class UnhandledAgeTestEvent extends Serializable implements EdaEvent
{
    private readonly _id: string;


    @serialize
    public get id(): string { return this._id; }

    @serialize
    public get name(): string { return (<Object>UnhandledAgeTestEvent).getTypeName(); }

    public get partitionKey(): string { return this._id; }
    public get refId(): string { return "neda"; }
    public get refType(): string { return "neda"; }


    public constructor(data: { id: string; })
    {
        super(data);

        const { id } = data;

        given(id, "id").ensureHasValue().ensureIsString();
        this._id = id;
    }
}

@event(AgeTestEvent)
class AgeTestEventHandler implements EdaEventHandler<AgeTestEvent>
{
    public handle(event: AgeTestEvent): Promise<void>
    {
        handledIds.add(event.id);
        return Promise.resolve();
    }
}

class AgeTestInstaller implements ComponentInstaller
{
    public install(registry: Registry): Promise<void>
    {
        const edaRedisClient = new Redis();

        registry
            .registerInstance("Logger", silentLogger)
            .registerInstance("EdaRedisClient", edaRedisClient)
            // Disposed with the container, which EdaManager.dispose() reaches only after consumption stops.
            .registerInstance("EdaRedisClientDisposable", new DisposableWrapper(async () =>
            {
                await edaRedisClient.quit();
            }));

        return Promise.resolve();
    }
}


await describe("event age tests", async () =>
{
    const topic = "event-age";
    const topicAttributes = { "messaging.destination.name": topic };

    let manager: EdaManager;
    let rawClient: Redis;

    async function processedCount(): Promise<number>
    {
        return (await pipeline.collect())
            .count("neda.event.processed", { ...topicAttributes, "neda.outcome": "success" });
    }

    /** Polls the pipeline until `check` passes, and returns what it last collected. */
    async function waitUntil(description: string, check: (collected: CollectedMetrics) => boolean)
        : Promise<CollectedMetrics>
    {
        const deadline = Date.now() + 30_000;
        let collected = await pipeline.collect();
        while (!check(collected))
        {
            if (Date.now() > deadline)
                throw new Error(`timed out waiting for ${description}`);

            await Delay.milliseconds(100);
            collected = await pipeline.collect();
        }

        return collected;
    }

    /** Resolves once `count` more events have reached `success`: by then each was picked up, and aged. */
    function waitForProcessed(baseline: number, count: number): Promise<CollectedMetrics>
    {
        return waitUntil(`${count} processed event(s)`, collected =>
            collected.count("neda.event.processed", { ...topicAttributes, "neda.outcome": "success" })
            >= baseline + count);
    }

    /**
     * Writes events straight into partition 0 the way the Producer lays them out — skipping `publish()`, so
     * nothing checks or stamps them. The Producer is only asked for its keys. The doorbell wakes the idle
     * consumer instead of waiting out its poll delay.
     */
    async function writeRawBatch(...events: Array<EdaEvent>): Promise<void>
    {
        const producer = new Producer(`${topic}+++0`, rawClient, silentLogger, topic, 10, 0);
        const payload = Zlib.deflateRawSync(Buffer.from(JSON.stringify(events.map(t => t.serialize())), "utf8"));
        const writeIndex = await rawClient.incr(producer.writeIndexKey);
        await rawClient.setex(`${producer.id}-${writeIndex}`, 600, payload);
        await rawClient.publish(`${producer.id}-changed`, producer.id);
    }

    before(async () =>
    {
        rawClient = new Redis();

        manager = new EdaManager()
            .useInstaller(new AgeTestInstaller())
            .registerTopics(new Topic(topic, Duration.fromMinutes(10), 1).subscribe())
            .registerEventHandlers(AgeTestEventHandler)
            // Throws for one id prefix, so a consumer meets an event it cannot place. Only written raw: publish()
            // maps events too, and would throw first.
            .usePartitionKeyMapper(event =>
            {
                if (event.id.startsWith("badkey"))
                    throw new Error("no partition key");

                return event.partitionKey;
            })
            .registerEventBus(RedisEventBus)
            .registerEventSubscriptionManager(RedisEventSubMgr, "event-age-group");

        await manager.bootstrap();

        manager.beginConsumption().catch(e => console.error(e));
    });

    after(async () =>
    {
        await manager.dispose();
        await rawClient.quit();
        await pipeline.shutdown();
    });


    await test("the producer stamps every event in a batch with one publish time", async () =>
    {
        // A topic nothing consumes, so the slot is still there to read back.
        const producer = new Producer("event-age-stamp+++0", rawClient, silentLogger, "event-age-stamp", 10, 0);

        const publishedFrom = Date.now();
        await producer.produce(
            new AgeTestEvent({ id: `stamp_1-${runId}` }),
            new AgeTestEvent({ id: `stamp_2-${runId}` }));
        const publishedTo = Date.now();

        const writeIndex = Number.parseInt((await rawClient.get(producer.writeIndexKey))!);
        const slot = await rawClient.getBuffer(`${producer.id}-${writeIndex}`);
        const batch = JSON.parse(Zlib.inflateRawSync(slot!).toString("utf8")) as Array<Record<string, unknown>>;

        assert.strictEqual(batch.length, 2);
        const [first, second] = batch.map(t => t["$publishedAt"]);
        assert.strictEqual(typeof first, "number");
        assert.strictEqual(first, second);
        assert.ok((first as number) >= publishedFrom && (first as number) <= publishedTo);

        // What makes the stamp safe for older consumers: deserialization passes the field to the
        // constructor, which ignores it.
        const deserialized = Deserializer.deserialize<AgeTestEvent>(batch[0]);
        assert.ok(deserialized instanceof AgeTestEvent);
        assert.strictEqual(deserialized.id, `stamp_1-${runId}`);
    });

    await test("a consumer records the age of a stamped event", async () =>
    {
        const baseline = await pipeline.collect();
        const agesBefore = baseline.histogramCount("neda.event.age", topicAttributes);
        const sentAttributes = { ...topicAttributes, "neda.event.type": "AgeTestEvent" };
        const sentBefore = baseline.count("messaging.client.sent.messages", sentAttributes);
        const processedBefore = await processedCount();

        const id = `stamped-${runId}`;
        await manager.serviceLocator.resolve<EventBus>("EventBus").publish(topic, new AgeTestEvent({ id }));

        const collected = await waitForProcessed(processedBefore, 1);

        assert.ok(handledIds.has(id));
        assert.strictEqual(collected.histogramCount("neda.event.age", topicAttributes) - agesBefore, 1);
        assert.strictEqual(collected.count("messaging.client.sent.messages", sentAttributes) - sentBefore, 1);
        assert.ok(collected.points("messaging.client.sent.messages", sentAttributes)
            .every(p => p.attributes["error.type"] == null));

        const [age] = collected.points<Histogram>("neda.event.age", topicAttributes);
        assert.strictEqual(age.attributes["neda.event.type"], "AgeTestEvent");
        assert.strictEqual(age.attributes["messaging.consumer.group.name"], "event-age-group");
        // Same-host clocks, so the age is real wall time — a few seconds at most, even on a loaded machine.
        assert.ok(age.value.max! >= 0 && age.value.max! < 30, `age was ${age.value.max}s`);
    });

    await test("an event without the stamp is processed but records no age", async () =>
    {
        const baseline = await pipeline.collect();
        const agesBefore = baseline.histogramCount("neda.event.age", topicAttributes);
        const processedBefore = await processedCount();

        // Written the way a producer from before the stamp wrote it: without `$publishedAt`.
        const id = `unstamped-${runId}`;
        await writeRawBatch(new AgeTestEvent({ id }));

        const collected = await waitForProcessed(processedBefore, 1);

        assert.ok(handledIds.has(id));
        assert.strictEqual(collected.histogramCount("neda.event.age", topicAttributes) - agesBefore, 0);
    });

    await test("publish drops an event this process has no handler for, and counts it", async () =>
    {
        const droppedAttributes = { ...topicAttributes, "neda.event.type": "UnhandledAgeTestEvent" };
        const droppedBefore = (await pipeline.collect()).count("neda.event.dropped", droppedAttributes);

        await manager.serviceLocator.resolve<EventBus>("EventBus")
            .publish(topic, new UnhandledAgeTestEvent({ id: `dropped-${runId}` }));

        // Counted at publish() itself, so it is there as soon as publish() returns; and nothing was sent.
        const collected = await pipeline.collect();
        assert.strictEqual(collected.count("neda.event.dropped", droppedAttributes) - droppedBefore, 1);
        assert.strictEqual(collected.count("messaging.client.sent.messages", droppedAttributes), 0);
    });

    await test("a consumer counts each event it skips, by reason", async () =>
    {
        const skipped = (collected: CollectedMetrics, reason: string): number =>
            collected.count("neda.event.skipped", { ...topicAttributes, "neda.skip.reason": reason });
        const baseline = await pipeline.collect();
        const before = {
            duplicate: skipped(baseline, "duplicate"),
            noHandler: skipped(baseline, "no_handler"),
            partitionKeyError: skipped(baseline, "partition_key_error")
        };

        // An id already handled above, published again.
        await manager.serviceLocator.resolve<EventBus>("EventBus")
            .publish(topic, new AgeTestEvent({ id: `stamped-${runId}` }));
        // A type with no handler, written raw since publish() would drop it (as the test above shows).
        await writeRawBatch(new UnhandledAgeTestEvent({ id: `unhandled-${runId}` }));
        // An event the manager's partition key mapper throws on.
        await writeRawBatch(new AgeTestEvent({ id: `badkey-${runId}` }));

        const collected = await waitUntil("one skip of each reason", t =>
            skipped(t, "duplicate") > before.duplicate
            && skipped(t, "no_handler") > before.noHandler
            && skipped(t, "partition_key_error") > before.partitionKeyError);

        assert.strictEqual(skipped(collected, "duplicate") - before.duplicate, 1);
        assert.strictEqual(skipped(collected, "no_handler") - before.noHandler, 1);
        assert.strictEqual(skipped(collected, "partition_key_error") - before.partitionKeyError, 1);
        assert.deepStrictEqual(
            collected.points("neda.event.skipped", { "neda.skip.reason": "no_handler" })
                .map(p => [p.attributes["neda.event.type"], p.attributes["messaging.consumer.group.name"]]),
            [["UnhandledAgeTestEvent", "event-age-group"]]);
        assert.ok(!handledIds.has(`badkey-${runId}`));
    });

    await test("the partition gauges report the consume loop's own samples", async () =>
    {
        // The consumer samples on its first loop iteration, which the tests above have long since passed.
        const partitionAttributes = { ...topicAttributes, "messaging.destination.partition.id": "0" };
        const collected = await pipeline.collect();

        const lags = collected.points("neda.partition.lag", partitionAttributes);
        assert.strictEqual(lags.length, 1, "expected one lag point for partition 0");

        const [lag] = lags;
        assert.strictEqual(lag.attributes["messaging.consumer.group.name"], "event-age-group");
        assert.ok(lag.value >= 0, `lag was ${lag.value}`);

        // The loop reads the lag on every pass, at most ~5 s apart while idle; 15 leaves room for Redis and
        // timer slack.
        const [sampleAge] = collected.points("neda.partition.sample_age", partitionAttributes);
        assert.ok(sampleAge.value >= 0 && sampleAge.value <= 15, `sample age was ${sampleAge.value}s`);

        // Fed by the same readings, so it is never below the lag gauge.
        const peaks = collected.points("neda.partition.lag_peak", partitionAttributes);
        assert.strictEqual(peaks.length, 1, "expected one lag_peak point for partition 0");
        assert.ok(peaks[0].value >= lag.value, `lag peak ${peaks[0].value} was below lag ${lag.value}`);

        // The indexes behind the lag, taken in the same reading.
        const [written] = collected.points("neda.partition.written", partitionAttributes);
        const [consumed] = collected.points("neda.partition.consumed", partitionAttributes);
        assert.ok(written.value >= 1, `written was ${written.value}`);
        assert.strictEqual(Math.max(0, written.value - consumed.value), lag.value);
    });
});
