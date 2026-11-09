import { Duration } from "@nivinjoseph/n-util";
import assert from "node:assert";
import test, { describe } from "node:test";
import { EdaManager } from "../src/eda-manager.js";
import { Broker } from "../src/redis-implementation/broker.js";
import { MetricsReporter } from "../src/redis-implementation/metrics-reporter.js";
import { createStubBroker as createBroker, silentLogger } from "./utils/test-stubs.js";


// Broker.report is pure in-memory arithmetic, so it is exercised directly rather than through Redis.

function createReporter(brokers: Array<Broker>): MetricsReporter
{
    // Never started in these tests — buildRecords is called directly, so no timer is involved.
    return new MetricsReporter(brokers, silentLogger, "test-group", "test-consumer", Duration.fromMinutes(1));
}

const minute = Duration.fromMinutes(1).toMilliSeconds();


await describe("Metrics tests", async () =>
{
    await describe("rate derivation", async () =>
    {
        await test("first report has no baseline, so rates are 0", () =>
        {
            const broker = createBroker();

            broker.report(0, 100, 60, 1_000_000);

            const metrics = broker.metrics.get(0)!;
            assert.strictEqual(metrics.lag, 40);
            assert.strictEqual(metrics.writeIndex, 100);
            assert.strictEqual(metrics.readIndex, 60);
            assert.strictEqual(metrics.productionRate, 0);
            assert.strictEqual(metrics.consumptionRate, 0);
            assert.strictEqual(metrics.sampledAt, 1_000_000);
        });

        await test("rates over exactly one minute equal the raw deltas", () =>
        {
            const broker = createBroker();

            broker.report(0, 100, 60, 1_000_000);
            broker.report(0, 130, 80, 1_000_000 + minute);

            const metrics = broker.metrics.get(0)!;
            assert.strictEqual(metrics.productionRate, 30);
            assert.strictEqual(metrics.consumptionRate, 20);
            assert.strictEqual(metrics.sampledAt, 1_000_000 + minute);
        });

        await test("rates are normalized to per minute, not left as raw deltas", () =>
        {
            const broker = createBroker();

            // The same deltas as above, but sampled over half the time, so the rates must double. This is
            // the property that makes a record safe for the reporter to re-log on its own timer.
            broker.report(0, 100, 60, 1_000_000);
            broker.report(0, 130, 80, 1_000_000 + minute / 2);

            const metrics = broker.metrics.get(0)!;
            assert.strictEqual(metrics.productionRate, 60);
            assert.strictEqual(metrics.consumptionRate, 40);
        });

        await test("a slow topic keeps a fractional rate instead of flooring to zero", () =>
        {
            const broker = createBroker();

            // 20 events an hour: rounding to whole batches would report 0 and read as a dead partition.
            broker.report(0, 100, 100, 1_000_000);
            broker.report(0, 120, 120, 1_000_000 + 60 * minute);

            const metrics = broker.metrics.get(0)!;
            assert.strictEqual(metrics.productionRate, 0.33);
            assert.strictEqual(metrics.consumptionRate, 0.33);
        });

        await test("a non-advancing clock yields 0 rather than Infinity or NaN", () =>
        {
            const broker = createBroker();

            broker.report(0, 100, 60, 1_000_000);
            broker.report(0, 130, 80, 1_000_000);

            const metrics = broker.metrics.get(0)!;
            assert.strictEqual(metrics.productionRate, 0);
            assert.strictEqual(metrics.consumptionRate, 0);
        });

        await test("a backwards clock yields 0 rather than a negative rate", () =>
        {
            const broker = createBroker();

            broker.report(0, 100, 60, 1_000_000);
            broker.report(0, 130, 80, 1_000_000 - minute);

            const metrics = broker.metrics.get(0)!;
            assert.strictEqual(metrics.productionRate, 0);
            assert.strictEqual(metrics.consumptionRate, 0);
        });

        await test("reading metrics twice without a new report returns the same rate", () =>
        {
            const broker = createBroker();

            broker.report(0, 100, 60, 1_000_000);
            broker.report(0, 130, 80, 1_000_000 + minute);

            // The regression this guards: the reporter logs on a timer independent of the consumers'
            // reports, so it re-reads entries no consumer has refreshed. A rate is unchanged by that; a raw
            // delta would have read as another 30 events produced.
            const first = broker.metrics.get(0)!;
            const second = broker.metrics.get(0)!;

            assert.strictEqual(first.productionRate, 30);
            assert.strictEqual(second.productionRate, 30);
            assert.strictEqual(first.sampledAt, second.sampledAt);
        });

        await test("an index regression resets the baseline instead of logging negative rates", () =>
        {
            const broker = createBroker();

            // A Redis flush/eviction restarts the write index below the previous sample; a raw delta would
            // log a huge negative rate into the dashboards.
            broker.report(0, 100_000, 99_000, 1_000_000);
            broker.report(0, 50, 20, 1_000_000 + minute);

            const metrics = broker.metrics.get(0)!;
            assert.strictEqual(metrics.productionRate, 0);
            assert.strictEqual(metrics.consumptionRate, 0);
            assert.strictEqual(metrics.lag, 30);
        });

        await test("lag clamps at 0 when the write index regressed below the read index", () =>
        {
            const broker = createBroker();

            broker.report(0, 0, 99_000, 1_000_000);

            assert.strictEqual(broker.metrics.get(0)!.lag, 0);
        });

        await test("partitions are tracked independently", () =>
        {
            const broker = createBroker();

            broker.report(0, 100, 100, 1_000_000);
            broker.report(1, 500, 200, 1_000_000);
            broker.report(0, 110, 110, 1_000_000 + minute);
            broker.report(1, 600, 250, 1_000_000 + minute);

            assert.strictEqual(broker.metrics.get(0)!.lag, 0);
            assert.strictEqual(broker.metrics.get(0)!.productionRate, 10);
            assert.strictEqual(broker.metrics.get(1)!.lag, 350);
            assert.strictEqual(broker.metrics.get(1)!.productionRate, 100);
        });
    });

    await describe("log records", async () =>
    {
        await test("every value in every record is a scalar", () =>
        {
            const orders = createBroker("orders");
            const shipments = createBroker("shipments");
            orders.report(0, 100, 60, 1_000_000);
            orders.report(1, 200, 200, 1_000_000);
            shipments.report(0, 50, 50, 1_000_000);

            const records = createReporter([orders, shipments]).buildRecords(1_000_000);

            // The property the dashboards depend on: log-based metrics can only read flat attributes off a
            // single event, and a nested value would break them silently rather than loudly.
            assert.ok(records.isNotEmpty);
            records.forEach(record =>
            {
                Object.entries(record).forEach(([key, value]) =>
                {
                    assert.ok(value !== null && typeof value !== "object",
                        `${key} must be a scalar, got ${JSON.stringify(value)}`);
                });
            });
        });

        await test("emits one record per topic-partition, each tagged with logType", () =>
        {
            const orders = createBroker("orders");
            const shipments = createBroker("shipments");
            orders.report(0, 100, 60, 1_000_000);
            orders.report(1, 200, 200, 1_000_000);
            shipments.report(0, 50, 50, 1_000_000);

            const records = createReporter([orders, shipments]).buildRecords(1_000_000);

            assert.strictEqual(records.length, 3);
            assert.deepStrictEqual(
                records.map(t => `${t.topic}-${t.partition}`),
                ["orders-0", "orders-1", "shipments-0"]);
            records.forEach(t => assert.strictEqual(t.logType, "n-eda.partition-metrics"));
        });

        await test("a record round-trips through JSON with its numbers intact", () =>
        {
            const broker = createBroker("orders");
            broker.report(0, 100, 60, 1_000_000);
            broker.report(0, 130, 80, 1_000_000 + minute);

            // The reporter logs JSON.stringify(record), so the parsed line is what a pipeline actually sees.
            const parsed = JSON.parse(JSON.stringify(
                createReporter([broker]).buildRecords(1_000_000 + minute)[0]));

            assert.deepStrictEqual(parsed, {
                logType: "n-eda.partition-metrics",
                topic: "orders",
                partition: 0,
                consumerGroupId: "test-group",
                consumerName: "test-consumer",
                lag: 50,
                writeIndex: 130,
                readIndex: 80,
                productionRate: 30,
                consumptionRate: 20,
                sampledAt: 1_000_000 + minute,
                sampleAgeMs: 0
            });
        });

        await test("sampleAgeMs reports how stale a re-logged record is", () =>
        {
            const broker = createBroker("orders");
            broker.report(0, 100, 60, 1_000_000);

            const reporter = createReporter([broker]);

            // Two ticks with no intervening report — the second must show the record ageing rather than
            // looking as fresh as the first.
            assert.strictEqual(reporter.buildRecords(1_000_000 + 5_000)[0].sampleAgeMs, 5_000);
            assert.strictEqual(reporter.buildRecords(1_000_000 + 65_000)[0].sampleAgeMs, 65_000);
        });

        await test("sampleAgeMs clamps at 0 when the sample time is in the future", () =>
        {
            const broker = createBroker("orders");
            broker.report(0, 100, 60, 1_000_000);

            // A backwards wall-clock step would otherwise emit a negative age, which passes the documented
            // `@sampleAgeMs:<N` staleness filter and makes frozen records look maximally fresh.
            assert.strictEqual(createReporter([broker]).buildRecords(1_000_000 - 5_000)[0].sampleAgeMs, 0);
        });

        await test("no record key is a reserved log-platform attribute", () =>
        {
            const broker = createBroker("orders");
            broker.report(0, 100, 60, 1_000_000);

            // Datadog remaps these names at ingest (e.g. `timestamp` shifts the event's own time, `status`
            // becomes log severity), silently breaking dashboards. Guards the class of bug, not just the
            // `timestamp` instance that already bit once. Union of Datadog's documented preprocessing
            // lookup lists (date, host, service, status, message, trace/span id) plus envelope names.
            const reservedAttributes = ["timestamp", "_timestamp", "date", "eventTime", "published_date",
                "status", "level", "severity", "host", "hostname", "service", "source",
                "message", "msg", "log", "env", "trace_id", "span_id"];

            const record = createReporter([broker]).buildRecords(1_000_000)[0];
            Object.keys(record).forEach(key =>
                assert.ok(!reservedAttributes.contains(key), `${key} is a reserved log attribute`));
        });
    });

    await describe("configuration", async () =>
    {
        await test("metrics are off by default", () =>
        {
            assert.strictEqual(new EdaManager().metricsEnabled, false);
        });

        await test("enableMetrics turns metrics on with the default 1-minute interval", () =>
        {
            const manager = new EdaManager();
            manager.enableMetrics();

            assert.strictEqual(manager.metricsEnabled, true);
            assert.strictEqual(manager.metricsInterval.toMilliSeconds(), minute);
        });

        await test("enableMetrics honors a custom interval", () =>
        {
            const manager = new EdaManager();
            manager.enableMetrics(Duration.fromMinutes(5));

            assert.strictEqual(manager.metricsEnabled, true);
            assert.strictEqual(manager.metricsInterval.toMilliSeconds(), 5 * minute);
        });

        await test("enableMetrics rejects an interval over Node's timer maximum", () =>
        {
            // Beyond 2^31-1 ms, setInterval clamps to 1ms and the reporter would flood the logs with
            // topics x partitions lines per millisecond — the opposite of a widened interval's intent.
            assert.throws(() => new EdaManager().enableMetrics(Duration.fromDays(30)));
        });

        await test("enableMetrics rejects a zero interval", () =>
        {
            assert.throws(() => new EdaManager().enableMetrics(Duration.fromMilliSeconds(0)));
        });
    });
});
