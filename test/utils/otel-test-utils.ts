import * as otelApi from "@opentelemetry/api";
import {
    AggregationTemporality, DataPoint, Histogram, InMemoryMetricExporter, MeterProvider, MetricData,
    PeriodicExportingMetricReader
} from "@opentelemetry/sdk-metrics";


/**
 * An in-memory metrics pipeline: a `MeterProvider` whose reader only exports when a test calls `collect()`.
 *
 * Cumulative temporality by default, so a counter or histogram reads as its running total — tests that share
 * one pipeline compare totals before and after rather than assuming they start at zero. Note that under
 * cumulative temporality the SDK keeps re-exporting an observable gauge's last value after its callback is
 * removed; a test that asserts a gauge has gone quiet needs delta temporality.
 */
export class TestMetricsPipeline
{
    private readonly _exporter: InMemoryMetricExporter;
    private readonly _reader: PeriodicExportingMetricReader;
    private readonly _provider: MeterProvider;


    public get provider(): MeterProvider { return this._provider; }


    public constructor(temporality = AggregationTemporality.CUMULATIVE)
    {
        this._exporter = new InMemoryMetricExporter(temporality);

        // The interval only has to outlast a test run; every read goes through forceFlush.
        this._reader = new PeriodicExportingMetricReader({
            exporter: this._exporter,
            exportIntervalMillis: 60 * 60 * 1000
        });

        this._provider = new MeterProvider({ readers: [this._reader] });
    }


    public meter(): otelApi.Meter
    {
        return this._provider.getMeter("n-eda");
    }

    public async collect(): Promise<CollectedMetrics>
    {
        this._exporter.reset();
        await this._reader.forceFlush();

        return new CollectedMetrics(this._exporter.getMetrics()
            .flatMap(t => t.scopeMetrics)
            .flatMap(t => t.metrics));
    }

    public shutdown(): Promise<void>
    {
        return this._provider.shutdown();
    }
}

export class CollectedMetrics
{
    private readonly _metrics: ReadonlyArray<MetricData>;


    public constructor(metrics: ReadonlyArray<MetricData>)
    {
        this._metrics = metrics;
    }


    public find(name: string): MetricData | null
    {
        return this._metrics.find(t => t.descriptor.name === name) ?? null;
    }

    /** Data points of the named metric, optionally narrowed to those carrying every given attribute value. */
    public points<T = number>(name: string, attributes: otelApi.Attributes = {}): Array<DataPoint<T>>
    {
        const metric = this.find(name);
        if (metric == null)
            return [];

        return (metric.dataPoints as Array<DataPoint<T>>)
            .filter(point => Object.entries(attributes).every(([key, value]) => point.attributes[key] === value));
    }

    /** Sum of the named counter's points matching the attributes; 0 when nothing was recorded. */
    public count(name: string, attributes: otelApi.Attributes = {}): number
    {
        return this.points(name, attributes).reduce((acc, t) => acc + t.value, 0);
    }

    /** Total observations in the named histogram's points matching the attributes. */
    public histogramCount(name: string, attributes: otelApi.Attributes = {}): number
    {
        return this.points<Histogram>(name, attributes).reduce((acc, t) => acc + t.value.count, 0);
    }
}
