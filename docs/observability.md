# Observability: metrics

n-eda reports metrics through two independent channels:

- **[OpenTelemetry instruments](#opentelemetry-metrics)** — handler latency and outcomes, event age,
  per-partition lag and throughput, lost and skipped events, and what was published or dropped, recorded
  through the OpenTelemetry metrics API. Live whenever the host registers a `MeterProvider` before
  `bootstrap()`; there is no n-eda switch.
- **[Log-based partition metrics](#log-based-partition-metrics)** — per-partition lag and throughput as
  structured log lines, for a log pipeline to turn into dashboard metrics. Opt-in via `enableMetrics()`.

Tracing is covered in the README's OpenTelemetry section.

## OpenTelemetry metrics

### Turning them on

n-eda takes the meter `"n-eda"` from the global `MeterProvider` once, in `EdaManager.bootstrap()`. Register
the provider before that point; in practice that means at process start, for example by preloading
n-strument (`node --import @nivinjoseph/n-strument`), which always registers one. If no provider is
registered when `bootstrap()` runs, the OpenTelemetry API hands n-eda a no-op meter for good, every recording
below costs next to nothing, and `bootstrap()` says so in one info-level log line.

Each process's series must be distinguishable from every other's. n-eda's metrics carry no process identity,
so two processes with the same `service.name`, consuming the same topic in the same consumer group and
reporting through one collector, would send identical series whose running totals merge. Running one
consumer per consumer group, or deploying partition-affinity consumers as separately named services, rules
that out, and so does n-strument's assumption of one collector or agent per process. Outside both, add
`service.instance.id` to the resource.

These instruments do not depend on `enableMetrics()`. The partition instruments report the consume loop's own
index readings, taken on every pass; they never read the log reporter's samples, so its interval has no
effect on them.

### Instruments

Twelve instruments, in three groups by where they are recorded.

**Handling events.** Recorded by the consuming process: per handler attempt, or once per delivery. Tagged with
topic, consumer group, and event type.

| Instrument | Type | Unit | Measures |
|---|---|---|---|
| `neda.event.process.duration` | histogram | `s` | How long each handler **attempt** took, excluding the sleeps between retries; failed attempts carry `error.type`. Under the Lambda, RPC, and gRPC proxies it includes the network round trip. |
| `neda.event.processed` | counter | `{event}` | Final outcomes: `neda.outcome` is `success`, or `failed` once all 10 attempts are exhausted. |
| `neda.event.age` | histogram | `s` | How long an event waited between publish and consumer pickup. |
| `neda.event.skipped` | counter | `{event}` | Events passed over without handling, by `neda.skip.reason`: `duplicate`, `no_handler`, or `partition_key_error`. |

**Per partition.** Read on every pass of the consume loop (every 2.5–5 s when idle, every batch when busy),
and reported at each collection. Tagged with topic, consumer group, and partition.

| Instrument | Type | Unit | Measures |
|---|---|---|---|
| `neda.partition.lag` | observable gauge | `{batch}` | Current lag: publish batches this consumer group has yet to read. |
| `neda.partition.lag_peak` | observable gauge | `{batch}` | The highest lag read since the previous collection, so a spike that drained still shows. |
| `neda.partition.sample_age` | observable gauge | `s` | Time since the loop last read the partition; climbs when it is stuck. |
| `neda.partition.written` | observable counter | `{batch}` | Batches written to the partition since this process first read it. Its rate is the production rate. |
| `neda.partition.consumed` | observable counter | `{batch}` | Batches this group consumed from the partition since this process first read it. Its rate is the consumption rate. |
| `neda.batch.lost` | counter | `{batch}` | Slots given up on after 200 reads (about 42 s) because the payload never appeared: lost events. |

**Publishing.** Recorded by the publishing process, in `publish()`. Tagged with topic and event type, but no
consumer group.

| Instrument | Type | Unit | Measures |
|---|---|---|---|
| `neda.event.sent` | counter | `{event}` | Events written to a partition, counted once stored; a write that failed after its retries carries `error.type`. |
| `neda.event.dropped` | counter | `{event}` | Events discarded unsent: this process has no handler for their type, and the topic is not forced. |

The two per-event counts are per **delivery**, not per event. Delivery is at least once, so one event can be
picked up twice:

- **After a graceful shutdown**, events still queued in the scheduler (behind a busy partition key, or
  waiting for a free processor), or waiting in the retry ladder, are replayed on restart. Their age is
  recorded again, and the second age includes the downtime. Their outcome is counted once, on the delivery
  that finishes them, unless the shutdown caught the consumer still reading the batch: it then stops without
  saving which of that batch's events it has seen, so the ones already handled are replayed too, and both
  their age and their outcome are counted twice.
- **After a crash**, everything read since the consumer's last completed batch is replayed, including events
  that were already handled. Both their age and their outcome are counted twice.

Every metric name is under `neda.`. The attributes follow OpenTelemetry's messaging conventions
(`messaging.system`, `messaging.destination.name`, `messaging.consumer.group.name`, `error.type`) wherever
one fits, and use `neda.` for the rest.

Histogram bucket boundaries are supplied as advice:

- processing, in seconds: `0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10, 30, 60`
- age, in seconds: `0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600`. The age
  buckets run to 10 minutes so an event held up behind a key's retry ladder (about 8.5 minutes) still lands
  in a real bucket.

A view configured on your `MeterProvider` overrides them.

### Attributes

The consumer's per-event instruments (`neda.event.process.duration`, `neda.event.processed`, `neda.event.age`,
and `neda.event.skipped`) carry:

| Attribute | Value |
|---|---|
| `messaging.system` | `n-eda` |
| `messaging.destination.name` | The topic name. Spans use `topic+++partition`; metrics use the topic alone, because a partition here would multiply every per-event series by the partition count. The partition instruments below carry it. |
| `messaging.consumer.group.name` | The consumer group from `registerEventSubscriptionManager`. |
| `neda.event.type` | The handled event's type name. For a distributed observer notification, this is the **observed** event's type, not `NedaDistributedObserverNotifyEvent`. |

`neda.event.process.duration` adds, on a failed attempt, `error.type` = the thrown error's class name
(`_OTHER` for a thrown non-object). `neda.event.processed`
adds `neda.outcome`, and `neda.event.skipped` adds `neda.skip.reason`.

The producer-side counters (`neda.event.sent` and `neda.event.dropped`) carry
`messaging.system`, `messaging.destination.name`, and `neda.event.type`, but no consumer group: publishing is
not done on behalf of one, even in a process that also consumes. `neda.event.sent` adds
`error.type` on a failed write.

The partition instruments (three gauges, two counters, and `neda.batch.lost`) carry `messaging.system`,
`messaging.destination.name`, `messaging.consumer.group.name`, and `messaging.destination.partition.id` (the
partition number, as a string): one series each per partition per consumer group. `neda.partition.written`
is the same for every group reading a partition, since each group's consumer reports it.

Event ids and partition keys are never attributes: each distinct value would be a new time series.
Per-event series grow as roughly **topics × event types × outcomes** (or error classes, for the duration
histogram, and skip reasons). That is fine for tens of event types per topic. Hundreds would call for dropping
`neda.event.type` with a view.

### Under a proxy

When processing is proxied to AWS Lambda, RPC, or gRPC, the processor sees the proxy's error rather than the
handler's, so `error.type` says less:

| Processing | Handler failure | Transport failure |
|---|---|---|
| In-process (default) | The handler's own error class | — |
| RPC | `ApplicationException` | `TypeError` from `fetch`, or `DOMException` when the 60 s timeout fires |
| AWS Lambda | `ApplicationException` | The AWS SDK's error class |
| gRPC | `Error` | `Error` |

Under gRPC the two cannot be told apart: `GrpcEventHandler` throws on failure, and the gRPC client rejects
with a plain `Error` either way.

The proxied side records nothing. `AwsLambdaEventHandler`, `RpcEventHandler`, and `GrpcEventHandler` call the
handler directly, so the processing side's metrics, whose durations include the network round trip, are the
only ones there are.

### What each one is for

Every instrument answers a question no other one does. Where two overlap, the overlap is noted.

| Instrument | Its job | Overlap |
|---|---|---|
| `neda.event.process.duration` | Handler latency, and what failing attempts threw | — |
| `neda.event.processed` | The only count of events dropped after all their retries | Its `success` count equals the duration histogram's successful attempts; it is kept as one cheap counter that is easier to query |
| `neda.event.age` | The delay users feel, from publish to pickup | — |
| `neda.partition.lag` | Current backlog per partition | The two counters start at 0 in each process, so `written − consumed` is not the lag; the gauge is the only measure of the backlog |
| `neda.partition.lag_peak` | Spikes that drain between two pushes | — |
| `neda.partition.sample_age` | A stuck consume loop | — |
| `neda.partition.written` / `neda.partition.consumed` | Production and consumption rates | The log-based `productionRate` and `consumptionRate`, while `enableMetrics()` is still on |
| `neda.batch.lost` | Data loss | — |
| `neda.event.skipped` | Events delivered but never handled, and why | — |
| `neda.event.sent` | Events published per type, and publish failures | For a topic read by one consumer group, it should equal `processed` + `skipped` once everything is delivered; a gap means lost events, or replays counted twice. Not for a `flushConsume()` topic, whose consumers discard events without counting them |
| `neda.event.dropped` | Events silently discarded at publish | — |

### Reading them

- **Failure rate.** Divide the count of `neda.event.process.duration` points that carry `error.type` by the
  total count. Group by `error.type` to see what is failing.
- **Events that exhausted their retries.** `neda.event.processed{neda.outcome="failed"}` counts events
  dropped after all 10 attempts. There is no dead-letter queue, so this count is the only place those events
  show up as a number.
- **Event age** is computed from `$publishedAt`, an epoch-ms stamp the producer writes onto every event
  (one value per `publish()` call). A few limits apply:
  - Events from producers on an n-eda version that predates the stamp carry no `$publishedAt`, so they are
    processed normally but not measured.
  - Producer and consumer hosts read different clocks, so clock skew adds to the age. A negative age is
    recorded as `0`.
  - Age stops at pickup. Time an event then spends queued in-process behind an earlier event with the
    same partition key is not included.
- **Lag is in batches, not events.** One write-index slot holds one whole `publish()` call. The same holds
  for the two counters and for the log-based figures below.
- **Production and consumption rates.** The backend derives them from the two counters; n-eda never
  precomputes a rate. In Grafana, `rate(neda_partition_written_total[5m])` and
  `rate(neda_partition_consumed_total[5m])` (the OTLP conversion typically adds `_total`; check the name in
  Explore). In Datadog, `.as_rate()` on `neda.partition.written` and `neda.partition.consumed`. The counters
  are not the Redis indexes: each starts at 0 on the partition's first reading in the process, so a restart
  does not replay the index's history as one huge interval. A Redis flush restarts the indexes from 0, and
  the counters keep counting from there rather than going back.
- **Peak lag.** The lag gauge reports only the lag current at each collection, so a spike that drains
  before then never shows on it. `neda.partition.lag_peak` reports the highest lag the consume loop read
  since the previous collection, exactly, under either temporality: with a 60 s export, each point is that
  minute's max. Each collection then starts the peak over from the latest reading, so an interval with no
  spike reports the lag the partition sat at, not `0`. One caveat: it assumes a **single metrics reader**,
  which is what n-strument sets up. Every collection resets the peak, so two readers would each see only the
  peaks since the other's last collection.
- **A stuck partition.** The consume loop reads each partition's lag on every pass: every 2.5–5 s when
  idle, every batch when busy. A loop blocked mid-batch, typically by a handler sitting in its retry
  ladder, stops reading it. `neda.partition.lag` and `neda.partition.lag_peak` then freeze and
  `neda.partition.sample_age` climbs. An idle loop keeps `sample_age` under about 5 s and a busy one holds it
  only as long as a batch takes, so alert when it stays above a couple of minutes. A process that dies stops
  reporting instead: its partitions' series simply end, so alert on missing data too.
- A partition appears in the partition instruments after its consume loop's first pass, and only on the
  process that owns it.
- **Lost data.** Any increase in `neda.batch.lost` means events are gone: a slot was allocated but its
  payload never arrived, because a producer failed between its two Redis calls, or the payload expired
  under the topic's TTL before this group read it. A partition lagging by more than its TTL loses data
  this way. Alert on any increase.
- **Skipped events.** `duplicate` rises on the replays after a shutdown or crash, and is expected then.
  `no_handler` rises during a rolling deploy, while this consumer runs a build that lacks a handler the
  publisher has. `partition_key_error` means `partitionKeyMapper` throws on some events, which is a bug.
- **Publishing.** `neda.event.sent` counts events per type from the publishing process, where
  `neda.partition.written` counts batches from the consumer's side. Its `error.type` points are publish
  failures, which also throw to the caller. `neda.event.dropped` catches the silent case: a service
  publishing events it does not handle itself, to a topic without `.forcePublish()`, sends nothing at all.

### What to alert on

| Signal | Condition | Means |
|---|---|---|
| `neda.batch.lost` | any increase | Events lost: a payload never arrived, or expired under the TTL |
| `neda.partition.sample_age` | above a couple of minutes, or no data | A stuck consume loop, or a dead consumer |
| `neda.event.processed{neda.outcome="failed"}` | any increase | Events dropped after all their retries |
| `neda.event.sent` with `error.type` | any | Publishes failing after their retries |
| `neda.event.skipped{neda.skip.reason="partition_key_error"}` | any | `partitionKeyMapper` throws on some events |
| `neda.partition.lag` | above a per-topic threshold | Falling behind; set the threshold once normal levels are known |

### Datadog

The Datadog agent turns `service.name` into the `service` tag and `service.version` into `version`; Grafana's
Prometheus shows the same `service.name` as the `job` label instead. n-eda's attributes arrive as tags under
their dotted names. n-strument's resource has no `deployment.environment`, so check that the `env` tag comes
through from the agent's own configuration. Counters arrive as per-interval counts, so use `.as_rate()` for
rates. The counters never carry the absolute indexes, which is why lag is a gauge of its own.

`neda.partition.lag` is the lag metric; the log-based setup below no longer generates one. If an older
setup created a log-based `neda.partition.lag` in Datadog (the setup documented up to 7.0.5 did), delete it
**before deploying the upgrade**: the two would collide under one name. The gauge needs no opt-in, so it
starts flowing the moment an upgraded service runs with a `MeterProvider` registered.

## Log-based partition metrics

n-eda can also emit per-partition lag and throughput as structured log lines, so they can be shipped to a
log platform, parsed into facets, and turned into dashboard metrics without a separate metrics pipeline.
For lag and the production and consumption rates, the OpenTelemetry `neda.partition.lag` gauge and the
`neda.partition.written` and `neda.partition.consumed` counters cover the same ground.

These metrics are **opt-in**: call `manager.enableMetrics()` before `bootstrap()`. Without it, a consuming
process emits no metrics lines (the consumer's own queue-depth warning still fires).

This part covers the log contract and how to wire it up in Datadog. For the runtime that produces it, see
`MetricsReporter` in `src/redis-implementation/metrics-reporter.ts`.

### What gets emitted

Every reporting tick, `MetricsReporter` emits **one log line per topic-partition** this process owns.
Each line's message is a flat JSON object:

```json
{"logType":"n-eda.partition-metrics","topic":"orders","partition":3,"consumerGroupId":"billing-svc",
 "consumerName":"billing-worker-2","lag":42,"writeIndex":1000,"readIndex":958,
 "productionRate":12.5,"consumptionRate":11.75,"sampledAt":1755600000000,"sampleAgeMs":4200}
```

| Field | Meaning |
|---|---|
| `logType` | Always `n-eda.partition-metrics`. The discriminator your pipeline filters on. |
| `topic` | Topic name. |
| `partition` | Partition number within the topic. |
| `consumerGroupId` | The consumer group whose read offset produced `readIndex`, `lag`, and `consumptionRate` (from `registerEventSubscriptionManager`). Distinct groups consuming the same topic report independent figures. |
| `consumerName` | Instance label from `useConsumerName` (`UNNAMED` if never set). When one group is scaled across replicas with disjoint partition-affinity ranges, this is what identifies which instance reported the line. |
| `lag` | `writeIndex - readIndex`, clamped at `0` — publish batches this consumer group is behind. To alert on lag, use the OpenTelemetry `neda.partition.lag` gauge (see [Alerting](#5-alerting)). |
| `writeIndex` | The producer's current slot counter. |
| `readIndex` | This consumer group's current offset. |
| `productionRate` | Batches written per minute, two decimals. `0` when there is no usable baseline: a partition's first report, or an index that regressed (Redis flush/eviction). |
| `consumptionRate` | Batches consumed per minute, two decimals. `0` when there is no usable baseline. |
| `sampledAt` | Epoch ms when the consumer sampled these indexes. |
| `sampleAgeMs` | How old the sample was when logged — `tickTime - sampledAt`, clamped at `0`. |

A batch is one `SETEX`ed slot, which may hold several events; these are batch rates, not event rates.

#### Why one line per partition

Log-based metrics can only read **flat attributes off a single log event**. A platform will not fan an
array of partition objects out into one metric point each, so an aggregate line carrying every topic
and partition cannot produce a per-partition metric no matter what pipeline is written against it.
Hence: one flat line each, no nesting.

`PartitionMetricRecord` is typed to hold only scalars and a test asserts that every emitted value is
one. Nesting a value here breaks dashboards silently rather than loudly, so keep it flat.

#### Why the rates are per minute

Consumers refresh these figures on their own schedule, independent of the reporter's timer, so the
reporter will sometimes re-log a record no consumer has refreshed. A per-minute **rate** re-logged is
merely stale; a raw delta re-logged would read as a second, phantom batch of the same events. Use
`sampleAgeMs` to tell fresh points from stale ones.

#### Why `sampledAt` and not `timestamp`

`timestamp` is a reserved attribute in Datadog. Its date remapper would treat the value as the log
event's own time and shift the event on the timeline. Don't rename it back.

### Cadence and volume

Default is one tick per minute, adjustable via `enableMetrics`'s optional parameter (positive, at
most 2^31-1 ms ≈ 24.8 days — Node's timer maximum; beyond it `enableMetrics` throws):

```typescript
manager.enableMetrics(Duration.fromMinutes(5));
```

Volume is **`topics × partitions` lines per interval, per process**. A service owning 5 topics of 16
partitions emits 80 lines/min — roughly 115k lines/day per instance, which most platforms bill on
ingest. Widen the interval to trade dashboard resolution for cost.

Consumers report to their `Broker` at half the configured interval, so on a healthy consume loop a
logged line is never more than one report stale. A loop blocked mid-batch (a handler stuck in its
retry ladder can hold an iteration for minutes) pauses reporting for that partition — `sampleAgeMs`
is what makes that visible, which is one more reason to keep the staleness filter below.

### Datadog setup

Verified against the Datadog docs (log preprocessing, Grok parsing, log search syntax, and
logs-to-metrics pages) as of August 2026.

n-log's JSON format already wraps each line in `{source, service, env, level, message, dateTime, time}`.
Datadog preprocesses whole-line JSON using its reserved-attribute lookup lists, so `service` (service
list), `level` (status list: `status`, `severity`, `level`) and `message` map automatically — you do
not need to add them to the payload. Two things it does **not** do automatically:

- **It does not look inside `message`.** A JSON string nested in the message attribute needs a parser
  (step 1).
- **It does not read the log's own time.** The date lookup list is `timestamp`, `date`, `_timestamp`,
  `Timestamp`, `eventTime`, `published_date`, `syslog.timestamp` — n-log's `time`/`dateTime` are not
  on it, so events get *intake* time. Usually fine (the reporter logs records the moment it builds
  them); add a **Date Remapper** on `time` if you want the log's own stamp.

#### 1. Parse the message

In **Logs → Pipelines**, add a pipeline filtered to your service, containing a **Grok Parser**
processor. Grok parsers apply to the `message` attribute by default, and the `json` filter parses the
whole payload in one rule:

```
rule %{data::json}
```

That lifts `logType`, `topic`, `partition`, `lag`, and the rest to top-level attributes (`@topic`,
`@lag`, …). The numeric fields are emitted as JSON numbers, which is what metric generation requires —
if they arrive as strings, the parser is misconfigured.

#### 2. Create facets

In the Log Explorer, add facets on `@logType`, `@topic`, and `@partition`, and a **measure** (numeric
facet) on `@sampleAgeMs`. Datadog requires a facet before numeric comparison operators
(`@sampleAgeMs:<90000`) work in queries, and facets make the group-bys below selectable.

#### 3. Generate metrics

Datadog generates two kinds of metrics from logs: **count** metrics (log occurrences) and
**distribution** metrics (a numeric attribute; there is no gauge type). A distribution automatically
provides `count`, `min`, `max`, `sum`, and `avg` aggregations, with optional percentiles. In
**Logs → Generate Metrics**, add a distribution per figure:

| Query | Field | Metric name | Group by |
|---|---|---|---|
| `@logType:n-eda.partition-metrics` | `@productionRate` | `neda.partition.production_rate` | `@topic`, `@partition` |
| `@logType:n-eda.partition-metrics` | `@consumptionRate` | `neda.partition.consumption_rate` | `@topic`, `@partition` |

Lag is deliberately not generated here: it comes from the OpenTelemetry `neda.partition.lag` gauge, and a
log-based metric under that name would collide with it. A service that runs without a `MeterProvider` can
still generate a lag distribution from `@lag` the same way, under any other name.

Query the distributions with `avg`. Because a tick may re-log an unrefreshed record, duplicates appear as
extra identical samples — harmless to `avg`, which is exactly why the payload carries normalized rates
rather than raw deltas (a duplicate delta summed as a count would double-count).

`service` and `env` come from the envelope, so you can scope any dashboard by them without adding them
as group-bys here. Add `@consumerGroupId` to the group-bys only if multiple consumer groups read the
same topics within one Datadog org — otherwise `service` already separates them and the extra
cardinality is wasted. Keep group-bys bounded — Datadog bills log-based metrics by cardinality.

#### 4. Filter out stale points

A partition whose consumer has wedged keeps reporting its last known figures. Exclude them in the
metric's filter query (requires the `@sampleAgeMs` measure from step 2):

```
@logType:n-eda.partition-metrics @sampleAgeMs:<90000
```

Tune the bound to roughly 1.5× your configured interval. `sampleAgeMs` is precomputed precisely
because log query languages cannot do date arithmetic.

#### 5. Alerting

Prefer a **monitor on the OpenTelemetry `neda.partition.lag` gauge** over alerting on log text — it gives
you per-partition thresholds, and recovery detection.

n-eda also logs a warning line when a partition exceeds a built-in lag threshold of 1000 batches
(`maxAcceptableLag` in `metrics-reporter.ts`). That line is human-readable and carries **no** `logType`,
so it never reaches the metrics pipeline above. It is there for someone tailing logs; the monitor is
the real alerting path.

### Suggested dashboard

- **Lag by partition** — the OpenTelemetry `neda.partition.lag` gauge, max, grouped by
  `messaging.destination.name`/`messaging.destination.partition.id`. A partition climbing while others stay
  flat is a stuck consumer or a hot partition key.
- **Production vs consumption** — the two rates overlaid per topic. Consumption below production is
  the leading indicator; lag is the lagging one.
- **Idle partitions** — count of partitions where both rates are `0`. Expected for sparse topics;
  suspicious for busy ones, and often the sign of a partition-affinity misconfiguration.

## Related

- [known-issues.md](known-issues.md) — open defects, including the inactive `consumer-profiler.ts`
- [../ARCHITECTURE.md](../ARCHITECTURE.md) — the consume loop that produces these figures
