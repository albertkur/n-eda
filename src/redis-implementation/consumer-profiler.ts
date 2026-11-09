// INACTIVE — this file is commented out in its entirety and is unwired. Lag and throughput are reported by
// metrics-reporter.ts (log lines, behind `EdaManager.enableMetrics`) and metrics/eda-metrics.ts
// (OpenTelemetry). Kept for reference only.

// import { given } from "@nivinjoseph/n-defensive";
// import { ProfilerTrace, Profiler } from "@nivinjoseph/n-util";


// export class ConsumerProfiler
// {
//     private readonly _eventTraces = new Array<ProfilerTrace>();
//     private readonly _eventProcessings: { [name: string]: number; } = {};
//     private readonly _eventRetries: { [name: string]: number; } = {};
//     private readonly _eventFailures: { [name: string]: number; } = {};

//     private _fetchPartitionWriteIndexProfiler: Profiler | null = null;
//     private _fetchConsumerPartitionReadIndexProfiler: Profiler | null = null;
//     private _incrementConsumerPartitionReadIndexProfiler: Profiler | null = null;
//     private _retrieveEventProfiler: Profiler | null = null;
//     private _batchRetrieveEventsProfiler: Profiler | null = null;
//     private _decompressEventProfiler: Profiler | null = null;
//     private _deserializeEventProfiler: Profiler | null = null;
//     private _eventProfiler: { name: string; id: string; profiler: Profiler } | null = null;
    
    
//     private static readonly _eventQueuePressure = new Array<{ time: number, count: number }>();
//     private static _startTime: number;
//     private static _eventQueuePressureInterval: NodeJS.Timeout;

    
//     public static initialize()
//     {
//         ConsumerProfiler._startTime = Date.now();
        
//         ConsumerProfiler.trackEventQueuePressure();
        
//         ConsumerProfiler._eventQueuePressureInterval = setInterval(() => ConsumerProfiler.trackEventQueuePressure(),
//             60000);
//     }
    
//     private static trackEventQueuePressure(): void
//     {
//         const handleCount = (<any>process)._getActiveHandles().length;
//         ConsumerProfiler._eventQueuePressure.push({ time: Date.now(), count: handleCount });
//     }
    

//     public fetchPartitionWriteIndexStarted(): void
//     {
//         this._fetchPartitionWriteIndexProfiler = new Profiler("fetchPartitionWriteIndex");
//     }

//     public fetchPartitionWriteIndexEnded(): void
//     {
//         given(this, "this")
//             .ensure(t => t._fetchPartitionWriteIndexProfiler != null);

//         this._fetchPartitionWriteIndexProfiler!.trace("$fetchPartitionWriteIndex");
//         this._eventTraces.push(this._fetchPartitionWriteIndexProfiler!.traces[1]);
//         this._fetchPartitionWriteIndexProfiler = null;
//     }

//     public fetchConsumerPartitionReadIndexStarted(): void
//     {
//         this._fetchConsumerPartitionReadIndexProfiler = new Profiler("fetchConsumerPartitionReadIndex");
//     }

//     public fetchConsumerPartitionReadIndexEnded(): void
//     {
//         given(this, "this")
//             .ensure(t => t._fetchConsumerPartitionReadIndexProfiler != null);

//         this._fetchConsumerPartitionReadIndexProfiler!.trace("$fetchConsumerPartitionReadIndex");
//         this._eventTraces.push(this._fetchConsumerPartitionReadIndexProfiler!.traces[1]);
//         this._fetchConsumerPartitionReadIndexProfiler = null;
//     }

//     public incrementConsumerPartitionReadIndexStarted(): void
//     {
//         this._incrementConsumerPartitionReadIndexProfiler = new Profiler("incrementConsumerPartitionReadIndex");
//     }

//     public incrementConsumerPartitionReadIndexEnded(): void
//     {
//         given(this, "this")
//             .ensure(t => t._incrementConsumerPartitionReadIndexProfiler != null);

//         this._incrementConsumerPartitionReadIndexProfiler!.trace("$incrementConsumerPartitionReadIndex");
//         this._eventTraces.push(this._incrementConsumerPartitionReadIndexProfiler!.traces[1]);
//         this._incrementConsumerPartitionReadIndexProfiler = null;
//     }

//     public retrieveEventStarted(): void
//     {
//         this._retrieveEventProfiler = new Profiler("retrieveEvent");
//     }

//     public retrieveEventEnded(): void
//     {
//         given(this, "this")
//             .ensure(t => t._retrieveEventProfiler != null);

//         this._retrieveEventProfiler!.trace("$retrieveEvent");
//         this._eventTraces.push(this._retrieveEventProfiler!.traces[1]);
//         this._retrieveEventProfiler = null;
//     }

//     public batchRetrieveEventsStarted(): void
//     {
//         this._batchRetrieveEventsProfiler = new Profiler("batchRetrieveEvents");
//     }

//     public batchRetrieveEventsEnded(): void
//     {
//         given(this, "this")
//             .ensure(t => t._batchRetrieveEventsProfiler != null);

//         this._batchRetrieveEventsProfiler!.trace("$batchRetrieveEvents");
//         this._eventTraces.push(this._batchRetrieveEventsProfiler!.traces[1]);
//         this._batchRetrieveEventsProfiler = null;
//     }
    
//     public decompressEventStarted(): void
//     {
//         this._decompressEventProfiler = new Profiler("decompressEvent");
//     }
    
//     public decompressEventEnded(): void
//     {
//         given(this, "this")
//             .ensure(t => t._decompressEventProfiler != null);

//         this._decompressEventProfiler!.trace("$decompressEvent");
//         this._eventTraces.push(this._decompressEventProfiler!.traces[1]);
//         this._decompressEventProfiler = null;
//     }
    
//     public deserializeEventStarted(): void
//     {
//         this._deserializeEventProfiler = new Profiler("deserializeEvent");
//     }

//     public deserializeEventEnded(): void
//     {
//         given(this, "this")
//             .ensure(t => t._deserializeEventProfiler != null);

//         this._deserializeEventProfiler!.trace("$deserializeEvent");
//         this._eventTraces.push(this._deserializeEventProfiler!.traces[1]);
//         this._deserializeEventProfiler = null;
//     }

//     public eventProcessingStarted(eventName: string, eventId: string): void
//     {
//         given(eventName, "eventName").ensureHasValue().ensureIsString();
//         given(eventId, "eventId").ensureHasValue().ensureIsString();

//         this._eventProfiler = { name: eventName, id: eventId, profiler: new Profiler("eventProcessing") };

//         if (!this._eventProcessings[eventName])
//             this._eventProcessings[eventName] = 0;

//         this._eventProcessings[eventName]++;
//     }

//     public eventProcessingEnded(eventName: string, eventId: string): void
//     {
//         given(eventName, "eventName").ensureHasValue().ensureIsString();
//         given(eventId, "eventId").ensureHasValue().ensureIsString();
//         given(this, "this")
//             .ensure(t => t._eventProfiler != null && t._eventProfiler.name === eventName && t._eventProfiler.id === eventId);

//         this._eventProfiler!.profiler.trace(eventName);
//         this._eventTraces.push(this._eventProfiler!.profiler.traces[1]);
//         this._eventProfiler = null;
//     }

//     public eventRetried(eventName: string): void
//     {
//         given(eventName, "eventName").ensureHasValue().ensureIsString();

//         if (!this._eventRetries[eventName])
//             this._eventRetries[eventName] = 0;

//         this._eventRetries[eventName]++;
//     }

//     public eventFailed(eventName: string): void
//     {
//         given(eventName, "eventName").ensureHasValue().ensureIsString();

//         if (!this._eventFailures[eventName])
//             this._eventFailures[eventName] = 0;

//         this._eventFailures[eventName]++;
//     }
    
    
//     public static aggregate(consumerName: string , consumerProfilers: ReadonlyArray<ConsumerProfiler>): void
//     {
//         given(consumerName, "consumerName").ensureHasValue().ensureIsString();
//         given(consumerProfilers, "consumerProfilers").ensureHasValue().ensureIsArray().ensure(t => t.length > 0);
        
//         const endTime = Date.now();
        
//         clearInterval(ConsumerProfiler._eventQueuePressureInterval);
//         ConsumerProfiler.trackEventQueuePressure();
        
//         const eventTraces = new Array<ProfilerTrace>();
//         const eventProcessings: { [name: string]: number; } = { };
//         const eventRetries: { [name: string]: number; } = { };
//         const eventFailures: { [name: string]: number; } = {};
        
//         consumerProfilers.forEach((profiler) =>
//         {
//             eventTraces.push(...profiler._eventTraces);
            
//             Object.entries(profiler._eventProcessings).forEach((entry) =>
//             {
//                 const key = entry[0];
//                 const value = entry[1];
                
//                 if (eventProcessings[key])
//                     eventProcessings[key] += value;
//                 else
//                     eventProcessings[key] = value;
//             });
            
//             Object.entries(profiler._eventRetries).forEach((entry) =>
//             {
//                 const key = entry[0];
//                 const value = entry[1];

//                 if (eventRetries[key])
//                     eventRetries[key] += value;
//                 else
//                     eventRetries[key] = value;
//             });
            
//             Object.entries(profiler._eventFailures).forEach((entry) =>
//             {
//                 const key = entry[0];
//                 const value = entry[1];

//                 if (eventFailures[key])
//                     eventFailures[key] += value;
//                 else
//                     eventFailures[key] = value;
//             });          
//         });
        
//         let totalEventCount = 0;
//         let totalEventsProcessingTime = 0;
//         let totalEventAverage = 0;
//         let groupCount = 0;
//         const messages = new Array<any>();
        
//         eventTraces.groupBy(t => t.message)
//             .forEach((group) =>
//             {
//                 const eventCount = group.values.length;
//                 const eventsProcessingTime = group.values.reduce((acc, t) => acc + t.diffMs, 0);
//                 const eventAverage = eventsProcessingTime / eventCount;

//                 if (!group.key.startsWith("$"))
//                 {
//                     totalEventCount += eventCount;
//                     totalEventsProcessingTime += eventsProcessingTime;
//                     totalEventAverage += eventAverage;
//                     groupCount++;   
//                 }

//                 const diffs = group.values.map(t => t.diffMs).orderBy();

//                 messages.push({
//                     name: group.key,
//                     procesings: eventProcessings[group.key] ?? null,
//                     retries: eventRetries[group.key] ?? null,
//                     failures: eventFailures[group.key] ?? null,
//                     processed: eventCount,
//                     totalPT: eventsProcessingTime,
//                     averagePT: Math.floor(eventAverage),
//                     minPT: Math.min(...diffs),
//                     maxPT: Math.max(...diffs),
//                     medianPT: group.values.length % 2 === 0
//                         ? Math.floor((diffs[(diffs.length / 2) - 1] + diffs[diffs.length / 2]) / 2)
//                         : diffs[Math.floor(diffs.length / 2) - 1]
//                 });
//             });
        
//         console.log(`[${consumerName}] AGGREGATE (does not include $events)`);
//         console.table({
//             startTime: (new Date(ConsumerProfiler._startTime)).toString().split("GMT")[0].trim(),
//             enfTime: (new Date(endTime)).toString().split("GMT")[0].trim(),
//             consumer: consumerName,
//             totalEventsProcessed: totalEventCount,
//             totalPT: totalEventsProcessingTime,
//             averagePT: groupCount === 0 ? 0 : Math.floor(totalEventAverage / groupCount)
//         });
        
//         console.log(`[${consumerName}] DETAILS`);
//         console.table(messages.orderBy(t => t.name));
        
//         console.log(`[${consumerName}] EVENT QUEUE PRESSURE`);
//         console.table(ConsumerProfiler._eventQueuePressure.map(t => ({
//             time: (new Date(t.time)).toString().split("GMT")[0].trim(),
//             count: t.count
//         })));
//     }
// }