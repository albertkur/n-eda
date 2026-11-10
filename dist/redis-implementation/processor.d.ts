import { Logger } from "@nivinjoseph/n-log";
import { Disposable, Observable } from "@nivinjoseph/n-util";
import { EdaManager } from "../eda-manager.js";
import { WorkItem } from "./scheduler.js";
/**
 * Runs one work item at a time, applying the retry policy that defines n-eda's failure semantics.
 *
 * Retry ladder: **10 attempts**, sleeping `(5 + n) * n` seconds between them — `6, 14, 24, 36, 50, 66, 84,
 * 104, 126` — about **8 minutes 30 seconds** in total. On the final failure the work item's deferred is
 * rejected; the consumer then logs, marks the event processed, and moves on. **There is no dead-letter
 * queue.**
 *
 * Note: a failing event holds its partition key locked for the whole ladder, stalling that key's queue and
 * its consumer's batch window. Every attempt logs the full serialized event payload, and the exhaustion path
 * writes it into span attributes too — a consideration if events carry personal data.
 *
 * Subclasses supply the actual dispatch: `DefaultProcessor` runs handlers in-process; the proxy processors
 * forward to Lambda, RPC, or gRPC.
 *
 * Metrics: every attempt records its duration (`neda.event.process.duration`, with `error.type` when it threw),
 * and every final outcome counts once in `neda.event.processed`. For the proxy processors the duration
 * includes the network round trip. Nothing is recorded when disposal interrupts an event, because it replays.
 */
export declare abstract class Processor implements Disposable {
    private readonly _manager;
    private readonly _logger;
    private readonly _otelMetrics;
    private readonly _availabilityObserver;
    private readonly _doneProcessingObserver;
    private _currentWorkItem;
    private _processPromise;
    private _isDisposed;
    private _delayCanceller;
    private get _isInitialized();
    protected get manager(): EdaManager;
    protected get logger(): Logger;
    get availability(): Observable<this>;
    get doneProcessing(): Observable<WorkItem>;
    get isBusy(): boolean;
    constructor(manager: EdaManager);
    process(workItem: WorkItem): void;
    dispose(): Promise<void>;
    protected abstract processEvent(workItem: WorkItem): Promise<void>;
    private _process;
}
//# sourceMappingURL=processor.d.ts.map