import { given } from "@nivinjoseph/n-defensive";
import { ApplicationException, InvalidOperationException, ObjectDisposedException } from "@nivinjoseph/n-exception";
import { Delay, Observer } from "@nivinjoseph/n-util";
import { EdaManager } from "../eda-manager.js";
// import { ConsumerTracer } from "../event-handler-tracer";
import * as otelApi from "@opentelemetry/api";
import { ATTR_MESSAGING_SYSTEM, ATTR_MESSAGING_OPERATION_TYPE, ATTR_MESSAGING_DESTINATION_NAME, ATTR_MESSAGING_DESTINATION_TEMPORARY, ATTR_MESSAGING_MESSAGE_ID, ATTR_MESSAGING_MESSAGE_CONVERSATION_ID } from "@opentelemetry/semantic-conventions/incubating";
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
 * Metrics: every attempt records its duration (`messaging.process.duration`, with `error.type` when it threw),
 * and every final outcome counts once in `neda.event.processed`. For the proxy processors the duration
 * includes the network round trip. Nothing is recorded when disposal interrupts an event, because it replays.
 */
export class Processor {
    _manager;
    // private readonly _consumerTracer: ConsumerTracer | null;
    // private readonly _hasConsumerTracer: boolean;
    _logger;
    _otelMetrics;
    _availabilityObserver = new Observer("available");
    _doneProcessingObserver = new Observer("done-processing");
    _currentWorkItem = null;
    _processPromise = null;
    _isDisposed = false;
    _delayCanceller = null;
    get _isInitialized() {
        return this._availabilityObserver.hasSubscriptions && this._doneProcessingObserver.hasSubscriptions;
    }
    get manager() { return this._manager; }
    get logger() { return this._logger; }
    get availability() { return this._availabilityObserver; }
    get doneProcessing() { return this._doneProcessingObserver; }
    get isBusy() { return this._currentWorkItem != null; }
    constructor(manager) {
        given(manager, "manager").ensureHasValue().ensureIsObject().ensureIsType(EdaManager);
        this._manager = manager;
        // this._consumerTracer = this._manager.consumerTracer;
        // this._hasConsumerTracer = this._consumerTracer != null;
        this._logger = this._manager.serviceLocator.resolve("Logger");
        this._otelMetrics = this._manager.otelMetrics;
    }
    process(workItem) {
        if (!this._isInitialized || this.isBusy)
            throw new InvalidOperationException("processor not initialized or processor is busy");
        if (this._isDisposed) {
            workItem.deferred.reject(new ObjectDisposedException("Processor"));
            return;
        }
        this._currentWorkItem = workItem;
        this._processPromise = this._process()
            .then(() => {
            const doneWorkItem = this._currentWorkItem;
            this._doneProcessingObserver.notify(doneWorkItem);
            this._currentWorkItem = null;
            if (!this._isDisposed)
                this._availabilityObserver.notify(this);
        })
            .catch((e) => this._logger.logError(e));
    }
    dispose() {
        if (!this._isDisposed) {
            this._isDisposed = true;
            // console.warn("Disposing processor");
        }
        if (this._delayCanceller)
            this._delayCanceller.cancel();
        return this._processPromise?.then(() => {
            // console.warn("Processor disposed");
        }) || Promise.resolve().then(() => {
            // console.warn("Processor disposed");
        });
    }
    async _process() {
        const workItem = this._currentWorkItem;
        const eventType = workItem.eventRegistration.eventTypeName;
        const parentContext = otelApi.trace.setSpan(otelApi.context.active(), workItem.span);
        const tracer = otelApi.trace.getTracer("n-eda");
        const span = tracer.startSpan(`event.${workItem.event.name} process`, {
            kind: otelApi.SpanKind.CONSUMER,
            attributes: {
                [ATTR_MESSAGING_SYSTEM]: "n-eda",
                [ATTR_MESSAGING_OPERATION_TYPE]: "process",
                [ATTR_MESSAGING_DESTINATION_NAME]: `${workItem.topic}+++${workItem.partition}`,
                [ATTR_MESSAGING_DESTINATION_TEMPORARY]: false,
                [ATTR_MESSAGING_MESSAGE_ID]: workItem.event.id,
                [ATTR_MESSAGING_MESSAGE_CONVERSATION_ID]: workItem.event.partitionKey
            }
        }, parentContext);
        // otelApi.trace.setSpan(otelApi.context.active(), span);
        await otelApi.context.with(otelApi.trace.setSpan(otelApi.context.active(), span), async () => {
            const maxProcessAttempts = 10;
            let numProcessAttempts = 0;
            try {
                while (numProcessAttempts < maxProcessAttempts) {
                    if (this._isDisposed) {
                        workItem.deferred.reject(new ObjectDisposedException("Processor"));
                        return;
                    }
                    numProcessAttempts++;
                    const attemptStart = performance.now();
                    try {
                        // await this._logger.logInfo(`Processing event ${workItem.eventName} with id ${workItem.eventId}`);
                        // if (this._hasConsumerTracer)
                        //     await this._consumerTracer!({
                        //         topic: workItem.topic,
                        //         partition: workItem.partition,
                        //         partitionKey: workItem.partitionKey,
                        //         eventName: workItem.eventName,
                        //         eventId: workItem.eventId
                        //     }, () => this.processEvent(workItem));
                        // else
                        //     await this.processEvent(workItem);
                        await this.processEvent(workItem);
                        this._otelMetrics.recordAttemptSuccess(workItem.topic, eventType, (performance.now() - attemptStart) / 1000);
                        workItem.deferred.resolve();
                        return;
                    }
                    catch (error) {
                        const attemptSeconds = (performance.now() - attemptStart) / 1000;
                        // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
                        if (this._isDisposed) {
                            workItem.deferred.reject(new ObjectDisposedException("Processor"));
                            return;
                        }
                        this._otelMetrics.recordAttemptFailure(workItem.topic, eventType, attemptSeconds, error);
                        // if (numProcessAttempts > 8)
                        // {
                        //     await this.logger.logWarning(`Error in EventHandler while handling event of type '${workItem.eventName}' (ATTEMPT = ${numProcessAttempts}) with data ${JSON.stringify(workItem.event.serialize())}.`);
                        //     await this.logger.logWarning(error as Exception);
                        // }
                        await this.logger.logWarning(`Error in EventHandler while handling event of type '${workItem.eventName}' (ATTEMPT = ${numProcessAttempts}) with data ${JSON.stringify(workItem.event.serialize())}.`);
                        await this.logger.logWarning(error);
                        if (numProcessAttempts >= maxProcessAttempts)
                            throw error;
                        else {
                            span.recordException(error);
                            const seconds = (5 + numProcessAttempts) * numProcessAttempts; // [6, 14, 24, 36, 50, 66, 84, 104, 126]
                            span.addEvent("Waiting before retry", {
                                "delay": `${seconds}s`,
                                "attempt": numProcessAttempts
                            });
                            this._delayCanceller = {};
                            await Delay.seconds(seconds, this._delayCanceller);
                            this._delayCanceller = null;
                        }
                    }
                }
            }
            catch (error) {
                this._otelMetrics.recordRetriesExhausted(workItem.topic, eventType);
                span.recordException(error);
                span.addEvent(`Failed to process event of type '${workItem.eventName}'`, {
                    eventData: JSON.stringify(workItem.event.serialize())
                });
                const message = `Failed to process event of type '${workItem.eventName}' with data ${JSON.stringify(workItem.event.serialize())}`;
                span.setStatus({
                    code: otelApi.SpanStatusCode.ERROR,
                    message
                });
                await this._logger.logError(message);
                await this._logger.logError(error);
                workItem.deferred.reject(new ApplicationException(message, error));
            }
            finally {
                span.end();
            }
        });
    }
}
//# sourceMappingURL=processor.js.map