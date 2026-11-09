import { Logger } from "@nivinjoseph/n-log";
import { Duration } from "@nivinjoseph/n-util";
import { Broker } from "../../src/redis-implementation/broker.js";
import { Consumer } from "../../src/redis-implementation/consumer.js";
import { Processor } from "../../src/redis-implementation/processor.js";
import { Topic } from "../../src/topic.js";


/** A `Logger` that discards everything, for suites that assert on behavior rather than log output. */
export const silentLogger: Logger = {
    logDebug: () => Promise.resolve(),
    logInfo: () => Promise.resolve(),
    logWarning: () => Promise.resolve(),
    logError: () => Promise.resolve()
};

/**
 * A `Broker` for exercising `report()` and `metrics` in memory, without Redis.
 *
 * The broker only requires its consumers and processors to be non-empty arrays of matching length, and the
 * scheduler only subscribes to a processor's two observables — hence the stubs. Nothing here ever routes.
 *
 * @param topicName - the topic the broker reports under
 * @param numPartitions - the topic's partition count. `report()` does not check partition numbers against it,
 * so set it to match the partitions a test reports on, to keep the fixture honest.
 */
export function createStubBroker(topicName = "metrics-test", numPartitions = 1): Broker
{
    const topic = new Topic(topicName, Duration.fromMinutes(10), numPartitions);

    const processor = {
        availability: { subscribe: (): void => { /* never fires in these tests */ } },
        doneProcessing: { subscribe: (): void => { /* never fires in these tests */ } }
    } as unknown as Processor;

    return new Broker(topic, [{} as unknown as Consumer], [processor]);
}
