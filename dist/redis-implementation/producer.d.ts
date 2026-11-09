import { Logger } from "@nivinjoseph/n-log";
import { Redis } from "ioredis";
import { EdaEvent } from "../eda-event.js";
/**
 * Writes events to one partition of one topic.
 *
 * The write path: serialize each event (injecting W3C trace context as `$traceData` and the publish time as
 * `$publishedAt`, epoch ms), deflate the whole batch as a single JSON array, `INCR` the partition's write
 * index to allocate a slot, `SETEX` the compressed blob into that slot with the topic's TTL, and `PUBLISH` a
 * doorbell so an idle consumer wakes immediately.
 *
 * RULE: one write-index slot holds **one entire `publish()` batch**, not one event. Both Redis calls are
 * separately retried with exponential backoff, and because they are distinct round-trips a consumer can
 * observe an allocated index before its payload exists.
 */
export declare class Producer {
    private readonly _edaPrefix;
    private readonly _key;
    private readonly _client;
    private readonly _logger;
    private readonly _topic;
    private readonly _ttlSeconds;
    private readonly _partition;
    get id(): string;
    get writeIndexKey(): string;
    constructor(key: string, client: Redis, logger: Logger, topic: string, ttlMinutes: number, partition: number);
    produce(...events: ReadonlyArray<EdaEvent>): Promise<void>;
    private _compressEvents;
    private _incrementPartitionWriteIndex;
    private _storeEvents;
}
//# sourceMappingURL=producer.d.ts.map