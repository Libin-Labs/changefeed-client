export type JsonValue =
  null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export interface ChangefeedEvent<T extends JsonValue = JsonValue> {
  token: string;
  position: number;
  payload: T;
}

export interface ChangefeedCheckpoint {
  token: string;
  position: number;
}

export interface ChangefeedSignal {
  readonly aborted: boolean;
}

/** Adapters expose an ordered async iterable and durable checkpoint operations. */
export interface ChangefeedAdapter<T extends JsonValue = JsonValue> {
  loadCheckpoint(): Promise<ChangefeedCheckpoint | undefined>;
  read(
    afterToken: string | undefined,
    signal: ChangefeedSignal | undefined,
  ): AsyncIterable<ChangefeedEvent<T>>;
  saveCheckpoint(checkpoint: ChangefeedCheckpoint): Promise<void>;
}

export interface ChangefeedOptions {
  maxItems?: number;
  maxTokenLength?: number;
  maxPayloadBytes?: number;
  maxPayloadNodes?: number;
}

export interface ConsumeResult {
  processed: number;
  duplicates: number;
  checkpoint: ChangefeedCheckpoint | undefined;
  stoppedByLimit: boolean;
}

export class ChangefeedError extends Error {
  constructor(
    readonly code: 'invalid' | 'capacity' | 'adapter' | 'handler' | 'aborted' | 'ordering',
    message: string,
    readonly phase?: 'load-checkpoint' | 'read' | 'save-checkpoint' | 'handler',
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = 'ChangefeedError';
  }
}

function plain(
  value: unknown,
  label: string,
  allowed?: readonly string[],
): Record<string, unknown> {
  if (
    typeof value !== 'object' ||
    value === null ||
    Array.isArray(value) ||
    Object.getPrototypeOf(value) !== Object.prototype ||
    Object.getOwnPropertySymbols(value).length !== 0
  ) {
    throw new ChangefeedError('invalid', `${label} must be a plain object.`);
  }
  const record = value as Record<string, unknown>;
  if (allowed && Object.keys(record).some((key) => !allowed.includes(key))) {
    throw new ChangefeedError('invalid', `${label} contains unknown fields.`);
  }
  return record;
}

function integer(value: unknown, min: number, max: number, label: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < min || value > max) {
    throw new ChangefeedError('invalid', `${label} must be a safe integer from ${min} to ${max}.`);
  }
  return value;
}

function clonePayload(
  value: unknown,
  limits: { nodes: number; maxNodes: number; depth: number; maxDepth: number },
  depth = 0,
): JsonValue {
  limits.nodes++;
  if (limits.nodes > limits.maxNodes)
    throw new ChangefeedError('capacity', 'Payload exceeds its node limit.');
  if (depth > limits.maxDepth)
    throw new ChangefeedError('capacity', 'Payload exceeds its depth limit.');
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (Array.isArray(value)) {
    return Array.from(value, (entry) => clonePayload(entry, limits, depth + 1));
  }
  if (
    typeof value === 'object' &&
    value !== null &&
    Object.getPrototypeOf(value) === Object.prototype &&
    Object.getOwnPropertySymbols(value).length === 0
  ) {
    const result: Record<string, JsonValue> = {};
    for (const key of Object.keys(value).sort()) {
      const entry = (value as Record<string, unknown>)[key];
      if (entry === undefined)
        throw new ChangefeedError('invalid', 'Payload cannot contain undefined.');
      Object.defineProperty(result, key, {
        value: clonePayload(entry, limits, depth + 1),
        enumerable: true,
        configurable: true,
        writable: true,
      });
    }
    return result;
  }
  throw new ChangefeedError(
    'invalid',
    'Payload must contain only finite JSON values and plain objects.',
  );
}

function parseCheckpoint(value: unknown, maxTokenLength: number): ChangefeedCheckpoint | undefined {
  if (value === undefined) return undefined;
  const raw = plain(value, 'Checkpoint', ['token', 'position']);
  if (
    typeof raw.token !== 'string' ||
    raw.token.length === 0 ||
    raw.token.length > maxTokenLength
  ) {
    throw new ChangefeedError('invalid', 'Checkpoint token has an invalid length.');
  }
  return {
    token: raw.token,
    position: integer(raw.position, 0, Number.MAX_SAFE_INTEGER, 'Checkpoint position'),
  };
}

function parseEvent(
  input: unknown,
  options: { maxTokenLength: number; maxPayloadBytes: number; maxPayloadNodes: number },
): ChangefeedEvent {
  const raw = plain(input, 'Changefeed event', ['token', 'position', 'payload']);
  if (
    typeof raw.token !== 'string' ||
    raw.token.length === 0 ||
    raw.token.length > options.maxTokenLength
  ) {
    throw new ChangefeedError('invalid', 'Event token has an invalid length.');
  }
  if (!Object.hasOwn(raw, 'payload'))
    throw new ChangefeedError('invalid', 'Event payload is required.');
  const payload = clonePayload(raw.payload, {
    nodes: 0,
    maxNodes: options.maxPayloadNodes,
    depth: 0,
    maxDepth: 64,
  });
  if (new TextEncoder().encode(JSON.stringify(payload)).byteLength > options.maxPayloadBytes) {
    throw new ChangefeedError('capacity', 'Event payload exceeds its byte limit.');
  }
  return {
    token: raw.token,
    position: integer(raw.position, 0, Number.MAX_SAFE_INTEGER, 'Event position'),
    payload,
  };
}

function adapterError(
  phase: 'load-checkpoint' | 'read' | 'save-checkpoint',
  cause: unknown,
): ChangefeedError {
  return new ChangefeedError('adapter', `Changefeed adapter failed during ${phase}.`, phase, {
    cause: cause instanceof Error ? cause : undefined,
  });
}

/** Pulls one event at a time: handlers and durable checkpoints apply backpressure to the source. */
export function createChangefeedClient<T extends JsonValue = JsonValue>(
  adapter: ChangefeedAdapter<T>,
  options: ChangefeedOptions = {},
) {
  if (
    !adapter ||
    typeof adapter.loadCheckpoint !== 'function' ||
    typeof adapter.read !== 'function' ||
    typeof adapter.saveCheckpoint !== 'function'
  )
    throw new ChangefeedError('invalid', 'Invalid adapter.');
  plain(options, 'Options', ['maxItems', 'maxTokenLength', 'maxPayloadBytes', 'maxPayloadNodes']);
  const limits = {
    maxItems: integer(options.maxItems ?? 10_000, 1, 100_000, 'maxItems'),
    maxTokenLength: integer(options.maxTokenLength ?? 4096, 1, 65_536, 'maxTokenLength'),
    maxPayloadBytes: integer(
      options.maxPayloadBytes ?? 1_048_576,
      1,
      16_777_216,
      'maxPayloadBytes',
    ),
    maxPayloadNodes: integer(options.maxPayloadNodes ?? 100_000, 1, 1_000_000, 'maxPayloadNodes'),
  };

  return {
    async consume(
      handler: (event: ChangefeedEvent<T>) => void | Promise<void>,
      signal?: ChangefeedSignal,
    ): Promise<ConsumeResult> {
      if (typeof handler !== 'function')
        throw new ChangefeedError('invalid', 'A handler function is required.');
      if (signal && typeof signal.aborted !== 'boolean') {
        throw new ChangefeedError('invalid', 'Signal must expose a boolean aborted property.');
      }
      let checkpoint: ChangefeedCheckpoint | undefined;
      try {
        checkpoint = parseCheckpoint(await adapter.loadCheckpoint(), limits.maxTokenLength);
      } catch (error) {
        if (error instanceof ChangefeedError) throw error;
        throw adapterError('load-checkpoint', error);
      }
      let lastPosition = checkpoint?.position ?? -1;
      let processed = 0;
      let duplicates = 0;
      let items = 0;
      const recentTokens = new Set<string>();
      let iterator: AsyncIterator<unknown>;
      try {
        iterator = adapter.read(checkpoint?.token, signal)[Symbol.asyncIterator]();
      } catch (error) {
        throw adapterError('read', error);
      }
      let stoppedByLimit = false;
      let primary: unknown;
      let failed = false;
      try {
        while (items < limits.maxItems) {
          if (signal?.aborted)
            throw new ChangefeedError('aborted', 'Changefeed consumption was aborted.');
          let next: IteratorResult<unknown>;
          try {
            next = await iterator.next();
          } catch (error) {
            throw adapterError('read', error);
          }
          if (next.done) break;
          items++;
          const event = parseEvent(next.value, limits) as ChangefeedEvent<T>;
          if (event.position < lastPosition || recentTokens.has(event.token)) {
            duplicates++;
            continue;
          }
          if (event.position === lastPosition) {
            if (event.token !== checkpoint?.token) {
              throw new ChangefeedError(
                'ordering',
                'Different events share the same feed position.',
              );
            }
            duplicates++;
            continue;
          }
          if (signal?.aborted)
            throw new ChangefeedError('aborted', 'Changefeed consumption was aborted.');
          try {
            await handler({ ...event });
          } catch (error) {
            throw new ChangefeedError(
              'handler',
              'Changefeed handler failed; the event was not checkpointed.',
              'handler',
              {
                cause: error instanceof Error ? error : undefined,
              },
            );
          }
          const nextCheckpoint = { token: event.token, position: event.position };
          try {
            await adapter.saveCheckpoint(nextCheckpoint);
          } catch (error) {
            throw adapterError('save-checkpoint', error);
          }
          checkpoint = nextCheckpoint;
          lastPosition = event.position;
          recentTokens.add(event.token);
          if (recentTokens.size > limits.maxItems)
            recentTokens.delete(recentTokens.values().next().value!);
          processed++;
        }
        stoppedByLimit = items === limits.maxItems;
        return { processed, duplicates, checkpoint, stoppedByLimit };
      } catch (error) {
        failed = true;
        primary = error;
        throw error;
      } finally {
        if (iterator.return) {
          try {
            await iterator.return();
          } catch (error) {
            if (failed)
              throw new AggregateError(
                [primary, error],
                'Consumption and iterator cleanup failed.',
              );
            throw adapterError('read', error);
          }
        }
      }
    },
  };
}
