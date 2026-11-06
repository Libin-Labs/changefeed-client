# @lp-labs/changefeed-client

Ordered pull-based changefeed consumption with durable adapter checkpoints.

Original independent MIT TypeScript implementation with **zero runtime/peer dependencies**. ESM and CommonJS with separate declarations, Node 18+ (browser use when adapters/APIs are available). Development dependencies are not consumer runtime dependencies.

## Install

```sh
npm install @lp-labs/changefeed-client
```

## Complete example

```js
import assert from 'node:assert/strict';
import { createChangefeedClient, ChangefeedError } from '@lp-labs/changefeed-client';
let checkpoint;
const seen = [];
const client = createChangefeedClient({
  async loadCheckpoint() {
    return checkpoint;
  },
  async *read() {
    yield { token: 't1', position: 1, payload: { name: 'A' } };
    yield { token: 't1', position: 1, payload: { name: 'A' } };
    yield { token: 't2', position: 2, payload: null };
  },
  async saveCheckpoint(value) {
    checkpoint = value;
  },
});
const result = await client.consume((event) => {
  seen.push(event.position);
});
assert.deepEqual(seen, [1, 2]);
assert.equal(result.duplicates, 1);
assert.equal(checkpoint.token, 't2');
```

CommonJS exposes the same API: `const api = require('@lp-labs/changefeed-client')`.

## Scope, customization and performance

Adapter must supply monotone integer positions and stable tokens. One consume call per adapter at a time. Handler finishes before checkpoint persistence and before pulling another event; failures can replay effects, so handlers require idempotency. Earlier positions are suppressed, equal position with a different token errors. maxItems counts pulled records (default 10,000); stoppedByLimit does not prove another record exists. Caller reconnects. Numeric bounds, finite JSON only; no DB driver or exactly-once guarantee.

Configure explicit limits, clocks, policies and adapters through the API below. Error classes and standard TypeError/RangeError reject malformed input; adapter/handler errors propagate. Await asynchronous methods and handle rejection in your application; no success-shaped fallback on failed work. Inputs must be trusted ordinary data, not hostile proxies; adapters must meet documented atomicity, isolation and ownership contracts. This package never publishes data, loads external code or starts background services at import time.

## API reference

The complete exported TypeScript contract (including option defaults discussed above, method return shapes, and error codes) is included below. TypeScript users import interfaces using `import type`; JavaScript users pass matching ordinary objects. Signature optionals indicate intentionally optional fields, not silent recovery.

```ts
export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | {
      [key: string]: JsonValue;
    };
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
export declare class ChangefeedError extends Error {
  readonly code: 'invalid' | 'capacity' | 'adapter' | 'handler' | 'aborted' | 'ordering';
  readonly phase?: 'load-checkpoint' | 'read' | 'save-checkpoint' | 'handler' | undefined;
  constructor(
    code: 'invalid' | 'capacity' | 'adapter' | 'handler' | 'aborted' | 'ordering',
    message: string,
    phase?: 'load-checkpoint' | 'read' | 'save-checkpoint' | 'handler' | undefined,
    options?: ErrorOptions,
  );
}
/** Pulls one event at a time: handlers and durable checkpoints apply backpressure to the source. */
export declare function createChangefeedClient<T extends JsonValue = JsonValue>(
  adapter: ChangefeedAdapter<T>,
  options?: ChangefeedOptions,
): {
  consume(
    handler: (event: ChangefeedEvent<T>) => void | Promise<void>,
    signal?: ChangefeedSignal,
  ): Promise<ConsumeResult>;
};
```

## Support

If this library is useful to you, consider supporting its maintenance with a [donation via PayPal](https://www.paypal.me/LibinPrasanth).

## Author

Created by [Libin Prasanth](https://www.linkedin.com/in/libinprasanth/). Connect on LinkedIn for updates and discussion.
