import test from 'node:test';
import assert from 'node:assert/strict';
import { createChangefeedClient } from '../dist/esm/index.js';

test('payload node count is aggregate across siblings', async () => {
  const client = createChangefeedClient(
    {
      async loadCheckpoint() {},
      async *read() {
        yield { token: 'x', position: 1, payload: [1, 2, 3] };
      },
      async saveCheckpoint() {
        throw new Error('must not persist');
      },
    },
    { maxPayloadNodes: 3 },
  );
  await assert.rejects(
    client.consume(() => {}),
    { code: 'capacity' },
  );
});

test('cleanup failure retains original handler error', async () => {
  const client = createChangefeedClient({
    async loadCheckpoint() {},
    read() {
      return {
        [Symbol.asyncIterator]() {
          return {
            async next() {
              return { done: false, value: { token: 'x', position: 1, payload: 1 } };
            },
            async return() {
              throw new Error('cleanup');
            },
          };
        },
      };
    },
    async saveCheckpoint() {},
  });
  await assert.rejects(
    client.consume(() => {
      throw new Error('handler');
    }),
    (error) => {
      assert.ok(error instanceof AggregateError);
      assert.equal(error.errors[0].code, 'handler');
      assert.equal(error.errors[1].message, 'cleanup');
      return true;
    },
  );
});
