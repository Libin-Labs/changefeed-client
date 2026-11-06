import test from 'node:test';
import assert from 'node:assert/strict';
import { createChangefeedClient, ChangefeedError } from '../dist/esm/index.js';

test('documented workflow produces required outputs', async () => {
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
});
test('handler failure does not checkpoint', async () => {
  let saves = 0;
  const c = createChangefeedClient({
    async loadCheckpoint() {},
    async *read() {
      yield { token: 'a', position: 1, payload: 1 };
    },
    async saveCheckpoint() {
      saves++;
    },
  });
  await assert.rejects(
    c.consume(() => {
      throw Error('handler');
    }),
    { code: 'handler' },
  );
  assert.equal(saves, 0);
});
test('same position different token errors', async () => {
  const c = createChangefeedClient({
    async loadCheckpoint() {
      return { token: 'a', position: 1 };
    },
    async *read() {
      yield { token: 'b', position: 1, payload: 1 };
    },
    async saveCheckpoint() {},
  });
  await assert.rejects(
    c.consume(() => {}),
    { code: 'ordering' },
  );
});
test('honors item limit and backpressure', async () => {
  let pulled = 0;
  let handled = false;
  const c = createChangefeedClient(
    {
      async loadCheckpoint() {},
      async *read() {
        pulled++;
        yield { token: 'a', position: 1, payload: 1 };
        assert.equal(handled, true);
        pulled++;
        yield { token: 'b', position: 2, payload: 2 };
      },
      async saveCheckpoint() {},
    },
    { maxItems: 1 },
  );
  const result = await c.consume(() => {
    handled = true;
  });
  assert.equal(pulled, 1);
  assert.equal(result.stoppedByLimit, true);
});
test('rejects nonfinite payload and cancellation', async () => {
  const a = {
    async loadCheckpoint() {},
    async *read() {
      yield { token: 'a', position: 1, payload: NaN };
    },
    async saveCheckpoint() {},
  };
  await assert.rejects(
    createChangefeedClient(a).consume(() => {}),
    { code: 'invalid' },
  );
  await assert.rejects(
    createChangefeedClient(a).consume(() => {}, { aborted: true }),
    { code: 'aborted' },
  );
});
