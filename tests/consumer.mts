import { createChangefeedClient, ChangefeedError } from '@lp-labs/changefeed-client';
const publicAPI = [createChangefeedClient, ChangefeedError];
const apiCount: number = publicAPI.length;
void apiCount;
const client = createChangefeedClient({
  async loadCheckpoint() {
    return undefined;
  },
  async *read() {
    yield { token: 'a', position: 1, payload: 'text' };
  },
  async saveCheckpoint() {},
});
void client.consume((event) => {
  const payload: string = event.payload;
  void payload;
});
