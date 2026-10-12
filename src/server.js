import { WorkerPool } from './pool.js';
import { LruCache } from './cache.js';
import { JobService } from './service.js';
import { createMetrics } from './metrics.js';
import { createApp } from './http.js';

export async function start({ port = 3000, host = '127.0.0.1', instance = 'i0', poolSize, maxQueue, timeoutMs } = {}) {
  const pool = new WorkerPool({ size: poolSize, maxQueue, timeoutMs });
  const service = new JobService({ pool, cache: new LruCache(), instance });
  const metrics = createMetrics({ service });
  const app = createApp({ service, metrics });
  await new Promise((resolve, reject) => {
    app.server.once('error', reject);
    app.server.listen(port, host, resolve);
  });
  const address = app.server.address();
  return {
    port: address.port,
    service,
    pool,
    metrics,
    async close() {
      await app.close();
      await pool.close();
      metrics.stop();
    },
  };
}
