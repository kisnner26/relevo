import cluster from 'node:cluster';
import { availableParallelism } from 'node:os';
import { createBalancer } from './balancer.js';
import { start } from './server.js';

const num = (v, fallback) => (v === undefined ? fallback : Number(v));

if (cluster.isPrimary) {
  const count = num(process.env.INSTANCES, Math.max(2, Math.min(4, availableParallelism())));
  const port = num(process.env.PORT, 3000);
  const basePort = num(process.env.BASE_PORT, port + 100);
  const host = process.env.HOST ?? '127.0.0.1';
  const targets = Array.from({ length: count }, (_, i) => ({ instance: `w${i}`, host: '127.0.0.1', port: basePort + i }));
  const workers = new Map();
  let stopping = false;
  let restartDelay = 500;

  const fork = (target) => {
    const worker = cluster.fork({ INSTANCE: target.instance, PORT: String(target.port) });
    workers.set(target.instance, worker);
    worker.on('exit', (code, signal) => {
      if (stopping) return;
      console.error(`[primary] ${target.instance} exited (${signal ?? code}), restarting in ${restartDelay} ms`);
      setTimeout(() => fork(target), restartDelay);
      restartDelay = Math.min(restartDelay * 2, 10_000);
    });
    worker.on('message', (msg) => {
      if (msg === 'listening') restartDelay = 500;
    });
  };
  targets.forEach(fork);

  const balancer = createBalancer({ targets, port, host, healthIntervalMs: 1000 });
  const bound = await balancer.listen();
  console.log(`relevo balancer on http://${host}:${bound} -> ${targets.map((t) => `${t.instance}:${t.port}`).join(', ')}`);

  const shutdown = async () => {
    stopping = true;
    await balancer.close();
    for (const worker of workers.values()) worker.kill('SIGTERM');
    setTimeout(() => process.exit(0), 1500).unref();
  };
  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);
} else {
  const app = await start({
    port: Number(process.env.PORT),
    instance: process.env.INSTANCE,
    poolSize: num(process.env.POOL_SIZE, undefined),
  });
  process.send?.('listening');
  process.once('SIGTERM', async () => {
    await app.close();
    process.exit(0);
  });
}
