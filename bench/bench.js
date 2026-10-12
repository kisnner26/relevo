import { availableParallelism } from 'node:os';
import { handlers } from '../src/handlers.js';
import { WorkerPool } from '../src/pool.js';
import { LruCache } from '../src/cache.js';
import { JobService } from '../src/service.js';

const JOBS = 16;
const N = 39;
const mode = process.argv[2] ?? 'all';
const params = () => ({ n: N });

// Measures how long the event loop was blocked: the longest gap between 5 ms timer ticks.
function lagProbe() {
  let last = performance.now();
  let max = 0;
  const timer = setInterval(() => {
    const now = performance.now();
    max = Math.max(max, now - last - 5);
    last = now;
  }, 5);
  return () => {
    clearInterval(timer);
    return Math.round(max);
  };
}

async function mainThread() {
  const stop = lagProbe();
  const start = performance.now();
  for (let i = 0; i < JOBS; i++) {
    handlers.fib(params(i), () => {});
    await new Promise((resolve) => setImmediate(resolve));
  }
  return { ms: Math.round(performance.now() - start), lag: stop() };
}

async function pool(size) {
  const workers = new WorkerPool({ size, maxQueue: JOBS });
  await Promise.all(Array.from({ length: workers.stats().size }, () => workers.submit('fib', { n: 5 }).promise));
  const stop = lagProbe();
  const start = performance.now();
  await Promise.all(Array.from({ length: JOBS }, (_, i) => workers.submit('fib', params(i)).promise));
  const result = { ms: Math.round(performance.now() - start), lag: stop(), size: workers.stats().size };
  await workers.close();
  return result;
}

async function singleFlight() {
  const workers = new WorkerPool({ size: 2 });
  const service = new JobService({ pool: workers, cache: new LruCache() });
  const start = performance.now();
  const ids = Array.from({ length: 200 }, () => service.enqueue({ type: 'fib', params: params(0) }).id);
  await new Promise((resolve) => {
    const check = () => ids.every((id) => service.get(id).status === 'done') && resolve();
    service.on('job', check);
    check();
  });
  const afterCache = performance.now();
  for (let i = 0; i < 200; i++) service.enqueue({ type: 'fib', params: params(0) });
  const result = {
    requests: 200,
    executions: workers.stats().completed,
    ms: Math.round(afterCache - start),
    cachedMs: Math.round(performance.now() - afterCache),
  };
  await workers.close();
  return result;
}

if (mode === 'main') {
  console.log(await mainThread());
} else {
  const cores = availableParallelism();
  console.log(`${JOBS} jobs of fib(${N}), ${cores} cores\n`);
  const main = await mainThread();
  const sizes = [...new Set([2, 4, Math.max(1, cores - 1)])].filter((n) => n <= Math.max(1, cores - 1));
  const runs = [];
  for (const size of sizes) runs.push(await pool(size));
  console.log('| mode | total | speedup | longest event loop block |');
  console.log('| --- | --- | --- | --- |');
  console.log(`| main thread | ${main.ms} ms | 1.0x | ${main.lag} ms |`);
  for (const run of runs) console.log(`| worker pool (${run.size} threads) | ${run.ms} ms | ${(main.ms / run.ms).toFixed(1)}x | ${run.lag} ms |`);
  const flight = await singleFlight();
  console.log(`\nsingle flight and cache: ${flight.requests} identical requests ran ${flight.executions} execution(s) in ${flight.ms} ms; ${flight.requests} more were answered from the cache in ${flight.cachedMs} ms`);
}
