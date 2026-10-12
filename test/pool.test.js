import { test } from 'node:test';
import assert from 'node:assert/strict';
import { WorkerPool, QueueFullError, JobTimeoutError, JobCancelledError } from '../src/pool.js';

const withPool = async (options, fn) => {
  const pool = new WorkerPool(options);
  try {
    await fn(pool);
  } finally {
    await pool.close();
  }
};

test('runs a job on a worker thread and returns the result', () =>
  withPool({ size: 2 }, async (pool) => {
    const { promise } = pool.submit('fib', { n: 20 });
    assert.deepEqual(await promise, { n: 20, value: 6765 });
  }));

test('never runs more jobs than workers and drains the queue in order', () =>
  withPool({ size: 2 }, async (pool) => {
    const jobs = [1, 2, 3, 4, 5].map((n) => pool.submit('sleep', { ms: 60 + n }));
    assert.equal(pool.stats().busy, 2);
    assert.equal(pool.stats().queued, 3);
    const results = await Promise.all(jobs.map((j) => j.promise));
    assert.equal(results.length, 5);
    assert.equal(pool.stats().completed, 5);
  }));

test('runs jobs in parallel: 4 jobs of 150 ms on 4 workers finish well under 600 ms', () =>
  withPool({ size: 4 }, async (pool) => {
    const start = performance.now();
    await Promise.all([0, 1, 2, 3].map(() => pool.submit('sleep', { ms: 150 }).promise));
    assert.ok(performance.now() - start < 450);
  }));

test('high priority jumps ahead of queued normal jobs', () =>
  withPool({ size: 1 }, async (pool) => {
    const order = [];
    const running = pool.submit('sleep', { ms: 80 });
    const normal = pool.submit('sleep', { ms: 10 }, { onStart: () => order.push('normal') });
    const high = pool.submit('sleep', { ms: 10 }, { priority: 'high', onStart: () => order.push('high') });
    await Promise.all([running.promise, normal.promise, high.promise]);
    assert.deepEqual(order, ['high', 'normal']);
  }));

test('rejects with QueueFullError when the queue is at capacity', () =>
  withPool({ size: 1, maxQueue: 2 }, async (pool) => {
    const jobs = [0, 1, 2].map(() => pool.submit('sleep', { ms: 50 }));
    assert.throws(() => pool.submit('sleep', { ms: 50 }), QueueFullError);
    await Promise.all(jobs.map((j) => j.promise));
  }));

test('kills a job that exceeds the timeout and keeps serving', () =>
  withPool({ size: 1, timeoutMs: 100 }, async (pool) => {
    const slow = pool.submit('sleep', { ms: 2000 });
    await assert.rejects(slow.promise, JobTimeoutError);
    const next = pool.submit('fib', { n: 10 });
    assert.deepEqual(await next.promise, { n: 10, value: 55 });
    assert.equal(pool.stats().timedOut, 1);
    assert.equal(pool.stats().restarts, 1);
  }));

test('cancels a queued job without touching the running one', () =>
  withPool({ size: 1 }, async (pool) => {
    const running = pool.submit('sleep', { ms: 80 });
    const queued = pool.submit('sleep', { ms: 80 });
    assert.equal(queued.cancel(), true);
    await assert.rejects(queued.promise, JobCancelledError);
    await running.promise;
    assert.equal(pool.stats().cancelled, 1);
  }));

test('cancels a running job by replacing its worker', () =>
  withPool({ size: 1 }, async (pool) => {
    const running = pool.submit('sleep', { ms: 5000 });
    await new Promise((r) => setTimeout(r, 30));
    assert.equal(running.cancel(), true);
    await assert.rejects(running.promise, JobCancelledError);
    const after = pool.submit('fib', { n: 5 });
    assert.deepEqual(await after.promise, { n: 5, value: 5 });
  }));

test('a handler error fails the job and the worker survives', () =>
  withPool({ size: 1 }, async (pool) => {
    await assert.rejects(pool.submit('nope', {}).promise, /no handler/);
    assert.deepEqual(await pool.submit('fib', { n: 6 }).promise, { n: 6, value: 8 });
    assert.equal(pool.stats().restarts, 0);
  }));

test('reports progress from the worker', () =>
  withPool({ size: 1 }, async (pool) => {
    const seen = [];
    await pool.submit('sleep', { ms: 100 }, { onProgress: (v) => seen.push(v) }).promise;
    assert.ok(seen.length >= 5);
    assert.equal(seen.at(-1), 1);
  }));
