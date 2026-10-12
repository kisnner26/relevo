import { test } from 'node:test';
import assert from 'node:assert/strict';
import { WorkerPool } from '../src/pool.js';
import { LruCache } from '../src/cache.js';
import { JobService, NotFoundError } from '../src/service.js';
import { ValidationError } from '../src/jobs.js';

const setup = async (poolOptions, fn) => {
  const pool = new WorkerPool(poolOptions);
  const service = new JobService({ pool, cache: new LruCache(), instance: 't0' });
  try {
    await fn(service, pool);
  } finally {
    await pool.close();
  }
};

const finished = (service, id) =>
  new Promise((resolve) => {
    const check = (job) => {
      if (job.id === id && ['done', 'failed', 'cancelled'].includes(job.status)) {
        service.off('job', check);
        resolve(job);
      }
    };
    service.on('job', check);
    const now = service.get(id);
    if (['done', 'failed', 'cancelled'].includes(now.status)) resolve(now);
  });

test('enqueue runs the job and emits status changes', () =>
  setup({ size: 1 }, async (service) => {
    const seen = [];
    service.on('job', (j) => seen.push(j.status));
    const job = service.enqueue({ type: 'fib', params: { n: 15 } });
    assert.match(job.id, /^t0-[0-9a-f]{10}$/);
    const done = await finished(service, job.id);
    assert.deepEqual(done.result, { n: 15, value: 610 });
    assert.deepEqual([...new Set(seen)], ['queued', 'running', 'done']);
  }));

test('a repeated job is answered from the cache without touching the pool', () =>
  setup({ size: 1 }, async (service, pool) => {
    const first = service.enqueue({ type: 'fib', params: { n: 22 } });
    await finished(service, first.id);
    const before = pool.stats().completed;
    const again = service.enqueue({ type: 'fib', params: { n: 22 } });
    assert.equal(again.status, 'done');
    assert.equal(again.cached, true);
    assert.equal(pool.stats().completed, before);
  }));

test('identical jobs in flight share a single pool task', () =>
  setup({ size: 2 }, async (service, pool) => {
    const a = service.enqueue({ type: 'sleep', params: { ms: 80 } });
    const b = service.enqueue({ type: 'sleep', params: { ms: 80 } });
    assert.equal(b.shared, true);
    const [ra, rb] = await Promise.all([finished(service, a.id), finished(service, b.id)]);
    assert.equal(ra.status, 'done');
    assert.equal(rb.status, 'done');
    assert.equal(pool.stats().completed, 1);
  }));

test('cancelling one of two sharers keeps the task alive for the other', () =>
  setup({ size: 1 }, async (service) => {
    const a = service.enqueue({ type: 'sleep', params: { ms: 120 } });
    const b = service.enqueue({ type: 'sleep', params: { ms: 120 } });
    assert.equal(service.cancel(a.id).status, 'cancelled');
    assert.equal((await finished(service, b.id)).status, 'done');
  }));

test('cancelling the last waiter kills the task', () =>
  setup({ size: 1 }, async (service, pool) => {
    const a = service.enqueue({ type: 'sleep', params: { ms: 5000 } });
    await new Promise((r) => setTimeout(r, 30));
    service.cancel(a.id);
    await new Promise((r) => setTimeout(r, 30));
    assert.equal(pool.stats().cancelled, 1);
    assert.equal(service.get(a.id).status, 'cancelled');
  }));

test('failed jobs are not cached', () =>
  setup({ size: 1, timeoutMs: 60 }, async (service) => {
    const a = service.enqueue({ type: 'sleep', params: { ms: 2000 } });
    const done = await finished(service, a.id);
    assert.equal(done.status, 'failed');
    assert.equal(done.error.code, 'TIMEOUT');
    const b = service.enqueue({ type: 'sleep', params: { ms: 2000 } });
    assert.equal(b.cached, false);
    service.cancel(b.id);
  }));

test('validates input and reports unknown ids', () =>
  setup({ size: 1 }, async (service) => {
    assert.throws(() => service.enqueue({ type: 'fib', params: { n: 99 } }), ValidationError);
    assert.throws(() => service.enqueue({ type: 'bogus', params: {} }), ValidationError);
    assert.throws(() => service.get('missing'), NotFoundError);
  }));
