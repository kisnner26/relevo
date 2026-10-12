import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { WebSocket } from 'ws';
import { start } from '../src/server.js';

let app;
let base;
before(async () => {
  app = await start({ port: 0, instance: 'h0', poolSize: 2, maxQueue: 4 });
  base = `http://127.0.0.1:${app.port}`;
});
after(() => app.close());

const call = async (method, path, body) => {
  const res = await fetch(base + path, {
    method,
    headers: { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { status: res.status, body: await res.json() };
};

const poll = async (id) => {
  for (let i = 0; i < 100; i++) {
    const { body } = await call('GET', `/jobs/${id}`);
    if (['done', 'failed', 'cancelled'].includes(body.status)) return body;
    await new Promise((r) => setTimeout(r, 20));
  }
  throw new Error('job did not finish');
};

test('REST: enqueue, poll and read the result', async () => {
  const created = await call('POST', '/jobs', { type: 'primes', params: { limit: 100 } });
  assert.equal(created.status, 202);
  const job = await poll(created.body.id);
  assert.deepEqual(job.result, { limit: 100, count: 25, last: 97 });
});

test('REST: validation errors are 400, unknown ids 404, bad json 400', async () => {
  assert.equal((await call('POST', '/jobs', { type: 'fib', params: { n: -1 } })).status, 400);
  assert.equal((await call('GET', '/jobs/nope')).status, 404);
  const res = await fetch(base + '/jobs', { method: 'POST', body: '{oops' });
  assert.equal(res.status, 400);
});

test('REST: a full queue answers 429', async () => {
  const sleeps = [];
  for (let i = 0; i < 8; i++) sleeps.push(await call('POST', '/jobs', { type: 'sleep', params: { ms: 100 + i } }));
  const accepted = sleeps.filter((r) => r.status === 202);
  for (const r of accepted) await poll(r.body.id);
  assert.equal(accepted.length, 6, 'two running plus four queued');
  assert.equal(sleeps.filter((r) => r.status === 429).length, 2);
});

test('REST: DELETE cancels a queued job', async () => {
  const fill = [];
  for (let i = 0; i < 2; i++) fill.push(await call('POST', '/jobs', { type: 'sleep', params: { ms: 200 + i } }));
  const queued = await call('POST', '/jobs', { type: 'sleep', params: { ms: 250 } });
  const cancelled = await call('DELETE', `/jobs/${queued.body.id}`);
  assert.equal(cancelled.body.status, 'cancelled');
  for (const r of fill) await poll(r.body.id);
});

test('GraphQL: mutation enqueues and query reads it back', async () => {
  const mutation = await call('POST', '/graphql', {
    query: 'mutation($p: JSON) { enqueue(type: fib, params: $p) { id status } }',
    variables: { p: { n: 18 } },
  });
  const { id } = mutation.body.data.enqueue;
  await poll(id);
  const query = await call('POST', '/graphql', {
    query: 'query($id: String!) { job(id: $id) { status result type } stats { instance pool } }',
    variables: { id },
  });
  assert.equal(query.body.data.job.status, 'done');
  assert.deepEqual(query.body.data.job.result, { n: 18, value: 2584 });
  assert.equal(query.body.data.stats.instance, 'h0');
});

test('GraphQL: errors are returned in the standard errors field', async () => {
  const res = await call('POST', '/graphql', { query: '{ job(id: "missing") { id } }' });
  assert.equal(res.status, 200);
  assert.match(res.body.errors[0].message, /not found/);
  const bad = await call('POST', '/graphql', { query: 'mutation { enqueue(type: fib, params: {n: 999}) { id } }' });
  assert.match(bad.body.errors[0].message, /between 0 and 45/);
});

test('WebSocket: snapshot on connect, live job events, enqueue over the socket', async () => {
  const ws = new WebSocket(`ws://127.0.0.1:${app.port}/ws`);
  const messages = [];
  ws.on('message', (raw) => messages.push(JSON.parse(raw.toString())));
  await new Promise((resolve) => ws.once('open', resolve));
  await new Promise((r) => setTimeout(r, 50));
  assert.equal(messages[0].type, 'hello');
  assert.equal(messages[1].type, 'snapshot');

  ws.send(JSON.stringify({ op: 'enqueue', type: 'fib', params: { n: 25 } }));
  for (let i = 0; i < 100 && !messages.some((m) => m.type === 'job' && m.job.status === 'done'); i++) {
    await new Promise((r) => setTimeout(r, 20));
  }
  const statuses = messages.filter((m) => m.type === 'job').map((m) => m.job.status);
  assert.ok(statuses.includes('queued') && statuses.includes('done'));
  assert.ok(messages.some((m) => m.type === 'enqueued'));

  ws.send(JSON.stringify({ op: 'enqueue', type: 'fib', params: { n: 'x' } }));
  await new Promise((r) => setTimeout(r, 50));
  assert.equal(messages.at(-1).type, 'error');
  ws.close();
});

test('/metrics exposes pool, cache and event loop numbers', async () => {
  const { body } = await call('GET', '/metrics');
  assert.equal(body.instance, 'h0');
  assert.equal(body.pool.size, 2);
  assert.ok(body.eventLoop.p99 >= 0);
  assert.ok('hitRate' in body.cache);
});
