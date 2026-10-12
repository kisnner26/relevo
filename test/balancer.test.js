import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { WebSocket } from 'ws';
import { start } from '../src/server.js';
import { createBalancer } from '../src/balancer.js';

let apps;
let balancer;
let base;

before(async () => {
  apps = await Promise.all(['w0', 'w1', 'w2'].map((instance) => start({ port: 0, instance, poolSize: 2 })));
  balancer = createBalancer({
    targets: apps.map((a, i) => ({ instance: `w${i}`, host: '127.0.0.1', port: a.port })),
    healthIntervalMs: 100,
  });
  base = `http://127.0.0.1:${await balancer.listen()}`;
});
after(async () => {
  await balancer.close();
  await Promise.all(apps.map((a) => a.close()));
});

const call = async (method, path, body) => {
  const res = await fetch(base + path, { method, headers: { 'content-type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body) });
  return { status: res.status, body: await res.json() };
};
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

test('spreads simultaneous jobs across instances (least connections)', async () => {
  const created = await Promise.all([0, 1, 2, 3, 4, 5].map((i) => call('POST', '/jobs', { type: 'sleep', params: { ms: 150 + i } })));
  const used = new Set(created.map((r) => r.body.instance));
  assert.equal(used.size, 3);
  await wait(400);
});

test('routes a job lookup to the instance that owns it', async () => {
  const created = await call('POST', '/jobs', { type: 'fib', params: { n: 19 } });
  const id = created.body.id;
  for (let i = 0; i < 5; i++) {
    const got = await call('GET', `/jobs/${id}`);
    assert.equal(got.status, 200);
    assert.equal(got.body.instance, created.body.instance);
  }
});

test('routes graphql queries that mention a job id to its owner', async () => {
  const created = await call('POST', '/jobs', { type: 'fib', params: { n: 20 } });
  const res = await call('POST', '/graphql', { query: `{ job(id: "${created.body.id}") { id instance } }` });
  assert.equal(res.body.data.job.instance, created.body.instance);
});

test('aggregates /jobs and /metrics across instances', async () => {
  const jobs = await call('GET', '/jobs?limit=500');
  const instances = new Set(jobs.body.jobs.map((j) => j.instance));
  assert.ok(instances.size >= 2);
  const sorted = jobs.body.jobs.every((j, i, all) => i === 0 || all[i - 1].createdAt >= j.createdAt);
  assert.ok(sorted);
  const metrics = await call('GET', '/metrics');
  assert.equal(metrics.body.instances.length, 3);
  assert.equal(metrics.body.balancer.healthy, 3);
});

test('fans in the live feed of every instance over one websocket', async () => {
  const ws = new WebSocket(`ws://${base.slice(7)}/ws`);
  const seen = new Set();
  ws.on('message', (raw) => {
    const msg = JSON.parse(raw.toString());
    if (msg.type === 'job') seen.add(msg.job.instance);
  });
  await new Promise((resolve) => ws.once('open', resolve));
  await wait(300);
  await Promise.all([0, 1, 2, 3, 4, 5].map((i) => call('POST', '/jobs', { type: 'sleep', params: { ms: 50 + i } })));
  await wait(500);
  assert.equal(seen.size, 3);

  const replies = [];
  ws.on('message', (raw) => replies.push(JSON.parse(raw.toString())));
  ws.send(JSON.stringify({ op: 'enqueue', type: 'fib', params: { n: 12 } }));
  await wait(200);
  assert.ok(replies.some((m) => m.type === 'enqueued'));
  ws.close();
});

test('commands sent right after the socket opens are not lost', async () => {
  const ws = new WebSocket(`ws://${base.slice(7)}/ws`);
  const enqueued = [];
  ws.on('message', (raw) => {
    const msg = JSON.parse(raw.toString());
    if (msg.type === 'enqueued') enqueued.push(msg.job.id);
  });
  await new Promise((resolve) => ws.once('open', resolve));
  for (let i = 0; i < 10; i++) ws.send(JSON.stringify({ op: 'enqueue', type: 'fib', params: { n: 5 + i } }));
  await wait(400);
  assert.equal(enqueued.length, 10);
  ws.close();
});

test('stops sending traffic to an instance that goes down and keeps serving', async () => {
  const down = apps[1];
  await down.close();
  await wait(400);
  const health = await call('GET', '/health');
  assert.equal(health.body.instances.find((i) => i.instance === 'w1').healthy, false);
  const created = await Promise.all([0, 1, 2, 3, 4].map((i) => call('POST', '/jobs', { type: 'fib', params: { n: 10 + i } })));
  assert.ok(created.every((r) => r.status === 202));
  assert.ok(created.every((r) => r.body.instance !== 'w1'));
  const lookup = await call('GET', '/jobs/w1-0123456789');
  assert.equal(lookup.status, 503);
  assert.equal(lookup.body.error.code, 'INSTANCE_DOWN');
});
