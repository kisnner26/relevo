import { test } from 'node:test';
import assert from 'node:assert/strict';
import { LruCache } from '../src/cache.js';

test('evicts the least recently used entry when full', () => {
  const cache = new LruCache({ max: 2 });
  cache.set('a', 1);
  cache.set('b', 2);
  cache.get('a');
  cache.set('c', 3);
  assert.equal(cache.get('b'), undefined);
  assert.equal(cache.get('a'), 1);
  assert.equal(cache.get('c'), 3);
});

test('entries expire after the ttl', () => {
  let now = 1000;
  const cache = new LruCache({ ttlMs: 500, now: () => now });
  cache.set('a', 1);
  now = 1499;
  assert.equal(cache.get('a'), 1);
  now = 1500;
  assert.equal(cache.get('a'), undefined);
  assert.equal(cache.stats().size, 0);
});

test('overwriting a key refreshes its value and position', () => {
  const cache = new LruCache({ max: 2 });
  cache.set('a', 1);
  cache.set('b', 2);
  cache.set('a', 10);
  cache.set('c', 3);
  assert.equal(cache.get('a'), 10);
  assert.equal(cache.get('b'), undefined);
});

test('tracks hits, misses and hit rate', () => {
  const cache = new LruCache();
  cache.set('a', 1);
  cache.get('a');
  cache.get('a');
  cache.get('zzz');
  assert.deepEqual(cache.stats(), { size: 1, max: 500, hits: 2, misses: 1, hitRate: 2 / 3 });
});
