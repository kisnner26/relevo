import { createHash } from 'node:crypto';

// CPU-bound handlers. They run inside worker threads, never on the main thread.

function fibNaive(n) {
  return n < 2 ? n : fibNaive(n - 1) + fibNaive(n - 2);
}

export const handlers = {
  fib({ n }) {
    return { n, value: fibNaive(n) };
  },

  primes({ limit }, progress) {
    const sieve = new Uint8Array(limit + 1);
    const root = Math.floor(Math.sqrt(limit));
    for (let i = 2; i <= root; i++) {
      if (!sieve[i]) for (let j = i * i; j <= limit; j += i) sieve[j] = 1;
      if (i % 1000 === 0) progress(i / root);
    }
    let count = 0;
    let last = 0;
    for (let i = 2; i <= limit; i++) {
      if (!sieve[i]) {
        count++;
        last = i;
      }
    }
    return { limit, count, last };
  },

  hash({ text, rounds }, progress) {
    let digest = Buffer.from(text);
    const step = Math.max(1, Math.floor(rounds / 20));
    for (let i = 0; i < rounds; i++) {
      digest = createHash('sha256').update(digest).digest();
      if (i % step === 0) progress(i / rounds);
    }
    return { rounds, digest: digest.toString('hex') };
  },

  wordfreq({ text, top }) {
    const counts = new Map();
    for (const word of text.toLowerCase().split(/[^\p{L}\p{N}']+/u)) {
      if (word) counts.set(word, (counts.get(word) ?? 0) + 1);
    }
    const ranked = [...counts].sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : 1));
    return { unique: counts.size, top: ranked.slice(0, top).map(([word, count]) => ({ word, count })) };
  },

  sleep({ ms }, progress) {
    // blocks the worker thread on purpose, to simulate a long synchronous job
    const cell = new Int32Array(new SharedArrayBuffer(4));
    const slices = 10;
    for (let i = 1; i <= slices; i++) {
      Atomics.wait(cell, 0, 0, ms / slices);
      progress(i / slices);
    }
    return { slept: ms };
  },
};
