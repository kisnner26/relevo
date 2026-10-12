import { EventEmitter } from 'node:events';
import { randomBytes } from 'node:crypto';
import { validate, cacheKey } from './jobs.js';
import { JobCancelledError } from './pool.js';

const FINISHED = new Set(['done', 'failed', 'cancelled']);

export class NotFoundError extends Error {
  constructor(id) {
    super(`job "${id}" not found`);
    this.name = 'NotFoundError';
    this.code = 'NOT_FOUND';
  }
}

// Owns job state. Identical jobs share one pool task (single flight) and finished
// results are served from the cache, so repeated requests cost nothing.
export class JobService extends EventEmitter {
  #pool;
  #cache;
  #instance;
  #maxJobs;
  #jobs = new Map();
  #inflight = new Map();

  constructor({ pool, cache, instance = 'i0', maxJobs = 1000 }) {
    super();
    this.#pool = pool;
    this.#cache = cache;
    this.#instance = instance;
    this.#maxJobs = maxJobs;
  }

  get instance() {
    return this.#instance;
  }

  enqueue({ type, params = {}, priority = 'normal' }) {
    const normalized = validate(type, params);
    const key = cacheKey(type, normalized);
    const now = Date.now();
    const job = {
      id: `${this.#instance}-${randomBytes(5).toString('hex')}`,
      type,
      params: normalized,
      priority,
      status: 'queued',
      progress: 0,
      cached: false,
      shared: false,
      result: null,
      error: null,
      instance: this.#instance,
      createdAt: now,
      startedAt: null,
      finishedAt: null,
    };
    this.#jobs.set(job.id, job);
    this.#evict();

    const hit = this.#cache.get(key);
    if (hit !== undefined) {
      Object.assign(job, { status: 'done', progress: 1, cached: true, result: hit, startedAt: now, finishedAt: now });
      this.#emit(job);
      return { ...job };
    }

    let entry = this.#inflight.get(key);
    if (entry) {
      job.shared = true;
      if (entry.started) {
        job.status = 'running';
        job.startedAt = now;
        job.progress = entry.progress;
      }
      entry.ids.add(job.id);
      this.#emit(job);
    } else {
      this.#emit(job);
      entry = { ids: new Set([job.id]), started: false, progress: 0, handle: null };
      this.#inflight.set(key, entry);
      entry.handle = this.#pool.submit(type, normalized, {
        priority,
        onStart: () => {
          entry.started = true;
          this.#patchAll(entry, { status: 'running', startedAt: Date.now() });
        },
        onProgress: (value) => {
          entry.progress = value;
          this.#patchAll(entry, { progress: value });
        },
      });
      entry.handle.promise.then(
        (result) => {
          this.#inflight.delete(key);
          this.#cache.set(key, result);
          this.#patchAll(entry, { status: 'done', progress: 1, result, finishedAt: Date.now() });
        },
        (err) => {
          this.#inflight.delete(key);
          this.#patchAll(entry, {
            status: err instanceof JobCancelledError ? 'cancelled' : 'failed',
            error: { code: err.code ?? 'FAILED', message: err.message },
            finishedAt: Date.now(),
          });
        },
      );
    }
    return { ...job };
  }

  get(id) {
    const job = this.#jobs.get(id);
    if (!job) throw new NotFoundError(id);
    return { ...job };
  }

  list({ status, limit = 50 } = {}) {
    const all = [...this.#jobs.values()].reverse();
    return all.filter((j) => !status || j.status === status).slice(0, limit).map((j) => ({ ...j }));
  }

  // the pool task is only killed when nobody is waiting for it any more
  cancel(id) {
    const job = this.#jobs.get(id);
    if (!job) throw new NotFoundError(id);
    if (FINISHED.has(job.status)) return { ...job };
    for (const [key, entry] of this.#inflight) {
      if (!entry.ids.delete(id)) continue;
      Object.assign(job, {
        status: 'cancelled',
        error: { code: 'CANCELLED', message: 'job was cancelled' },
        finishedAt: Date.now(),
      });
      this.#emit(job);
      if (entry.ids.size === 0) {
        this.#inflight.delete(key);
        entry.handle.cancel();
      }
      break;
    }
    return { ...job };
  }

  stats() {
    const byStatus = { queued: 0, running: 0, done: 0, failed: 0, cancelled: 0 };
    for (const job of this.#jobs.values()) byStatus[job.status]++;
    return { jobs: byStatus, pool: this.#pool.stats(), cache: this.#cache.stats(), inflight: this.#inflight.size };
  }

  #patchAll(entry, patch) {
    for (const id of entry.ids) {
      const job = this.#jobs.get(id);
      if (!job || FINISHED.has(job.status)) continue;
      Object.assign(job, patch);
      this.#emit(job);
    }
  }

  #emit(job) {
    this.emit('job', { ...job });
  }

  #evict() {
    if (this.#jobs.size <= this.#maxJobs) return;
    for (const [id, job] of this.#jobs) {
      if (this.#jobs.size <= this.#maxJobs) break;
      if (FINISHED.has(job.status)) this.#jobs.delete(id);
    }
  }
}
