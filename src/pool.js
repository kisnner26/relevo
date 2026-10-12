import { Worker } from 'node:worker_threads';
import { availableParallelism } from 'node:os';

export class QueueFullError extends Error {
  constructor(max) {
    super(`queue is full (max ${max})`);
    this.name = 'QueueFullError';
    this.code = 'QUEUE_FULL';
  }
}

export class JobTimeoutError extends Error {
  constructor(ms) {
    super(`job exceeded ${ms} ms`);
    this.name = 'JobTimeoutError';
    this.code = 'TIMEOUT';
  }
}

export class JobCancelledError extends Error {
  constructor() {
    super('job was cancelled');
    this.name = 'JobCancelledError';
    this.code = 'CANCELLED';
  }
}

const PRIORITIES = { high: 0, normal: 1, low: 2 };

export class WorkerPool {
  #size;
  #maxQueue;
  #timeoutMs;
  #file;
  #slots = [];
  #queues = [[], [], []];
  #closed = false;
  #seq = 0;
  #counters = { completed: 0, failed: 0, cancelled: 0, timedOut: 0, restarts: 0 };

  constructor({
    size = Math.max(1, availableParallelism() - 1),
    maxQueue = 100,
    timeoutMs = 30_000,
    workerFile = new URL('./job-worker.js', import.meta.url),
  } = {}) {
    this.#size = size;
    this.#maxQueue = maxQueue;
    this.#timeoutMs = timeoutMs;
    this.#file = workerFile;
    for (let i = 0; i < size; i++) {
      const slot = { worker: null, task: null };
      this.#slots.push(slot);
      this.#spawn(slot);
    }
  }

  #spawn(slot) {
    const worker = new Worker(this.#file);
    worker.unref();
    slot.worker = worker;
    worker.on('message', (msg) => this.#onMessage(slot, msg));
    worker.on('error', (err) => this.#onCrash(slot, worker, err));
    worker.on('exit', (code) => {
      if (slot.worker === worker) this.#onCrash(slot, worker, new Error(`worker exited with code ${code}`));
    });
  }

  #onMessage(slot, msg) {
    const task = slot.task;
    if (!task || task.id !== msg.id) return;
    if (msg.kind === 'progress') {
      task.onProgress?.(msg.value);
      return;
    }
    this.#release(slot);
    if (msg.kind === 'done') {
      this.#counters.completed++;
      task.resolve(msg.result);
    } else {
      this.#counters.failed++;
      task.reject(new Error(msg.message));
    }
    this.#dispatch();
  }

  // a worker died on its own: fail its job and bring a fresh thread into the slot
  #onCrash(slot, worker, err) {
    if (slot.worker !== worker || this.#closed) return;
    const task = slot.task;
    this.#release(slot);
    this.#counters.restarts++;
    this.#spawn(slot);
    if (task) {
      this.#counters.failed++;
      task.reject(err);
    }
    this.#dispatch();
  }

  #release(slot) {
    if (slot.task) clearTimeout(slot.task.timer);
    slot.task = null;
  }

  // used for timeouts and cancellation: only terminate() can stop synchronous code
  #killRunning(slot) {
    const worker = slot.worker;
    this.#release(slot);
    slot.worker = null;
    this.#counters.restarts++;
    worker.terminate();
    this.#spawn(slot);
  }

  #dispatch() {
    for (const slot of this.#slots) {
      if (slot.task) continue;
      const task = this.#next();
      if (!task) return;
      slot.task = task;
      task.state = 'running';
      task.timer = setTimeout(() => this.#onTimeout(slot, task), this.#timeoutMs);
      task.timer.unref();
      task.onStart?.();
      slot.worker.postMessage({ id: task.id, type: task.type, params: task.params });
    }
  }

  #next() {
    for (const queue of this.#queues) if (queue.length) return queue.shift();
    return null;
  }

  #onTimeout(slot, task) {
    if (slot.task !== task) return;
    this.#killRunning(slot);
    this.#counters.timedOut++;
    this.#counters.failed++;
    task.reject(new JobTimeoutError(this.#timeoutMs));
    this.#dispatch();
  }

  submit(type, params, { priority = 'normal', onProgress, onStart } = {}) {
    if (this.#closed) throw new Error('pool is closed');
    if (!(priority in PRIORITIES)) throw new Error(`unknown priority "${priority}"`);
    if (this.queued >= this.#maxQueue) throw new QueueFullError(this.#maxQueue);

    const task = { id: ++this.#seq, type, params, priority, onProgress, onStart, state: 'queued', timer: null };
    const promise = new Promise((resolve, reject) => {
      task.resolve = resolve;
      task.reject = reject;
    });
    this.#queues[PRIORITIES[priority]].push(task);
    this.#dispatch();

    const cancel = () => {
      if (task.state === 'queued') {
        const queue = this.#queues[PRIORITIES[priority]];
        const at = queue.indexOf(task);
        if (at === -1) return false;
        queue.splice(at, 1);
      } else {
        const slot = this.#slots.find((s) => s.task === task);
        if (!slot) return false;
        this.#killRunning(slot);
        this.#dispatch();
      }
      task.state = 'cancelled';
      this.#counters.cancelled++;
      task.reject(new JobCancelledError());
      return true;
    };
    return { promise, cancel };
  }

  get queued() {
    return this.#queues.reduce((n, q) => n + q.length, 0);
  }

  stats() {
    const busy = this.#slots.filter((s) => s.task).length;
    return { size: this.#size, busy, queued: this.queued, maxQueue: this.#maxQueue, ...this.#counters };
  }

  async close() {
    this.#closed = true;
    for (const queue of this.#queues) {
      for (const task of queue.splice(0)) task.reject(new JobCancelledError());
    }
    await Promise.all(
      this.#slots.map((slot) => {
        this.#release(slot);
        return slot.worker?.terminate();
      }),
    );
  }
}
