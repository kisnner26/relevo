import { parentPort } from 'node:worker_threads';
import { handlers } from './handlers.js';

parentPort.on('message', ({ id, type, params }) => {
  try {
    const handler = handlers[type];
    if (!handler) throw new Error(`no handler for "${type}"`);
    const result = handler(params, (value) => parentPort.postMessage({ kind: 'progress', id, value }));
    parentPort.postMessage({ kind: 'done', id, result });
  } catch (err) {
    parentPort.postMessage({ kind: 'error', id, message: err.message });
  }
});
