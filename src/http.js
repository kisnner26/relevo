import http from 'node:http';
import { readFileSync } from 'node:fs';
import { graphql } from 'graphql';
import { WebSocketServer } from 'ws';
import { buildSchema } from './schema.js';
import { ValidationError } from './jobs.js';
import { QueueFullError } from './pool.js';
import { NotFoundError } from './service.js';

const MAX_BODY = 1_000_000;
const MAX_BUFFERED = 1_000_000;
const dashboard = readFileSync(new URL('../public/index.html', import.meta.url));

class HttpError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

export function statusFor(err) {
  if (err instanceof ValidationError || err instanceof HttpError) return err.status ?? 400;
  if (err instanceof NotFoundError) return 404;
  if (err instanceof QueueFullError) return 429;
  return 500;
}

export function errorBody(err) {
  return { error: { code: err.code ?? 'INTERNAL', message: statusFor(err) === 500 ? 'internal error' : err.message } };
}

async function readJson(req) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > MAX_BODY) throw new HttpError(413, 'body too large');
    chunks.push(chunk);
  }
  if (size === 0) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    throw new HttpError(400, 'body is not valid json');
  }
}

function send(res, status, body) {
  const payload = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'content-length': Buffer.byteLength(payload) });
  res.end(payload);
}

export function createApp({ service, metrics }) {
  const schema = buildSchema({ service, metrics });

  async function route(req, res) {
    const url = new URL(req.url, 'http://x');
    const path = url.pathname;
    const method = req.method;

    if (method === 'GET' && (path === '/' || path === '/index.html')) {
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      return res.end(dashboard);
    }
    if (method === 'GET' && path === '/health') return send(res, 200, { ok: true, instance: service.instance });
    if (method === 'GET' && path === '/metrics') return send(res, 200, metrics.snapshot());

    if (path === '/jobs' && method === 'GET') {
      const status = url.searchParams.get('status') ?? undefined;
      const limit = Math.min(Number(url.searchParams.get('limit') ?? 50) || 50, 500);
      return send(res, 200, { jobs: service.list({ status, limit }) });
    }
    if (path === '/jobs' && method === 'POST') {
      const body = await readJson(req);
      return send(res, 202, service.enqueue(body));
    }
    const match = path.match(/^\/jobs\/([\w-]+)$/);
    if (match && method === 'GET') return send(res, 200, service.get(match[1]));
    if (match && method === 'DELETE') return send(res, 200, service.cancel(match[1]));

    if (path === '/graphql' && method === 'POST') {
      const { query, variables, operationName } = await readJson(req);
      if (typeof query !== 'string') throw new HttpError(400, 'query is required');
      const result = await graphql({ schema, source: query, variableValues: variables, operationName });
      return send(res, 200, result);
    }
    throw new HttpError(404, `no route for ${method} ${path}`);
  }

  const server = http.createServer((req, res) => {
    route(req, res).catch((err) => {
      if (res.headersSent) return res.destroy();
      send(res, statusFor(err), errorBody(err));
    });
  });

  const wss = new WebSocketServer({ noServer: true, maxPayload: 100_000 });
  server.on('upgrade', (req, socket, head) => {
    if (new URL(req.url, 'http://x').pathname !== '/ws') return socket.destroy();
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
  });

  const push = (ws, message) => {
    if (ws.readyState === ws.OPEN && ws.bufferedAmount < MAX_BUFFERED) ws.send(JSON.stringify(message));
  };

  wss.on('connection', (ws) => {
    ws.isAlive = true;
    ws.on('pong', () => (ws.isAlive = true));
    push(ws, { type: 'hello', instance: service.instance });
    push(ws, { type: 'snapshot', jobs: service.list({ limit: 50 }), stats: metrics.snapshot() });

    ws.on('message', (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        return push(ws, { type: 'error', error: { code: 'BAD_REQUEST', message: 'message is not valid json' } });
      }
      try {
        if (msg.op === 'enqueue') push(ws, { type: 'enqueued', job: service.enqueue(msg) });
        else if (msg.op === 'cancel') push(ws, { type: 'cancelled', job: service.cancel(msg.id) });
        else throw new ValidationError('op must be "enqueue" or "cancel"');
      } catch (err) {
        push(ws, { type: 'error', ...errorBody(err) });
      }
    });
  });

  const onJob = (job) => {
    for (const ws of wss.clients) push(ws, { type: 'job', job });
  };
  service.on('job', onJob);

  const heartbeat = setInterval(() => {
    for (const ws of wss.clients) {
      if (!ws.isAlive) {
        ws.terminate();
        continue;
      }
      ws.isAlive = false;
      ws.ping();
    }
  }, 15_000);
  heartbeat.unref();

  return {
    server,
    wss,
    async close() {
      clearInterval(heartbeat);
      service.off('job', onJob);
      for (const ws of wss.clients) ws.terminate();
      wss.close();
      server.closeAllConnections();
      await new Promise((resolve) => server.close(resolve));
    },
  };
}
