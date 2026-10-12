import http from 'node:http';
import { WebSocketServer, WebSocket } from 'ws';
import { errorBody, statusFor } from './http.js';

const MAX_BODY = 1_000_000;

class BalancerError extends Error {
  constructor(status, code, message) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

// HTTP and WebSocket front door for several relevo instances.
// - least-connections choice with round-robin tie break
// - jobs carry their instance in the id ("w1-ab12..."), so lookups are routed to the owner
// - /jobs and /metrics fan out to every healthy instance and merge
// - /ws fans in the live feed of all instances
export function createBalancer({ targets, port = 0, host = '127.0.0.1', healthIntervalMs = 2000 }) {
  const nodes = targets.map((t) => ({ ...t, healthy: true, active: 0, served: 0, ws: null }));
  const byInstance = new Map(nodes.map((n) => [n.instance, n]));
  const idPattern = new RegExp(`\\b(${nodes.map((n) => n.instance).join('|')})-[0-9a-f]{10}\\b`);
  let rr = 0;
  let closed = false;

  const owner = (text) => byInstance.get(text.match(idPattern)?.[1]);

  function pick(preferred) {
    if (preferred) {
      if (!preferred.healthy) throw new BalancerError(503, 'INSTANCE_DOWN', `instance ${preferred.instance} is down`);
      return preferred;
    }
    const up = nodes.filter((n) => n.healthy);
    if (!up.length) throw new BalancerError(503, 'NO_INSTANCES', 'no healthy instances');
    const min = Math.min(...up.map((n) => n.active));
    const ties = up.filter((n) => n.active === min);
    return ties[rr++ % ties.length];
  }

  function forward(node, { method, path, headers = {}, body }) {
    return new Promise((resolve, reject) => {
      node.active++;
      const req = http.request(
        { host: node.host, port: node.port, method, path, headers: { ...headers, host: `${node.host}:${node.port}`, 'content-length': body ? body.length : 0 } },
        (res) => {
          const chunks = [];
          res.on('data', (c) => chunks.push(c));
          res.on('end', () => {
            node.active--;
            node.served++;
            resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) });
          });
        },
      );
      req.setTimeout(35_000, () => req.destroy(new Error('upstream timeout')));
      req.on('error', (err) => {
        node.active--;
        node.healthy = false;
        reject(new BalancerError(502, 'BAD_GATEWAY', `instance ${node.instance} failed: ${err.message}`));
      });
      req.end(body);
    });
  }

  async function readBody(req) {
    const chunks = [];
    let size = 0;
    for await (const chunk of req) {
      size += chunk.length;
      if (size > MAX_BODY) throw new BalancerError(413, 'BAD_REQUEST', 'body too large');
      chunks.push(chunk);
    }
    return Buffer.concat(chunks);
  }

  async function fanOut(path) {
    const up = nodes.filter((n) => n.healthy);
    const settled = await Promise.allSettled(up.map((n) => forward(n, { method: 'GET', path })));
    return settled.filter((r) => r.status === 'fulfilled' && r.value.status === 200).map((r) => JSON.parse(r.value.body));
  }

  async function aggregatedJobs(url) {
    const limit = Math.min(Number(url.searchParams.get('limit') ?? 50) || 50, 500);
    const lists = await fanOut(`/jobs${url.search}`);
    const jobs = lists.flatMap((l) => l.jobs).sort((a, b) => b.createdAt - a.createdAt).slice(0, limit);
    return jobs;
  }

  const sendJson = (res, status, body) => {
    const payload = JSON.stringify(body);
    res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'content-length': Buffer.byteLength(payload) });
    res.end(payload);
  };

  async function route(req, res) {
    const url = new URL(req.url, 'http://x');
    if (req.method === 'GET' && url.pathname === '/health') {
      return sendJson(res, 200, { ok: nodes.some((n) => n.healthy), instances: nodes.map(({ instance, healthy, active, served }) => ({ instance, healthy, active, served })) });
    }
    if (req.method === 'GET' && url.pathname === '/metrics') {
      const instances = await fanOut('/metrics');
      return sendJson(res, 200, { balancer: { nodes: nodes.length, healthy: nodes.filter((n) => n.healthy).length }, instances });
    }
    if (req.method === 'GET' && url.pathname === '/jobs') {
      return sendJson(res, 200, { jobs: await aggregatedJobs(url) });
    }

    const body = await readBody(req);
    const preferred = owner(url.pathname) ?? (url.pathname === '/graphql' && body.length ? owner(body.toString('utf8')) : undefined);
    const upstream = await forward(pick(preferred), { method: req.method, path: req.url, headers: { 'content-type': req.headers['content-type'] ?? 'application/json' }, body });
    const headers = { 'content-type': upstream.headers['content-type'], 'content-length': upstream.body.length };
    res.writeHead(upstream.status, headers);
    res.end(upstream.body);
  }

  const server = http.createServer((req, res) => {
    route(req, res).catch((err) => {
      if (res.headersSent) return res.destroy();
      sendJson(res, err.status ?? statusFor(err), errorBody(err));
    });
  });

  const wss = new WebSocketServer({ noServer: true, maxPayload: 100_000 });
  server.on('upgrade', (req, socket, head) => {
    if (new URL(req.url, 'http://x').pathname !== '/ws') return socket.destroy();
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws));
  });

  const push = (ws, message) => ws.readyState === ws.OPEN && ws.bufferedAmount < 1_000_000 && ws.send(JSON.stringify(message));

  async function command(msg) {
    if (msg.op === 'enqueue') {
      const r = await forward(pick(), { method: 'POST', path: '/jobs', headers: { 'content-type': 'application/json' }, body: Buffer.from(JSON.stringify(msg)) });
      const json = JSON.parse(r.body);
      return r.status === 202 ? { type: 'enqueued', job: json } : { type: 'error', ...json };
    }
    if (msg.op === 'cancel' && typeof msg.id === 'string') {
      const node = owner(msg.id);
      if (!node) return { type: 'error', error: { code: 'NOT_FOUND', message: `job "${msg.id}" not found` } };
      const r = await forward(pick(node), { method: 'DELETE', path: `/jobs/${msg.id}` });
      const json = JSON.parse(r.body);
      return r.status === 200 ? { type: 'cancelled', job: json } : { type: 'error', ...json };
    }
    return { type: 'error', error: { code: 'BAD_REQUEST', message: 'op must be "enqueue" or "cancel"' } };
  }

  wss.on('connection', async (ws) => {
    ws.isAlive = true;
    ws.on('pong', () => (ws.isAlive = true));
    push(ws, { type: 'hello', instance: 'balancer' });
    const [jobs, stats] = await Promise.all([aggregatedJobs(new URL('http://x/jobs')), fanOut('/metrics')]);
    push(ws, { type: 'snapshot', jobs, stats: { instances: stats } });
    ws.on('message', async (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        return push(ws, { type: 'error', error: { code: 'BAD_REQUEST', message: 'message is not valid json' } });
      }
      try {
        push(ws, await command(msg));
      } catch (err) {
        push(ws, { type: 'error', ...errorBody(err) });
      }
    });
  });

  // keep one upstream socket per healthy instance and relay job events to every client
  function connectUpstream(node) {
    if (closed || node.ws || !node.healthy) return;
    const ws = new WebSocket(`ws://${node.host}:${node.port}/ws`);
    node.ws = ws;
    ws.on('message', (raw) => {
      const text = raw.toString();
      if (!text.startsWith('{"type":"job"')) return;
      for (const client of wss.clients) if (client.readyState === client.OPEN && client.bufferedAmount < 1_000_000) client.send(text);
    });
    const drop = () => {
      if (node.ws === ws) node.ws = null;
    };
    ws.on('close', drop);
    ws.on('error', drop);
  }

  async function checkHealth() {
    await Promise.all(
      nodes.map(async (node) => {
        try {
          const res = await fetch(`http://${node.host}:${node.port}/health`, { signal: AbortSignal.timeout(1000) });
          node.healthy = res.ok;
        } catch {
          node.healthy = false;
        }
        if (node.healthy) connectUpstream(node);
        else node.ws?.terminate();
      }),
    );
  }

  const timer = setInterval(checkHealth, healthIntervalMs);
  timer.unref();
  const ping = setInterval(() => {
    for (const ws of wss.clients) {
      if (!ws.isAlive) ws.terminate();
      else {
        ws.isAlive = false;
        ws.ping();
      }
    }
  }, 15_000);
  ping.unref();

  return {
    server,
    nodes,
    checkHealth,
    async listen() {
      await new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(port, host, resolve);
      });
      await checkHealth();
      return server.address().port;
    },
    async close() {
      closed = true;
      clearInterval(timer);
      clearInterval(ping);
      for (const n of nodes) n.ws?.terminate();
      for (const ws of wss.clients) ws.terminate();
      wss.close();
      server.closeAllConnections();
      await new Promise((resolve) => server.close(resolve));
    },
  };
}
