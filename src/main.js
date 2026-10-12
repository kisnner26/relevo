import { start } from './server.js';

const instance = process.env.INSTANCE ?? 'i0';
const num = (v) => (v === undefined ? undefined : Number(v));
const app = await start({
  port: num(process.env.PORT) ?? 3000,
  host: process.env.HOST ?? '127.0.0.1',
  instance,
  poolSize: num(process.env.POOL_SIZE),
  maxQueue: num(process.env.MAX_QUEUE),
  timeoutMs: num(process.env.JOB_TIMEOUT_MS),
});
console.log(`relevo ${instance} listening on http://${process.env.HOST ?? '127.0.0.1'}:${app.port} (pid ${process.pid})`);

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, async () => {
    await app.close();
    process.exit(0);
  });
}
