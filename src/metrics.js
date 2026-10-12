import { monitorEventLoopDelay } from 'node:perf_hooks';

// Event loop delay is the number that tells you whether CPU work is leaking onto the main thread.
export function createMetrics({ service, startedAt = Date.now() }) {
  const histogram = monitorEventLoopDelay({ resolution: 10 });
  histogram.enable();
  const ms = (ns) => Math.round((ns / 1e6) * 100) / 100;

  return {
    snapshot() {
      const mem = process.memoryUsage();
      return {
        instance: service.instance,
        pid: process.pid,
        uptimeSec: Math.round((Date.now() - startedAt) / 1000),
        eventLoop: { p50: ms(histogram.percentile(50)), p99: ms(histogram.percentile(99)), max: ms(histogram.max) },
        memoryMb: Math.round(mem.rss / 1024 / 1024),
        ...service.stats(),
      };
    },
    resetEventLoop: () => histogram.reset(),
    stop: () => histogram.disable(),
  };
}
