import { spawnSync } from 'node:child_process';
import { mkdtempSync, readdirSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

// Runs the main-thread scenario under --cpu-prof and prints where the time went.
const dir = mkdtempSync(join(tmpdir(), 'relevo-prof-'));
const run = spawnSync(process.execPath, ['--cpu-prof', `--cpu-prof-dir=${dir}`, new URL('./bench.js', import.meta.url).pathname, 'main'], { encoding: 'utf8' });
if (run.status !== 0) {
  console.error(run.stderr);
  process.exit(1);
}
const file = readdirSync(dir).find((f) => f.endsWith('.cpuprofile'));
const profile = JSON.parse(readFileSync(join(dir, file), 'utf8'));
rmSync(dir, { recursive: true });

const self = new Map();
const byId = new Map(profile.nodes.map((n) => [n.id, n]));
profile.samples.forEach((id, i) => {
  const { functionName, url, lineNumber } = byId.get(id).callFrame;
  const key = `${functionName || '(anonymous)'} ${url.split('/').slice(-2).join('/')}:${lineNumber + 1}`;
  self.set(key, (self.get(key) ?? 0) + profile.timeDeltas[i]);
});
const total = [...self.values()].reduce((a, b) => a + b, 0);
console.log(`profile of the main-thread run (${(total / 1000).toFixed(0)} ms sampled)\n`);
for (const [key, us] of [...self].sort((a, b) => b[1] - a[1]).slice(0, 6)) {
  console.log(`${((us / total) * 100).toFixed(1).padStart(5)}%  ${(us / 1000).toFixed(0).padStart(5)} ms  ${key}`);
}
