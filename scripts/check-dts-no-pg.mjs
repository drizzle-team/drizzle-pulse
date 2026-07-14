#!/usr/bin/env node
import { readdirSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const scanDir = join(scriptDir, '..', 'packages', 'drizzle-pulse', 'dist');

// attw's esm-only profile is blind to external dep types (v1.2 CR-01), so this grep
// over the emitted .d.ts files is the real gate for a @types/pg leak into the public API.
const leakPattern =
  /from\s+['"]pg['"]|import\(\s*['"]pg['"]\s*\)|@types\/pg|pg-logical-replication|node_modules\/pg(?:['"/]|$)/m;

const dtsFiles = readdirSync(scanDir, { recursive: true })
  .filter((entry) => entry.endsWith('.d.ts'))
  .map((entry) => join(scanDir, entry));

if (dtsFiles.length === 0) {
  console.error(`check-dts-no-pg: FAILED — no .d.ts files found under ${scanDir}`);
  process.exit(1);
}

const leakingFiles = dtsFiles.filter((file) => leakPattern.test(readFileSync(file, 'utf8')));

if (leakingFiles.length > 0) {
  console.error('check-dts-no-pg: FAILED');
  for (const file of leakingFiles) {
    console.error(`- ${file}`);
  }
  process.exit(1);
}

console.log(`check-dts-no-pg: PASSED (${dtsFiles.length} .d.ts files scanned, no pg leaks)`);
