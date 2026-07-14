#!/usr/bin/env node
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const defaultScanDir = join(scriptDir, '..', 'packages', 'drizzle-pulse', 'dist');
const scanDir = process.argv[2] ? resolve(process.argv[2]) : defaultScanDir;

// attw's esm-only profile is blind to external dep types (v1.2 CR-01), so this grep
// over the emitted .d.ts files is the real gate for a @types/pg leak into the public API.
const leakPattern =
  /from\s+['"]pg['"]|import\(\s*['"]pg['"]\s*\)|@types\/pg|pg-logical-replication|node_modules\/pg(?:['"/]|$)/;

function collectDtsFiles(dir) {
  const files = [];
  for (const entry of readdirSync(dir)) {
    const fullPath = join(dir, entry);
    const stats = statSync(fullPath);
    if (stats.isDirectory()) {
      files.push(...collectDtsFiles(fullPath));
    } else if (entry.endsWith('.d.ts')) {
      files.push(fullPath);
    }
  }
  return files;
}

const errors = [];
const dtsFiles = collectDtsFiles(scanDir);

if (dtsFiles.length === 0) {
  console.error(`check-dts-no-pg: FAILED — no .d.ts files found under ${scanDir}`);
  process.exit(1);
}

for (const file of dtsFiles) {
  const lines = readFileSync(file, 'utf8').split('\n');
  lines.forEach((line, index) => {
    if (leakPattern.test(line)) {
      errors.push(`${file}:${index + 1}: ${line.trim()}`);
    }
  });
}

if (errors.length > 0) {
  console.error('check-dts-no-pg: FAILED');
  for (const error of errors) {
    console.error(`- ${error}`);
  }
  process.exit(1);
}

console.log(`check-dts-no-pg: PASSED (${dtsFiles.length} .d.ts files scanned, no pg leaks)`);
