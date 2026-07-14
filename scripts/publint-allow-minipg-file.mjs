#!/usr/bin/env node
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { publint } from 'publint';
import { formatMessage } from 'publint/utils';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const packageDir = join(scriptDir, '..', 'packages', 'drizzle-pulse');

const { messages, pkg } = await publint({ pkgDir: packageDir, strict: true });

const errors = messages.filter((message) => message.type === 'error');

// ponytail: publint has no per-rule ignore, so this wrapper is the only way to tolerate
// the one documented minipg file: exception (PUB-01/D-06) without disabling the gate entirely.
const isToleratedMinipgFileError = (message) =>
  message.code === 'LOCAL_DEPENDENCY' && message.path.join('.') === 'dependencies.minipg';

const untoleratedErrors = errors.filter((message) => !isToleratedMinipgFileError(message));

if (untoleratedErrors.length > 0) {
  console.error('publint-allow-minipg-file: FAILED');
  for (const message of messages) {
    console.error(`- [${message.type}] ${formatMessage(message, pkg, { color: false })}`);
  }
  process.exit(1);
}

console.log('publint-allow-minipg-file: PASSED');
for (const message of errors) {
  console.log(`- tolerated (PUB-01/D-06): ${formatMessage(message, pkg, { color: false })}`);
}
