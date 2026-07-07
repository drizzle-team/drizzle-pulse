import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync, rmSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

// The LOCAL pulse-branch drizzle-kit build — NOT the npm `drizzle-kit` dep (which has no
// pulse support). Resolved by path, not as a package dependency: a `file:` dep here would
// pin the sibling repo's whole tree into this package's lockfile and break
// `bun install --frozen-lockfile` on any machine/CI without the sibling checkout.
// `DRIZZLE_KIT_PULSE_BIN` overrides the default sibling drizzle-orm/drizzle-kit build path.
// Callers `skipIf(!kitBinExists())`.
export const KIT_BIN = process.env.DRIZZLE_KIT_PULSE_BIN
  ? resolve(process.env.DRIZZLE_KIT_PULSE_BIN)
  : resolve(
      dirname(fileURLToPath(import.meta.url)),
      '../../../../../drizzle-orm/drizzle-kit/dist/bin.cjs',
    );

export function kitBinExists(): boolean {
  return existsSync(KIT_BIN);
}

type GenerateJsonResult =
  | { status: 'ok'; migration_path: string }
  | { status: 'error'; error: unknown };

function runKitGenerate(configPath: string): GenerateJsonResult {
  const args = [KIT_BIN, 'generate', '--config', configPath, '--output', 'json'];

  try {
    // Always the real Node binary, never `process.execPath`: under `bun test` that's the
    // bun executable, and bun's own module loader chokes on kit's bundled dist/bin.cjs
    // (a dynamic `import("node:sqlite")` bun doesn't support).
    const raw = execFileSync('node', args, { encoding: 'utf8' });
    return JSON.parse(raw.trim().split('\n').pop() ?? '') as GenerateJsonResult;
  } catch (error) {
    // execFileSync throws on non-zero exit (kit's own error responses exit 1); the JSON
    // payload is still on stdout, captured on the thrown error object.
    const stdout = (error as { stdout?: string }).stdout;
    if (typeof stdout === 'string' && stdout.trim().length > 0) {
      return JSON.parse(stdout.trim().split('\n').pop() ?? '') as GenerateJsonResult;
    }
    throw error;
  }
}

/**
 * Runs the LOCAL pulse-branch kit's `generate` CLI (never `generateDrizzleJson`/
 * `generateMigration` from api-postgres — that path skips the pulse guard) against a
 * fixture's drizzle.config and returns the generated migration SQL.
 *
 * `generate` is stateless here: the fixture's configured `out` dir is wiped before and
 * after each call, so every invocation produces one fresh "initial" migration rather than
 * diffing against a real project's incremental history.
 */
export function generatePulseMigrationSql(configPath: string, outDir: string): string {
  if (!existsSync(KIT_BIN)) {
    throw new Error(
      `Local pulse-branch drizzle-kit build not found at ${KIT_BIN}. Build the ` +
        'drizzle-kit package in a sibling drizzle-orm checkout (or set ' +
        'DRIZZLE_KIT_PULSE_BIN to the built bin.cjs).',
    );
  }

  rmSync(outDir, { recursive: true, force: true });

  const result = runKitGenerate(configPath);
  if (result.status !== 'ok') {
    throw new Error(`drizzle-kit generate failed: ${JSON.stringify(result)}`);
  }

  const sql = readFileSync(result.migration_path, 'utf8');
  rmSync(outDir, { recursive: true, force: true });
  return sql;
}
