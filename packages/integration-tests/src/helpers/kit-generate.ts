import { readFileSync, rmSync } from 'node:fs';

// drizzle-kit is an optional dependency: during development it links to the local
// pulse-branch build (a `file:` tarball of the sibling drizzle-orm/drizzle-kit); once the
// pulse-enabled version ships to npm it becomes an ordinary versioned dep. Either way the
// pulse support lives only in that package — the runtime never generates this DDL itself.
// Absent (a checkout without the sibling tarball) → `kitAvailable()` is false and the
// conformance suite skips. Callers `skipIf(!kitAvailable())`.
export function kitAvailable(): boolean {
  try {
    import.meta.resolve('drizzle-kit/cli');
    return true;
  } catch {
    return false;
  }
}

type GenerateResult =
  | { status: 'ok'; migration_path: string }
  | { status: string; [key: string]: unknown };

/**
 * Runs drizzle-kit's `generate` SDK (never `generateMigration` from api-postgres — that
 * path skips the pulse guard that synthesizes the publication + events table) against a
 * fixture's drizzle.config and returns the generated migration SQL.
 *
 * `generate` is stateless here: the fixture's configured `out` dir is wiped before and
 * after each call, so every invocation produces one fresh "initial" migration rather than
 * diffing against a real project's incremental history.
 */
export async function generatePulseMigrationSql(
  configPath: string,
  outDir: string,
): Promise<string> {
  const { generate } = await import('drizzle-kit/cli');

  rmSync(outDir, { recursive: true, force: true });

  const result = (await generate({ config: configPath })) as GenerateResult;
  if (result.status !== 'ok' || typeof result.migration_path !== 'string') {
    throw new Error(`drizzle-kit generate failed: ${JSON.stringify(result)}`);
  }

  const sql = readFileSync(result.migration_path, 'utf8');
  rmSync(outDir, { recursive: true, force: true });
  return sql;
}
