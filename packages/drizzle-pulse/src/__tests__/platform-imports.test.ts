import { describe, expect, test } from 'bun:test';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import ts from 'typescript';

// Static import-graph audit: every module reachable via a VALUE import from the
// platform-agnostic entrypoints must stay free of server/node-only runtime deps.
// `import type` edges are erased at compile time and never execute, so they're
// excluded from traversal. See .planning/seeds/SEED-001-sdk-platform-imports-test.md
// (drizzle-orm) for the philosophy this port is based on.

const SRC_ROOT = resolve(import.meta.dir, '..');
const ENTRY_POINTS = [
  join(SRC_ROOT, 'client/index.ts'),
  join(SRC_ROOT, 'client/react/index.ts'),
  join(SRC_ROOT, 'client/embedded/index.ts'),
  join(SRC_ROOT, 'index.ts'),
];

const NODE_BUILTINS = new Set([
  'assert',
  'buffer',
  'child_process',
  'cluster',
  'crypto',
  'dns',
  'events',
  'fs',
  'http',
  'https',
  'net',
  'os',
  'path',
  'process',
  'querystring',
  'readline',
  'stream',
  'tls',
  'url',
  'util',
  'worker_threads',
  'zlib',
]);

function bannedReason(specifier: string): string | null {
  if (specifier.startsWith('.') || specifier.startsWith('/')) return null;
  if (specifier.startsWith('node:') || NODE_BUILTINS.has(specifier)) {
    return 'node builtin';
  }
  if (specifier === 'pg' || specifier.startsWith('pg/')) {
    return 'pg (server-only Postgres driver)';
  }
  if (specifier === 'pg-logical-replication' || specifier.startsWith('pg-logical-replication/')) {
    return 'pg-logical-replication (server-only WAL client)';
  }
  if (specifier === 'postgres' || specifier.startsWith('postgres/')) {
    return 'postgres (server-only driver)';
  }
  if (specifier === 'hono' || specifier.startsWith('hono/')) {
    return 'hono (server-only HTTP framework)';
  }
  // Bare `drizzle-orm` (query-building/metadata utils) is dialect-agnostic and allowed.
  // Any subpath (`drizzle-orm/node-postgres`, `drizzle-orm/pg-core`, ...) binds to a
  // specific dialect/driver and is treated as server-only runtime surface.
  if (specifier.startsWith('drizzle-orm/')) {
    return 'drizzle-orm/* runtime driver subpath';
  }
  return null;
}

function resolveFirstPartyModule(fromFile: string, specifier: string): string {
  const base = resolve(dirname(fromFile), specifier);
  const candidates = base.endsWith('.js')
    ? [`${base.slice(0, -3)}.ts`, `${base.slice(0, -3)}.tsx`]
    : [base, `${base}.ts`, `${base}.tsx`, join(base, 'index.ts')];

  for (const candidate of candidates) {
    if (existsSync(candidate)) return candidate;
  }
  throw new Error(`platform-imports test: could not resolve "${specifier}" from ${fromFile}`);
}

type NamedBindingsLike = { elements: ReadonlyArray<{ isTypeOnly: boolean }> } | undefined;

function namedElementsHaveValueBinding(bindings: NamedBindingsLike): boolean {
  return bindings ? bindings.elements.some((el) => !el.isTypeOnly) : false;
}

function importHasValueBinding(node: ts.ImportDeclaration): boolean {
  const clause = node.importClause;
  if (!clause) return true; // side-effect import: `import 'x';`
  if (clause.isTypeOnly) return false;
  if (clause.name) return true; // default import
  const bindings = clause.namedBindings;
  if (!bindings) return false;
  if (ts.isNamespaceImport(bindings)) return true;
  if (ts.isNamedImports(bindings)) {
    return namedElementsHaveValueBinding(bindings);
  }
  return false;
}

function exportFromHasValueBinding(node: ts.ExportDeclaration): boolean {
  if (node.isTypeOnly) return false;
  if (!node.exportClause) return true; // `export * from 'x'`
  if (ts.isNamespaceExport(node.exportClause)) return true;
  if (ts.isNamedExports(node.exportClause)) {
    return namedElementsHaveValueBinding(node.exportClause);
  }
  return false;
}

interface Violation {
  file: string;
  specifier: string;
  reason: string;
}

interface ImportEdge {
  specifier: string;
  isValueImport: boolean;
}

function collectEdges(node: ts.Node): ImportEdge[] {
  const edges: ImportEdge[] = [];

  const visit = (n: ts.Node): void => {
    if (ts.isImportDeclaration(n) && ts.isStringLiteral(n.moduleSpecifier)) {
      edges.push({ specifier: n.moduleSpecifier.text, isValueImport: importHasValueBinding(n) });
    } else if (
      ts.isExportDeclaration(n) &&
      n.moduleSpecifier &&
      ts.isStringLiteral(n.moduleSpecifier)
    ) {
      edges.push({
        specifier: n.moduleSpecifier.text,
        isValueImport: exportFromHasValueBinding(n),
      });
    } else if (
      ts.isCallExpression(n) &&
      n.expression.kind === ts.SyntaxKind.ImportKeyword &&
      n.arguments[0] &&
      ts.isStringLiteral(n.arguments[0])
    ) {
      // dynamic import() always executes the target module
      edges.push({ specifier: n.arguments[0].text, isValueImport: true });
    } else if (
      ts.isCallExpression(n) &&
      ts.isIdentifier(n.expression) &&
      n.expression.text === 'require' &&
      n.arguments[0] &&
      ts.isStringLiteral(n.arguments[0])
    ) {
      edges.push({ specifier: n.arguments[0].text, isValueImport: true });
    }
    ts.forEachChild(n, visit);
  };

  visit(node);
  return edges;
}

function walkImportGraph(entryPoints: string[]): { visited: string[]; violations: Violation[] } {
  const visited = new Set<string>();
  const violations: Violation[] = [];
  const queue = [...entryPoints];

  while (queue.length > 0) {
    const file = queue.shift();
    if (!file || visited.has(file)) continue;
    visited.add(file);

    const source = ts.createSourceFile(
      file,
      readFileSync(file, 'utf8'),
      ts.ScriptTarget.ES2022,
      true,
      ts.ScriptKind.TS,
    );

    for (const edge of collectEdges(source)) {
      if (!edge.isValueImport) continue;

      if (edge.specifier.startsWith('.') || edge.specifier.startsWith('/')) {
        const resolved = resolveFirstPartyModule(file, edge.specifier);
        if (!visited.has(resolved)) queue.push(resolved);
        continue;
      }

      const reason = bannedReason(edge.specifier);
      if (reason) {
        violations.push({ file, specifier: edge.specifier, reason });
      }
    }
  }

  return { visited: [...visited], violations };
}

describe('platform-agnostic entrypoint import purity', () => {
  test('client and embedded entrypoints stay free of server/node-only value imports', () => {
    for (const entry of ENTRY_POINTS) {
      expect(existsSync(entry)).toBe(true);
    }

    const { visited, violations } = walkImportGraph(ENTRY_POINTS);

    // Sanity check: the traversal must actually walk a real subgraph, not just the
    // two entry files, or this test would pass vacuously.
    expect(visited.length).toBeGreaterThan(5);

    if (violations.length > 0) {
      const report = violations
        .map((v) => `  ${v.file}\n    imports "${v.specifier}" (${v.reason})`)
        .join('\n');
      throw new Error(
        `Found server/node-only value imports reachable from client/embedded:\n${report}`,
      );
    }
  });
});
