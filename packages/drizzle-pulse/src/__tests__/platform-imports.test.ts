import { describe, expect, test } from 'bun:test';
import { existsSync, readFileSync } from 'node:fs';
import { builtinModules } from 'node:module';
import { dirname, join, resolve } from 'node:path';
import ts from 'typescript';

// Static import-graph audit: every module reachable via a VALUE import from the
// platform-agnostic entrypoints must stay free of server/node-only runtime deps.
// `import type` edges are erased at compile time and never execute, so they're
// excluded from traversal. Mirrors drizzle-orm's own platform-imports test philosophy.

const SRC_ROOT = resolve(import.meta.dir, '..');
const ENTRY_POINTS = [
  join(SRC_ROOT, 'client/index.ts'),
  join(SRC_ROOT, 'client/react/index.ts'),
  join(SRC_ROOT, 'client/embedded/index.ts'),
  join(SRC_ROOT, 'index.ts'),
];

// The schema-definition surface: reached only from the root entrypoint and consumed
// server-side — never browser bundles — so this module alone may value-import
// drizzle-orm/pg-core (pulse-table.ts needs getTableConfig). Every other banned specifier
// still applies, and client/react/embedded reach none of it.
const PG_CORE_ALLOWED = new Set([join(SRC_ROOT, 'pulse-table.ts')]);

// Covers every bare (non-`node:`-prefixed) builtin specifier, including subpaths
// like `fs/promises`; `node:`-prefixed specifiers are caught separately below.
const NODE_BUILTINS = new Set(builtinModules);

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
  if (clause.phaseModifier === ts.SyntaxKind.TypeKeyword) return false;
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
        if (PG_CORE_ALLOWED.has(file) && edge.specifier.startsWith('drizzle-orm/pg-core')) continue;
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
    // four entry files, or this test would pass vacuously. Assert one known-reachable
    // module per layer (client, react, shared) so a partially broken traversal can't
    // hide behind a raw count. The embedded client reaches the server only through
    // `import type` edges (the runtime is passed in, never imported), so no server
    // module is value-reachable — exactly the purity this test guards.
    expect(visited.length).toBeGreaterThan(10);
    const KNOWN_REACHABLE = [
      join(SRC_ROOT, 'client/pulse-query.ts'),
      join(SRC_ROOT, 'client/react/use-pulse-query.ts'),
      join(SRC_ROOT, 'shared/pulse-merge-core.ts'),
      join(SRC_ROOT, 'shared/superjson.ts'),
    ];
    for (const file of KNOWN_REACHABLE) {
      expect(visited).toContain(file);
    }

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
