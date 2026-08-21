#!/usr/bin/env node
// Checks that every code-shaped name mentioned in the docs and in source comments still
// resolves against comment-stripped source. See scripts/pack-check.mjs for house style.
import { globSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), '..');

// Documents whose names must resolve: markdown corpus (backticked spans only) and TS
// comment text from both packages.
const markdownGlobs = [
  'docs/*.md',
  'README.md',
  'packages/drizzle-pulse/README.md',
  'packages/drizzle-pulse/AGENTS.md',
  'packages/integration-tests/AGENTS.md',
  'packages/drizzle-pulse/type-tests/AGENTS.md',
];
const tsCommentGlobs = [
  'packages/drizzle-pulse/src/**/*.ts',
  'packages/drizzle-pulse/type-tests/**/*.ts',
  'packages/integration-tests/src/**/*.ts',
];
// TS trees the haystack (what counts as existing) is built from, comments stripped.
const haystackGlobs = tsCommentGlobs;
// External .d.ts trees whose identifiers also resolve a candidate.
const externalDtsGlobs = [
  'node_modules/minipg/**/*.d.{ts,cts}',
  'node_modules/drizzle-orm/**/*.d.{ts,cts}',
  'node_modules/hono/**/*.d.{ts,cts}',
  'node_modules/zod/**/*.d.{ts,cts}',
  'node_modules/superjson/**/*.d.{ts,cts}',
  'node_modules/@types/react/**/*.d.{ts,cts}',
];

// Each entry names the external tool or document-local construct that owns the name; the
// check must never gain an entry that silences a real finding.
const ALLOWLIST = new Map([
  ['schemaFilter', 'drizzle-kit generate/push option; drizzle-kit is not a dependency here'],
  ['sourceSchema', 'derivation variable defined by docs/events-table-convention.md itself'],
  [
    'postBuild',
    'drizzle-orm runtime column hook; exists in its shipped JS but not its type declarations',
  ],
]);

function globFiles(patterns) {
  const files = [];
  for (const pattern of patterns) {
    const matches = globSync(pattern, { cwd: repoRoot });
    if (matches.length === 0) {
      console.error(`doc-names: corpus pattern matched no files: ${pattern}`);
      process.exit(2);
    }
    for (const match of matches) files.push(match);
  }
  return files;
}

// Blanks out comment text, character by character, tracking string/template state so an
// apostrophe inside a comment ("the collection's snapshot") is never misread as opening a
// real string and swallowing unrelated code between it and the next apostrophe. Mirrors
// extractCommentLines' scan so the two can never disagree about where a comment starts.
function stripTsComments(source) {
  let out = '';
  let i = 0;
  const n = source.length;
  while (i < n) {
    const ch = source[i];
    if (ch === '"' || ch === "'" || ch === '`') {
      const quote = ch;
      const start = i;
      i++;
      while (i < n && source[i] !== quote) {
        if (source[i] === '\\') i++;
        i++;
      }
      i++;
      out += source.slice(start, i);
      continue;
    }
    if (ch === '/' && source[i + 1] === '/') {
      while (i < n && source[i] !== '\n') i++;
      continue;
    }
    if (ch === '/' && source[i + 1] === '*') {
      i += 2;
      while (i < n && !(source[i] === '*' && source[i + 1] === '/')) i++;
      i += 2;
      continue;
    }
    out += ch;
    i++;
  }
  return out;
}

// Extracts { text, line } comment segments with a full character scan, tracking string and
// template state so a glob-like string such as 'src/server/*' can never be misread as a
// block-comment opener (a line-based indexOf('/*') scan gets this wrong and then never
// finds a closing '*/', silently swallowing the rest of the file as "comment"). Skips
// lines that start a linter suppression directive.
function extractCommentLines(source) {
  const segments = [];
  let line = 1;
  let i = 0;
  const n = source.length;
  let currentComment = null; // { text: string[], line: number } | null

  function flushComment() {
    if (currentComment) {
      segments.push({ text: currentComment.text.join(''), line: currentComment.line });
      currentComment = null;
    }
  }

  while (i < n) {
    const ch = source[i];
    if (ch === '\n') {
      flushComment();
      line++;
      i++;
      continue;
    }
    // String and template literals: skip to the matching close, respecting escapes.
    if (ch === '"' || ch === "'" || ch === '`') {
      const quote = ch;
      i++;
      while (i < n && source[i] !== quote) {
        if (source[i] === '\\') i++;
        if (source[i] === '\n') line++;
        i++;
      }
      i++;
      continue;
    }
    if (ch === '/' && source[i + 1] === '/') {
      const start = i;
      while (i < n && source[i] !== '\n') i++;
      segments.push({ text: source.slice(start, i), line });
      continue;
    }
    if (ch === '/' && source[i + 1] === '*') {
      currentComment = { text: [], line };
      i += 2;
      while (i < n && !(source[i] === '*' && source[i + 1] === '/')) {
        if (source[i] === '\n') {
          flushComment();
          currentComment = { text: [], line: line + 1 };
          line++;
        } else {
          currentComment.text.push(source[i]);
        }
        i++;
      }
      flushComment();
      i += 2;
      continue;
    }
    i++;
  }
  flushComment();
  return segments.filter((seg) => !/^\s*(?:\/\/|\*)?\s*biome-ignore\b/.test(seg.text));
}

// Tokenizes a span into identifier / dotted-pair tokens, stripping generic suffixes and
// leading/trailing punctuation so `` `PulseTable<T>` `` or `.reconcile()` still yield the
// inner identifier. `<name>` naming-template placeholders (`<eventsTableName>_snapshot_seq`,
// describing a pattern rather than referencing a symbol) are dropped first, since they use
// the same angle-bracket syntax as a generic but never name a real declaration.
function tokenize(span) {
  const withoutPlaceholders = span.replace(/<([A-Za-z_$][A-Za-z0-9_$]*)>/g, '');
  // Collapses real call arguments (`pullClient(router, subscriptionId, snapshot)` ->
  // `pullClient()`) so a parameter name in an illustrative call or a documented signature
  // is never extracted as its own candidate: it is not a reference to any declared symbol,
  // just a local label. The callee itself still tokenizes the same either way.
  const withoutCallArgs = withoutPlaceholders.replace(/\(([^()]*)\)/g, '()');
  const re = /[A-Za-z_$][A-Za-z0-9_$]*(?:\.[A-Za-z_$][A-Za-z0-9_$]*)*(?:\(\))?/g;
  return [...withoutCallArgs.matchAll(re)].map((match) => match[0]);
}

function extractBacktickSpans(text) {
  const re = /`([^`]+)`/g;
  return [...text.matchAll(re)].map((match) => match[1]);
}

function extractLinkTargets(text) {
  const re = /\{@link\s+([^}\s|]+)/g;
  return [...text.matchAll(re)].map((match) => match[1]);
}

// Real call syntax has no space between the name and the paren; an English parenthetical
// like "retryable (not a permanent no-op)" does, and must not be read as a call.
function extractCallShapes(text) {
  const re = /\b([A-Za-z_$][A-Za-z0-9_$]*(?:\.[A-Za-z_$][A-Za-z0-9_$]*)?)\(/g;
  return [...text.matchAll(re)].map((match) => match[1]);
}

function hasTwoLowercase(token) {
  return (token.match(/[a-z]/g) ?? []).length >= 2;
}

// A token is code-shaped (a candidate) rather than prose if it has a call suffix, is
// PascalCase or camelCase with an internal case change plus two lowercase letters, or
// starts with $. Bare lowercase words never qualify.
function isCandidateSegment(base) {
  if (base.startsWith('$')) return true;
  if (/^[A-Z][A-Za-z0-9]*[a-z][A-Za-z0-9]*$/.test(base)) return hasTwoLowercase(base);
  if (/^[a-z][A-Za-z0-9]*[A-Z][A-Za-z0-9]*$/.test(base)) return hasTwoLowercase(base);
  return false;
}

// A token is code-shaped (a candidate) rather than prose if it has a call suffix, or if
// it (or, for a dotted `Root.member` token, either half) is PascalCase or camelCase with
// two lowercase letters, or starts with $. Bare lowercase words never qualify.
function isCandidate(token) {
  if (token.endsWith('()')) return true;
  const base = token.replace(/\(\)$/, '');
  if (base.includes('.')) return base.split('.').some(isCandidateSegment);
  return isCandidateSegment(base);
}

function isReservedPrefixed(token) {
  return token.replace(/\(\)$/, '').startsWith('$old_');
}

// Splits `Root.member` into its parts.
function splitDotted(token) {
  const base = token.replace(/\(\)$/, '');
  const parts = base.split('.');
  if (parts.length !== 2) return null;
  const [root, member] = parts;
  return { root, member };
}

function findDeclarationBlock(root, haystackSources) {
  for (const source of haystackSources) {
    // Excludes `,` and `}` before the opening brace so `import { type Root, Other }`
    // (Root immediately followed by a comma, not a real declaration body) never matches.
    const re = new RegExp(`\\b(?:type|interface|class)\\s+${root}\\b[^{;,}]*\\{`);
    const match = re.exec(source);
    if (!match) continue;
    let depth = 0;
    let start = -1;
    for (let i = match.index; i < source.length; i++) {
      const ch = source[i];
      if (ch === '{') {
        if (depth === 0) start = i + 1;
        depth++;
      } else if (ch === '}') {
        depth--;
        if (depth === 0) return source.slice(start, i);
      }
    }
  }
  return null;
}

function memberAtDepthOne(block, member) {
  let depth = 0;
  const re = /[{}]|\b[A-Za-z_$][A-Za-z0-9_$]*\b/g;
  for (const match of block.matchAll(re)) {
    const tok = match[0];
    if (tok === '{') {
      depth++;
      continue;
    }
    if (tok === '}') {
      depth--;
      continue;
    }
    if (depth === 0 && tok === member) {
      // A member declaration: a field (`name: T`, `name?: T`) or a method (`name(...)`,
      // possibly with a generic parameter list first).
      const lastIndex = match.index + match[0].length;
      const after = block.slice(lastIndex).match(/^\s*\??\s*[:(<]/);
      if (after) return true;
    }
  }
  return false;
}

function main() {
  const markdownFiles = globFiles(markdownGlobs);
  const tsFiles = globFiles(tsCommentGlobs);
  const haystackFiles = globFiles(haystackGlobs);
  const externalDtsFiles = globFiles(externalDtsGlobs);

  // Build the haystack: every identifier token from the TS trees with comments stripped.
  const haystack = new Set();
  const haystackSources = [];
  for (const file of haystackFiles) {
    const raw = readFileSync(join(repoRoot, file), 'utf8');
    const stripped = stripTsComments(raw);
    haystackSources.push(stripped);
    const re = /[A-Za-z_$][A-Za-z0-9_$]*/g;
    for (const match of stripped.matchAll(re)) haystack.add(match[0]);
  }
  for (const file of externalDtsFiles) {
    const raw = readFileSync(join(repoRoot, file), 'utf8');
    const re = /[A-Za-z_$][A-Za-z0-9_$]*/g;
    for (const match of raw.matchAll(re)) haystack.add(match[0]);
  }

  const findings = [];
  let scannedFiles = 0;
  let totalCandidates = 0;

  function evaluateCandidate(token) {
    const dotted = splitDotted(token);
    if (dotted) {
      if (/^[A-Z]/.test(dotted.root)) {
        // A type/interface/class member path: resolve the root, then require the member
        // at brace depth 1 of its declaration block. A root with no declaration block in
        // this repo's source (an external type) cannot be checked further; pass it through.
        if (!haystack.has(dotted.root)) return `${dotted.root}.${dotted.member}`;
        const block = findDeclarationBlock(dotted.root, haystackSources);
        if (block && !memberAtDepthOne(block, dotted.member)) {
          return `${dotted.root}.${dotted.member}`;
        }
        return null;
      }
      // A value-call reference (`rep.start()`, `this.runReplicationLoop()`): root and
      // member each must exist somewhere as identifiers in the flat haystack, independent
      // of whether they are declared on the same object. This is what catches a
      // driver-internal variable name (`rep`) or a deleted method name that a live root
      // happens to precede. Restricted to genuine call syntax: a bare member-access
      // reference with no call suffix (`drizzlePulse.$client` illustrating "whatever your
      // registry instance is called") is describing a value's shape by example, not
      // naming a specific declared symbol, and cannot be resolved this way.
      if (!token.endsWith('()')) return null;
      if (!haystack.has(dotted.root)) return dotted.root;
      if (!haystack.has(dotted.member)) return dotted.member;
      return null;
    }
    const base = token.replace(/\(\)$/, '');
    if (ALLOWLIST.has(base)) return null;
    if (isReservedPrefixed(base)) return null;
    if (haystack.has(base)) return null;
    return base;
  }

  function processFile(file, candidateSpans) {
    scannedFiles++;
    const seenTokens = new Set();
    for (const { span, line } of candidateSpans) {
      for (const token of tokenize(span)) {
        if (!isCandidate(token)) continue;
        const key = `${token}:${line}`;
        if (seenTokens.has(key)) continue;
        seenTokens.add(key);
        totalCandidates++;
        const finding = evaluateCandidate(token);
        if (finding) findings.push({ file, line, name: finding });
      }
    }
    console.log(`scanned ${file} (${seenTokens.size} candidates)`);
  }

  for (const file of markdownFiles) {
    const raw = readFileSync(join(repoRoot, file), 'utf8');
    const lines = raw.split('\n');
    const spans = [];
    let inFence = false;
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      if (/^\s*```/.test(line)) {
        inFence = !inFence;
        continue;
      }
      if (inFence) continue;
      for (const span of extractBacktickSpans(line)) {
        spans.push({ span, line: i + 1 });
      }
    }
    processFile(file, spans);
  }

  for (const file of tsFiles) {
    const raw = readFileSync(join(repoRoot, file), 'utf8');
    const segments = extractCommentLines(raw);
    const spans = [];
    for (const { text, line } of segments) {
      for (const span of extractBacktickSpans(text)) spans.push({ span, line });
      for (const span of extractLinkTargets(text)) spans.push({ span, line });
      for (const span of extractCallShapes(text)) spans.push({ span: `${span}()`, line });
    }
    processFile(file, spans);
  }

  console.log(`\n${scannedFiles} files scanned, ${totalCandidates} candidates checked`);

  if (findings.length > 0) {
    console.log(`\n${findings.length} finding(s):`);
    for (const { file, line, name } of findings) {
      console.log(`${file}:${line}: ${name}`);
    }
    process.exit(1);
  }

  console.log('doc-names: PASSED');
  process.exit(0);
}

main();
