# minipg: proposals

## 1. `binary: 'auto'` negotiation should account for declared shapes *(bug)*

With `binary: 'auto'`, `start()` probes the catalog for the published columns' type OIDs
and enables binary mode only when every OID decodes identically in binary and text
(`replBinaryMatchesText`). Declared shapes are never consulted — even though
`this.shaped` is already built before the probe runs. A shape can pin a column to a
target that has no binary decoder (`replShapedLeaf` returns `null` for targets outside
`BIN_TEXT_EXACT`, e.g. `timestamp:string`, `timestamptz:string`, `date:string`). If the
probe still negotiates binary, the first non-null value for that column throws
`"has no binary decoder for its declared/default decode"` at decode time.

**Why it matters:** negotiation and decode disagree with each other. The crash is
unpredictable from the consumer's side — whether it fires depends on the *other* columns
in the table (adding any text-only column flips the probe and masks it), so a schema
migration on an unrelated column can turn a working stream into a crashing one. The only
defenses available to a consumer are pinning `binary: false` (losing binary mode
entirely) or reimplementing the probe's logic.

**Proposal:** fold shapes into the gate — negotiate binary only if the catalog probe
passes *and* every shaped column resolves a binary decoder through the same resolver
decode later uses (`replBinaryForCol`), skipping `oid === 0` columns (they fall back to
the relation default, which the catalog probe already vets). Negotiation and decode then
agree by construction; the worst case is a fallback to text. A regression test: one
shape with `timestamp:string` on an otherwise all-binary-capable table, assert text
negotiation and correct string decode.

---

## 2. `ack()` liveness footgun: document (or normalize) the LSN field taxonomy *(docs + ergonomics)*

A `commit` event carries both `lsn` (the commit record's own LSN, equal to
`begin.finalLsn`) and `endLsn` (the first LSN after the record — strictly greater,
always). The idle-keepalive advance is gated on the flushed position reaching the last
delivered `endLsn`. A consumer who acks `lsn` — the natural-looking field — never opens
that gate: the stream keeps delivering, tests pass, but `confirmed_flush_lsn` stops
advancing whenever the stream is idle, and WAL retention grows without any error
surfacing.

**Why it matters:** two near-identical fields where the wrong choice type-checks, works
under load, and only manifests as unbounded disk growth during production idle periods.
Nothing in the types or docs currently says which field to ack, and the
`lsn`/`endLsn`/`finalLsn` relationships are discoverable only empirically.

**Proposal:** minimally, one JSDoc line on `endLsn` and on `ack()`: "ack the commit's
`endLsn`; acking `lsn` permanently gates the idle keepalive advance." Better, make
`ack()` forgiving: an acked value within `[commit.lsn, commit.endLsn)` advances the
flushed position to that commit's `endLsn` (or equivalently, gate the idle advance on
the commit's start LSN). And document the taxonomy itself:
`begin.finalLsn === commit.lsn ≠ commit.endLsn`.

---

## 3. Server-initiated CopyDone should be distinguishable from an ordinary clean end *(API semantics)*

On a CopyDone frame (`'c'`) the `start()` generator simply returns — the same shape as
any other clean termination. But server-initiated CopyDone (Postgres restart, failover,
a pooler closing the copy stream) is an event the consumer *must* react to by
reconnecting, whereas a clean return can equally mean "the consumer stopped iterating."

**Why it matters:** because the two are indistinguishable, a consumer that treats a
clean return as benign silently stops replicating — no error, no event, just a stream
that ended while the slot keeps retaining WAL. This failure mode is invisible by
construction, and the only robust consumer strategy is "treat every clean return as a
failure" — which is a strong sign the API should encode that itself.

**Proposal:** on server-initiated CopyDone, throw a typed error (e.g.
`ReplicationStreamEnded { reason: 'copy-done' }` — catchable, with the connection
remaining usable for a follow-up `START_REPLICATION`), making "the stream only ends by
throwing" the documented contract. A weaker alternative: type the generator as
`AsyncGenerator<ReplicationEvent, 'copy-done'>` and document the return value's meaning.

---

## 4. `end()` should deterministically finish an in-flight `start()` iterator; accept an `AbortSignal` *(enhancement)*

A consumer blocked in `next()` is not woken by `end()`: the socket-close handler is
guarded by `!this.ended`, so after `end()` the generator only completes when the
keepalive interval's next write against the closed socket happens to surface an error —
at which point the generator's `finally` (which clears that same interval) finally runs.

**Why it matters:** shutdown latency becomes a side effect of keepalive timing —
graceful teardown takes up to `statusIntervalMs` and is nondeterministic, so orderly
shutdown (drain, then await the consumer loop) can't be written reliably. Anything
long-running needs a deterministic stop.

**Proposal:** (a) have `end()` resolve the internal wake promise so a blocked `next()`
observes the end and returns cleanly through its `finally`; (b) optionally accept
`signal?: AbortSignal` in `StartOptions` (and/or the connection config) performing that
same clean finish plus socket close. Part (a) alone fixes the determinism.

---

## 5. Fill TOAST-omitted UPDATE columns from the full old tuple *(enhancement)*

pgoutput omits unchanged TOASTed columns from an UPDATE's new tuple. minipg reports
their names in `unchanged: string[]` but leaves them out of `row` — even when the same
event carries a full old tuple (`oldKind === 'full'`) containing exactly those values
under exactly those keys.

**Why it matters:** every consumer that wants complete rows must hand-write the same
old-under-new merge, and forgetting it yields rows with silently missing columns — the
classic TOAST surprise that only appears once values grow past the inline threshold in
production. When a full old tuple is present the merge is lossless and cannot change
semantics, so leaving it to each consumer only distributes the bug.

**Proposal:** in the UPDATE decode path, when `oldKind === 'full'`, copy each column
named in `unchanged` from the decoded old tuple into `row`; keep reporting `unchanged`
so consumers who care about the distinction still can. No option flag needed.

---

## 6. Discriminate `old` on `oldKind` in the update event type *(typing)*

The update variant types `old: Row | null` and `oldKind: 'key' | 'full' | null` as
independent fields, but the decoder sets them together — `'key'`/`'full'` always comes
with a non-null `Row`. Consumers are forced into a runtime null check for a state the
decoder never produces.

**Proposal:** `({ oldKind: 'key' | 'full'; old: Row } | { oldKind: null; old: null })`.
The delete variant already types `old: Row` correctly; this brings update in line and
lets TypeScript narrow instead of the consumer guarding.

---

## 7. `createSlot`'s return type should reflect the `snapshot` option *(typing)*

`createSlot(name, { snapshot: 'export' })` is typed as returning
`snapshot: string | null`, though with `'export'` the server always returns a snapshot
name. Every typed consumer must write a runtime throw whose only purpose is narrowing a
case that cannot occur.

**Proposal:** overloads or a conditional return type — `snapshot: 'export'` →
`{ snapshot: string }`; `'nothing'`/omitted → `{ snapshot: null }`.

---

*Papercut, mentioned for completeness: `query(sql, opts)` throws a bare
`TypeError: params must be an array`; for untyped callers, either arg-shift a plain
non-array object into `opts` or make the message say the fix — "pass options as the
third argument: `query(sql, [], opts)`".*
