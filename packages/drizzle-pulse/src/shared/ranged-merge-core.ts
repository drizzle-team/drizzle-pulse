import { comparePkValues, isPkComparable } from './pk-utils.js';
import { PulseMergeCore, type PulsePk } from './pulse-merge-core.js';

export interface RangedMergeCoreOptions {
  order: 'asc' | 'desc';
  limit: number | null;
  rangeStart: PulsePk | null;
  rangeEnd: PulsePk | null;
}

// HTTP-only variant: adds load-more (appendRows) and the range-window insert gate on top of
// the full-set base. The embedded client never constructs this class — keeping it out of
// embedded's import graph is what SPLIT-01/SPLIT-05 enforce.
export class RangedPulseMergeCore<
  TRow extends Record<string, unknown> & { $pk: unknown },
> extends PulseMergeCore<TRow> {
  limit: number | null;
  rangeStart: PulsePk | null;
  rangeEnd: PulsePk | null;

  constructor(opts: RangedMergeCoreOptions) {
    super({ order: opts.order });
    this.limit = opts.limit;
    this.rangeStart = opts.rangeStart;
    this.rangeEnd = opts.rangeEnd;
  }

  // Appends load-more rows to the end without re-sorting. Returns true if any row was added.
  appendRows(rows: readonly TRow[]): boolean {
    const toAppend: TRow[] = [];
    for (const row of rows) {
      if (!isPkComparable(row.$pk)) continue;
      if (this._pkMap.has(row.$pk)) continue;
      toAppend.push(row);
    }
    if (toAppend.length === 0) return false;
    for (const row of toAppend) {
      this._pkMap.set(row.$pk as PulsePk, row);
    }
    this.data = [...this.data, ...toAppend];
    return true;
  }

  override clear(): void {
    super.clear();
    this.rangeStart = null;
    this.rangeEnd = null;
  }

  // Returns true when a new insert should be accepted into the current window.
  // limit === null means no-range mode: every matching insert is accepted.
  protected override acceptInsert(rowPk: PulsePk): boolean {
    if (this.limit === null) return true;
    if (this.rangeStart === null || this.rangeEnd === null) return true;
    if (this.order === 'desc') {
      return comparePkValues(rowPk, this.rangeEnd) > 0;
    }
    return comparePkValues(rowPk, this.rangeStart) < 0;
  }
}
