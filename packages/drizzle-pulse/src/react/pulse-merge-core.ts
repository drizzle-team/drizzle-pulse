import { comparePkValues, isPkComparable } from '../shared/pk-utils.js';
import type { PulseEvent } from './pulse-query.js';

type PulsePk = string | number;

export interface MergeCoreOptions {
  order: 'asc' | 'desc';
  limit: number | null;
  rangeStart: PulsePk | null;
  rangeEnd: PulsePk | null;
}

export class PulseMergeCore<TRow extends Record<string, unknown> & { $pk: unknown }> {
  private _pkMap = new Map<PulsePk, TRow>();
  private _data: TRow[] = [];
  order: 'asc' | 'desc';
  limit: number | null;
  rangeStart: PulsePk | null;
  rangeEnd: PulsePk | null;

  constructor(opts: MergeCoreOptions) {
    this.order = opts.order;
    this.limit = opts.limit;
    this.rangeStart = opts.rangeStart;
    this.rangeEnd = opts.rangeEnd;
  }

  get data(): readonly TRow[] {
    return this._data;
  }

  rebuildFromRows(rows: TRow[]): void {
    const nextPkMap = new Map<PulsePk, TRow>();
    for (const row of rows) {
      if (!isPkComparable(row.$pk)) continue;
      nextPkMap.set(row.$pk, row);
    }
    this._pkMap = nextPkMap;
    this._data = rows;
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
    this._data = [...this._data, ...toAppend];
    return true;
  }

  // Returns true only if at least one event mutated state (no-op-batch guard).
  applyEvents(events: readonly PulseEvent<TRow>[]): boolean {
    if (events.length === 0) return false;
    let updated = [...this._data];
    let mutated = false;

    for (const event of events) {
      if (event.op === 'insert') {
        const row = event.row;
        if (!isPkComparable(row.$pk)) continue;
        if (this._pkMap.has(row.$pk)) continue;
        if (!this.shouldApplyPrependInsert(row.$pk)) continue;
        this._pkMap.set(row.$pk, row);
        updated = this.insertSorted(updated, row);
        mutated = true;
        continue;
      }

      if (event.op === 'update') {
        const row = event.row;
        const rowPk = row.$pk;
        if (!isPkComparable(rowPk)) continue;
        const existingIndex = updated.findIndex((candidate) => candidate.$pk === rowPk);

        if (existingIndex >= 0) {
          if (event.matchesNew) {
            this._pkMap.set(rowPk, row);
            updated[existingIndex] = row;
            mutated = true;
          } else if (event.matchesOld) {
            this._pkMap.delete(rowPk);
            updated.splice(existingIndex, 1);
            mutated = true;
          }
          continue;
        }

        if (event.matchesNew) {
          this._pkMap.set(rowPk, row);
          updated = this.insertSorted(updated, row);
          mutated = true;
        }
        continue;
      }

      // delete
      const rowPk = event.pk;
      if (!isPkComparable(rowPk)) continue;
      if (!this._pkMap.has(rowPk)) continue;
      if (!event.matchesOld) continue;
      this._pkMap.delete(rowPk);
      const existingIndex = updated.findIndex((candidate) => candidate.$pk === rowPk);
      if (existingIndex >= 0) {
        updated.splice(existingIndex, 1);
      }
      mutated = true;
    }

    if (mutated) this._data = updated;
    return mutated;
  }

  clear(): void {
    this._pkMap = new Map();
    this._data = [];
    this.rangeStart = null;
    this.rangeEnd = null;
  }

  private insertSorted(rows: TRow[], row: TRow): TRow[] {
    const updated = [...rows];
    const rowPk = row.$pk;
    if (!isPkComparable(rowPk)) return updated;

    let low = 0;
    let high = updated.length;

    while (low < high) {
      const mid = Math.floor((low + high) / 2);
      const midRow = updated[mid];
      if (!midRow) break;

      if (!isPkComparable(midRow.$pk)) {
        low = mid + 1;
        continue;
      }

      const comparison = comparePkValues(rowPk, midRow.$pk);
      const goesBefore = this.order === 'desc' ? comparison > 0 : comparison < 0;
      if (goesBefore) {
        high = mid;
      } else {
        low = mid + 1;
      }
    }

    updated.splice(low, 0, row);
    return updated;
  }

  // Returns true when a new insert should be accepted into the current window.
  // limit === null means no-range mode: every matching insert is accepted.
  private shouldApplyPrependInsert(rowPk: PulsePk): boolean {
    if (this.limit === null) return true;
    if (this.rangeStart === null || this.rangeEnd === null) return true;
    if (this.order === 'desc') {
      return comparePkValues(rowPk, this.rangeEnd) > 0;
    }
    return comparePkValues(rowPk, this.rangeStart) < 0;
  }
}
