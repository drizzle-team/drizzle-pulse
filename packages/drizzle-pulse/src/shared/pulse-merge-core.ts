import { comparePkValues, isPkComparable } from './pk-utils.js';
import type { PulseEvent } from './pulse-events.js';

export type PulsePk = string | number;

export interface MergeCoreOptions {
  order: 'asc' | 'desc';
}

// Full-set merge core: every matching insert is accepted unconditionally (no window/pagination
// gate). This is the variant the embedded client value-imports — the ranged/HTTP-only surface
// (appendRows, the range window gate) lives in RangedPulseMergeCore instead.
export class PulseMergeCore<TRow extends Record<string, unknown> & { $pk: unknown }> {
  protected _pkMap = new Map<PulsePk, TRow>();
  data: TRow[] = [];
  order: 'asc' | 'desc';

  constructor(opts: MergeCoreOptions) {
    this.order = opts.order;
  }

  rebuildFromRows(rows: TRow[]): void {
    const nextPkMap = new Map<PulsePk, TRow>();
    for (const row of rows) {
      if (!isPkComparable(row.$pk)) continue;
      nextPkMap.set(row.$pk, row);
    }
    this._pkMap = nextPkMap;
    this.data = rows;
  }

  // Returns true only if at least one event mutated state (no-op-batch guard).
  applyEvents(events: readonly PulseEvent<TRow>[]): boolean {
    if (events.length === 0) return false;
    let updated = [...this.data];
    let mutated = false;

    for (const event of events) {
      if (event.op === 'insert') {
        const row = event.row;
        if (!isPkComparable(row.$pk)) continue;
        if (this._pkMap.has(row.$pk)) continue;
        if (!this.acceptInsert(row.$pk)) continue;
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

    if (mutated) this.data = updated;
    return mutated;
  }

  clear(): void {
    this._pkMap = new Map();
    this.data = [];
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

  // Returns true when a new insert should be accepted into the current state. The full-set
  // base always accepts — the ranged/HTTP variant overrides this with a window gate.
  protected acceptInsert(_rowPk: PulsePk): boolean {
    return true;
  }
}
