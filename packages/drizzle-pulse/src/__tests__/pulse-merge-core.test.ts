import { describe, expect, test } from 'bun:test';
import { PulseMergeCore } from '../react/pulse-merge-core.js';

type TestRow = {
  $pk: number;
  label: string;
};

describe('PulseMergeCore', () => {
  describe('no-range mode (limit: null)', () => {
    test('accepts every insert with a comparable pk', () => {
      const core = new PulseMergeCore<TestRow>({
        order: 'asc',
        limit: null,
        rangeStart: null,
        rangeEnd: null,
      });

      const changed = core.applyEvents([
        { op: 'insert', row: { $pk: 999, label: 'big' }, pk: 999 },
      ]);
      expect(changed).toBe(true);
      expect(core.data).toHaveLength(1);
      expect(core.data[0]?.$pk).toBe(999);
    });

    test('accepts arbitrary pk magnitudes regardless of window', () => {
      const core = new PulseMergeCore<TestRow>({
        order: 'asc',
        limit: null,
        rangeStart: null,
        rangeEnd: null,
      });

      // No range is set, so any pk should be accepted
      core.applyEvents([
        { op: 'insert', row: { $pk: 100, label: 'hundred' }, pk: 100 },
        { op: 'insert', row: { $pk: 1, label: 'one' }, pk: 1 },
        { op: 'insert', row: { $pk: 50, label: 'fifty' }, pk: 50 },
      ]);

      expect(core.data).toHaveLength(3);
    });

    test('maintains asc order for inserts', () => {
      const core = new PulseMergeCore<TestRow>({
        order: 'asc',
        limit: null,
        rangeStart: null,
        rangeEnd: null,
      });

      core.applyEvents([
        { op: 'insert', row: { $pk: 3, label: 'c' }, pk: 3 },
        { op: 'insert', row: { $pk: 1, label: 'a' }, pk: 1 },
        { op: 'insert', row: { $pk: 2, label: 'b' }, pk: 2 },
      ]);

      expect(core.data.map((r) => r.$pk)).toEqual([1, 2, 3]);
    });

    test('maintains desc order for inserts', () => {
      const core = new PulseMergeCore<TestRow>({
        order: 'desc',
        limit: null,
        rangeStart: null,
        rangeEnd: null,
      });

      core.applyEvents([
        { op: 'insert', row: { $pk: 1, label: 'a' }, pk: 1 },
        { op: 'insert', row: { $pk: 3, label: 'c' }, pk: 3 },
        { op: 'insert', row: { $pk: 2, label: 'b' }, pk: 2 },
      ]);

      expect(core.data.map((r) => r.$pk)).toEqual([3, 2, 1]);
    });
  });

  describe('no-op-batch guard', () => {
    test('empty batch returns false', () => {
      const core = new PulseMergeCore<TestRow>({
        order: 'asc',
        limit: null,
        rangeStart: null,
        rangeEnd: null,
      });

      const changed = core.applyEvents([]);
      expect(changed).toBe(false);
    });

    test('insert rejected by range window returns false', () => {
      // asc, rangeStart=100 means prepend inserts must have pk < 100
      // pk=200 fails that check → batch is a no-op
      const core = new PulseMergeCore<TestRow>({
        order: 'asc',
        limit: 10,
        rangeStart: 100,
        rangeEnd: 110,
      });

      const changed = core.applyEvents([
        { op: 'insert', row: { $pk: 200, label: 'outside' }, pk: 200 },
      ]);
      expect(changed).toBe(false);
      expect(core.data).toHaveLength(0);
    });

    test('all-rejected batch leaves data unchanged', () => {
      const core = new PulseMergeCore<TestRow>({
        order: 'asc',
        limit: 10,
        rangeStart: 100,
        rangeEnd: 110,
      });
      core.rebuildFromRows([{ $pk: 105, label: 'x' }]);

      const before = core.data;
      const changed = core.applyEvents([
        { op: 'insert', row: { $pk: 200, label: 'outside' }, pk: 200 },
      ]);
      expect(changed).toBe(false);
      expect(core.data).toBe(before);
    });

    test('duplicate-insert batch (pk already in pkMap) returns false', () => {
      const core = new PulseMergeCore<TestRow>({
        order: 'asc',
        limit: null,
        rangeStart: null,
        rangeEnd: null,
      });
      core.rebuildFromRows([{ $pk: 1, label: 'a' }]);

      const changed = core.applyEvents([{ op: 'insert', row: { $pk: 1, label: 'a-dup' }, pk: 1 }]);
      expect(changed).toBe(false);
      expect(core.data[0]?.label).toBe('a');
    });

    test('returns true when at least one event mutates state', () => {
      const core = new PulseMergeCore<TestRow>({
        order: 'asc',
        limit: null,
        rangeStart: null,
        rangeEnd: null,
      });

      const changed = core.applyEvents([{ op: 'insert', row: { $pk: 1, label: 'a' }, pk: 1 }]);
      expect(changed).toBe(true);
    });
  });

  describe('merge operations', () => {
    test('insert then update(matchesNew) replaces the row', () => {
      const core = new PulseMergeCore<TestRow>({
        order: 'asc',
        limit: null,
        rangeStart: null,
        rangeEnd: null,
      });
      core.applyEvents([{ op: 'insert', row: { $pk: 1, label: 'a' }, pk: 1 }]);

      const changed = core.applyEvents([
        {
          op: 'update',
          row: { $pk: 1, label: 'a2' },
          old_row: { $pk: 1, label: 'a' },
          pk: 1,
          matchesNew: true,
          matchesOld: true,
        },
      ]);
      expect(changed).toBe(true);
      expect(core.data).toEqual([{ $pk: 1, label: 'a2' }]);
    });

    test('update(matchesOld only) removes the row', () => {
      const core = new PulseMergeCore<TestRow>({
        order: 'asc',
        limit: null,
        rangeStart: null,
        rangeEnd: null,
      });
      core.rebuildFromRows([{ $pk: 1, label: 'a' }]);

      const changed = core.applyEvents([
        {
          op: 'update',
          row: { $pk: 1, label: 'a-new' },
          old_row: { $pk: 1, label: 'a' },
          pk: 1,
          matchesNew: false,
          matchesOld: true,
        },
      ]);
      expect(changed).toBe(true);
      expect(core.data).toHaveLength(0);
    });

    test('delete(matchesOld) removes the row', () => {
      const core = new PulseMergeCore<TestRow>({
        order: 'asc',
        limit: null,
        rangeStart: null,
        rangeEnd: null,
      });
      core.rebuildFromRows([{ $pk: 1, label: 'a' }]);

      const changed = core.applyEvents([
        { op: 'delete', old_row: { $pk: 1, label: 'a' }, pk: 1, matchesOld: true },
      ]);
      expect(changed).toBe(true);
      expect(core.data).toHaveLength(0);
      expect(core.data.some((r) => r.$pk === 1)).toBe(false);
    });

    test('update with matchesNew for absent pk inserts it sorted', () => {
      const core = new PulseMergeCore<TestRow>({
        order: 'asc',
        limit: null,
        rangeStart: null,
        rangeEnd: null,
      });
      core.rebuildFromRows([
        { $pk: 1, label: 'a' },
        { $pk: 3, label: 'c' },
      ]);

      core.applyEvents([
        {
          op: 'update',
          row: { $pk: 2, label: 'b' },
          old_row: { $pk: 2, label: 'b-old' },
          pk: 2,
          matchesNew: true,
          matchesOld: false,
        },
      ]);
      expect(core.data.map((r) => r.$pk)).toEqual([1, 2, 3]);
    });

    test('delete for pk not in pkMap is a no-op', () => {
      const core = new PulseMergeCore<TestRow>({
        order: 'asc',
        limit: null,
        rangeStart: null,
        rangeEnd: null,
      });
      core.rebuildFromRows([{ $pk: 1, label: 'a' }]);

      const changed = core.applyEvents([
        { op: 'delete', old_row: { $pk: 99, label: 'missing' }, pk: 99, matchesOld: true },
      ]);
      expect(changed).toBe(false);
      expect(core.data).toHaveLength(1);
    });
  });

  describe('appendRows', () => {
    test('appends new rows and dedupes existing pks', () => {
      const core = new PulseMergeCore<TestRow>({
        order: 'asc',
        limit: null,
        rangeStart: null,
        rangeEnd: null,
      });
      core.rebuildFromRows([{ $pk: 1, label: 'a' }]);

      const appended = core.appendRows([
        { $pk: 1, label: 'a-dup' },
        { $pk: 2, label: 'b' },
      ]);
      expect(appended).toBe(true);
      expect(core.data).toEqual([
        { $pk: 1, label: 'a' },
        { $pk: 2, label: 'b' },
      ]);
    });

    test('returns false when all rows are duplicates', () => {
      const core = new PulseMergeCore<TestRow>({
        order: 'asc',
        limit: null,
        rangeStart: null,
        rangeEnd: null,
      });
      core.rebuildFromRows([{ $pk: 1, label: 'a' }]);

      const appended = core.appendRows([{ $pk: 1, label: 'a-dup' }]);
      expect(appended).toBe(false);
    });
  });

  describe('clear', () => {
    test('resets all state', () => {
      const core = new PulseMergeCore<TestRow>({
        order: 'asc',
        limit: 10,
        rangeStart: 1,
        rangeEnd: 10,
      });
      core.rebuildFromRows([{ $pk: 1, label: 'a' }]);
      core.clear();

      expect(core.data).toHaveLength(0);
      // pkMap must also be cleared — re-adding the same pk succeeds instead of being deduped
      const appended = core.appendRows([{ $pk: 1, label: 'a2' }]);
      expect(appended).toBe(true);
      expect(core.data).toEqual([{ $pk: 1, label: 'a2' }]);
    });
  });
});
