import { describe, expect, test } from 'bun:test';

describe('smoke', () => {
  test('basic assertion', () => {
    expect(1 + 1).toBe(2);
  });
});
