import { describe, expect, it } from 'vitest';

import { assertReleaseManifest } from './release-identity.mjs';

const expected = {
  expectedName: 'animus-environment-railway',
  expectedVersion: '0.4.23',
};

describe('assertReleaseManifest', () => {
  it('accepts the exact plugin name and package version', () => {
    expect(
      assertReleaseManifest(
        JSON.stringify({ name: 'animus-environment-railway', version: '0.4.23' }),
        expected,
      ),
    ).toEqual({ name: 'animus-environment-railway', version: '0.4.23' });
  });

  it('rejects a stale executable version', () => {
    expect(() =>
      assertReleaseManifest(
        JSON.stringify({ name: 'animus-environment-railway', version: '0.4.22' }),
        expected,
      ),
    ).toThrow('refusing to release animus-environment-railway@0.4.22 as 0.4.23');
  });

  it('rejects an unexpected plugin name', () => {
    expect(() =>
      assertReleaseManifest(JSON.stringify({ name: 'wrong-plugin', version: '0.4.23' }), expected),
    ).toThrow('expected plugin name animus-environment-railway');
  });

  it('rejects malformed manifest output', () => {
    expect(() => assertReleaseManifest('not-json', expected)).toThrow('invalid JSON manifest');
  });
});
