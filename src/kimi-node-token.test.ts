import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { afterEach, describe, expect, it, vi } from 'vitest';

import { RelayErrorCode, RelayRpcError, type BackendServicer } from '@launchapp-dev/animus-env-transport';

import { freshKimiNodeCredential } from './environment.js';
import { KIMI_TOKEN_METHOD, makeKimiNodeTokenServicer } from './kimi-node-token.js';

function kimiStore(credential: Record<string, unknown>): string {
  const dir = mkdtempSync(join(tmpdir(), 'kimi-servicer-'));
  mkdirSync(join(dir, 'credentials'), { recursive: true });
  writeFileSync(join(dir, 'credentials', 'kimi-code.json'), JSON.stringify(credential));
  return dir;
}

function validCredential(now: number): Record<string, unknown> {
  return {
    access_token: 'valid-access',
    refresh_token: 'valid-refresh',
    expires_at: Math.floor((now + 3_600_000) / 1000),
    expires_in: 3600,
    scope: 's',
    token_type: 'bearer',
  };
}

function servicerFor(dir: string, inner?: BackendServicer): BackendServicer {
  return makeKimiNodeTokenServicer(
    inner ?? { call: async () => ({ delegated: true }) },
    { KIMI_CODE_HOME: dir } as NodeJS.ProcessEnv,
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('makeKimiNodeTokenServicer', () => {
  it('serves the stored credential minus the refresh token while the access token is valid', async () => {
    const now = Date.now();
    const dir = kimiStore(validCredential(now));
    vi.stubGlobal('fetch', vi.fn(async () => {
      throw new Error('must not refresh a valid token');
    }));
    const result = (await servicerFor(dir).call(KIMI_TOKEN_METHOD, {})) as { credential: Record<string, unknown> };
    expect(result.credential.access_token).toBe('valid-access');
    expect(result.credential.expires_at).toBe(Math.floor((now + 3_600_000) / 1000));
    expect(result.credential.refresh_token).toBeUndefined();
    expect(result.credential.refreshToken).toBeUndefined();
    expect(vi.mocked(fetch)).not.toHaveBeenCalled();
  });

  it('refreshes a near-expiry credential centrally and serves the rotated access token', async () => {
    const now = Date.now();
    const dir = kimiStore({
      ...validCredential(now),
      access_token: 'old-access',
      refresh_token: 'old-refresh',
      expires_at: Math.floor((now - 60_000) / 1000),
      expires_in: 900,
    });
    const fetchMock = vi.fn(async (_url: unknown, init?: { body?: string }) => {
      expect(String(init?.body ?? '')).toContain('old-refresh');
      return new Response(JSON.stringify({ access_token: 'fresh-access', refresh_token: 'fresh-refresh', expires_in: 900 }), { status: 200 });
    });
    vi.stubGlobal('fetch', fetchMock);
    const result = (await servicerFor(dir).call(KIMI_TOKEN_METHOD, {})) as { credential: Record<string, unknown> };
    expect(result.credential.access_token).toBe('fresh-access');
    expect(result.credential.refresh_token).toBeUndefined();
    // The daemon-side store absorbed the rotation (single writer).
    const stored = JSON.parse(readFileSync(join(dir, 'credentials', 'kimi-code.json'), 'utf8'));
    expect(stored.access_token).toBe('fresh-access');
    expect(stored.refresh_token).toBe('fresh-refresh');
  });

  it('coalesces concurrent kimi/token calls onto one in-flight refresh', async () => {
    const now = Date.now();
    const dir = kimiStore({
      ...validCredential(now),
      access_token: 'old-access',
      refresh_token: 'old-refresh',
      expires_at: Math.floor((now - 60_000) / 1000),
      expires_in: 900,
    });
    let resolveFetch: ((res: Response) => void) | null = null;
    const fetchMock = vi.fn(
      async () =>
        new Promise<Response>((resolve) => {
          resolveFetch = resolve;
        }),
    );
    vi.stubGlobal('fetch', fetchMock);
    const servicer = servicerFor(dir);
    const first = servicer.call(KIMI_TOKEN_METHOD, {});
    const second = freshKimiNodeCredential({ KIMI_CODE_HOME: dir } as NodeJS.ProcessEnv, now);
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(fetchMock).toHaveBeenCalledTimes(1);
    resolveFetch?.(new Response(JSON.stringify({ access_token: 'fresh-access', expires_in: 900 }), { status: 200 }));
    const [a, b] = await Promise.all([first, second]);
    expect((a as { credential: Record<string, unknown> }).credential.access_token).toBe('fresh-access');
    expect((b as Record<string, unknown> | null)?.access_token).toBe('fresh-access');
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('refreshes a token the daemon still considers valid but the node CLI would not (min TTL)', async () => {
    // 400s remaining: above the prepare-time 60s validity bar, below the CLI's
    // own 450s stale threshold — the servicer must mint a fresh one anyway.
    const now = Date.now();
    const dir = kimiStore({
      ...validCredential(now),
      expires_at: Math.floor((now + 400_000) / 1000),
      expires_in: 900,
    });
    const fetchMock = vi.fn(async () =>
      new Response(JSON.stringify({ access_token: 'fresh-access', refresh_token: 'fresh-refresh', expires_in: 900 }), { status: 200 }));
    vi.stubGlobal('fetch', fetchMock);
    const result = (await servicerFor(dir).call(KIMI_TOKEN_METHOD, {})) as { credential: Record<string, unknown> };
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(result.credential.access_token).toBe('fresh-access');
    expect(Number(result.credential.expires_at) * 1000 - now).toBeGreaterThan(450_000);
    expect(result.credential.refresh_token).toBeUndefined();
  });

  it('fails the call (not the run) when no usable credential exists', async () => {
    const dir = kimiStore({ access_token: 'x', expires_at: 1 });
    await expect(servicerFor(dir).call(KIMI_TOKEN_METHOD, {})).rejects.toMatchObject({
      code: RelayErrorCode.InternalError,
    });
    const missing = mkdtempSync(join(tmpdir(), 'kimi-servicer-empty-'));
    await expect(servicerFor(missing).call(KIMI_TOKEN_METHOD, {})).rejects.toBeInstanceOf(RelayRpcError);
  });

  it('delegates every other credentials method to the inner servicer', async () => {
    const dir = kimiStore(validCredential(Date.now()));
    const inner: BackendServicer = {
      call: async (method, params) => ({ delegated: method, params }),
    };
    const result = (await servicerFor(dir, inner).call('git/token', { owner: 'o', repo: 'r' })) as Record<string, unknown>;
    expect(result.delegated).toBe('git/token');
    expect(result.params).toEqual({ owner: 'o', repo: 'r' });
  });
});
