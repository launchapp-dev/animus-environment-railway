import { describe, expect, it, vi } from 'vitest';
import { generateKeyPairSync } from 'node:crypto';
import { makeCredentialsServicer } from './credentials-servicer.js';

const { privateKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
const PEM = privateKey.export({ type: 'pkcs8', format: 'pem' }).toString();

function ghFetchMock(routes: Record<string, unknown>) {
  const calls: string[] = [];
  const impl = vi.fn(async (url: RequestInfo | URL) => {
    const path = String(url).replace('https://api.github.com', '');
    calls.push(path);
    if (!(path in routes)) {
      return new Response('not found', { status: 404 });
    }
    return new Response(JSON.stringify(routes[path]), { status: 200 });
  });
  return { impl: impl as unknown as typeof fetch, calls };
}

describe('makeCredentialsServicer', () => {
  it('mints an installation-wide token and caches the installation id', async () => {
    const { impl, calls } = ghFetchMock({
      '/app/installations': [{ id: 1234 }],
      '/app/installations/1234/access_tokens': { token: 'ghs_fresh', expires_at: '2026-07-22T18:00:00Z' },
    });
    const servicer = makeCredentialsServicer(
      { GITHUB_APP_ID: '99', GITHUB_APP_PRIVATE_KEY: PEM },
      impl,
      () => 1_000_000,
    );

    const first = (await servicer.call('git/token', {})) as Record<string, unknown>;
    expect(first.token).toBe('ghs_fresh');
    expect(first.expires_at).toBe('2026-07-22T18:00:00Z');
    expect(first.source).toBe('github_app');

    await servicer.call('git/token', {});
    // Installation resolved exactly once; access_tokens minted per call.
    expect(calls.filter((c) => c === '/app/installations')).toHaveLength(1);
    expect(calls.filter((c) => c === '/app/installations/1234/access_tokens')).toHaveLength(2);
  });

  it('uses GITHUB_APP_INSTALLATION_ID without listing installations', async () => {
    const { impl, calls } = ghFetchMock({
      '/app/installations/777/access_tokens': { token: 'ghs_pinned', expires_at: null },
    });
    const servicer = makeCredentialsServicer(
      { GITHUB_APP_ID: '99', GITHUB_APP_PRIVATE_KEY: PEM, GITHUB_APP_INSTALLATION_ID: '777' },
      impl,
      () => 1_000_000,
    );
    const out = (await servicer.call('git/token', {})) as Record<string, unknown>;
    expect(out.token).toBe('ghs_pinned');
    expect(calls).not.toContain('/app/installations');
  });

  it('falls back to the static GITHUB_TOKEN when the App is unconfigured', async () => {
    const servicer = makeCredentialsServicer({ GITHUB_TOKEN: 'ghp_static' });
    const out = (await servicer.call('git/token', {})) as Record<string, unknown>;
    expect(out).toEqual({ token: 'ghp_static', expires_at: null, source: 'static' });
  });

  it('throws a structured error with no App and no fallback token', async () => {
    const servicer = makeCredentialsServicer({});
    await expect(servicer.call('git/token', {})).rejects.toThrow(/no GITHUB_TOKEN fallback/);
  });

  it('rejects unknown methods', async () => {
    const servicer = makeCredentialsServicer({ GITHUB_TOKEN: 'x' });
    await expect(servicer.call('nope', {})).rejects.toThrow(/unknown method/);
  });

  it('surfaces GitHub errors without leaking token material', async () => {
    const { impl } = ghFetchMock({ '/app/installations': [{ id: 1 }] });
    const servicer = makeCredentialsServicer(
      { GITHUB_APP_ID: '99', GITHUB_APP_PRIVATE_KEY: PEM },
      impl,
      () => 1_000_000,
    );
    // access_tokens route missing -> 404 surfaced as an HTTP error, no secrets.
    await expect(servicer.call('git/token', {})).rejects.toThrow(/HTTP 404/);
  });
});
