import { describe, expect, it, vi } from 'vitest';
import { generateKeyPairSync } from 'node:crypto';
import { makeCredentialsServicer } from './credentials-servicer.js';

const { privateKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
const PEM = privateKey.export({ type: 'pkcs8', format: 'pem' }).toString();

function ghFetchMock(routes: Record<string, unknown>) {
  const calls: Array<{ path: string; init?: RequestInit }> = [];
  const impl = vi.fn(async (url: RequestInfo | URL, init?: RequestInit) => {
    const path = String(url).replace('https://api.github.com', '');
    calls.push({ path, init });
    if (!(path in routes)) {
      return new Response('not found', { status: 404 });
    }
    return new Response(JSON.stringify(routes[path]), { status: 200 });
  });
  return { impl: impl as unknown as typeof fetch, calls };
}

describe('makeCredentialsServicer', () => {
  it('mints a one-repository token and caches that repository installation id', async () => {
    const scope = { owner: 'launchapp-dev', repo: 'animus' };
    const routes = ghFetchMock({
      '/repos/launchapp-dev/animus/installation': { id: 1234 },
      '/app/installations/1234/access_tokens': { token: 'ghs_fresh', expires_at: '2026-07-22T18:00:00Z' },
    });
    const scopedServicer = makeCredentialsServicer(
      { GITHUB_APP_ID: '99', GITHUB_APP_PRIVATE_KEY: PEM },
      routes.impl,
      () => 1_000_000,
    );
    const first = (await scopedServicer.call('git/token', scope)) as Record<string, unknown>;
    expect(first.token).toBe('ghs_fresh');
    expect(first.expires_at).toBe('2026-07-22T18:00:00Z');
    expect(first.source).toBe('github_app');

    await scopedServicer.call('git/token', scope);
    expect(routes.calls.filter((call) => call.path === '/repos/launchapp-dev/animus/installation')).toHaveLength(1);
    const tokenCalls = routes.calls.filter((call) => call.path === '/app/installations/1234/access_tokens');
    expect(tokenCalls).toHaveLength(2);
    expect(tokenCalls[0]?.init).toMatchObject({
      method: 'POST',
      body: JSON.stringify({ repositories: ['animus'] }),
    });
  });

  it('keeps installation caches separate per repository', async () => {
    const { impl, calls } = ghFetchMock({
      '/repos/launchapp-dev/one/installation': { id: 111 },
      '/repos/launchapp-dev/two/installation': { id: 222 },
      '/app/installations/111/access_tokens': { token: 'ghs_one', expires_at: null },
      '/app/installations/222/access_tokens': { token: 'ghs_two', expires_at: null },
    });
    const servicer = makeCredentialsServicer(
      { GITHUB_APP_ID: '99', GITHUB_APP_PRIVATE_KEY: PEM },
      impl,
      () => 1_000_000,
    );
    expect((await servicer.call('git/token', { owner: 'launchapp-dev', repo: 'one' }) as Record<string, unknown>).token).toBe('ghs_one');
    expect((await servicer.call('git/token', { owner: 'launchapp-dev', repo: 'two' }) as Record<string, unknown>).token).toBe('ghs_two');
    expect(calls.map((call) => call.path)).toContain('/repos/launchapp-dev/one/installation');
    expect(calls.map((call) => call.path)).toContain('/repos/launchapp-dev/two/installation');
  });

  it('never forwards a static host token to a remote node', async () => {
    const servicer = makeCredentialsServicer({ GITHUB_TOKEN: 'ghp_static' });
    await expect(servicer.call('git/token', { owner: 'launchapp-dev', repo: 'animus' })).rejects.toThrow(
      /repository-scoped GitHub App credentials are not configured/,
    );
  });

  it('rejects missing or malformed repository scope', async () => {
    const servicer = makeCredentialsServicer({ GITHUB_APP_ID: '99', GITHUB_APP_PRIVATE_KEY: PEM });
    await expect(servicer.call('git/token', {})).rejects.toThrow(/requires repository scope|scope is invalid/);
    await expect(servicer.call('git/token', { owner: 'launchapp-dev', repo: '../escape' })).rejects.toThrow(
      /scope is invalid/,
    );
  });

  it('rejects unknown methods', async () => {
    const servicer = makeCredentialsServicer({ GITHUB_TOKEN: 'x' });
    await expect(servicer.call('nope', {})).rejects.toThrow(/unknown method/);
  });

  it('surfaces GitHub errors without leaking token material', async () => {
    const { impl } = ghFetchMock({ '/repos/launchapp-dev/animus/installation': { id: 1 } });
    const servicer = makeCredentialsServicer(
      { GITHUB_APP_ID: '99', GITHUB_APP_PRIVATE_KEY: PEM },
      impl,
      () => 1_000_000,
    );
    // access_tokens route missing -> 404 surfaced as an HTTP error, no secrets.
    await expect(servicer.call('git/token', { owner: 'launchapp-dev', repo: 'animus' })).rejects.toThrow(/HTTP 404/);
  });
});
