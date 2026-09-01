import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { afterEach, describe, expect, it, vi } from 'vitest';

import { capacityBrokerFromEnv, capacityNodeBundle, stripRefreshMaterial } from './capacity-broker.js';
import { claudeNodeCredentials, kimiNodeCredentials } from './environment.js';
import { KIMI_TOKEN_METHOD, makeKimiNodeTokenServicer } from './kimi-node-token.js';

const BROKER_ENV = {
  CAPACITY_BROKER_URL: 'http://127.0.0.1:8088/api/capacity/internal',
  CAPACITY_BROKER_TOKEN: 'test-broker-token',
} as NodeJS.ProcessEnv;

function brokerFetch(value: unknown, status = 200) {
  return vi.fn(async (url: unknown, init?: { headers?: Record<string, string>; body?: string }) => {
    expect(String(url)).toBe('http://127.0.0.1:8088/api/capacity/internal/credential-refresh');
    expect(init?.headers?.authorization).toBe('Bearer test-broker-token');
    return new Response(JSON.stringify({ ok: status === 200, data: { value, generation: 3, refreshed: true } }), { status });
  }) as unknown as typeof fetch;
}

describe('capacityBrokerFromEnv', () => {
  it('resolves only when both URL and token are set', () => {
    expect(capacityBrokerFromEnv({} as NodeJS.ProcessEnv)).toBeNull();
    expect(capacityBrokerFromEnv({ CAPACITY_BROKER_URL: 'http://x' } as NodeJS.ProcessEnv)).toBeNull();
    expect(capacityBrokerFromEnv({ CAPACITY_BROKER_TOKEN: 't' } as NodeJS.ProcessEnv)).toBeNull();
    expect(capacityBrokerFromEnv(BROKER_ENV)).toEqual({
      url: 'http://127.0.0.1:8088/api/capacity/internal',
      token: 'test-broker-token',
    });
    // trailing slashes are normalized
    expect(
      capacityBrokerFromEnv({ CAPACITY_BROKER_URL: 'http://x/', CAPACITY_BROKER_TOKEN: 't' } as NodeJS.ProcessEnv)?.url,
    ).toBe('http://x');
  });
});

describe('stripRefreshMaterial', () => {
  it('strips refresh material from the claude wrapper shape', () => {
    const out = stripRefreshMaterial(
      JSON.stringify({ claudeAiOauth: { accessToken: 'A', refreshToken: 'R', expiresAt: 1 }, other: true }),
    );
    const parsed = JSON.parse(out!);
    expect(parsed.claudeAiOauth.accessToken).toBe('A');
    expect(parsed.claudeAiOauth.refreshToken).toBeUndefined();
    expect(parsed.other).toBe(true);
  });

  it('strips refresh material inside the kimi files archive', () => {
    const out = stripRefreshMaterial(
      JSON.stringify({
        files: {
          'config.toml': 'model = "k3"\n',
          'credentials/kimi-code.json': JSON.stringify({ access_token: 'A', refresh_token: 'R', expires_at: 1 }),
        },
      }),
    );
    const parsed = JSON.parse(out!);
    expect(parsed.files['config.toml']).toBe('model = "k3"\n');
    const cred = JSON.parse(parsed.files['credentials/kimi-code.json']);
    expect(cred.access_token).toBe('A');
    expect(cred.refresh_token).toBeUndefined();
  });

  it('returns null on unparseable or non-object bundles', () => {
    expect(stripRefreshMaterial('not json')).toBeNull();
    expect(stripRefreshMaterial('[1,2]')).toBeNull();
  });
});

describe('capacityNodeBundle', () => {
  it('returns null without contacting the network when the broker env is unset', async () => {
    const fetchMock = vi.fn(async () => new Response('{}'));
    expect(await capacityNodeBundle({} as NodeJS.ProcessEnv, 'claude', fetchMock as unknown as typeof fetch)).toBeNull();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('posts family-only to credential-refresh and returns the stripped bundle', async () => {
    const fetchMock = brokerFetch(JSON.stringify({ claudeAiOauth: { accessToken: 'A', refreshToken: 'R', expiresAt: 1 } }));
    const out = await capacityNodeBundle(BROKER_ENV, 'claude', fetchMock);
    expect(out).not.toBeNull();
    const parsed = JSON.parse(out!);
    expect(parsed.claudeAiOauth.accessToken).toBe('A');
    expect(parsed.claudeAiOauth.refreshToken).toBeUndefined();
    const body = JSON.parse(String((fetchMock as unknown as ReturnType<typeof vi.fn>).mock.calls[0][1]?.body));
    expect(body).toEqual({ family: 'claude' });
  });

  it('returns null on HTTP errors, ok:false, and missing value', async () => {
    expect(await capacityNodeBundle(BROKER_ENV, 'kimi', brokerFetch(null, 404))).toBeNull();
    const okFalse = vi.fn(async () => new Response(JSON.stringify({ ok: false, error: 'needs_relogin' }), { status: 409 }));
    expect(await capacityNodeBundle(BROKER_ENV, 'kimi', okFalse as unknown as typeof fetch)).toBeNull();
    const noValue = vi.fn(async () => new Response(JSON.stringify({ ok: true, data: {} }), { status: 200 }));
    expect(await capacityNodeBundle(BROKER_ENV, 'kimi', noValue as unknown as typeof fetch)).toBeNull();
    const throwing = vi.fn(async () => {
      throw new Error('connection refused');
    });
    expect(await capacityNodeBundle(BROKER_ENV, 'kimi', throwing as unknown as typeof fetch)).toBeNull();
  });
});

describe('broker-sourced node credentials', () => {
  it('claudeNodeCredentials injects the broker bundle and never touches the file when the broker is configured', async () => {
    const bundle = JSON.stringify({ claudeAiOauth: { accessToken: 'broker-access', expiresAt: Date.now() + 3_600_000 } });
    const fetchMock = brokerFetch(bundle);
    const vars = await claudeNodeCredentials(
      { ...BROKER_ENV, CLAUDE_CONFIG_DIR: '/nonexistent-claude-dir' } as NodeJS.ProcessEnv,
      Date.now(),
      fetchMock,
    );
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const decoded = JSON.parse(Buffer.from(vars.ANIMUS_NODE_CLAUDE_CREDENTIALS_B64, 'base64').toString('utf8'));
    expect(decoded.claudeAiOauth.accessToken).toBe('broker-access');
    expect(decoded.claudeAiOauth.refreshToken).toBeUndefined();
  });

  it('claudeNodeCredentials falls back to the file when the broker is not configured', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'claude-file-'));
    const now = Date.now();
    writeFileSync(
      join(dir, '.credentials.json'),
      JSON.stringify({ claudeAiOauth: { accessToken: 'file-access', refreshToken: 'R', expiresAt: now + 3_600_000 } }),
    );
    const fetchMock = vi.fn(async () => {
      throw new Error('must not call the network for a valid file credential without broker env');
    });
    const vars = await claudeNodeCredentials(
      { CLAUDE_CONFIG_DIR: dir } as NodeJS.ProcessEnv,
      now,
      fetchMock as unknown as typeof fetch,
    );
    const decoded = JSON.parse(Buffer.from(vars.ANIMUS_NODE_CLAUDE_CREDENTIALS_B64, 'base64').toString('utf8'));
    expect(decoded.claudeAiOauth.accessToken).toBe('file-access');
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('claudeNodeCredentials falls back to the file when the broker errors', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'claude-fallback-'));
    const now = Date.now();
    writeFileSync(
      join(dir, '.credentials.json'),
      JSON.stringify({ claudeAiOauth: { accessToken: 'file-access', refreshToken: 'R', expiresAt: now + 3_600_000 } }),
    );
    const vars = await claudeNodeCredentials(
      { ...BROKER_ENV, CLAUDE_CONFIG_DIR: dir } as NodeJS.ProcessEnv,
      now,
      brokerFetch(null, 404),
    );
    const decoded = JSON.parse(Buffer.from(vars.ANIMUS_NODE_CLAUDE_CREDENTIALS_B64, 'base64').toString('utf8'));
    expect(decoded.claudeAiOauth.accessToken).toBe('file-access');
  });

  it('kimiNodeCredentials injects the broker files archive when the broker is configured', async () => {
    const archive = JSON.stringify({
      files: {
        'config.toml': 'model = "k3"\n',
        'credentials/kimi-code.json': JSON.stringify({
          access_token: 'broker-kimi',
          refresh_token: 'must-be-stripped',
          expires_at: Math.floor(Date.now() / 1000) + 900,
          expires_in: 900,
        }),
      },
    });
    const vars = await kimiNodeCredentials(
      { ...BROKER_ENV, KIMI_CODE_HOME: '/nonexistent-kimi-dir' } as NodeJS.ProcessEnv,
      Date.now(),
      brokerFetch(archive),
    );
    const decoded = JSON.parse(Buffer.from(vars.ANIMUS_NODE_KIMI_BUNDLE_B64, 'base64').toString('utf8'));
    expect(decoded.files['config.toml']).toBe('model = "k3"\n');
    const cred = JSON.parse(decoded.files['credentials/kimi-code.json']);
    expect(cred.access_token).toBe('broker-kimi');
    expect(cred.refresh_token).toBeUndefined();
  });

  it('the kimi/token servicer answers from the broker archive without reading the daemon file', async () => {
    const archive = JSON.stringify({
      files: {
        'credentials/kimi-code.json': JSON.stringify({
          access_token: 'serviced-kimi',
          expires_at: Math.floor(Date.now() / 1000) + 900,
          expires_in: 900,
        }),
      },
    });
    const inner = { call: async () => ({ delegated: true }) };
    const servicer = makeKimiNodeTokenServicer(inner, {
      ...BROKER_ENV,
      KIMI_CODE_HOME: '/nonexistent-kimi-dir',
    } as NodeJS.ProcessEnv);
    // The servicer calls capacityNodeBundle with the default global fetch.
    const fetchMock = brokerFetch(archive);
    vi.stubGlobal('fetch', fetchMock);
    const result = (await servicer.call(KIMI_TOKEN_METHOD, {})) as { credential: Record<string, unknown> };
    expect(result.credential.access_token).toBe('serviced-kimi');
    expect(result.credential.refresh_token).toBeUndefined();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});

afterEach(() => {
  vi.unstubAllGlobals();
});
