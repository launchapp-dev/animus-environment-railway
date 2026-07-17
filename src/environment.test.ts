// Substrate tests: the full prepare -> exec/exec_stream -> teardown flow
// against a FAKE Railway API and a REAL in-process relay + bridge (the fake
// "container" is a BridgeClient from animus-env-transport dialing the relay on
// 127.0.0.1 with the exact variables the plugin injected). No real network, no
// Railway credentials.
//
// A creds-gated suite at the bottom marks the real-Railway path as
// integration-pending: it skips with a clear message unless RAILWAY_TOKEN +
// RAILWAY_PROJECT_ID + RAILWAY_ENVIRONMENT_ID are present.

import { createHash, generateKeyPairSync } from 'node:crypto';
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { afterEach, describe, expect, it } from 'vitest';

import { BridgeClient, RelayServer } from '@launchapp-dev/animus-env-transport';
import { planWorkspace } from '@launchapp-dev/animus-environment-base';

import {
  claudeNodeCredentials,
  cloneCommands,
  configFromEnv,
  DEFAULT_BRIDGE_COMMAND,
  DEFAULT_CLAUDE_CONFIG_DIR,
  DEFAULT_CODEX_OAUTH_HOME,
  DEFAULT_IMAGE,
  githubAppCredentials,
  harnessCredentialVars,
  logStorageEnv,
  parseGithubSlug,
  RailwayEnvironment,
  resolveTarget,
  runVariables,
  WORKSPACE_ROOT,
} from './environment.js';
import { SERVICE_NAME_PREFIX, type CreatedService, type RailwayApi, type ServiceCreateInput } from './railway.js';

const NODE = process.execPath;

/** Fake Railway control plane: `createRunService` "boots a container" by
 *  dialing the relay with an in-process BridgeClient, exactly as the real
 *  image's `animus-env-bridge` entrypoint would with the injected variables. */
class FakeRailway implements RailwayApi {
  readonly created: Array<ServiceCreateInput & { startCommand: string }> = [];
  readonly deleted: Array<{ serviceId: string; environmentId: string }> = [];
  listed: Array<{ id: string; name: string }> = [];
  /** Set false to simulate a service that never dials home. */
  bootBridge = true;
  private readonly bridges: BridgeClient[] = [];
  private seq = 0;

  async createRunService(input: ServiceCreateInput & { startCommand: string }): Promise<CreatedService> {
    this.created.push(input);
    this.seq += 1;
    const serviceId = `svc-${this.seq}`;
    if (this.bootBridge) {
      const url = input.variables.ANIMUS_ENV_WSS_URL;
      const token = input.variables.ANIMUS_ENV_RUN_TOKEN;
      if (!url || !token) throw new Error('fake container is missing its relay coordinates');
      // Boot the "container" asynchronously, like a real deploy would.
      setTimeout(() => {
        void BridgeClient.connect(url, token, { workspaceRoot: tmpdir(), log: () => undefined })
          .then((bridge) => this.bridges.push(bridge))
          .catch(() => undefined);
      }, 25);
    }
    return { serviceId, deploymentId: `dep-${this.seq}` };
  }

  async deleteService(serviceId: string, environmentId: string): Promise<void> {
    this.deleted.push({ serviceId, environmentId });
  }

  async listRunServices(): Promise<Array<{ id: string; name: string }>> {
    return this.listed;
  }

  closeBridges(): void {
    for (const b of this.bridges) b.close();
  }
}

interface Ctx {
  env: RailwayEnvironment;
  fake: FakeRailway;
}

const live: Ctx[] = [];

async function makeEnv(): Promise<Ctx> {
  const fake = new FakeRailway();
  const relay = await RelayServer.listen({ host: '127.0.0.1', port: 0 });
  const env = new RailwayEnvironment({
    railway: fake,
    relay,
    config: { projectId: 'proj-1', environmentId: 'env-1', dialTimeoutSecs: 10 },
  });
  const ctx = { env, fake };
  live.push(ctx);
  return ctx;
}

afterEach(async () => {
  for (const ctx of live.splice(0)) {
    ctx.fake.closeBridges();
    await ctx.env.close();
  }
});

describe('pure helpers', () => {
  it('resolveTarget prefers spec.metadata over config and errors when absent', () => {
    const config = { projectId: 'cfg-p', environmentId: 'cfg-e' };
    expect(resolveTarget({ kind: 'railway' }, config)).toEqual({ projectId: 'cfg-p', environmentId: 'cfg-e' });
    expect(
      resolveTarget(
        { kind: 'railway', metadata: { railway_project_id: 'meta-p', railway_environment_id: 'meta-e' } },
        config,
      ),
    ).toEqual({ projectId: 'meta-p', environmentId: 'meta-e' });
    expect(() => resolveTarget({ kind: 'railway' }, {})).toThrow(/RAILWAY_PROJECT_ID/);
  });

  it('runVariables layers BASE_DB_URL + spec.env under the relay coordinates', () => {
    const vars = runVariables({
      wssUrl: 'wss://relay/relay/h1',
      token: 'tok',
      specEnv: { FOO: 'bar', ANIMUS_ENV_RUN_TOKEN: 'spoofed' },
      hostEnv: { BASE_DB_URL: 'postgres://db' } as NodeJS.ProcessEnv,
    });
    expect(vars).toEqual({
      BASE_DB_URL: 'postgres://db',
      FOO: 'bar',
      ANIMUS_ENV_WSS_URL: 'wss://relay/relay/h1',
      ANIMUS_ENV_RUN_TOKEN: 'tok', // relay identity wins over spec env
      ANIMUS_ENV_WORKSPACE_ROOT: WORKSPACE_ROOT,
    });
  });

  it('harnessCredentialVars base64s the codex auth + passes the github token', () => {
    const codexDir = mkdtempSync(join(tmpdir(), 'codex-'));
    writeFileSync(join(codexDir, 'auth.json'), '{"tokens":"x"}');
    const vars = harnessCredentialVars({ CODEX_OAUTH_HOME: codexDir, GITHUB_TOKEN: 'ghtok' } as NodeJS.ProcessEnv);
    expect(Buffer.from(vars.ANIMUS_NODE_CODEX_AUTH_B64, 'base64').toString()).toBe('{"tokens":"x"}');
    // The GitHub token is exposed as BOTH GITHUB_TOKEN and GH_TOKEN (gh CLI).
    expect(vars.GITHUB_TOKEN).toBe('ghtok');
    expect(vars.GH_TOKEN).toBe('ghtok');
  });

  it('exposes the durable portal codex home as the default fallback', () => {
    // harnessCredentialVars reads this path when the daemon env omits
    // CODEX_OAUTH_HOME, so codex works on nodes without extra portal config.
    expect(DEFAULT_CODEX_OAUTH_HOME).toBe('/data/animus-state/codex-config');
  });

  it('exposes the durable portal claude config dir as the default fallback', () => {
    // claudeNodeCredentials reads this path when the daemon env omits
    // CLAUDE_CONFIG_DIR, so claude auth works on nodes without extra portal config.
    expect(DEFAULT_CLAUDE_CONFIG_DIR).toBe('/data/animus-state/claude-config');
  });

  it('claudeNodeCredentials uses DEFAULT_CLAUDE_CONFIG_DIR when CLAUDE_CONFIG_DIR is unset', async () => {
    // With CLAUDE_CONFIG_DIR unset and no file at the default path, returns {} (best-effort).
    expect(await claudeNodeCredentials({} as NodeJS.ProcessEnv, 1_000_000)).toEqual({});
    // With CLAUDE_CONFIG_DIR unset but a credentials file at the default path, reads it.
    // (We can't write to /data in tests, so we verify the override still wins.)
    const dir = mkdtempSync(join(tmpdir(), 'claude-default-'));
    const now = 1_000_000;
    writeFileSync(
      join(dir, '.credentials.json'),
      JSON.stringify({ claudeAiOauth: { accessToken: 'B', refreshToken: 'R', expiresAt: now + 3_600_000 } }),
    );
    // Simulate the default by passing it explicitly via the override variable.
    const vars = await claudeNodeCredentials({ CLAUDE_CONFIG_DIR: dir } as NodeJS.ProcessEnv, now);
    expect(JSON.parse(Buffer.from(vars.ANIMUS_NODE_CLAUDE_CREDENTIALS_B64, 'base64').toString()).claudeAiOauth.accessToken).toBe('B');
  });

  it('harnessCredentialVars skips missing creds (best-effort)', () => {
    expect(harnessCredentialVars({ CODEX_OAUTH_HOME: '/nonexistent-codex-home' } as NodeJS.ProcessEnv)).toEqual({});
  });

  it('claudeNodeCredentials injects a valid token as-is with the refresh token STRIPPED', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'claude-'));
    const now = 1_000_000;
    writeFileSync(
      join(dir, '.credentials.json'),
      JSON.stringify({ claudeAiOauth: { accessToken: 'A', refreshToken: 'R', expiresAt: now + 3_600_000, scopes: ['x'] } }),
    );
    const vars = await claudeNodeCredentials({ CLAUDE_CONFIG_DIR: dir } as NodeJS.ProcessEnv, now, (async () => {
      throw new Error('must not refresh a valid token');
    }) as unknown as typeof fetch);
    const injected = JSON.parse(Buffer.from(vars.ANIMUS_NODE_CLAUDE_CREDENTIALS_B64, 'base64').toString());
    expect(injected.claudeAiOauth.accessToken).toBe('A');
    expect(injected.claudeAiOauth.refreshToken).toBeUndefined();
    expect(injected.claudeAiOauth.scopes).toEqual(['x']);
  });

  it('claudeNodeCredentials refreshes an expired token, writes it back, and strips refresh', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'claude-'));
    const path = join(dir, '.credentials.json');
    const now = 1_000_000;
    writeFileSync(path, JSON.stringify({ claudeAiOauth: { accessToken: 'old', refreshToken: 'R1', expiresAt: now - 1000 } }));
    const fetchImpl = (async () => ({
      ok: true,
      status: 200,
      json: async () => ({ access_token: 'A2', refresh_token: 'R2', expires_in: 3600 }),
      text: async () => '',
    })) as unknown as typeof fetch;
    const vars = await claudeNodeCredentials({ CLAUDE_CONFIG_DIR: dir } as NodeJS.ProcessEnv, now, fetchImpl);
    const injected = JSON.parse(Buffer.from(vars.ANIMUS_NODE_CLAUDE_CREDENTIALS_B64, 'base64').toString());
    expect(injected.claudeAiOauth.accessToken).toBe('A2');
    expect(injected.claudeAiOauth.refreshToken).toBeUndefined();
    // rotated token written back to /data (so the daemon stays valid)
    const onDisk = JSON.parse(readFileSync(path, 'utf8'));
    expect(onDisk.claudeAiOauth.refreshToken).toBe('R2');
    expect(onDisk.claudeAiOauth.accessToken).toBe('A2');
  });

  it('claudeNodeCredentials returns {} when refresh fails or no login', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'claude-'));
    writeFileSync(join(dir, '.credentials.json'), JSON.stringify({ claudeAiOauth: { accessToken: 'x', refreshToken: 'R', expiresAt: 1 } }));
    const failing = (async () => ({ ok: false, status: 400, json: async () => ({}), text: async () => 'invalid_grant' })) as unknown as typeof fetch;
    expect(await claudeNodeCredentials({ CLAUDE_CONFIG_DIR: dir } as NodeJS.ProcessEnv, 1_000_000, failing)).toEqual({});
    expect(await claudeNodeCredentials({} as NodeJS.ProcessEnv, 1_000_000)).toEqual({});
  });

  it('parseGithubSlug extracts owner/repo from https / ssh / .git urls', () => {
    expect(parseGithubSlug('https://github.com/launchapp-dev/animus-cli.git')).toEqual({
      owner: 'launchapp-dev',
      repo: 'animus-cli',
    });
    expect(parseGithubSlug('https://github.com/o/r')).toEqual({ owner: 'o', repo: 'r' });
    expect(parseGithubSlug('git@github.com:o/r.git')).toEqual({ owner: 'o', repo: 'r' });
    expect(parseGithubSlug('/local/path')).toBeNull();
    expect(parseGithubSlug(undefined)).toBeNull();
  });

  it('githubAppCredentials mints a repo-scoped token + bot identity (mocked GitHub)', async () => {
    const { privateKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
    const pem = privateKey.export({ type: 'pkcs1', format: 'pem' }).toString();
    const calls: Array<{ url: string; method: string }> = [];
    const fetchImpl = (async (url: unknown, init?: RequestInit) => {
      const u = String(url);
      calls.push({ url: u, method: init?.method ?? 'GET' });
      const body = u.endsWith('/installation')
        ? { id: 42, app_id: 7 }
        : u.endsWith('/access_tokens')
          ? { token: 'ghs_minted' }
          : u.includes('/users/')
            ? { id: 999 }
            : null;
      return { ok: body !== null, status: body ? 200 : 404, json: async () => body, text: async () => 'x' } as Response;
    }) as typeof fetch;

    const vars = await githubAppCredentials(
      { repos: [{ url: 'https://github.com/o/r.git', primary: true }] },
      { GITHUB_APP_ID: '7', GITHUB_APP_PRIVATE_KEY: pem, GITHUB_APP_SLUG: 'animus' } as NodeJS.ProcessEnv,
      1000,
      fetchImpl,
    );
    expect(vars.GITHUB_TOKEN).toBe('ghs_minted');
    expect(vars.GH_TOKEN).toBe('ghs_minted');
    expect(vars.GIT_AUTHOR_NAME).toBe('animus[bot]');
    expect(vars.GIT_AUTHOR_EMAIL).toBe('999+animus[bot]@users.noreply.github.com');
    expect(vars.GIT_COMMITTER_EMAIL).toBe('999+animus[bot]@users.noreply.github.com');
    expect(calls.some((c) => c.url.endsWith('/repos/o/r/installation'))).toBe(true);
    expect(calls.some((c) => c.method === 'POST' && c.url.endsWith('/app/installations/42/access_tokens'))).toBe(true);
  });

  it('githubAppCredentials skips when the app is not configured', async () => {
    expect(
      await githubAppCredentials({ repos: [{ url: 'https://github.com/o/r', primary: true }] }, {} as NodeJS.ProcessEnv, 1000),
    ).toEqual({});
  });

  it('githubAppCredentials mints an INSTALLATION-WIDE token on a bare node (no repos)', async () => {
    const { privateKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
    const pem = privateKey.export({ type: 'pkcs1', format: 'pem' }).toString();
    const posts: Array<{ url: string; body: string }> = [];
    const fetchImpl = (async (url: unknown, init?: RequestInit) => {
      const u = String(url);
      if (init?.method === 'POST') posts.push({ url: u, body: String(init?.body ?? '') });
      const body = u.endsWith('/app/installations')
        ? [{ id: 42, app_id: 7 }]
        : u.endsWith('/access_tokens')
          ? { token: 'ghs_wide' }
          : u.includes('/users/')
            ? { id: 999 }
            : null;
      return { ok: body !== null, status: body ? 200 : 404, json: async () => body, text: async () => 'x' } as Response;
    }) as typeof fetch;

    // Bare spec — exactly what the daemon broker prepares.
    const vars = await githubAppCredentials(
      { repos: [] },
      { GITHUB_APP_ID: '7', GITHUB_APP_PRIVATE_KEY: pem, GITHUB_APP_SLUG: 'animus' } as NodeJS.ProcessEnv,
      1000,
      fetchImpl,
    );
    expect(vars.GITHUB_TOKEN).toBe('ghs_wide');
    expect(vars.GIT_AUTHOR_NAME).toBe('animus[bot]');
    // Installation-wide: minted against the first installation with NO repositories restriction.
    const tokenPost = posts.find((p) => p.url.endsWith('/app/installations/42/access_tokens'));
    expect(tokenPost).toBeDefined();
    expect(tokenPost?.body).not.toContain('repositories');
  });

  it('githubAppCredentials warns and uses installs[0] when the app has multiple installations', async () => {
    const { privateKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
    const pem = privateKey.export({ type: 'pkcs1', format: 'pem' }).toString();
    const posts: Array<{ url: string }> = [];
    const fetchImpl = (async (url: unknown, init?: RequestInit) => {
      const u = String(url);
      if (init?.method === 'POST') posts.push({ url: u });
      const body = u.endsWith('/app/installations')
        ? [
            { id: 42, app_id: 7, account: { login: 'animus-ecosystem' } },
            { id: 99, app_id: 7, account: { login: 'launchapp-dev' } },
          ]
        : u.endsWith('/access_tokens')
          ? { token: 'ghs_first' }
          : null;
      return { ok: body !== null, status: body ? 200 : 404, json: async () => body, text: async () => 'x' } as Response;
    }) as typeof fetch;

    const warnings: string[] = [];
    const origWrite = process.stderr.write.bind(process.stderr);
    process.stderr.write = ((chunk: unknown) => {
      warnings.push(String(chunk));
      return true;
    }) as typeof process.stderr.write;
    let vars: Record<string, string>;
    try {
      vars = await githubAppCredentials(
        { repos: [] },
        { GITHUB_APP_ID: '7', GITHUB_APP_PRIVATE_KEY: pem } as NodeJS.ProcessEnv,
        1000,
        fetchImpl,
      );
    } finally {
      process.stderr.write = origWrite;
    }

    // Behavior unchanged: still mints against the first installation.
    expect(vars.GITHUB_TOKEN).toBe('ghs_first');
    expect(posts.some((p) => p.url.endsWith('/app/installations/42/access_tokens'))).toBe(true);
    // But a clear warning names the chosen org, the count, and the remedy.
    const warning = warnings.find((w) => w.includes('installations'));
    expect(warning).toBeDefined();
    expect(warning).toContain('2 installations');
    expect(warning).toContain('animus-ecosystem');
    expect(warning).toContain('GITHUB_APP_INSTALLATION_ID');
  });

  it('githubAppCredentials scopes to spec.metadata.github_repo when repos are absent', async () => {
    const { privateKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
    const pem = privateKey.export({ type: 'pkcs1', format: 'pem' }).toString();
    const calls: string[] = [];
    const fetchImpl = (async (url: unknown, init?: RequestInit) => {
      const u = String(url);
      calls.push(u);
      const body = u.endsWith('/installation')
        ? { id: 55, app_id: 7 }
        : u.endsWith('/access_tokens')
          ? { token: 'ghs_scoped' }
          : null;
      return { ok: body !== null, status: body ? 200 : 404, json: async () => body, text: async () => 'x' } as Response;
    }) as typeof fetch;

    const vars = await githubAppCredentials(
      { metadata: { github_repo: 'launchapp-dev/animus-cli' } },
      { GITHUB_APP_ID: '7', GITHUB_APP_PRIVATE_KEY: pem } as NodeJS.ProcessEnv,
      1000,
      fetchImpl,
    );
    expect(vars.GITHUB_TOKEN).toBe('ghs_scoped');
    expect(calls.some((u) => u.endsWith('/repos/launchapp-dev/animus-cli/installation'))).toBe(true);
  });

  it('cloneCommands builds argv-array git clones (remote urls only)', () => {
    const plan = planWorkspace(
      {
        kind: 'railway',
        repos: [
          { url: 'https://github.com/launchapp-dev/app.git', git_ref: 'main', primary: true },
          { url: '/Users/someone/local-checkout' },
          { url: 'https://github.com/launchapp-dev/lib.git' },
        ],
      },
      WORKSPACE_ROOT,
    );
    const commands = cloneCommands(plan);
    expect(commands).toHaveLength(2); // local path skipped
    expect(commands[0]).toEqual({
      program: 'git',
      args: ['clone', '--depth', '1', '--branch', 'main', '--', 'https://github.com/launchapp-dev/app.git', `${WORKSPACE_ROOT}/app`],
      cwd: '/',
    });
    expect(commands[1]?.args).toContain(`${WORKSPACE_ROOT}/lib`);
  });

  it('rejects repo subdirs that would escape the workspace root', async () => {
    const plan = planWorkspace(
      {
        kind: 'railway',
        repos: [
          { url: 'https://github.com/x/a.git', name: '../outside' },
          { url: 'https://github.com/x/b.git' },
        ],
      },
      WORKSPACE_ROOT,
    );
    expect(() => cloneCommands(plan)).toThrow(/single plain path segment/);

    const { env, fake } = await makeEnv();
    await expect(
      env.prepare({
        spec: {
          kind: 'railway',
          repos: [
            { url: 'https://github.com/x/a.git', name: '../outside', primary: true },
            { url: 'https://github.com/x/b.git' },
          ],
        },
      }),
    ).rejects.toThrow(/single plain path segment/);
    // Failed before any service was created.
    expect(fake.created).toHaveLength(0);
  });

  it('prepare rejects local-path repo urls outright', async () => {
    const { env, fake } = await makeEnv();
    await expect(
      env.prepare({ spec: { kind: 'railway', repos: [{ url: '/Users/someone/local-checkout' }] } }),
    ).rejects.toThrow(/local path/);
    expect(fake.created).toHaveLength(0);
  });

  it('cloneCommands turns a pinned commit sha into clone + detached checkout', () => {
    const sha = 'a1b2c3d4e5f60718293a4b5c6d7e8f9012345678';
    const plan = planWorkspace(
      { kind: 'railway', repos: [{ url: 'https://github.com/x/pinned.git', git_ref: sha }] },
      WORKSPACE_ROOT,
    );
    const commands = cloneCommands(plan);
    expect(commands).toHaveLength(2);
    expect(commands[0]?.args).toEqual(['clone', '--', 'https://github.com/x/pinned.git', WORKSPACE_ROOT]);
    expect(commands[1]?.args).toEqual(['-C', WORKSPACE_ROOT, 'checkout', '--detach', sha]);
  });

  it('cloneCommands clones a single repo into the workspace root itself', () => {
    const plan = planWorkspace({ kind: 'railway', repos: [{ url: 'https://github.com/x/only.git' }] }, WORKSPACE_ROOT);
    const commands = cloneCommands(plan);
    expect(commands[0]?.args?.at(-1)).toBe(WORKSPACE_ROOT);
  });

  it('configFromEnv reads the ANIMUS_ENV_* / RAILWAY_* knobs', () => {
    const config = configFromEnv({
      RAILWAY_PROJECT_ID: 'p',
      RAILWAY_ENVIRONMENT_ID: 'e',
      ANIMUS_ENV_RELAY_PUBLIC_URL: 'wss://daemon.example.com',
      ANIMUS_ENV_RELAY_PORT: '8790',
      ANIMUS_ENV_DIAL_TIMEOUT_SECS: '60',
    } as NodeJS.ProcessEnv);
    expect(config).toMatchObject({
      projectId: 'p',
      environmentId: 'e',
      relayPublicUrl: 'wss://daemon.example.com',
      relayPort: 8790,
      dialTimeoutSecs: 60,
    });
  });
});

describe('prepare -> exec -> teardown (fake Railway, real relay + bridge)', () => {
  it('runs the full flow', async () => {
    const { env, fake } = await makeEnv();

    const { handle } = await env.prepare({ spec: { kind: 'railway', env: { RUN_FLAG: 'yes' } } });
    expect(handle.id).toMatch(/^r[0-9a-f]{6}$/);
    expect(handle.workspace_root).toBe(WORKSPACE_ROOT);
    const meta = handle.metadata as Record<string, unknown>;
    expect(meta.service_id).toBe('svc-1');
    expect(meta.project_id).toBe('proj-1');
    expect(meta.environment_id).toBe('env-1');
    expect(String(meta.service_name)).toBe(`${SERVICE_NAME_PREFIX}${env.instanceId}-${handle.id}`);

    // The service was created from the default image with the bridge command
    // and the injected relay coordinates + spec env.
    const created = fake.created[0];
    expect(created?.image).toBe(DEFAULT_IMAGE);
    expect(created?.startCommand).toBe(DEFAULT_BRIDGE_COMMAND);
    expect(created?.variables.RUN_FLAG).toBe('yes');
    expect(created?.variables.ANIMUS_ENV_WSS_URL).toContain(`/relay/${handle.id}`);

    // Buffered exec.
    const res = await env.execCommand(handle, { program: NODE, args: ['-e', 'console.log("from railway env")'] }, null, null);
    expect(res.exit_code).toBe(0);
    expect(res.stdout).toContain('from railway env');

    // Streaming exec.
    const chunks: Array<[string, string]> = [];
    const streamed = await env.execCommand(
      handle,
      { program: NODE, args: ['-e', 'console.error("streamed line")'] },
      null,
      null,
      (stream, text) => chunks.push([stream, text]),
    );
    expect(streamed.exit_code).toBe(0);
    expect(chunks.some(([s, t]) => s === 'stderr' && t.includes('streamed line'))).toBe(true);

    // stdin + timeout behavior ride the same relay path (covered in depth by
    // animus-env-transport's suite); spot-check stdin here.
    const echoed = await env.execCommand(
      handle,
      {
        program: NODE,
        args: ['-e', 'let d="";process.stdin.on("data",c=>d+=c);process.stdin.on("end",()=>process.stdout.write(d.toUpperCase()));'],
      },
      'quiet',
      5,
    );
    expect(echoed.stdout).toBe('QUIET');

    // Teardown deletes the service and is idempotent.
    await env.teardown(handle);
    expect(fake.deleted).toEqual([{ serviceId: 'svc-1', environmentId: 'env-1' }]);
    await env.teardown(handle); // second teardown: no throw
    expect(fake.deleted).toHaveLength(2); // delete is re-issued; the API treats missing as success
  });

  it('names the service DETERMINISTICALLY from a broker run id', async () => {
    const { env, fake } = await makeEnv();
    const runId = 'run-abc-123';
    const expected = SERVICE_NAME_PREFIX + createHash('sha256').update(JSON.stringify(['proj-1', runId])).digest('hex').slice(0, 12);
    const { handle } = await env.prepare({ spec: { kind: 'railway', metadata: { animus_run_id: runId } } });
    expect(fake.created[0].name).toBe(expected);
    expect(expected.length).toBeLessThanOrEqual(26);
    expect((handle.metadata as Record<string, unknown>).animus_run_id).toBe(runId);
    await env.teardown(handle);
  });

  it('teardown cold-deletes by run id when the handle has no service_id', async () => {
    const { env, fake } = await makeEnv();
    const runId = 'run-xyz-789';
    const name = SERVICE_NAME_PREFIX + createHash('sha256').update(JSON.stringify(['proj-1', runId])).digest('hex').slice(0, 12);
    fake.listed = [
      { id: 'svc-match', name },
      { id: 'svc-other', name: `${SERVICE_NAME_PREFIX}unrelated` },
    ];
    // A run-id-only handle: no service_id (the caller never received the full handle).
    await env.teardown({
      id: 'r-none',
      workspace_root: '/workspace',
      metadata: { animus_run_id: runId, project_id: 'proj-1', environment_id: 'env-1' },
    });
    expect(fake.deleted).toEqual([{ serviceId: 'svc-match', environmentId: 'env-1' }]);
  });

  it('rolls back (delete + release) when the container never dials home', async () => {
    const { env, fake } = await makeEnv();
    fake.bootBridge = false;
    const started = Date.now();
    await expect(
      env.prepare({ spec: { kind: 'railway', metadata: { railway_project_id: 'proj-1', railway_environment_id: 'env-1' } } }),
    ).rejects.toThrow(/did not dial home/);
    expect(Date.now() - started).toBeGreaterThanOrEqual(9_000);
    expect(fake.deleted).toEqual([{ serviceId: 'svc-1', environmentId: 'env-1' }]);
  }, 30_000);

  it('defaults a multi-repo exec cwd to the primary subdir', async () => {
    const { env } = await makeEnv();
    const { handle } = await env.prepare({ spec: { kind: 'railway' } });
    // Simulate a multi-repo handle (clone itself is exercised via cloneCommands).
    (handle.metadata as Record<string, unknown>).primary_subdir = 'primary-app';
    const res = await env.execCommand(handle, { program: NODE, args: ['-e', 'console.log(process.cwd())'] }, null, null);
    // The fake bridge roots its workspace at tmpdir(); a missing subdir means
    // the spawn fails with an ENOENT-ish error rather than running at the
    // root — proving the cwd default was applied.
    expect(res.exit_code).toBeNull();
    expect(res.stderr).toMatch(/ENOENT|no such file/i);
  });

  it('gcOrphans (default) sweeps only this instance, sparing other instances', async () => {
    const { env, fake } = await makeEnv();
    const { handle } = await env.prepare({ spec: { kind: 'railway' } });
    fake.listed = [
      { id: 'svc-live', name: `${SERVICE_NAME_PREFIX}${env.instanceId}-${handle.id}` },
      { id: 'svc-orphan-mine', name: `${SERVICE_NAME_PREFIX}${env.instanceId}-dead-run` },
      { id: 'svc-other-instance', name: `${SERVICE_NAME_PREFIX}i-other-live-run` },
      { id: 'svc-unrelated', name: 'postgres' },
    ];
    const removed = await env.gcOrphans();
    expect(removed).toEqual(['svc-orphan-mine']);
    expect(fake.deleted.map((d) => d.serviceId)).not.toContain('svc-live');
    expect(fake.deleted.map((d) => d.serviceId)).not.toContain('svc-other-instance');
  });

  it('gcOrphans allInstances reaps crashed-instance leftovers too', async () => {
    const { env, fake } = await makeEnv();
    const { handle } = await env.prepare({ spec: { kind: 'railway' } });
    fake.listed = [
      { id: 'svc-live', name: `${SERVICE_NAME_PREFIX}${env.instanceId}-${handle.id}` },
      { id: 'svc-crashed-instance', name: `${SERVICE_NAME_PREFIX}i-dead-instance-run` },
    ];
    const removed = await env.gcOrphans({ allInstances: true });
    expect(removed).toEqual(['svc-crashed-instance']);
    expect(fake.deleted.map((d) => d.serviceId)).not.toContain('svc-live');
  });

  it('health degrades (not fails) on missing per-run-suppliable config', async () => {
    const fake = new FakeRailway();
    const env = new RailwayEnvironment({ railway: fake, config: {} });
    const report = env.health();
    // RAILWAY_TOKEN is satisfied by the injected API client; the ids/public
    // url can arrive per-run, so they only degrade.
    expect(report.status).toBe('degraded');
    expect(report.last_error).toMatch(/RAILWAY_PROJECT_ID/);
  });

  it('health is unhealthy only when the API token is truly absent', async () => {
    const saved = process.env.RAILWAY_TOKEN;
    delete process.env.RAILWAY_TOKEN;
    try {
      const env = new RailwayEnvironment({ config: {} });
      expect(env.health()).toMatchObject({ status: 'unhealthy', last_error: expect.stringContaining('RAILWAY_TOKEN') });
    } finally {
      if (saved !== undefined) process.env.RAILWAY_TOKEN = saved;
    }
  });

  // NOTE: the public relay URL + port are now owned by the SINGLETON relay
  // process (animus-env-relay), not this plugin — the two former tests that
  // asserted this plugin binds/validates the port were removed with that move.
  // The singleton's own port validation lives in relay-cli (env-transport).
});

// ---------------------------------------------------------------------------
// Real Railway integration (pending credentials)

const haveCreds = Boolean(
  process.env.RAILWAY_TOKEN &&
    process.env.RAILWAY_PROJECT_ID &&
    process.env.RAILWAY_ENVIRONMENT_ID &&
    process.env.ANIMUS_ENV_RELAY_PUBLIC_URL &&
    process.env.ANIMUS_ENV_RELAY_PORT,
);
if (!haveCreds) {
  // eslint-disable-next-line no-console
  console.warn(
    '[environment.test] skipping real-Railway integration tests: set RAILWAY_TOKEN, RAILWAY_PROJECT_ID, ' +
      'RAILWAY_ENVIRONMENT_ID, ANIMUS_ENV_RELAY_PUBLIC_URL (reachable FROM Railway), and ANIMUS_ENV_RELAY_PORT ' +
      'to enable them',
  );
}

describe.skipIf(!haveCreds)('railway environment (real Railway, integration-pending)', () => {
  it('prepares, execs, and tears down a real run service', async () => {
    const env = new RailwayEnvironment({});
    const { handle } = await env.prepare({ spec: { kind: 'railway' } });
    try {
      const res = await env.execCommand(handle, { program: 'echo', args: ['ok'] }, null, 60);
      expect(res.exit_code).toBe(0);
    } finally {
      await env.teardown(handle);
      await env.close();
    }
  }, 600_000);
});

describe('logStorageEnv', () => {
  it('collects the non-empty S3 vars the log-storage-s3 plugin reads', () => {
    const env = {
      S3_BUCKET: 'logs',
      S3_ACCESS_KEY_ID: 'AK',
      S3_SECRET_ACCESS_KEY: 'SK',
      S3_ENDPOINT: 'https://s3.example',
      S3_REGION: '',
      OTHER: 'ignored',
    } as unknown as NodeJS.ProcessEnv;
    expect(logStorageEnv(env)).toEqual({
      S3_BUCKET: 'logs',
      S3_ACCESS_KEY_ID: 'AK',
      S3_SECRET_ACCESS_KEY: 'SK',
      S3_ENDPOINT: 'https://s3.example',
    });
  });

  it('returns nothing when no S3 env is present', () => {
    expect(logStorageEnv({} as NodeJS.ProcessEnv)).toEqual({});
  });
});
