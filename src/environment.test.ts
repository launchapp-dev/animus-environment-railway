// Substrate tests: the full prepare -> exec/exec_stream -> teardown flow
// against a FAKE Railway API and a REAL in-process relay + bridge (the fake
// "container" is a BridgeClient from animus-env-transport dialing the relay on
// 127.0.0.1 with the exact variables the plugin injected). No real network, no
// Railway credentials.
//
// A creds-gated suite at the bottom marks the real-Railway path as
// integration-pending: it skips with a clear message unless RAILWAY_TOKEN +
// RAILWAY_PROJECT_ID + RAILWAY_ENVIRONMENT_ID are present.

import { mkdirSync, mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { afterEach, describe, expect, it } from 'vitest';

import { BridgeClient, RelayServer } from '@launchapp-dev/animus-env-transport';
import { planWorkspace } from '@launchapp-dev/animus-environment-base';

import {
  cloneCommands,
  configFromEnv,
  DEFAULT_BRIDGE_COMMAND,
  DEFAULT_IMAGE,
  harnessCredentialVars,
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

  it('harnessCredentialVars base64s the subscription creds + passes the github token', () => {
    const dir = mkdtempSync(join(tmpdir(), 'creds-'));
    const claudeDir = join(dir, 'claude');
    const codexDir = join(dir, 'codex');
    mkdirSync(claudeDir);
    mkdirSync(codexDir);
    writeFileSync(join(claudeDir, '.credentials.json'), '{"claudeAiOauth":"c"}');
    writeFileSync(join(codexDir, 'auth.json'), '{"tokens":"x"}');
    const vars = harnessCredentialVars({
      CLAUDE_CONFIG_DIR: claudeDir,
      CODEX_OAUTH_HOME: codexDir,
      GITHUB_TOKEN: 'ghtok',
    } as NodeJS.ProcessEnv);
    expect(Buffer.from(vars.ANIMUS_NODE_CLAUDE_CREDENTIALS_B64, 'base64').toString()).toBe('{"claudeAiOauth":"c"}');
    expect(Buffer.from(vars.ANIMUS_NODE_CODEX_AUTH_B64, 'base64').toString()).toBe('{"tokens":"x"}');
    expect(vars.GITHUB_TOKEN).toBe('ghtok');
  });

  it('harnessCredentialVars skips missing creds (best-effort)', () => {
    expect(harnessCredentialVars({ CLAUDE_CONFIG_DIR: '/nonexistent' } as NodeJS.ProcessEnv)).toEqual({});
    expect(harnessCredentialVars({} as NodeJS.ProcessEnv)).toEqual({});
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

  it('health flags a public URL without a fixed relay port as unhealthy', async () => {
    const fake = new FakeRailway();
    const env = new RailwayEnvironment({
      railway: fake,
      config: { projectId: 'p', environmentId: 'e', relayPublicUrl: 'wss://daemon.example.com' },
    });
    expect(env.health()).toMatchObject({
      status: 'unhealthy',
      last_error: expect.stringContaining('ANIMUS_ENV_RELAY_PORT'),
    });
  });

  it('refuses an ephemeral relay port when a fixed public URL is configured', async () => {
    const fake = new FakeRailway();
    const env = new RailwayEnvironment({
      railway: fake,
      config: { projectId: 'p', environmentId: 'e', relayPublicUrl: 'wss://daemon.example.com' },
    });
    await expect(env.prepare({ spec: { kind: 'railway' } })).rejects.toThrow(/ANIMUS_ENV_RELAY_PORT/);
  });
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
