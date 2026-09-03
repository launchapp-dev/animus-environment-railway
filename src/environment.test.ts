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
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PassThrough } from 'node:stream';

import { afterEach, describe, expect, it } from 'vitest';

import { BridgeClient, RelayServer, RelaySingleton, type ExecutionFence, type RelayListedRun } from '@launchapp-dev/animus-env-transport';
import {
  defineEnvironmentPlugin,
  planWorkspace,
  type EnvironmentPluginSpec,
  type HarnessCommand,
} from '@launchapp-dev/animus-environment-base';

import {
  claudeConfigDir,
  claudeNodeCredentials,
  cloneCommands,
  clientServicePrefix,
  configFromEnv,
  DEFAULT_BRIDGE_COMMAND,
  DEFAULT_CODEX_OAUTH_HOME,
  DEFAULT_CLAUDE_CONFIG_DIR,
  DEFAULT_IMAGE,
  DEFAULT_KIMI_CODE_HOME,
  DEFAULT_MAX_MANAGED_NODES,
  deterministicServiceName,
  githubAppCredentials,
  harnessCredentialVars,
  kimiConfigDir,
  kimiNodeBundle,
  kimiNodeCredentials,
  logStorageEnv,
  actorBinding,
  makeActorBoundReverseHandler,
  parseGithubSlug,
  RailwayEnvironment,
  type RelayTransport,
  resolveTarget,
  runVariables,
  skillsSyncFiles,
  ACTOR_ENV,
  WORKSPACE_ROOT,
} from './environment.js';
import {
  SERVICE_NAME_PREFIX,
  type CreatedService,
  type RailwayApi,
  type RunServiceDetail,
  type ServiceCreateInput,
} from './railway.js';

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
  /** Model Railway's restart-visible service inventory for multi-client tests. */
  trackCreatedServices = true;
  /** Delay control-plane visibility after create has already returned. */
  createdVisibilityDelayMs = 0;
  /** Hold creation open to exercise cross-process admission races. */
  createDelayMs = 0;
  private readonly bridges: BridgeClient[] = [];
  private seq = 0;

  async createRunService(input: ServiceCreateInput & { startCommand: string }): Promise<CreatedService> {
    this.created.push(input);
    this.seq += 1;
    const serviceId = `svc-${this.seq}`;
    if (this.createDelayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.createDelayMs));
    }
    if (this.trackCreatedServices) {
      const expose = (): void => {
        if (!this.deleted.some((entry) => entry.serviceId === serviceId)) {
          this.listed.push({ id: serviceId, name: input.name });
        }
      };
      if (this.createdVisibilityDelayMs > 0) setTimeout(expose, this.createdVisibilityDelayMs);
      else expose();
    }
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
    if (this.trackCreatedServices) this.listed = this.listed.filter((service) => service.id !== serviceId);
  }

  async listRunServices(): Promise<Array<{ id: string; name: string }>> {
    return this.listed;
  }

  /** Set to drive the state-aware node-management surface (list/get/reap). */
  detailed: RunServiceDetail[] | null = null;
  async listRunServicesDetailed(): Promise<RunServiceDetail[]> {
    return this.detailed ?? this.listed.map((s) => ({ ...s, status: null, createdAt: null }));
  }

  closeBridges(): void {
    for (const b of this.bridges) b.close();
  }
}

class RecordingRelay implements RelayTransport {
  readonly sessions: Array<{ handleId: string; params: Record<string, unknown> }> = [];
  readonly attachments: Array<{ handleId: string; token: string; ownerToken: string }> = [];
  readonly releases: Array<{ handleId: string; token?: string; ownerToken?: string }> = [];
  readonly registrations: Array<{ handleId: string; token: string; ownerToken: string; binding: string | null }> = [];
  /** Handles THIS "process" has attached — empty after a simulated restart. */
  private readonly attached = new Set<string>();

  /** Durable registry state, shared across instances to simulate the singleton
   *  outliving an owner (plugin) process restart. */
  constructor(
    readonly store = new Map<string, { token: string; ownerToken: string; binding: string | null }>(),
  ) {}

  registerRun(
    handleId = 'relay-handle',
    credentials?: { token: string; ownerToken: string },
    _scope?: unknown,
    _attach?: boolean,
    binding?: string,
  ): { url: string; token: string; ownerToken: string } {
    const creds = credentials ?? { token: 'T'.repeat(43), ownerToken: 'O'.repeat(43) };
    this.store.set(handleId, { ...creds, binding: binding ?? null });
    this.attached.add(handleId);
    this.registrations.push({ handleId, ...creds, binding: binding ?? null });
    return { url: `ws://relay/relay/${handleId}`, ...creds };
  }

  async attachRun(
    handleId: string,
    credentials: { token: string; ownerToken: string },
    _scope?: unknown,
    binding?: string,
  ): Promise<{ url: string; token: string; ownerToken: string }> {
    const existing = this.store.get(handleId);
    if (!existing || existing.token !== credentials.token || existing.ownerToken !== credentials.ownerToken) {
      throw new Error('reattach forbidden');
    }
    if (binding !== undefined) existing.binding = binding;
    this.attachments.push({ handleId, ...credentials });
    this.attached.add(handleId);
    return { url: `ws://relay/relay/${handleId}`, ...credentials };
  }

  async listRuns(): Promise<RelayListedRun[]> {
    return [...this.store].map(([handleId, run]) => ({
      handleId,
      binding: run.binding,
      attached: this.attached.has(handleId),
      nodeConnected: true,
    }));
  }

  async waitForConnection(): Promise<void> {}

  /** Every exec the plugin issued, in order. */
  readonly execCalls: Array<{ handleId?: string; command?: HarnessCommand; stdin?: string | null }> = [];
  /** Exit code the next execs report (default success). */
  execExitCode = 0;

  async exec(
    handleId?: string,
    command?: HarnessCommand,
    opts?: { stdin?: string | null },
  ): Promise<{ exit_code: number; stdout: string; stderr: string; timed_out: boolean }> {
    this.execCalls.push({ handleId, command, stdin: opts?.stdin ?? null });
    return { exit_code: this.execExitCode, stdout: '', stderr: '', timed_out: false };
  }

  async runSession(
    handleId: string,
    params: Record<string, unknown>,
  ): Promise<{ workflow_id: string | null; status: string; execution_fence?: ExecutionFence | null }> {
    this.sessions.push({ handleId, params });
    const executionFence = params.execution_fence as ExecutionFence | null | undefined;
    return {
      workflow_id: typeof params.workflow_id === 'string' ? params.workflow_id : null,
      status: 'completed',
      ...(executionFence ? { execution_fence: executionFence } : {}),
    };
  }

  releaseRun(handleId: string, credentials?: { token: string; ownerToken: string }): void {
    this.releases.push({ handleId, ...credentials });
    this.store.delete(handleId);
    this.attached.delete(handleId);
  }

  registeredRuns(): string[] {
    return [...this.attached];
  }

  async close(): Promise<void> {
    // Detach-only, like RelayClient.close(): the durable store survives.
    this.attached.clear();
  }
}

interface Ctx {
  env: RailwayEnvironment;
  fake: FakeRailway;
}

interface RpcFrame {
  jsonrpc: '2.0';
  id?: number | string | null;
  method?: string;
  params?: unknown;
  result?: unknown;
  error?: { code: number; message: string; data?: unknown };
}

async function drivePlugin(
  impl: Omit<EnvironmentPluginSpec, 'input' | 'output' | 'skipCliArgs' | 'name' | 'version' | 'description'>,
  frames: RpcFrame[],
): Promise<RpcFrame[]> {
  const input = new PassThrough();
  const output = new PassThrough();
  const out: RpcFrame[] = [];
  let buffered = '';
  output.on('data', (chunk: Buffer) => {
    buffered += chunk.toString('utf8');
    let newline = buffered.indexOf('\n');
    while (newline !== -1) {
      const line = buffered.slice(0, newline).trim();
      buffered = buffered.slice(newline + 1);
      if (line) out.push(JSON.parse(line) as RpcFrame);
      newline = buffered.indexOf('\n');
    }
  });

  const plugin = defineEnvironmentPlugin({
    name: 'animus-environment-railway-dispatch-test',
    version: '0.0.0',
    description: 'Railway actor dispatch regression harness',
    skipCliArgs: true,
    input,
    output,
    ...impl,
  });
  const done = plugin.run();
  for (const frame of frames) input.write(`${JSON.stringify(frame)}\n`);
  input.end();
  await done;
  return out;
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

  it('runVariables layers daemon auth + BASE_DB_URL over spec.env and under relay coordinates', () => {
    const vars = runVariables({
      wssUrl: 'wss://relay/relay/h1',
      token: 'tok',
      specEnv: {
        FOO: 'bar',
        ANIMUS_ENV_RUN_TOKEN: 'spoofed',
        ANTHROPIC_AUTH_TOKEN: 'spoofed-anthropic-token',
        ANTHROPIC_BASE_URL: 'https://spoofed.invalid',
      },
      hostEnv: {
        BASE_DB_URL: 'postgres://db',
        ANTHROPIC_AUTH_TOKEN: 'daemon-anthropic-token',
        ANTHROPIC_API_KEY: 'daemon-anthropic-key',
        ANTHROPIC_BASE_URL: 'https://provider.example/v1',
      } as NodeJS.ProcessEnv,
    });
    expect(vars).toEqual({
      BASE_DB_URL: 'postgres://db',
      FOO: 'bar',
      ANTHROPIC_AUTH_TOKEN: 'daemon-anthropic-token',
      ANTHROPIC_API_KEY: 'daemon-anthropic-key',
      ANTHROPIC_BASE_URL: 'https://provider.example/v1',
      ANIMUS_ENV_WSS_URL: 'wss://relay/relay/h1',
      ANIMUS_ENV_RUN_TOKEN: 'tok', // relay identity wins over spec env
      ANIMUS_ENV_WORKSPACE_ROOT: WORKSPACE_ROOT,
    });
  });

  it('reserves the workflow actor env from spec spoofing', () => {
    const alice = actorBinding({ user_id: 'alice', claims: ['admin'], tenant_id: 'acme' });
    const scoped = runVariables({
      wssUrl: 'wss://relay/relay/h1',
      token: 'tok',
      specEnv: { [ACTOR_ENV]: '{"user_id":"mallory"}' },
      hostEnv: {} as NodeJS.ProcessEnv,
      actorJson: alice.actorJson,
    });
    expect(scoped[ACTOR_ENV]).toBe(alice.actorJson);

    const system = runVariables({
      wssUrl: 'wss://relay/relay/h2',
      token: 'tok-2',
      specEnv: { [ACTOR_ENV]: '{"user_id":"mallory"}' },
      hostEnv: {} as NodeJS.ProcessEnv,
    });
    expect(system[ACTOR_ENV]).toBeUndefined();
  });

  it('overwrites node-supplied actors on reverse calls and strips them for system handles', async () => {
    const actor = actorBinding({ user_id: 'alice', claims: ['admin'], tenant_id: 'acme' });
    const system = actorBinding(null);
    const seen: unknown[] = [];
    const next = async (_method: string, params: unknown): Promise<unknown> => {
      seen.push(params);
      return { ok: true };
    };
    const handler = makeActorBoundReverseHandler(
      new Map([
        ['actor-handle', actor],
        ['system-handle', system],
      ]),
      next,
    );

    await handler(
      'backend/call',
      {
        role: 'subject',
        method: 'subject/get',
        actor: { user_id: 'outer-spoof' },
        params: { id: 'TASK-1', actor: { user_id: 'mallory' } },
      },
      { handleId: 'actor-handle' },
    );
    expect(seen[0]).toEqual({
      role: 'subject',
      method: 'subject/get',
      actor: actor.actor,
      params: { id: 'TASK-1', actor: actor.actor },
    });

    await handler(
      'backend/call',
      {
        role: 'subject',
        method: 'subject/get',
        actor: { user_id: 'outer-spoof' },
        params: { id: 'TASK-2', actor: { user_id: 'mallory' } },
      },
      { handleId: 'system-handle' },
    );
    expect(seen[1]).toEqual({ role: 'subject', method: 'subject/get', params: { id: 'TASK-2' } });

    await expect(
      handler('backend/call', { role: 'subject', method: 'subject/get', params: {} }, { handleId: 'unknown' }),
    ).rejects.toThrow(/unknown or unbound/);
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

  it('resolves the durable Claude config fallback only when the env is unset', () => {
    const explicit = mkdtempSync(join(tmpdir(), 'claude-explicit-'));
    expect(claudeConfigDir({} as NodeJS.ProcessEnv)).toBe(DEFAULT_CLAUDE_CONFIG_DIR);
    expect(claudeConfigDir({ CLAUDE_CONFIG_DIR: explicit } as NodeJS.ProcessEnv)).toBe(explicit);
    expect(claudeConfigDir({ CLAUDE_CONFIG_DIR: '' } as NodeJS.ProcessEnv)).toBeNull();
  });

  it('harnessCredentialVars skips missing creds (best-effort)', () => {
    expect(harnessCredentialVars({ CODEX_OAUTH_HOME: '/nonexistent-codex-home' } as NodeJS.ProcessEnv)).toEqual({});
  });

  it('kimiNodeBundle emits the exact JSON bundle when both files exist', () => {
    const dir = mkdtempSync(join(tmpdir(), 'kimi-'));
    writeFileSync(join(dir, 'config.toml'), 'model = "k3"\n');
    mkdirSync(join(dir, 'credentials'), { recursive: true });
    writeFileSync(join(dir, 'credentials', 'kimi-code.json'), '{"token":"k"}');
    const vars = kimiNodeBundle({ KIMI_CODE_HOME: dir } as NodeJS.ProcessEnv);
    const decoded = JSON.parse(Buffer.from(vars.ANIMUS_NODE_KIMI_BUNDLE_B64, 'base64').toString('utf8'));
    expect(decoded).toEqual({
      files: {
        'config.toml': 'model = "k3"\n',
        'credentials/kimi-code.json': '{"token":"k"}',
      },
    });
  });

  it('kimiNodeBundle includes only the files that exist', () => {
    const dir = mkdtempSync(join(tmpdir(), 'kimi-'));
    writeFileSync(join(dir, 'config.toml'), 'model = "k3"\n');
    const vars = kimiNodeBundle({ KIMI_CODE_HOME: dir } as NodeJS.ProcessEnv);
    const decoded = JSON.parse(Buffer.from(vars.ANIMUS_NODE_KIMI_BUNDLE_B64, 'base64').toString('utf8'));
    expect(decoded).toEqual({ files: { 'config.toml': 'model = "k3"\n' } });
  });

  it('kimiNodeBundle injects nothing when the dir or files are absent', () => {
    expect(kimiNodeBundle({ KIMI_CODE_HOME: '/nonexistent-kimi-home' } as NodeJS.ProcessEnv)).toEqual({});
    // dir exists but holds neither bundle file
    const empty = mkdtempSync(join(tmpdir(), 'kimi-empty-'));
    expect(kimiNodeBundle({ KIMI_CODE_HOME: empty } as NodeJS.ProcessEnv)).toEqual({});
  });

  it('kimiNodeCredentials refreshes a near-expiry bundle centrally and strips the refresh token', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'kimi-refresh-'));
    writeFileSync(join(dir, 'config.toml'), 'model = "k3"\n');
    mkdirSync(join(dir, 'credentials'), { recursive: true });
    const now = 1_800_000_000_000;
    const expired = {
      access_token: 'old-access',
      refresh_token: 'old-refresh',
      expires_at: Math.floor((now - 60_000) / 1000),
      expires_in: 900,
      scope: 's',
      token_type: 'bearer',
    };
    writeFileSync(join(dir, 'credentials', 'kimi-code.json'), JSON.stringify(expired));

    const fetchImpl = (async (url: unknown, init: unknown) => {
      const body = String((init as { body?: string }).body ?? '');
      expect(body).toContain('old-refresh');
      return new Response(JSON.stringify({ access_token: 'fresh-access', refresh_token: 'fresh-refresh', expires_in: 900 }), { status: 200 });
    }) as typeof fetch;

    const vars = await kimiNodeCredentials({ KIMI_CODE_HOME: dir } as NodeJS.ProcessEnv, now, fetchImpl);
    const decoded = JSON.parse(Buffer.from(vars.ANIMUS_NODE_KIMI_BUNDLE_B64, 'base64').toString('utf8'));
    const nodeCreds = JSON.parse(decoded.files['credentials/kimi-code.json']);
    expect(nodeCreds.access_token).toBe('fresh-access');
    expect(nodeCreds.refresh_token).toBeUndefined();

    // The daemon-side store absorbed the rotation (next prepare starts fresh).
    const stored = JSON.parse(readFileSync(join(dir, 'credentials', 'kimi-code.json'), 'utf8'));
    expect(stored.access_token).toBe('fresh-access');
    expect(stored.refresh_token).toBe('fresh-refresh');
  });

  it('kimiNodeCredentials injects the current bundle (refresh-stripped) when the token is still valid', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'kimi-valid-'));
    writeFileSync(join(dir, 'config.toml'), 'model = "k3"\n');
    mkdirSync(join(dir, 'credentials'), { recursive: true });
    const now = 1_800_000_000_000;
    writeFileSync(join(dir, 'credentials', 'kimi-code.json'), JSON.stringify({
      access_token: 'valid-access',
      refresh_token: 'valid-refresh',
      expires_at: Math.floor((now + 3_600_000) / 1000),
      expires_in: 3600,
    }));
    const fetchImpl = (async () => { throw new Error('must not refresh a valid token'); }) as typeof fetch;
    const vars = await kimiNodeCredentials({ KIMI_CODE_HOME: dir } as NodeJS.ProcessEnv, now, fetchImpl);
    const decoded = JSON.parse(Buffer.from(vars.ANIMUS_NODE_KIMI_BUNDLE_B64, 'base64').toString('utf8'));
    const nodeCreds = JSON.parse(decoded.files['credentials/kimi-code.json']);
    expect(nodeCreds.access_token).toBe('valid-access');
    expect(nodeCreds.refresh_token).toBeUndefined();
  });

  it('kimiNodeCredentials returns {} on refresh failure or missing tokens', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'kimi-dead-'));
    mkdirSync(join(dir, 'credentials'), { recursive: true });
    writeFileSync(join(dir, 'credentials', 'kimi-code.json'), JSON.stringify({ access_token: 'x', expires_at: 1 }));
    const vars = await kimiNodeCredentials({ KIMI_CODE_HOME: dir } as NodeJS.ProcessEnv, Date.now());
    expect(vars).toEqual({});
  });

  it('kimiConfigDir resolves the durable portal fallback only when the env is unset', () => {
    const explicit = mkdtempSync(join(tmpdir(), 'kimi-explicit-'));
    expect(DEFAULT_KIMI_CODE_HOME).toBe('/data/animus-state/kimi-config');
    expect(kimiConfigDir({} as NodeJS.ProcessEnv)).toBe(DEFAULT_KIMI_CODE_HOME);
    expect(kimiConfigDir({ KIMI_CODE_HOME: explicit } as NodeJS.ProcessEnv)).toBe(explicit);
    expect(kimiConfigDir({ KIMI_CODE_HOME: '' } as NodeJS.ProcessEnv)).toBeNull();
  });

  it('harnessCredentialVars includes the kimi bundle alongside codex auth', () => {
    const codexDir = mkdtempSync(join(tmpdir(), 'codex-'));
    writeFileSync(join(codexDir, 'auth.json'), '{"tokens":"x"}');
    const kimiDir = mkdtempSync(join(tmpdir(), 'kimi-'));
    mkdirSync(join(kimiDir, 'credentials'), { recursive: true });
    writeFileSync(join(kimiDir, 'credentials', 'kimi-code.json'), '{"token":"k"}');
    const vars = harnessCredentialVars({
      CODEX_OAUTH_HOME: codexDir,
      KIMI_CODE_HOME: kimiDir,
    } as NodeJS.ProcessEnv);
    const decoded = JSON.parse(Buffer.from(vars.ANIMUS_NODE_KIMI_BUNDLE_B64, 'base64').toString('utf8'));
    expect(decoded).toEqual({ files: { 'credentials/kimi-code.json': '{"token":"k"}' } });
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

  it('claudeNodeCredentials loads a valid token from the fallback when the env is unset', async () => {
    const fallbackDir = mkdtempSync(join(tmpdir(), 'claude-default-'));
    const now = 1_000_000;
    writeFileSync(
      join(fallbackDir, '.credentials.json'),
      JSON.stringify({ claudeAiOauth: { accessToken: 'fallback-A', refreshToken: 'R', expiresAt: now + 3_600_000 } }),
    );
    const vars = await claudeNodeCredentials(
      {} as NodeJS.ProcessEnv,
      now,
      (async () => {
        throw new Error('must not refresh a valid fallback token');
      }) as unknown as typeof fetch,
      fallbackDir,
    );
    const injected = JSON.parse(Buffer.from(vars.ANIMUS_NODE_CLAUDE_CREDENTIALS_B64, 'base64').toString());
    expect(injected.claudeAiOauth.accessToken).toBe('fallback-A');
    expect(injected.claudeAiOauth.refreshToken).toBeUndefined();
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

  it('claudeNodeCredentials rejects an empty unexpired access token', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'claude-'));
    const now = 1_000_000;
    writeFileSync(
      join(dir, '.credentials.json'),
      JSON.stringify({ claudeAiOauth: { accessToken: '', refreshToken: '', expiresAt: now + 3_600_000 } }),
    );
    expect(await claudeNodeCredentials({ CLAUDE_CONFIG_DIR: dir } as NodeJS.ProcessEnv, now)).toEqual({});
  });

  it('claudeNodeCredentials rejects a refresh response without an access token', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'claude-'));
    const now = 1_000_000;
    writeFileSync(
      join(dir, '.credentials.json'),
      JSON.stringify({ claudeAiOauth: { accessToken: 'old', refreshToken: 'R', expiresAt: now - 1 } }),
    );
    const missingAccessToken = (async () => ({
      ok: true,
      status: 200,
      json: async () => ({ expires_in: 3600 }),
    })) as unknown as typeof fetch;
    expect(
      await claudeNodeCredentials({ CLAUDE_CONFIG_DIR: dir } as NodeJS.ProcessEnv, now, missingAccessToken),
    ).toEqual({});
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
      ANIMUS_ENV_CLIENT_ID: 'portal-production',
      ANIMUS_ENV_MAX_MANAGED_NODES: '7',
      RAILWAY_VOLUME_MOUNT_PATH: '/data',
      ANIMUS_ENV_CAPACITY_CONFIRMATION_TIMEOUT_MS: '1234',
    } as NodeJS.ProcessEnv);
    expect(config).toMatchObject({
      projectId: 'p',
      environmentId: 'e',
      relayPublicUrl: 'wss://daemon.example.com',
      relayPort: 8790,
      dialTimeoutSecs: 60,
      clientId: 'portal-production',
      maxManagedNodes: 7,
      capacityLockRoot: '/data/animus-environment-railway-capacity',
      capacityConfirmationTimeoutMs: 1234,
    });
    expect(DEFAULT_MAX_MANAGED_NODES).toBe(5);
    expect(
      configFromEnv({ ANIMUS_ENV_MAX_MANAGED_NODES: '   ' } as NodeJS.ProcessEnv).maxManagedNodes,
    ).toBeUndefined();
    expect(
      configFromEnv({ ANIMUS_ENV_CAPACITY_CONFIRMATION_TIMEOUT_MS: '   ' } as NodeJS.ProcessEnv)
        .capacityConfirmationTimeoutMs,
    ).toBeUndefined();
    expect(
      configFromEnv({ ANIMUS_ENV_MAX_MANAGED_NODES: '0' } as NodeJS.ProcessEnv).maxManagedNodes,
    ).toBe(0);
    expect(
      configFromEnv({
        RAILWAY_VOLUME_MOUNT_PATH: '/data',
        ANIMUS_ENV_CAPACITY_LOCK_DIR: '/custom/capacity',
      } as NodeJS.ProcessEnv).capacityLockRoot,
    ).toBe('/custom/capacity');
    expect(
      configFromEnv({ ANIMUS_ENV_CAPACITY_CONFIRMATION_TIMEOUT_MS: '0' } as NodeJS.ProcessEnv)
        .capacityConfirmationTimeoutMs,
    ).toBe(0);
    expect(configFromEnv({ RAILWAY_TOKEN: 'railway-secret' } as NodeJS.ProcessEnv).actorBindingSecret).toBe(
      'railway-secret',
    );
    expect(
      configFromEnv({
        RAILWAY_TOKEN: 'railway-secret',
        ANIMUS_ENV_ACTOR_BINDING_SECRET: 'dedicated-secret',
      } as NodeJS.ProcessEnv).actorBindingSecret,
    ).toBe('dedicated-secret');
    expect(
      () =>
        new RailwayEnvironment({
          railway: new FakeRailway(),
          config: { maxManagedNodes: 1.5 },
        }),
    ).toThrow(/non-negative integer/);
    expect(
      () =>
        new RailwayEnvironment({
          railway: new FakeRailway(),
          config: { capacityConfirmationTimeoutMs: -1 },
        }),
    ).toThrow(/CAPACITY_CONFIRMATION_TIMEOUT_MS must be a non-negative integer/);
  });
});

describe('skills sync (SPEC-001)', () => {
  function makeRecordingEnv(): Ctx & { relay: RecordingRelay } {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    const relay = new RecordingRelay();
    const env = new RailwayEnvironment({
      railway: fake,
      relay,
      config: { projectId: 'proj-1', environmentId: 'env-1' },
    });
    live.push({ env, fake });
    return { env, fake, relay };
  }

  it('skillsSyncFiles returns null for absent/empty metadata and unreadable dirs', async () => {
    expect(await skillsSyncFiles({})).toBeNull();
    expect(await skillsSyncFiles({ metadata: {} })).toBeNull();
    expect(await skillsSyncFiles({ metadata: { skills_sync_dir: '   ' } })).toBeNull();
    // Unreadable dir: a warn + null, never a throw.
    await expect(
      skillsSyncFiles({ metadata: { skills_sync_dir: '/nonexistent/skills-sync-dir' } }),
    ).resolves.toBeNull();
  });

  it('skillsSyncFiles base64s sanitized *.yaml files and skips the rest', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'skills-sync-'));
    try {
      writeFileSync(join(dir, 'code-review.yaml'), 'name: code-review\n');
      writeFileSync(join(dir, 'test-writer.yaml'), 'name: test-writer\n');
      writeFileSync(join(dir, 'NOTES.md'), 'not yaml'); // ignored silently
      writeFileSync(join(dir, 'Bad_Name.yaml'), 'name: bad\n'); // unsafe name: skipped with a warn
      const files = await skillsSyncFiles({ metadata: { skills_sync_dir: dir } });
      expect(files?.map((f) => f.name)).toEqual(['code-review.yaml', 'test-writer.yaml']);
      expect(Buffer.from(files?.[0]?.contentBase64 ?? '', 'base64').toString('utf8')).toBe('name: code-review\n');
      expect(Buffer.from(files?.[1]?.contentBase64 ?? '', 'base64').toString('utf8')).toBe('name: test-writer\n');
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('prepare writes each synced skill onto the node before any phase runs', async () => {
    const { env, relay } = makeRecordingEnv();
    const dir = mkdtempSync(join(tmpdir(), 'skills-sync-'));
    try {
      writeFileSync(join(dir, 'code-review.yaml'), 'name: code-review\n');
      writeFileSync(join(dir, 'test-writer.yaml'), 'name: test-writer\n');
      const { handle } = await env.prepare({ spec: { kind: 'railway', metadata: { skills_sync_dir: dir } } });
      expect(relay.execCalls).toHaveLength(2);
      for (const [i, name] of ['code-review.yaml', 'test-writer.yaml'].entries()) {
        const call = relay.execCalls[i];
        expect(call?.handleId).toBe(handle.id);
        expect(call?.command?.program).toBe('sh');
        const script = call?.command?.args?.[1] ?? '';
        expect(script).toContain('d="${HOME:-/root}/.animus/config/skill_definitions"');
        expect(script).toContain('mkdir -p "$d"');
        expect(script).toContain(`base64 -d > "$d/${name}"`);
        expect(Buffer.from(call?.stdin ?? '', 'base64').toString('utf8')).toBe(`name: ${name.replace('.yaml', '')}\n`);
      }
      await env.teardown(handle);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('prepare without skills_sync_dir makes no sync calls', async () => {
    const { env, relay } = makeRecordingEnv();
    const { handle } = await env.prepare({ spec: { kind: 'railway' } });
    expect(relay.execCalls).toHaveLength(0);
    await env.teardown(handle);
  });

  it('prepare with an unreadable skills_sync_dir still prepares (no sync calls)', async () => {
    const { env, relay } = makeRecordingEnv();
    const { handle } = await env.prepare({
      spec: { kind: 'railway', metadata: { skills_sync_dir: '/nonexistent/skills-sync-dir' } },
    });
    expect(relay.execCalls).toHaveLength(0);
    await env.teardown(handle);
  });

  it('a failed skills-sync write fails prepare like a failed clone', async () => {
    const { env, fake, relay } = makeRecordingEnv();
    const dir = mkdtempSync(join(tmpdir(), 'skills-sync-'));
    try {
      writeFileSync(join(dir, 'code-review.yaml'), 'name: code-review\n');
      relay.execExitCode = 1;
      await expect(
        env.prepare({ spec: { kind: 'railway', metadata: { skills_sync_dir: dir } } }),
      ).rejects.toThrow(/skills-sync write failed \(exit 1\) for code-review\.yaml/);
      // Rolled back: the half-prepared service was deleted.
      expect(fake.deleted).toHaveLength(1);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe('prepare -> exec -> teardown (fake Railway, real relay + bridge)', () => {
  it('runs the full flow', async () => {
    const { env, fake } = await makeEnv();

    const { handle } = await env.prepare({ spec: { kind: 'railway', env: { RUN_FLAG: 'yes' } } });
    expect(handle.id).toMatch(/^r[0-9a-f]{32}$/);
    expect(handle.workspace_root).toBe(WORKSPACE_ROOT);
    const meta = handle.metadata as Record<string, unknown>;
    expect(meta.service_id).toBe('svc-1');
    expect(meta.project_id).toBe('proj-1');
    expect(meta.environment_id).toBe('env-1');
    expect(String(meta.service_name)).toBe(
      `${clientServicePrefix('proj-1', 'animus-environment-railway')}${handle.id.slice(0, 11)}`,
    );

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
  }, 30_000);

  it('names the service DETERMINISTICALLY from a broker run id', async () => {
    const { env, fake } = await makeEnv();
    const runId = 'run-abc-123';
    const expected = deterministicServiceName('proj-1', 'animus-environment-railway', runId);
    const { handle } = await env.prepare({ spec: { kind: 'railway', metadata: { animus_run_id: runId } } });
    expect(fake.created[0].name).toBe(expected);
    expect(expected.length).toBeLessThanOrEqual(32);
    expect((handle.metadata as Record<string, unknown>).animus_run_id).toBe(runId);
    await env.teardown(handle);
  });

  it('enforces the configurable node cap across concurrent prepares for one client', async () => {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    const relay = new RecordingRelay();
    const env = new RailwayEnvironment({
      railway: fake,
      relay,
      config: {
        projectId: 'proj-1',
        environmentId: 'env-1',
        clientId: 'portal-production',
        maxManagedNodes: 2,
      },
    });
    live.push({ env, fake });

    const results = await Promise.allSettled([
      env.prepare({ spec: { kind: 'railway', metadata: { animus_run_id: 'run-cap-1' } } }),
      env.prepare({ spec: { kind: 'railway', metadata: { animus_run_id: 'run-cap-2' } } }),
      env.prepare({ spec: { kind: 'railway', metadata: { animus_run_id: 'run-cap-3' } } }),
    ]);

    expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(2);
    const rejected = results.find((result) => result.status === 'rejected');
    expect(rejected).toMatchObject({
      status: 'rejected',
      reason: expect.objectContaining({ message: expect.stringMatching(/2\/2 managed nodes/) }),
    });
    expect(fake.created).toHaveLength(2);
    expect(relay.releases).toHaveLength(1);

    const first = results.find((result) => result.status === 'fulfilled');
    if (!first || first.status !== 'fulfilled') throw new Error('expected a fulfilled prepare');
    await env.teardown(first.value.handle);
    await expect(
      env.prepare({ spec: { kind: 'railway', metadata: { animus_run_id: 'run-cap-4' } } }),
    ).resolves.toBeDefined();
    expect(fake.created).toHaveLength(3);
  });

  it('serializes capacity across separate plugin instances for one logical client', async () => {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    fake.trackCreatedServices = true;
    fake.createDelayMs = 75;
    // Keep the literal path short enough for macOS's Unix-socket path limit.
    const capacityLockRoot = mkdtempSync('/tmp/animus-cap-');
    const config = {
      projectId: 'proj-1',
      environmentId: 'env-1',
      clientId: 'portal-production',
      maxManagedNodes: 1,
      capacityLockRoot,
    };
    const envA = new RailwayEnvironment({ railway: fake, relay: new RecordingRelay(), config });
    const envB = new RailwayEnvironment({ railway: fake, relay: new RecordingRelay(), config });
    live.push({ env: envA, fake }, { env: envB, fake });

    const results = await Promise.allSettled([
      envA.prepare({ spec: { kind: 'railway', metadata: { animus_run_id: 'run-process-a' } } }),
      envB.prepare({ spec: { kind: 'railway', metadata: { animus_run_id: 'run-process-b' } } }),
    ]);

    expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1);
    expect(results.filter((result) => result.status === 'rejected')).toHaveLength(1);
    const rejected = results.find((result) => result.status === 'rejected');
    expect(rejected).toMatchObject({
      status: 'rejected',
      reason: expect.objectContaining({ message: expect.stringMatching(/1\/1 managed nodes/) }),
    });
    expect(fake.created).toHaveLength(1);
  });

  it('holds cross-process admission until a created service becomes visible', async () => {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    fake.trackCreatedServices = true;
    fake.createdVisibilityDelayMs = 75;
    const capacityLockRoot = mkdtempSync('/tmp/animus-cap-');
    const config = {
      projectId: 'proj-1',
      environmentId: 'env-1',
      clientId: 'portal-production',
      maxManagedNodes: 1,
      capacityLockRoot,
      capacityConfirmationTimeoutMs: 1_000,
    };
    const envA = new RailwayEnvironment({ railway: fake, relay: new RecordingRelay(), config });
    const envB = new RailwayEnvironment({ railway: fake, relay: new RecordingRelay(), config });
    live.push({ env: envA, fake }, { env: envB, fake });

    const results = await Promise.allSettled([
      envA.prepare({ spec: { kind: 'railway', metadata: { animus_run_id: 'run-delayed-a' } } }),
      envB.prepare({ spec: { kind: 'railway', metadata: { animus_run_id: 'run-delayed-b' } } }),
    ]);

    expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1);
    expect(results.filter((result) => result.status === 'rejected')).toHaveLength(1);
    expect(results.find((result) => result.status === 'rejected')).toMatchObject({
      status: 'rejected',
      reason: expect.objectContaining({ message: expect.stringMatching(/1\/1 managed nodes/) }),
    });
    expect(fake.created).toHaveLength(1);
  });

  it('deletes an unconfirmed create before releasing cross-process admission', async () => {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    fake.trackCreatedServices = false;
    const relay = new RecordingRelay();
    const env = new RailwayEnvironment({
      railway: fake,
      relay,
      config: {
        projectId: 'proj-1',
        environmentId: 'env-1',
        clientId: 'portal-production',
        maxManagedNodes: 1,
        capacityConfirmationTimeoutMs: 75,
      },
    });
    live.push({ env, fake });

    await expect(
      env.prepare({ spec: { kind: 'railway', metadata: { animus_run_id: 'run-never-visible' } } }),
    ).rejects.toThrow(/did not confirm created service.*75ms/);
    expect(fake.created).toHaveLength(1);
    expect(fake.deleted).toEqual([{ serviceId: 'svc-1', environmentId: 'env-1' }]);
    expect(relay.releases).toHaveLength(1);
  });

  it('retains shared capacity when an unconfirmed create cannot be rolled back', async () => {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    fake.trackCreatedServices = false;
    fake.deleteService = async () => {
      throw new Error('Railway delete unavailable');
    };
    const capacityLockRoot = mkdtempSync('/tmp/animus-cap-');
    const config = {
      projectId: 'proj-1',
      environmentId: 'env-1',
      clientId: 'portal-production',
      maxManagedNodes: 1,
      capacityLockRoot,
      capacityConfirmationTimeoutMs: 0,
    };
    const envA = new RailwayEnvironment({
      railway: fake,
      relay: new RecordingRelay(),
      config,
    });
    const envB = new RailwayEnvironment({ railway: fake, relay: new RecordingRelay(), config });
    live.push({ env: envA, fake }, { env: envB, fake });

    await expect(envA.prepare({ spec: { kind: 'railway' } })).rejects.toThrow(
      /rollback delete failed: Railway delete unavailable/,
    );
    await expect(envB.prepare({ spec: { kind: 'railway' } })).rejects.toThrow(/1\/1 managed nodes/);
    expect(fake.created).toHaveLength(1);

    // Once Railway exposes the retained service, an explicit lifecycle delete
    // proves it is gone and releases the durable reservation.
    fake.listed = [{ id: 'svc-1', name: fake.created[0]!.name }];
    fake.trackCreatedServices = true;
    fake.deleteService = FakeRailway.prototype.deleteService.bind(fake);
    await expect(envA.teardownNode('svc-1')).resolves.toEqual(['svc-1']);
    fake.trackCreatedServices = false;
    await expect(envB.prepare({ spec: { kind: 'railway' } })).rejects.toThrow(
      /did not confirm created service/,
    );
    expect(fake.created).toHaveLength(2);
  });

  it('retains admission after an ambiguous create even when immediate inventory is stale', async () => {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    fake.createRunService = async (input) => {
      fake.created.push(input);
      throw new Error('connection reset after mutation');
    };
    const capacityLockRoot = mkdtempSync('/tmp/animus-cap-');
    const config = {
      projectId: 'proj-1',
      environmentId: 'env-1',
      clientId: 'portal-production',
      maxManagedNodes: 1,
      capacityLockRoot,
    };
    const envA = new RailwayEnvironment({ railway: fake, relay: new RecordingRelay(), config });
    const envB = new RailwayEnvironment({ railway: fake, relay: new RecordingRelay(), config });
    live.push({ env: envA, fake }, { env: envB, fake });

    await expect(envA.prepare({ spec: { kind: 'railway' } })).rejects.toThrow(
      /connection reset after mutation/,
    );
    await expect(envB.prepare({ spec: { kind: 'railway' } })).rejects.toThrow(/1\/1 managed nodes/);
    expect(fake.created).toHaveLength(1);
  });

  it('retains admission when a failed create is present in inventory', async () => {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    fake.createRunService = async (input) => {
      fake.created.push(input);
      fake.listed.push({ id: 'svc-ambiguous', name: input.name });
      throw new Error('connection reset after mutation');
    };
    const capacityLockRoot = mkdtempSync('/tmp/animus-cap-');
    const config = {
      projectId: 'proj-1',
      environmentId: 'env-1',
      clientId: 'portal-production',
      maxManagedNodes: 1,
      capacityLockRoot,
    };
    const envA = new RailwayEnvironment({ railway: fake, relay: new RecordingRelay(), config });
    const envB = new RailwayEnvironment({ railway: fake, relay: new RecordingRelay(), config });
    live.push({ env: envA, fake }, { env: envB, fake });

    await expect(envA.prepare({ spec: { kind: 'railway' } })).rejects.toThrow(
      /connection reset after mutation/,
    );
    await expect(envB.prepare({ spec: { kind: 'railway' } })).rejects.toThrow(/1\/1 managed nodes/);
    expect(fake.created).toHaveLength(1);
  });

  it('releases a named reservation after restart when inventory verifies the service is absent', async () => {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    let listCalls = 0;
    fake.listRunServices = async () => {
      listCalls += 1;
      if (listCalls > 1) throw new Error('Railway inventory unavailable');
      return [];
    };
    fake.createRunService = async (input) => {
      fake.created.push(input);
      throw new Error('connection reset after mutation');
    };
    const capacityLockRoot = mkdtempSync('/tmp/animus-cap-');
    const config = {
      projectId: 'proj-1',
      environmentId: 'env-1',
      clientId: 'portal-production',
      maxManagedNodes: 1,
      capacityLockRoot,
    };
    const env = new RailwayEnvironment({ railway: fake, relay: new RecordingRelay(), config });
    live.push({ env, fake });

    await expect(env.prepare({ spec: { kind: 'railway' } })).rejects.toThrow(
      /connection reset after mutation/,
    );
    expect(listCalls).toBe(1);

    // Model a daemon restart. Once inventory is healthy and authoritatively
    // empty, the next process must reconcile the named intent and create.
    fake.listRunServices = FakeRailway.prototype.listRunServices.bind(fake);
    fake.createRunService = FakeRailway.prototype.createRunService.bind(fake);
    fake.trackCreatedServices = true;
    const restarted = new RailwayEnvironment({ railway: fake, relay: new RecordingRelay(), config });
    live.push({ env: restarted, fake });

    await expect(restarted.teardownNode(fake.created[0]!.name)).resolves.toEqual([]);
    await expect(restarted.prepare({ spec: { kind: 'railway' } })).resolves.toBeDefined();
    expect(fake.created).toHaveLength(2);
  });

  it('run-id-only teardown reconciles an ambiguous create after restart', async () => {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    fake.createRunService = async (input) => {
      fake.created.push(input);
      throw new Error('connection reset after mutation');
    };
    const capacityLockRoot = mkdtempSync('/tmp/animus-cap-');
    const config = {
      projectId: 'proj-1',
      environmentId: 'env-1',
      clientId: 'portal-production',
      maxManagedNodes: 1,
      capacityLockRoot,
    };
    const runId = 'ambiguous-run';
    const env = new RailwayEnvironment({ railway: fake, relay: new RecordingRelay(), config });
    live.push({ env, fake });

    await expect(
      env.prepare({ spec: { kind: 'railway', metadata: { animus_run_id: runId } } }),
    ).rejects.toThrow(/connection reset after mutation/);

    // Model a restart with only the persisted run id. Successful empty
    // inventory proves the ambiguous create is absent and releases its slot.
    fake.createRunService = FakeRailway.prototype.createRunService.bind(fake);
    const restarted = new RailwayEnvironment({ railway: fake, relay: new RecordingRelay(), config });
    live.push({ env: restarted, fake });
    await expect(
      restarted.teardown({
        id: runId,
        workspace_root: '/workspace',
        metadata: { animus_run_id: runId, project_id: 'proj-1', environment_id: 'env-1' },
      }),
    ).resolves.toBeUndefined();

    await expect(
      restarted.prepare({ spec: { kind: 'railway', metadata: { animus_run_id: 'next-run' } } }),
    ).resolves.toBeDefined();
    expect(fake.created).toHaveLength(2);
  });

  it('does not clear an unrelated ambiguous-create reservation on a teardown miss', async () => {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    fake.createRunService = async (input) => {
      fake.created.push(input);
      throw new Error('connection reset after mutation');
    };
    const capacityLockRoot = mkdtempSync('/tmp/animus-cap-');
    const config = {
      projectId: 'proj-1',
      environmentId: 'env-1',
      clientId: 'portal-production',
      maxManagedNodes: 1,
      capacityLockRoot,
    };
    const env = new RailwayEnvironment({ railway: fake, relay: new RecordingRelay(), config });
    live.push({ env, fake });

    await expect(
      env.prepare({ spec: { kind: 'railway', metadata: { animus_run_id: 'ambiguous-run' } } }),
    ).rejects.toThrow(/connection reset after mutation/);
    await expect(env.teardownNode('different-run')).resolves.toEqual([]);
    await expect(
      env.prepare({ spec: { kind: 'railway', metadata: { animus_run_id: 'new-run' } } }),
    ).rejects.toThrow(/1\/1 managed nodes/);
    expect(fake.created).toHaveLength(1);
  });

  it('conservatively counts unscoped pre-cap names against a configured client', async () => {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    fake.listed = [
      { id: 'legacy-deterministic', name: `${SERVICE_NAME_PREFIX}${'a'.repeat(12)}` },
      { id: 'legacy-random', name: `${SERVICE_NAME_PREFIX}i123abc-r1234567890` },
      { id: 'legacy-unknown', name: `${SERVICE_NAME_PREFIX}older-format` },
    ];
    const relay = new RecordingRelay();
    const env = new RailwayEnvironment({
      railway: fake,
      relay,
      config: {
        projectId: 'proj-1',
        environmentId: 'env-1',
        clientId: 'portal-production',
        maxManagedNodes: 3,
      },
    });
    live.push({ env, fake });

    await expect(env.prepare({ spec: { kind: 'railway' } })).rejects.toThrow(/3\/3 managed nodes/);
    expect(fake.created).toHaveLength(0);
  });

  it('allows zero to disable prepares without calling Railway create', async () => {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    const runId = 'existing-paused-run';
    const existingName = deterministicServiceName('proj-1', 'paused-client', runId);
    fake.listed = [{ id: 'svc-existing', name: existingName }];
    const relay = new RecordingRelay();
    const env = new RailwayEnvironment({
      railway: fake,
      relay,
      config: {
        projectId: 'proj-1',
        environmentId: 'env-1',
        clientId: 'paused-client',
        maxManagedNodes: 0,
      },
    });
    live.push({ env, fake });

    await expect(
      env.prepare({ spec: { kind: 'railway', metadata: { animus_run_id: runId } } }),
    ).rejects.toThrow(/0\/0 managed nodes/);
    expect(fake.created).toHaveLength(0);
    expect(fake.deleted).toHaveLength(0);
    expect(fake.listed).toEqual([{ id: 'svc-existing', name: existingName }]);
    expect(relay.releases).toHaveLength(1);

    // Capacity is an admission-only guard: operators must still be able to
    // drain existing nodes while new prepares are intentionally disabled.
    await expect(env.teardownNode('svc-existing')).resolves.toEqual(['svc-existing']);
    expect(fake.deleted).toContainEqual({ serviceId: 'svc-existing', environmentId: 'env-1' });
  });

  it('defaults to five and counts restart-visible nodes only for the configured client', async () => {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    const clientId = 'portal-production';
    const ownPrefix = clientServicePrefix('proj-1', clientId);
    const otherPrefix = clientServicePrefix('proj-1', 'another-client');
    fake.listed = [
      ...Array.from({ length: 5 }, (_, i) => ({
        id: `other-${i}`,
        name: `${otherPrefix}${i.toString(16).padStart(10, '0')}`,
      })),
      ...Array.from({ length: 5 }, (_, i) => ({
        id: `own-${i}`,
        name: `${ownPrefix}${i.toString(16).padStart(10, '0')}`,
      })),
    ];
    const relay = new RecordingRelay();
    const env = new RailwayEnvironment({
      railway: fake,
      relay,
      config: { projectId: 'proj-1', environmentId: 'env-1', clientId },
    });
    live.push({ env, fake });

    await expect(
      env.prepare({ spec: { kind: 'railway', metadata: { animus_run_id: 'run-over-cap' } } }),
    ).rejects.toThrow(/5\/5 managed nodes/);
    expect(fake.created).toHaveLength(0);

    // Another client in the same Railway project does not consume this
    // client's slots once the five own nodes are removed from the listing.
    fake.listed = fake.listed.filter((service) => service.name.startsWith(otherPrefix));
    await expect(
      env.prepare({ spec: { kind: 'railway', metadata: { animus_run_id: 'run-own-slot' } } }),
    ).resolves.toBeDefined();
    expect(fake.created).toHaveLength(1);
  });

  it('reconciles a same-run service inside the cap instead of rejecting the retry', async () => {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    const clientId = 'portal-production';
    const runId = 'run-retry';
    fake.listed = [
      { id: 'svc-old', name: deterministicServiceName('proj-1', clientId, runId) },
    ];
    const relay = new RecordingRelay();
    const env = new RailwayEnvironment({
      railway: fake,
      relay,
      config: {
        projectId: 'proj-1',
        environmentId: 'env-1',
        clientId,
        maxManagedNodes: 1,
      },
    });
    live.push({ env, fake });

    await expect(
      env.prepare({ spec: { kind: 'railway', metadata: { animus_run_id: runId } } }),
    ).resolves.toBeDefined();
    expect(fake.deleted).toContainEqual({ serviceId: 'svc-old', environmentId: 'env-1' });
    expect(fake.created).toHaveLength(1);
  });

  it('fails closed without creating a node when capacity discovery fails', async () => {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    fake.listRunServices = async () => {
      throw new Error('Railway list unavailable');
    };
    const relay = new RecordingRelay();
    const env = new RailwayEnvironment({
      railway: fake,
      relay,
      config: { projectId: 'proj-1', environmentId: 'env-1' },
    });
    live.push({ env, fake });

    await expect(env.prepare({ spec: { kind: 'railway' } })).rejects.toThrow(/Railway list unavailable/);
    expect(fake.created).toHaveLength(0);
    expect(relay.releases).toHaveLength(1);
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

  it('teardown runs the workflow cleanup hook IN the node before deleting the service', async () => {
    const { env, fake } = await makeEnv();
    const dir = mkdtempSync(join(tmpdir(), 'cleanup-'));
    const marker = join(dir, 'ran.marker');
    const { handle } = await env.prepare({ spec: { kind: 'railway', metadata: { cleanup: `echo done > ${marker}` } } });
    // The workflow-declared cleanup is stored on the handle.
    expect((handle.metadata as Record<string, unknown>).cleanup).toBe(`echo done > ${marker}`);
    await env.teardown(handle);
    // The fake bridge executed the cleanup in the node (flushing work) BEFORE the
    // service was deleted — proving push-before-teardown is possible.
    expect(readFileSync(marker, 'utf8').trim()).toBe('done');
    expect(fake.deleted.map((d) => d.serviceId)).toContain('svc-1');
  });

  it('teardown with no cleanup hook declared deletes the service as before', async () => {
    const { env, fake } = await makeEnv();
    const { handle } = await env.prepare({ spec: { kind: 'railway' } });
    expect((handle.metadata as Record<string, unknown>).cleanup).toBeNull();
    await env.teardown(handle);
    expect(fake.deleted.length).toBeGreaterThan(0);
  });

  it('a failing cleanup hook never blocks teardown (best-effort)', async () => {
    const { env, fake } = await makeEnv();
    const { handle } = await env.prepare({ spec: { kind: 'railway', metadata: { cleanup: 'exit 3' } } });
    await expect(env.teardown(handle)).resolves.toBeUndefined();
    expect(fake.deleted.map((d) => d.serviceId)).toContain('svc-1');
  });

  it('teardown falls back to the global ANIMUS_ENV_CLEANUP default when no per-run cleanup is set', async () => {
    const fake = new FakeRailway();
    const relay = await RelayServer.listen({ host: '127.0.0.1', port: 0 });
    const dir = mkdtempSync(join(tmpdir(), 'cleanup-cfg-'));
    const marker = join(dir, 'ran.marker');
    const env = new RailwayEnvironment({
      railway: fake,
      relay,
      config: { projectId: 'proj-1', environmentId: 'env-1', dialTimeoutSecs: 10, cleanup: `echo global > ${marker}` },
    });
    live.push({ env, fake });
    const { handle } = await env.prepare({ spec: { kind: 'railway' } });
    await env.teardown(handle);
    expect(readFileSync(marker, 'utf8').trim()).toBe('global');
  });

  it('a per-run spec.metadata.cleanup overrides the global default', async () => {
    const fake = new FakeRailway();
    const relay = await RelayServer.listen({ host: '127.0.0.1', port: 0 });
    const dir = mkdtempSync(join(tmpdir(), 'cleanup-ovr-'));
    const globalMarker = join(dir, 'global.marker');
    const runMarker = join(dir, 'run.marker');
    const env = new RailwayEnvironment({
      railway: fake,
      relay,
      config: {
        projectId: 'proj-1',
        environmentId: 'env-1',
        dialTimeoutSecs: 10,
        cleanup: `echo global > ${globalMarker}`,
      },
    });
    live.push({ env, fake });
    const { handle } = await env.prepare({
      spec: { kind: 'railway', metadata: { cleanup: `echo perrun > ${runMarker}` } },
    });
    await env.teardown(handle);
    expect(readFileSync(runMarker, 'utf8').trim()).toBe('perrun');
    expect(existsSync(globalMarker)).toBe(false);
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
      { id: 'svc-live', name: `${SERVICE_NAME_PREFIX}${env.instanceId}-${handle.id.slice(0, 11)}` },
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
      { id: 'svc-live', name: `${SERVICE_NAME_PREFIX}${env.instanceId}-${handle.id.slice(0, 11)}` },
      { id: 'svc-crashed-instance', name: `${SERVICE_NAME_PREFIX}i-dead-instance-run` },
    ];
    const removed = await env.gcOrphans({ allInstances: true });
    expect(removed).toEqual(['svc-crashed-instance']);
    expect(fake.deleted.map((d) => d.serviceId)).not.toContain('svc-live');
  });

  it('gcOrphans serializes its full cleanup with a same-run retry', async () => {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    const runId = 'gc-retry';
    const serviceName = deterministicServiceName('proj-1', 'portal-production', runId);
    fake.listed = [{ id: 'svc-old', name: serviceName }];
    let releaseDelete!: () => void;
    const deleteBlocked = new Promise<void>((resolve) => { releaseDelete = resolve; });
    let deleteStarted!: () => void;
    const deleting = new Promise<void>((resolve) => { deleteStarted = resolve; });
    let firstDelete = true;
    fake.deleteService = async (serviceId, environmentId) => {
      fake.deleted.push({ serviceId, environmentId });
      if (firstDelete) {
        firstDelete = false;
        deleteStarted();
        await deleteBlocked;
      }
      fake.listed = fake.listed.filter((service) => service.id !== serviceId);
    };
    fake.createRunService = async (input) => {
      fake.created.push(input);
      throw new Error('create reached');
    };
    const capacityLockRoot = mkdtempSync('/tmp/animus-cap-');
    const config = {
      projectId: 'proj-1', environmentId: 'env-1', clientId: 'portal-production', capacityLockRoot,
    };
    const gcEnv = new RailwayEnvironment({ railway: fake, relay: new RecordingRelay(), config });
    const retryEnv = new RailwayEnvironment({ railway: fake, relay: new RecordingRelay(), config });
    live.push({ env: gcEnv, fake }, { env: retryEnv, fake });

    const gc = gcEnv.gcOrphans({ allInstances: true });
    await deleting;
    const retry = retryEnv.prepare({
      spec: { kind: 'railway', metadata: { animus_run_id: runId } },
    });
    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(fake.created).toHaveLength(0);
    releaseDelete();
    await expect(gc).resolves.toEqual(['svc-old']);
    await expect(retry).rejects.toThrow('create reached');
    expect(fake.created).toHaveLength(1);
  });

  it('list reports nodes with state + a state-based orphan flag', async () => {
    const { env, fake } = await makeEnv();
    fake.detailed = [
      { id: 'svc-ok', name: `${SERVICE_NAME_PREFIX}aaa`, status: 'SUCCESS', createdAt: null },
      { id: 'svc-dead', name: `${SERVICE_NAME_PREFIX}bbb`, status: 'FAILED', createdAt: null },
    ];
    const nodes = await env.listNodes();
    expect(nodes.map((n) => [n.id, n.state, n.orphan])).toEqual([
      ['svc-ok', 'SUCCESS', false],
      ['svc-dead', 'FAILED', true],
    ]);
  });

  it('get returns one node by id or name, else null', async () => {
    const { env, fake } = await makeEnv();
    fake.detailed = [{ id: 'svc-ok', name: `${SERVICE_NAME_PREFIX}aaa`, status: 'SUCCESS', createdAt: null }];
    expect((await env.getNode('svc-ok'))?.id).toBe('svc-ok');
    expect((await env.getNode(`${SERVICE_NAME_PREFIX}aaa`))?.id).toBe('svc-ok');
    expect(await env.getNode('nope')).toBeNull();
  });

  it('teardownNode deletes by id or name and is idempotent', async () => {
    const { env, fake } = await makeEnv();
    // Model an eventually consistent delete listing for the second lookup.
    fake.trackCreatedServices = false;
    fake.listed = [{ id: 'svc-1', name: `${SERVICE_NAME_PREFIX}aaa` }];
    expect(await env.teardownNode('svc-1')).toEqual(['svc-1']);
    expect(fake.deleted).toEqual([{ serviceId: 'svc-1', environmentId: 'env-1' }]);
    expect(await env.teardownNode('unknown-id')).toEqual([]); // no match -> no-op
    expect(await env.teardownNode(`${SERVICE_NAME_PREFIX}aaa`)).toEqual(['svc-1']); // by name too
  });

  it('reap (default) deletes ONLY dead-state nodes, sparing healthy ones', async () => {
    const { env, fake } = await makeEnv();
    fake.detailed = [
      { id: 'svc-ok', name: `${SERVICE_NAME_PREFIX}aaa`, status: 'SUCCESS', createdAt: null },
      { id: 'svc-fail', name: `${SERVICE_NAME_PREFIX}bbb`, status: 'FAILED', createdAt: null },
      { id: 'svc-crash', name: `${SERVICE_NAME_PREFIX}ccc`, status: 'CRASHED', createdAt: null },
    ];
    const res = await env.reap();
    expect(res.deleted.sort()).toEqual(['svc-crash', 'svc-fail']);
    expect(res.kept.map((n) => n.id)).toEqual(['svc-ok']);
    expect(fake.deleted.map((d) => d.serviceId).sort()).toEqual(['svc-crash', 'svc-fail']);
  });

  it('reap serializes its full cleanup with a same-run retry', async () => {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    const runId = 'reap-retry';
    const serviceName = deterministicServiceName('proj-1', 'portal-production', runId);
    fake.detailed = [{ id: 'svc-old', name: serviceName, status: 'FAILED', createdAt: null }];
    fake.listed = [{ id: 'svc-old', name: serviceName }];
    let releaseDelete!: () => void;
    const deleteBlocked = new Promise<void>((resolve) => { releaseDelete = resolve; });
    let deleteStarted!: () => void;
    const deleting = new Promise<void>((resolve) => { deleteStarted = resolve; });
    let firstDelete = true;
    fake.deleteService = async (serviceId, environmentId) => {
      fake.deleted.push({ serviceId, environmentId });
      if (firstDelete) {
        firstDelete = false;
        deleteStarted();
        await deleteBlocked;
      }
      fake.listed = fake.listed.filter((service) => service.id !== serviceId);
      fake.detailed = fake.detailed?.filter((service) => service.id !== serviceId) ?? null;
    };
    fake.createRunService = async (input) => {
      fake.created.push(input);
      throw new Error('create reached');
    };
    const capacityLockRoot = mkdtempSync('/tmp/animus-cap-');
    const config = {
      projectId: 'proj-1', environmentId: 'env-1', clientId: 'portal-production', capacityLockRoot,
    };
    const reapEnv = new RailwayEnvironment({ railway: fake, relay: new RecordingRelay(), config });
    const retryEnv = new RailwayEnvironment({ railway: fake, relay: new RecordingRelay(), config });
    live.push({ env: reapEnv, fake }, { env: retryEnv, fake });

    const reap = reapEnv.reap();
    await deleting;
    const retry = retryEnv.prepare({
      spec: { kind: 'railway', metadata: { animus_run_id: runId } },
    });
    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(fake.created).toHaveLength(0);
    releaseDelete();
    await expect(reap).resolves.toMatchObject({ deleted: ['svc-old'] });
    await expect(retry).rejects.toThrow('create reached');
    expect(fake.created).toHaveLength(1);
  });

  it('reap dry_run reports the plan without deleting anything', async () => {
    const { env, fake } = await makeEnv();
    fake.detailed = [{ id: 'svc-fail', name: `${SERVICE_NAME_PREFIX}bbb`, status: 'FAILED', createdAt: null }];
    const res = await env.reap({ dryRun: true });
    expect(res.deleted).toEqual(['svc-fail']);
    expect(res.dryRun).toBe(true);
    expect(fake.deleted).toEqual([]);
  });

  it('reap spares healthy orphans unless all+force (the empty-liveness guard)', async () => {
    const { env, fake } = await makeEnv();
    fake.detailed = [{ id: 'svc-ok', name: `${SERVICE_NAME_PREFIX}aaa`, status: 'SUCCESS', createdAt: null }];
    expect((await env.reap({ all: true })).deleted).toEqual([]); // all without force: still spared
    expect((await env.reap({ all: true, force: true })).deleted).toEqual(['svc-ok']); // now reaped
  });

  it('teardownNode resolves a bare run id to its deterministic service name', async () => {
    const { env, fake } = await makeEnv();
    const runId = 'run-teardown-1';
    const name = SERVICE_NAME_PREFIX + createHash('sha256').update(JSON.stringify(['proj-1', runId])).digest('hex').slice(0, 12);
    fake.listed = [
      { id: 'svc-run', name },
      { id: 'svc-other', name: `${SERVICE_NAME_PREFIX}unrelated` },
    ];
    // A restarted daemon that persisted only the run id can drive teardown by it.
    expect(await env.teardownNode(runId)).toEqual(['svc-run']);
    expect(fake.deleted).toEqual([{ serviceId: 'svc-run', environmentId: 'env-1' }]);
    expect(await env.teardownNode('run-not-live')).toEqual([]); // unknown run id -> no-op
  });

  it('reap owner-known keeps the live node but reaps a non-live healthy node WITHOUT force', async () => {
    const { env, fake } = await makeEnv();
    const liveRun = 'run-live';
    const deadRun = 'run-orphaned';
    const liveName = SERVICE_NAME_PREFIX + createHash('sha256').update(JSON.stringify(['proj-1', liveRun])).digest('hex').slice(0, 12);
    const orphanName = SERVICE_NAME_PREFIX + createHash('sha256').update(JSON.stringify(['proj-1', deadRun])).digest('hex').slice(0, 12);
    const old = new Date(Date.now() - 10 * 60 * 1000).toISOString(); // older than the 300s grace
    fake.detailed = [
      { id: 'svc-live', name: liveName, status: 'SUCCESS', createdAt: old },
      { id: 'svc-orphan', name: orphanName, status: 'SUCCESS', createdAt: old },
    ];
    const res = await env.reap({ liveRunIds: [liveRun] }); // no force
    expect(res.deleted).toEqual(['svc-orphan']);
    expect(res.kept.map((n) => n.id)).toEqual(['svc-live']);
    expect(fake.deleted).toEqual([{ serviceId: 'svc-orphan', environmentId: 'env-1' }]);
  });

  it('reap owner-known enforces the grace floor: a mid-prepare (young) node is spared', async () => {
    const { env, fake } = await makeEnv();
    const young = new Date(Date.now() - 5 * 1000).toISOString(); // 5s old — under the 300s grace
    fake.detailed = [
      { id: 'svc-young', name: `${SERVICE_NAME_PREFIX}young`, status: 'SUCCESS', createdAt: young },
    ];
    // Empty live set => every healthy node is unowned, but the grace floor still
    // protects a node that may be mid-`prepare` (created before it was leased).
    const res = await env.reap({ liveRunIds: [] });
    expect(res.deleted).toEqual([]);
    expect(res.kept.map((n) => n.id)).toEqual(['svc-young']);
    expect(fake.deleted).toEqual([]);
  });

  it('reap owner-known reaps the terminal-run zombie but NEVER the live run’s dead node (TASK-1466)', async () => {
    const { env, fake } = await makeEnv();
    const liveRun = 'run-live-crashed';
    const terminalRun = 'run-terminal';
    const liveName = SERVICE_NAME_PREFIX + createHash('sha256').update(JSON.stringify(['proj-1', liveRun])).digest('hex').slice(0, 12);
    const zombieName = SERVICE_NAME_PREFIX + createHash('sha256').update(JSON.stringify(['proj-1', terminalRun])).digest('hex').slice(0, 12);
    const old = new Date(Date.now() - 10 * 60 * 1000).toISOString(); // older than the 300s grace
    fake.detailed = [
      // The live run's node CRASHED: the dead-state branch must not reap it out
      // from under its owner (it may still hold unpublished work; the owner
      // drives teardown).
      { id: 'svc-live-crashed', name: liveName, status: 'CRASHED', createdAt: old },
      // The terminal run's healthy SUCCESS node is the TASK-1466 zombie: the
      // owner died with a terminal run, so default dead-state-only reap never
      // touched it. Owner-known mode must delete it WITHOUT force.
      { id: 'svc-zombie', name: zombieName, status: 'SUCCESS', createdAt: old },
    ];
    const res = await env.reap({ liveRunIds: [liveRun] });
    expect(res.deleted).toEqual(['svc-zombie']);
    expect(res.kept.map((n) => n.id)).toEqual(['svc-live-crashed']);
    expect(fake.deleted).toEqual([{ serviceId: 'svc-zombie', environmentId: 'env-1' }]);
  });

  it('reap owner-known still reaps a non-live node when the substrate cannot report state', async () => {
    const { env, fake } = await makeEnv();
    // Force the describeNodes fallback (no detailed query): nodes come back with
    // state 'unknown' and no created_at. The authoritative live set, not state,
    // decides: the non-live node is reaped, the live one is kept.
    const api: RailwayApi = fake; // the interface types the detailed query as optional
    api.listRunServicesDetailed = undefined;
    const liveRun = 'run-live';
    const terminalRun = 'run-terminal';
    const liveName = SERVICE_NAME_PREFIX + createHash('sha256').update(JSON.stringify(['proj-1', liveRun])).digest('hex').slice(0, 12);
    const zombieName = SERVICE_NAME_PREFIX + createHash('sha256').update(JSON.stringify(['proj-1', terminalRun])).digest('hex').slice(0, 12);
    fake.listed = [
      { id: 'svc-live', name: liveName },
      { id: 'svc-zombie', name: zombieName },
    ];
    const res = await env.reap({ liveRunIds: [liveRun] });
    expect(res.deleted).toEqual(['svc-zombie']);
    expect(res.kept.map((n) => [n.id, n.state])).toEqual([['svc-live', 'unknown']]);
    expect(fake.deleted).toEqual([{ serviceId: 'svc-zombie', environmentId: 'env-1' }]);
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

describe('prepare -> exec_session actor binding', () => {
  const alice = { user_id: 'alice', claims: ['admin'], tenant_id: 'acme' };
  const bob = { user_id: 'bob', claims: [], tenant_id: 'acme' };
  const executionFence: ExecutionFence = {
    schema: 'animus.execution-fence.v1',
    version: 1,
    workflow_id: 'wf-1',
    workflow_generation: 2,
    subject: { qualified_id: 'task:TASK-1', generation: 3 },
    queue_lease: {
      entry_id: 'queue-1',
      owner_id: 'daemon-1',
      generation: 4,
      expires_at: '2026-07-30T09:00:00Z',
    },
    repository: {
      repository: 'launchapp-dev/example',
      base_ref: 'refs/heads/main',
      head_ref: 'refs/heads/animus/TASK-1',
    },
  };

  async function boundEnvironment(actor?: typeof alice, specActor = actor): Promise<{
    env: RailwayEnvironment;
    fake: FakeRailway;
    relay: RecordingRelay;
    handle: Awaited<ReturnType<RailwayEnvironment['prepare']>>['handle'];
  }> {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    const relay = new RecordingRelay();
    const env = new RailwayEnvironment({
      railway: fake,
      relay,
      config: {
        projectId: 'proj-1',
        environmentId: 'env-1',
        dialTimeoutSecs: 1,
        actorBindingSecret: 'test-binding-secret',
      },
    });
    live.push({ env, fake });
    const request = {
      spec: {
        kind: 'railway',
        env: { [ACTOR_ENV]: '{"user_id":"mallory"}' },
      },
      ...(specActor === undefined ? {} : { actor: specActor }),
    };
    const { handle } = await env.prepare(
      request,
      actor,
      specActor,
      Object.prototype.hasOwnProperty.call(request, 'actor'),
    );
    return { env, fake, relay, handle };
  }

  it('injects only the context actor and accepts that same actor for exec_session', async () => {
    const { env, fake, relay, handle } = await boundEnvironment(alice);
    expect(handle.id).toMatch(/^r[0-9a-f]{32}$/);
    expect(JSON.parse(fake.created[0]?.variables[ACTOR_ENV] ?? '{}')).toEqual(alice);
    expect(handle.metadata).toMatchObject({
      animus_actor_scope: 'actor',
      animus_actor_fingerprint: expect.stringMatching(/^sha256:[0-9a-f]{64}$/),
    });

    await expect(
      env.runSession(
        handle,
        { subject_id: 'task:TASK-1', workflow_id: 'wf-1' },
        undefined,
        alice,
        alice,
        true,
      ),
    ).resolves.toEqual({ workflow_id: 'wf-1', status: 'completed' });
    expect(relay.sessions).toHaveLength(1);
  });

  it('forwards and verifies the exact execution fence before accepting terminal state', async () => {
    const { env, relay, handle } = await boundEnvironment(alice);
    await expect(
      env.runSession(
        handle,
        { subject_id: 'task:TASK-1', workflow_id: 'wf-1', execution_fence: executionFence },
        undefined,
        alice,
        alice,
        true,
      ),
    ).resolves.toEqual({ workflow_id: 'wf-1', status: 'completed', execution_fence: executionFence });
    expect(relay.sessions[0]?.params.execution_fence).toEqual(executionFence);

    await expect(
      env.runSession(
        handle,
        { subject_id: 'task:TASK-1', workflow_id: 'wf-stale', execution_fence: executionFence },
        undefined,
        alice,
        alice,
        true,
      ),
    ).rejects.toThrow(/workflow_id does not match/);
    await expect(
      env.runSession(
        handle,
        { subject_id: 'task:TASK-2', workflow_id: 'wf-1', execution_fence: executionFence },
        undefined,
        alice,
        alice,
        true,
      ),
    ).rejects.toThrow(/subject_id does not match/);

    relay.runSession = async () => ({
      workflow_id: 'wf-1',
      status: 'completed',
      execution_fence: { ...executionFence, workflow_generation: executionFence.workflow_generation + 1 },
    });
    await expect(
      env.runSession(
        handle,
        { subject_id: 'task:TASK-1', workflow_id: 'wf-1', execution_fence: executionFence },
        undefined,
        alice,
        alice,
        true,
      ),
    ).rejects.toThrow(/did not echo the exact execution_fence/);
  });

  it('rejects omitted, mismatched, and malformed actors for an actor-bound handle', async () => {
    const { env, handle } = await boundEnvironment(alice);
    await expect(
      env.runSession(handle, { subject_id: 'task:TASK-1' }, undefined, undefined, undefined, false),
    ).rejects.toThrow(/does not match/);
    await expect(
      env.runSession(handle, { subject_id: 'task:TASK-1' }, undefined, bob, bob, true),
    ).rejects.toThrow(/does not match/);
    await expect(
      env.runSession(handle, { subject_id: 'task:TASK-1' }, undefined, undefined, { user_id: 42 }, true),
    ).rejects.toThrow(/present but malformed/);

    const malformedHandle = {
      ...handle,
      metadata: { ...(handle.metadata as Record<string, unknown>), animus_actor_fingerprint: 'not-a-hash' },
    };
    await expect(
      env.runSession(malformedHandle, { subject_id: 'task:TASK-1' }, undefined, alice, alice, true),
    ).rejects.toThrow(/invalid actor binding signature/);

    const legacyHandle = { ...handle, metadata: { service_id: 'legacy' } };
    await expect(
      env.runSession(legacyHandle, { subject_id: 'task:TASK-1' }, undefined, undefined, undefined, false),
    ).rejects.toThrow(/malformed actor binding metadata/);
  });

  it('does not let forged handle metadata overwrite a live actor binding', async () => {
    const { env, handle, relay } = await boundEnvironment(alice);
    const forged = {
      ...handle,
      metadata: {
        ...(handle.metadata as Record<string, unknown>),
        animus_actor_fingerprint: actorBinding(bob).fingerprint,
      },
    };
    await expect(
      env.runSession(forged, { subject_id: 'task:TASK-1' }, undefined, bob, bob, true),
    ).rejects.toThrow(/invalid actor binding signature|does not match live authority/);
    await expect(
      env.runSession(handle, { subject_id: 'task:TASK-1' }, undefined, alice, alice, true),
    ).resolves.toMatchObject({ status: 'completed' });
    expect(relay.sessions).toHaveLength(1);
  });

  it('rejects forged workflow and repository scope metadata', async () => {
    const { env, handle } = await boundEnvironment(alice);
    for (const metadata of [
      { ...(handle.metadata as Record<string, unknown>), animus_run_id: 'wf-attacker' },
      {
        ...(handle.metadata as Record<string, unknown>),
        animus_github_owner: 'attacker',
        animus_github_repo: 'other',
      },
    ]) {
      await expect(
        env.runSession(
          { ...handle, metadata },
          { subject_id: 'task:TASK-1' },
          undefined,
          alice,
          alice,
          true,
        ),
      ).rejects.toThrow(/invalid actor binding signature|does not match live authority/);
    }
  });

  it('rehydrates only metadata signed by the same stable server secret', async () => {
    const { handle, relay } = await boundEnvironment(alice);
    const fake = new FakeRailway();
    fake.bootBridge = false;
    const restarted = new RailwayEnvironment({
      railway: fake,
      relay,
      config: {
        projectId: 'proj-1',
        environmentId: 'env-1',
        actorBindingSecret: 'test-binding-secret',
      },
    });
    live.push({ env: restarted, fake });
    await expect(
      restarted.runSession(handle, { subject_id: 'task:TASK-1' }, undefined, alice, alice, true),
    ).resolves.toMatchObject({ status: 'completed' });

    const otherFake = new FakeRailway();
    otherFake.bootBridge = false;
    const wrongSecret = new RailwayEnvironment({
      railway: otherFake,
      relay: new RecordingRelay(),
      config: {
        projectId: 'proj-1',
        environmentId: 'env-1',
        actorBindingSecret: 'different-secret',
      },
    });
    live.push({ env: wrongSecret, fake: otherFake });
    await expect(
      wrongSecret.runSession(handle, { subject_id: 'task:TASK-1' }, undefined, alice, alice, true),
    ).rejects.toThrow(/invalid actor binding signature/);
  });

  it('reattaches a persisted handle to a fresh relay with sealed exact credentials', async () => {
    const { handle, relay: original } = await boundEnvironment(alice);
    const metadata = handle.metadata as Record<string, unknown>;
    expect(metadata.animus_relay_binding).toEqual(expect.stringMatching(/^v1\./));
    const registered = original.registrations[0];
    expect(registered).toBeDefined();
    expect(String(metadata.animus_relay_binding)).not.toContain(registered!.token);
    expect(String(metadata.animus_relay_binding)).not.toContain(registered!.ownerToken);

    const fake = new FakeRailway();
    fake.bootBridge = false;
    // The restarted process faces the SAME durable registry (singleton survives).
    const relay = new RecordingRelay(original.store);
    const restarted = new RailwayEnvironment({
      railway: fake,
      relay,
      config: {
        projectId: 'proj-1',
        environmentId: 'env-1',
        actorBindingSecret: 'test-binding-secret',
      },
    });
    live.push({ env: restarted, fake });

    await expect(
      restarted.runSession(
        handle,
        { subject_id: 'task:TASK-1', workflow_id: 'wf-restarted' },
        undefined,
        alice,
        alice,
        true,
      ),
    ).resolves.toEqual({ workflow_id: 'wf-restarted', status: 'completed' });
    expect(relay.attachments).toEqual([{
      handleId: handle.id,
      token: registered!.token,
      ownerToken: registered!.ownerToken,
    }]);
    expect(relay.sessions).toHaveLength(1);
  });

  it('fails closed when a persisted relay binding is tampered', async () => {
    const { handle } = await boundEnvironment(alice);
    const metadata = handle.metadata as Record<string, unknown>;
    const forged = {
      ...handle,
      metadata: {
        ...metadata,
        animus_relay_binding: `${String(metadata.animus_relay_binding).slice(0, -1)}x`,
      },
    };
    const fake = new FakeRailway();
    fake.bootBridge = false;
    const relay = new RecordingRelay();
    const restarted = new RailwayEnvironment({
      railway: fake,
      relay,
      config: {
        projectId: 'proj-1',
        environmentId: 'env-1',
        actorBindingSecret: 'test-binding-secret',
      },
    });
    live.push({ env: restarted, fake });
    await expect(
      restarted.runSession(forged, { subject_id: 'task:TASK-1' }, undefined, alice, alice, true),
    ).rejects.toThrow(/invalid actor binding signature|could not be authenticated/);
    expect(relay.attachments).toHaveLength(0);
  });

  it('tears down from a restarted process using the sealed relay credentials', async () => {
    const { handle, relay: original } = await boundEnvironment(alice);
    const fake = new FakeRailway();
    fake.bootBridge = false;
    const relay = new RecordingRelay(original.store);
    const restarted = new RailwayEnvironment({
      railway: fake,
      relay,
      config: {
        projectId: 'proj-1',
        environmentId: 'env-1',
        actorBindingSecret: 'test-binding-secret',
      },
    });
    live.push({ env: restarted, fake });

    await restarted.teardown(handle);
    // The restarted process's first relay use runs the startup reattach, so the
    // handle is attached once (with its exact sealed credentials) before the
    // teardown releases it.
    expect(relay.attachments).toEqual([{
      handleId: handle.id,
      token: original.registrations[0]!.token,
      ownerToken: original.registrations[0]!.ownerToken,
    }]);
    expect(relay.releases).toEqual([{
      handleId: handle.id,
      token: original.registrations[0]!.token,
      ownerToken: original.registrations[0]!.ownerToken,
    }]);
    expect(fake.deleted).toEqual([{ serviceId: 'svc-1', environmentId: 'env-1' }]);
  });

  it('stores a sealed restart payload with the relay registration at prepare', async () => {
    const { handle, relay } = await boundEnvironment(alice);
    const stored = relay.store.get(handle.id);
    expect(stored).toBeDefined();
    expect(stored!.binding).toEqual(expect.stringMatching(/^v1\./));
    // The blob is AEAD-sealed: the plaintext run credentials never appear in it.
    expect(String(stored!.binding)).not.toContain(stored!.token);
    expect(String(stored!.binding)).not.toContain(stored!.ownerToken);
  });

  it('reattaches every still-registered run at startup and rehydrates reverse-RPC authority', async () => {
    // The TASK-1420 orphan shape: the plugin process dies mid-session while the
    // singleton keeps the registration + node leg. The restarted process must
    // reattach the handle at boot — without waiting for a fresh exec_session.
    const { env: first, handle, relay: firstRelay } = await boundEnvironment(alice);
    await first.close();

    const fake = new FakeRailway();
    fake.bootBridge = false;
    const relay = new RecordingRelay(firstRelay.store);
    const restarted = new RailwayEnvironment({
      railway: fake,
      relay,
      config: {
        projectId: 'proj-1',
        environmentId: 'env-1',
        actorBindingSecret: 'test-binding-secret',
      },
    });
    live.push({ env: restarted, fake });

    await restarted.restoreRelayRuns();
    expect(relay.registeredRuns()).toEqual([handle.id]);
    expect(relay.attachments).toEqual([{
      handleId: handle.id,
      token: firstRelay.registrations[0]!.token,
      ownerToken: firstRelay.registrations[0]!.ownerToken,
    }]);

    // The restored handle runs a same-actor session with NO further attach...
    await expect(
      restarted.runSession(
        handle,
        { subject_id: 'task:TASK-1', workflow_id: 'wf-restored' },
        undefined,
        alice,
        alice,
        true,
      ),
    ).resolves.toEqual({ workflow_id: 'wf-restored', status: 'completed' });
    expect(relay.attachments).toHaveLength(1);

    // ...and the rehydrated actor binding still rejects a different actor.
    await expect(
      restarted.runSession(handle, { subject_id: 'task:TASK-1' }, undefined, bob, bob, true),
    ).rejects.toThrow(/actor does not match/);
  });

  it('skips registrations sealed under a different binding secret at startup', async () => {
    const { handle, relay: firstRelay } = await boundEnvironment(alice);
    const fake = new FakeRailway();
    fake.bootBridge = false;
    const relay = new RecordingRelay(firstRelay.store);
    const restarted = new RailwayEnvironment({
      railway: fake,
      relay,
      config: {
        projectId: 'proj-1',
        environmentId: 'env-1',
        actorBindingSecret: 'different-secret',
      },
    });
    live.push({ env: restarted, fake });

    await restarted.restoreRelayRuns();
    expect(relay.attachments).toHaveLength(0);
    expect(relay.registeredRuns()).toEqual([]);
    // The durable registration itself is untouched — the rightful owner can
    // still reattach it later.
    expect(relay.store.has(handle.id)).toBe(true);
  });

  it('does not resurrect a released handle at startup', async () => {
    const { env: first, handle, relay: firstRelay } = await boundEnvironment(alice);
    await first.teardown(handle);
    expect(firstRelay.store.has(handle.id)).toBe(false);

    const fake = new FakeRailway();
    fake.bootBridge = false;
    const relay = new RecordingRelay(firstRelay.store);
    const restarted = new RailwayEnvironment({
      railway: fake,
      relay,
      config: {
        projectId: 'proj-1',
        environmentId: 'env-1',
        actorBindingSecret: 'test-binding-secret',
      },
    });
    live.push({ env: restarted, fake });

    await restarted.restoreRelayRuns();
    expect(relay.attachments).toHaveLength(0);
    expect(relay.registeredRuns()).toEqual([]);
  });

  it('treats a relay without run listing as no startup reattach (legacy transport)', async () => {
    const { handle, relay: firstRelay } = await boundEnvironment(alice);
    const fake = new FakeRailway();
    fake.bootBridge = false;
    const relay = new RecordingRelay(firstRelay.store);
    (relay as { listRuns?: unknown }).listRuns = undefined;
    const restarted = new RailwayEnvironment({
      railway: fake,
      relay,
      config: {
        projectId: 'proj-1',
        environmentId: 'env-1',
        actorBindingSecret: 'test-binding-secret',
      },
    });
    live.push({ env: restarted, fake });

    await expect(restarted.restoreRelayRuns()).resolves.toBeUndefined();
    expect(relay.attachments).toHaveLength(0);
    expect(relay.store.has(handle.id)).toBe(true);
  });

  it('serves a live node\'s reverse RPC after the plugin process restarts (TASK-1420 end-to-end)', async () => {
    // The production defect, full stack: a real singleton relay, a real
    // BridgeClient "node", and the plugin's real reverse-RPC wiring (actor-bound
    // handler + scoped authorizer + parent backend plugin). The plugin process
    // "dies" mid-session; the restarted process must reattach the orphaned
    // handle AT STARTUP so the same node's backend/call answers again.
    const root = mkdtempSync(join(tmpdir(), 'env-railway-restore-'));
    const socketPath = join(root, 'relay.sock');
    const backendBin = join(root, 'fake-backend.mjs');
    writeFileSync(
      backendBin,
      `#!/usr/bin/env node
let buf = '';
process.stdin.on('data', (d) => {
  buf += d;
  let i;
  while ((i = buf.indexOf('\\n')) >= 0) {
    const line = buf.slice(0, i);
    buf = buf.slice(i + 1);
    if (!line.trim()) continue;
    const msg = JSON.parse(line);
    if (msg.id !== undefined) {
      process.stdout.write(JSON.stringify({ id: msg.id, result: { ok: true, method: msg.method } }) + '\\n');
    }
  }
});
`,
    );
    chmodSync(backendBin, 0o755);

    const singleton = await RelaySingleton.listen({
      host: '127.0.0.1',
      port: 0,
      socketPath,
      registryPath: join(root, 'relay-registry.json'),
    });
    const config = {
      projectId: 'proj-1',
      environmentId: 'env-1',
      dialTimeoutSecs: 10,
      actorBindingSecret: 'test-binding-secret',
      relaySocketPath: socketPath,
      upstreamBackendBin: backendBin,
      databaseUrl: 'postgres://fake',
    };
    const fake = new FakeRailway();
    const first = new RailwayEnvironment({ railway: fake, config });
    live.push({ env: first, fake });
    try {
      const { handle } = await first.prepare(
        { spec: { kind: 'railway' } },
        alice,
        alice,
        true,
      );
      const bridge = fake.bridges[0];
      expect(bridge).toBeDefined();
      await expect(
        bridge!.backendCall('workflow_journal', 'journal/schema', {}),
      ).resolves.toEqual({ ok: true, method: 'journal/schema' });

      // Plugin process dies mid-session (daemon redeploy / runtime activation).
      // Close is detach-only: the singleton keeps the registration + node leg.
      await first.close();

      const restartedFake = new FakeRailway();
      const restarted = new RailwayEnvironment({ railway: restartedFake, config });
      live.push({ env: restarted, fake: restartedFake });
      await restarted.restoreRelayRuns();
      // Startup reattach never prepares a second node.
      expect(restartedFake.created).toHaveLength(0);

      // The SAME bridge process's reverse RPC is answered by the NEW plugin
      // process (its rehydrated authority + freshly spawned backend).
      await expect(
        bridge!.backendCall('workflow_journal', 'journal/schema', {}),
      ).resolves.toEqual({ ok: true, method: 'journal/schema' });

      // Teardown from the restarted process still releases the exact handle.
      await restarted.teardown(handle);
      expect(singleton.registeredRuns()).toEqual([]);
    } finally {
      await singleton.close().catch(() => undefined);
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  it('rejects malformed prepare actors before creating a service', async () => {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    const env = new RailwayEnvironment({
      railway: fake,
      relay: new RecordingRelay(),
      config: { projectId: 'proj-1', environmentId: 'env-1' },
    });
    live.push({ env, fake });
    await expect(
      env.prepare(
        { spec: { kind: 'railway' }, actor: { user_id: 42 } } as never,
        undefined,
        { user_id: 42 },
        true,
      ),
    ).rejects.toThrow(/present but malformed/);
    await expect(
      env.prepare({ spec: { kind: 'railway' }, actor: null } as never, undefined, null, true),
    ).rejects.toThrow(/present but malformed/);
    expect(fake.created).toHaveLength(0);
  });

  it('preserves explicit system scope while stripping a spec actor spoof', async () => {
    const { env, fake, handle } = await boundEnvironment(undefined);
    expect(fake.created[0]?.variables[ACTOR_ENV]).toBeUndefined();
    expect(handle.metadata).toMatchObject({
      animus_actor_scope: 'system',
      animus_actor_fingerprint: null,
    });
    await expect(
      env.runSession(handle, { subject_id: 'task:TASK-2' }, undefined, undefined, undefined, false),
    ).resolves.toMatchObject({ status: 'completed' });
  });

  it('preserves valid and malformed actor presence through actual SDK dispatch', async () => {
    const fake = new FakeRailway();
    fake.bootBridge = false;
    const relay = new RecordingRelay();
    const env = new RailwayEnvironment({
      railway: fake,
      relay,
      config: {
        projectId: 'proj-1',
        environmentId: 'env-1',
        actorBindingSecret: 'dispatch-test-secret',
      },
    });
    live.push({ env, fake });
    const impl = {
      prepare: (params: Parameters<EnvironmentPluginSpec['prepare']>[0], ctx: Parameters<EnvironmentPluginSpec['prepare']>[1]) =>
        env.prepare(
          params,
          ctx.actor,
          (params as Record<string, unknown>).actor,
          Object.prototype.hasOwnProperty.call(params, 'actor'),
        ),
      exec: () => ({ exit_code: 0, stdout: '', stderr: '', timed_out: false }),
      execSession: async (
        params: Parameters<NonNullable<EnvironmentPluginSpec['execSession']>>[0],
        _emit: Parameters<NonNullable<EnvironmentPluginSpec['execSession']>>[1],
        ctx: Parameters<NonNullable<EnvironmentPluginSpec['execSession']>>[2],
      ) => {
        const result = await env.runSession(
          params.handle,
          {
            subject_id: params.subject_id,
            workflow_ref: params.workflow_ref,
            dispatch_input: params.dispatch_input,
            workflow_id: params.workflow_id,
          },
          undefined,
          ctx.actor,
          (params as Record<string, unknown>).actor,
          Object.prototype.hasOwnProperty.call(params, 'actor'),
        );
        return { workflow_id: result.workflow_id, status: result.status };
      },
      teardown: () => ({}),
    };

    const prepareOut = await drivePlugin(impl, [
      { jsonrpc: '2.0', id: 1, method: 'environment/prepare', params: { spec: { kind: 'railway' }, actor: alice } },
      {
        jsonrpc: '2.0',
        id: 2,
        method: 'environment/prepare',
        params: { spec: { kind: 'railway' }, actor: { user_id: 42 } },
      },
    ]);
    const prepared = prepareOut.find((frame) => frame.id === 1)?.result as { handle: { id: string } };
    expect(prepared.handle.id).toMatch(/^r[0-9a-f]{32}$/);
    expect(prepareOut.find((frame) => frame.id === 2)?.error?.message).toMatch(/present but malformed/);

    const sessionOut = await drivePlugin(impl, [
      {
        jsonrpc: '2.0',
        id: 3,
        method: 'environment/exec_session',
        params: { handle: prepared.handle, subject_id: 'task:TASK-1', workflow_id: 'wf-dispatch', actor: alice },
      },
      {
        jsonrpc: '2.0',
        id: 4,
        method: 'environment/exec_session',
        params: { handle: prepared.handle, subject_id: 'task:TASK-1', actor: { user_id: 42 } },
      },
    ]);
    expect(sessionOut.find((frame) => frame.id === 3)?.result).toEqual({
      workflow_id: 'wf-dispatch',
      status: 'completed',
    });
    expect(sessionOut.find((frame) => frame.id === 4)?.error?.message).toMatch(/present but malformed/);
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
