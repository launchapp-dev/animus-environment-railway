// Railway substrate: prepare/exec/exec_stream/teardown mapped onto the Railway
// GraphQL API + the animus-env-transport WS relay.
//
// Railway has no inbound exec/attach, so the SHAPE mirrors the docker plugin
// but the exec primitive is swapped: instead of `docker exec`, the prepared
// container dials an outbound WebSocket home to a RelayServer hosted INSIDE
// this plugin process, and every HarnessCommand rides that socket.
//
// Shell-safety: nothing from `EnvironmentSpec` / `HarnessCommand` is ever
// concatenated into a shell string. Commands cross the relay as argv arrays
// and the in-container bridge spawns them with `shell: false`. The provision
// (git clone / animus install) commands assembled here are argv arrays too.

import { createHash, createSign, randomBytes } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';

import {
  planWorkspace,
  provisionAnimus,
  type EnvironmentHandle,
  type ExecResponse,
  type HarnessCommand,
  type HealthReport,
  type PrepareRequest,
  type WorkspacePlan,
} from '@launchapp-dev/animus-environment-base';
import { RelayServer, type RelayServerOptions } from '@launchapp-dev/animus-env-transport';

import { RailwayClient, SERVICE_NAME_PREFIX, type RailwayApi } from './railway.js';

// Short, Railway-valid id token: `<prefix><6 hex>`. The service name is
// `animus-run-<instanceId>-<id>`, and Railway rejects long service names
// (~32-char limit), so both the per-process instance id and the per-run id
// must stay compact. 6 hex (2^24) is ample uniqueness for a project's
// concurrent runs, and the GC sweep still matches by the shared prefix.
function shortId(prefix: string): string {
  return `${prefix}${randomBytes(3).toString('hex')}`;
}

/** The workflow run id a broker passes on `spec.metadata` so this plugin can
 *  name the node DETERMINISTICALLY (same run -> same service name across plugin
 *  processes), which makes a node reconcilable + cold-reapable by run id even
 *  when a caller never received the returned handle. Absent => legacy per-run
 *  random naming. Accepts either `animus_run_id` or `run_id`. */
function specRunId(spec: { metadata?: unknown }): string | null {
  const meta = (spec.metadata ?? {}) as Record<string, unknown>;
  const raw = meta.animus_run_id ?? meta.run_id;
  const runId = typeof raw === 'string' ? raw.trim() : '';
  return runId.length > 0 ? runId : null;
}

/** Deterministic, Railway-valid service name for a run: `animus-run-<12 hex>`
 *  (23 chars, under Railway's ~32-char limit). Scoped by project id so the same
 *  run id in different projects never collides. No per-process `instanceId` —
 *  determinism across processes is the whole point (reconcile + cold teardown). */
function deterministicServiceName(projectId: string, runId: string): string {
  const digest = createHash('sha256').update(JSON.stringify([projectId, runId])).digest('hex').slice(0, 12);
  return `${SERVICE_NAME_PREFIX}${digest}`;
}

/** Default base image when `EnvironmentSpec.image` is unset. */
export const DEFAULT_IMAGE = process.env.ANIMUS_ENV_RAILWAY_IMAGE ?? 'ghcr.io/launchapp-dev/animus:v0.7.0-rc.2';

/** Absolute workspace root inside every prepared container. */
export const WORKSPACE_ROOT = '/workspace';

/** Default in-container start command: the WS bridge entrypoint. The base
 *  image does not ship it yet — see INTEGRATION.md. */
export const DEFAULT_BRIDGE_COMMAND = 'animus-env-bridge';

/** Metadata this plugin writes into `EnvironmentHandle.metadata`. */
export interface RailwayHandleMeta {
  service_id: string;
  service_name: string;
  project_id: string;
  environment_id: string;
  deployment_id?: string | null;
  image: string;
  /** Primary repo subdir under the workspace root (multi-repo default cwd). */
  primary_subdir?: string | null;
  /** The broker run id this node was named from, if any. Lets `teardown`
   *  cold-delete by deterministic name when a caller only has the run id (e.g.
   *  the daemon persisted the run id but crashed before recording service_id). */
  animus_run_id?: string | null;
}

export interface RailwayEnvironmentConfig {
  /** Railway project the run services are created in. */
  projectId?: string;
  /** Railway environment within the project. */
  environmentId?: string;
  /** Public WSS URL containers dial (TLS terminated at the Railway edge).
   *  Required for real Railway runs; local tests dial the bound port. */
  relayPublicUrl?: string;
  /** Port the relay binds (default 0 = ephemeral; set a fixed port when the
   *  daemon's service exposes it publicly). */
  relayPort?: number;
  /** Host interface the relay binds (default 0.0.0.0). */
  relayHost?: string;
  /** In-container start command (default `animus-env-bridge`). */
  bridgeCommand?: string;
  /** Bound wait for the container to dial home (default 300s — a Railway
   *  image pull + deploy is not fast). */
  dialTimeoutSecs?: number;
  /** Extra TLS material for an in-process WSS listener. */
  tls?: RelayServerOptions['tls'];
}

/** Read the config from the process env (the plugin's runtime posture). */
export function configFromEnv(env: NodeJS.ProcessEnv = process.env): RailwayEnvironmentConfig {
  return {
    projectId: env.RAILWAY_PROJECT_ID,
    environmentId: env.RAILWAY_ENVIRONMENT_ID,
    relayPublicUrl: env.ANIMUS_ENV_RELAY_PUBLIC_URL,
    relayPort: env.ANIMUS_ENV_RELAY_PORT ? Number(env.ANIMUS_ENV_RELAY_PORT) : undefined,
    bridgeCommand: env.ANIMUS_ENV_BRIDGE_COMMAND,
    dialTimeoutSecs: env.ANIMUS_ENV_DIAL_TIMEOUT_SECS ? Number(env.ANIMUS_ENV_DIAL_TIMEOUT_SECS) : undefined,
  };
}

/** Resolve the project/environment ids for a prepare: explicit spec.metadata
 *  wins, then the plugin config (env). */
export function resolveTarget(
  spec: PrepareRequest['spec'],
  config: RailwayEnvironmentConfig,
): { projectId: string; environmentId: string } {
  const meta = (spec.metadata ?? {}) as Record<string, unknown>;
  const projectId = (typeof meta.railway_project_id === 'string' && meta.railway_project_id) || config.projectId;
  const environmentId =
    (typeof meta.railway_environment_id === 'string' && meta.railway_environment_id) || config.environmentId;
  if (!projectId) {
    throw new Error(
      'no Railway project id: set RAILWAY_PROJECT_ID or pass spec.metadata.railway_project_id',
    );
  }
  if (!environmentId) {
    throw new Error(
      'no Railway environment id: set RAILWAY_ENVIRONMENT_ID or pass spec.metadata.railway_environment_id',
    );
  }
  return { projectId, environmentId };
}

/** Build the argv-array `git clone` (+ optional checkout) commands that
 *  materialize the planned repos inside the container. Local-path repo urls
 *  are skipped — a remote container cannot see the daemon host's filesystem. */
const COMMIT_SHA_RE = /^[0-9a-f]{7,40}$/i;

/** A repo checkout subdir must stay INSIDE the workspace root: a single plain
 *  path segment, no separators, no `.`/`..` traversal. */
export function assertSafeSubdir(subdir: string): string {
  const s = subdir.trim();
  if (!s || s === '.' || s === '..' || /[/\\]/.test(s)) {
    throw new Error(`repo subdir '${subdir}' must be a single plain path segment inside the workspace root`);
  }
  return s;
}

export function cloneCommands(plan: WorkspacePlan): HarnessCommand[] {
  const commands: HarnessCommand[] = [];
  for (const planned of plan.repos) {
    const url = planned.repo.url.trim();
    if (url.startsWith('/')) continue; // local path: meaningless remotely
    const dest = plan.single ? plan.workspaceRoot : `${plan.workspaceRoot}/${assertSafeSubdir(planned.subdir)}`;
    const ref = planned.repo.git_ref?.trim();
    if (ref && COMMIT_SHA_RE.test(ref)) {
      // `--branch` only accepts branch/tag names; a pinned commit needs a full
      // clone followed by a detached checkout.
      commands.push({ program: 'git', args: ['clone', '--', url, dest], cwd: '/' });
      commands.push({ program: 'git', args: ['-C', dest, 'checkout', '--detach', ref], cwd: '/' });
      continue;
    }
    const cloneArgs = ['clone', '--depth', '1'];
    if (ref) cloneArgs.push('--branch', ref);
    cloneArgs.push('--', url, dest);
    commands.push({ program: 'git', args: cloneArgs, cwd: '/' });
  }
  return commands;
}

const CLAUDE_OAUTH_TOKEN_URL =
  process.env.CLAUDE_OAUTH_TOKEN_URL ?? 'https://console.anthropic.com/v1/oauth/token';
const CLAUDE_OAUTH_CLIENT_ID = process.env.CLAUDE_OAUTH_CLIENT_ID ?? '9d1c250a-e61b-44d9-88ed-5944d1962f5e';
/** Refresh the daemon `.credentials.json` this many ms before its access token
 *  actually expires, so an in-flight node run never straddles the boundary. */
const CLAUDE_REFRESH_SKEW_MS = 5 * 60 * 1000;

/** Read the daemon-side Codex auth.json + GitHub token and encode them for the
 *  node bootstrap. Claude is handled separately (async central refresh). */
export function harnessCredentialVars(hostEnv: NodeJS.ProcessEnv): Record<string, string> {
  const vars: Record<string, string> = {};
  if (hostEnv.CODEX_OAUTH_HOME) {
    try {
      vars.ANIMUS_NODE_CODEX_AUTH_B64 = readFileSync(
        `${hostEnv.CODEX_OAUTH_HOME.replace(/\/$/, '')}/auth.json`,
      ).toString('base64');
    } catch {
      // no codex login on the daemon; skip
    }
  }
  if (hostEnv.GITHUB_TOKEN) vars.GITHUB_TOKEN = hostEnv.GITHUB_TOKEN;
  return vars;
}

/** Central Claude-subscription refresher. A node must NEVER hold the refresh
 *  token: the claude CLI rotates it single-use, and since a node's rotation is
 *  lost (never written back to the daemon), a refreshing node would corrupt the
 *  shared daemon credential after one run. So the DAEMON is the sole refresher —
 *  it refreshes `.credentials.json` in place when the access token is near expiry
 *  (writing the rotated token back), then injects only a short-lived access token
 *  with the refresh token STRIPPED. The node uses that access token directly and
 *  cannot rotate anything. Best-effort: returns {} when there is no login or the
 *  refresh fails (the node then just has no claude auth). */
export async function claudeNodeCredentials(
  hostEnv: NodeJS.ProcessEnv,
  now: number,
  fetchImpl: typeof fetch = fetch,
): Promise<Record<string, string>> {
  const dir = hostEnv.CLAUDE_CONFIG_DIR;
  if (!dir) return {};
  const path = `${dir.replace(/\/$/, '')}/.credentials.json`;
  let file: Record<string, unknown>;
  let oauth: Record<string, unknown>;
  try {
    file = JSON.parse(readFileSync(path, 'utf8')) as Record<string, unknown>;
    oauth = (file.claudeAiOauth as Record<string, unknown>) ?? file;
  } catch {
    return {};
  }
  const expiresAt = Number(oauth.expiresAt ?? oauth.expires_at ?? 0);
  const refreshToken = (oauth.refreshToken ?? oauth.refresh_token) as string | undefined;
  if (expiresAt && expiresAt - now > CLAUDE_REFRESH_SKEW_MS) {
    // Access token still valid: inject it as-is, minus the refresh token.
    return { ANIMUS_NODE_CLAUDE_CREDENTIALS_B64: encodeNodeClaudeCreds(file, oauth) };
  }
  if (!refreshToken) return {};
  try {
    const res = await fetchImpl(CLAUDE_OAUTH_TOKEN_URL, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ grant_type: 'refresh_token', refresh_token: refreshToken, client_id: CLAUDE_OAUTH_CLIENT_ID }),
    });
    if (!res.ok) {
      process.stderr.write(
        `[animus-environment-railway] claude token refresh failed (HTTP ${res.status}); node will lack claude auth\n`,
      );
      return {};
    }
    const t = (await res.json()) as { access_token: string; refresh_token?: string; expires_in?: number };
    const newExpiry = now + Number(t.expires_in ?? 28800) * 1000;
    const refreshed = { ...oauth, accessToken: t.access_token, refreshToken: t.refresh_token ?? refreshToken, expiresAt: newExpiry };
    // Write the rotated token back to the daemon so it (and the next run) stay valid.
    try {
      writeFileSync(path, JSON.stringify({ ...file, claudeAiOauth: refreshed }));
    } catch {
      // /data read-only in some environments; the injected token is still fresh
    }
    return { ANIMUS_NODE_CLAUDE_CREDENTIALS_B64: encodeNodeClaudeCreds(file, refreshed) };
  } catch (err) {
    process.stderr.write(`[animus-environment-railway] claude token refresh error: ${String(err)}\n`);
    return {};
  }
}

/** Build the base64 `.credentials.json` injected into the node: the fresh oauth
 *  block with the refresh token REMOVED (the node must not be able to rotate). */
function encodeNodeClaudeCreds(file: Record<string, unknown>, oauth: Record<string, unknown>): string {
  const { refreshToken: _r, refresh_token: _r2, ...noRefresh } = oauth;
  return Buffer.from(JSON.stringify({ ...file, claudeAiOauth: noRefresh })).toString('base64');
}

/** The per-run variables injected into the created service's environment. */
export function runVariables(args: {
  wssUrl: string;
  token: string;
  specEnv: Record<string, string> | undefined;
  hostEnv?: NodeJS.ProcessEnv;
}): Record<string, string> {
  const hostEnv = args.hostEnv ?? process.env;
  const vars: Record<string, string> = {};
  // BASE_DB_URL passthrough: cloud deployments hand every run container the
  // shared database endpoint the daemon itself uses.
  if (hostEnv.BASE_DB_URL) vars.BASE_DB_URL = hostEnv.BASE_DB_URL;
  Object.assign(vars, args.specEnv ?? {});
  // Subscription creds + GitHub token win over spec env (they are the daemon's
  // authoritative harness auth, base64'd for the run-image bootstrap).
  Object.assign(vars, harnessCredentialVars(hostEnv));
  // Relay coordinates always win (they are the run's identity).
  vars.ANIMUS_ENV_WSS_URL = args.wssUrl;
  vars.ANIMUS_ENV_RUN_TOKEN = args.token;
  vars.ANIMUS_ENV_WORKSPACE_ROOT = WORKSPACE_ROOT;
  return vars;
}

/** owner/repo parsed from a github remote url (https or ssh), or null. */
export function parseGithubSlug(url: string | undefined): { owner: string; repo: string } | null {
  if (!url) return null;
  const m = url.match(/github\.com[:/]+([^/]+)\/([^/]+?)(?:\.git)?\/?$/i);
  return m && m[1] && m[2] ? { owner: m[1], repo: m[2] } : null;
}

/** Sign a short-lived (<=10 min) GitHub App JWT (RS256) with the app private key. */
function githubAppJwt(appId: string, privateKeyPem: string, now: number): string {
  const b64 = (o: unknown) => Buffer.from(JSON.stringify(o)).toString('base64url');
  const data = `${b64({ alg: 'RS256', typ: 'JWT' })}.${b64({ iat: now - 60, exp: now + 540, iss: appId })}`;
  const sig = createSign('RSA-SHA256').update(data).sign(privateKeyPem, 'base64url');
  return `${data}.${sig}`;
}

/** Mint a repo-scoped GitHub App installation token for the run's primary repo and
 *  return it + the app's bot commit identity (so pushes + PRs + commits are attributed
 *  to the App, not a personal account). Best-effort: returns {} when the app is not
 *  configured or any GitHub call fails (the node then just has no push credential). */
export async function githubAppCredentials(
  spec: { repos?: Array<{ url: string; primary?: boolean }> },
  hostEnv: NodeJS.ProcessEnv,
  now: number,
  fetchImpl: typeof fetch = fetch,
): Promise<Record<string, string>> {
  const appId = hostEnv.GITHUB_APP_ID;
  const rawKey = hostEnv.GITHUB_APP_PRIVATE_KEY;
  if (!appId || !rawKey) return {};
  const slug = parseGithubSlug((spec.repos ?? []).find((r) => r.primary)?.url ?? spec.repos?.[0]?.url);
  if (!slug) return {};
  const privateKey = rawKey.includes('\\n') ? rawKey.replace(/\\n/g, '\n') : rawKey;
  try {
    const jwt = githubAppJwt(appId, privateKey, now);
    const gh = async (path: string, init?: RequestInit): Promise<Record<string, unknown>> => {
      const res = await fetchImpl(`https://api.github.com${path}`, {
        ...init,
        headers: {
          authorization: `Bearer ${jwt}`,
          accept: 'application/vnd.github+json',
          'user-agent': 'animus-environment-railway',
          ...init?.headers,
        },
      });
      if (!res.ok) throw new Error(`GitHub ${path} -> HTTP ${res.status}: ${(await res.text()).slice(0, 200)}`);
      return (await res.json()) as Record<string, unknown>;
    };
    const install = await gh(`/repos/${slug.owner}/${slug.repo}/installation`);
    const minted = await gh(`/app/installations/${install.id}/access_tokens`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ repositories: [slug.repo] }),
    });
    const vars: Record<string, string> = { GITHUB_TOKEN: String(minted.token) };
    const appSlug = hostEnv.GITHUB_APP_SLUG;
    if (appSlug) {
      let botId: unknown = install.app_id;
      try {
        botId = (await gh(`/users/${appSlug}[bot]`)).id;
      } catch {
        // fall back to app_id if the bot user lookup fails
      }
      const name = `${appSlug}[bot]`;
      const email = `${botId}+${appSlug}[bot]@users.noreply.github.com`;
      vars.GIT_AUTHOR_NAME = name;
      vars.GIT_AUTHOR_EMAIL = email;
      vars.GIT_COMMITTER_NAME = name;
      vars.GIT_COMMITTER_EMAIL = email;
    }
    return vars;
  } catch (err) {
    process.stderr.write(`[animus-environment-railway] github app token mint failed: ${String(err)}\n`);
    return {};
  }
}

export interface RailwayEnvironmentDeps {
  /** Railway API (mockable). Default: a `RailwayClient` over `RAILWAY_TOKEN`. */
  railway?: RailwayApi;
  /** Pre-started relay (tests). Default: lazily `RelayServer.listen(...)`. */
  relay?: RelayServer;
  config?: RailwayEnvironmentConfig;
}

/** The substrate implementation behind the plugin's four methods. */
export class RailwayEnvironment {
  private readonly config: RailwayEnvironmentConfig;
  private railwayApi: RailwayApi | null;
  private relayInstance: RelayServer | null;
  /** Random per-process identity baked into service names, so a GC sweep can
   *  distinguish this instance's runs from another live plugin instance's. */
  readonly instanceId: string = shortId('i');

  constructor(deps: RailwayEnvironmentDeps = {}) {
    this.config = deps.config ?? configFromEnv();
    this.railwayApi = deps.railway ?? null;
    this.relayInstance = deps.relay ?? null;
  }

  /** Service-name prefix for THIS plugin instance's runs. */
  private instancePrefix(): string {
    return `${SERVICE_NAME_PREFIX}${this.instanceId}-`;
  }

  private railway(): RailwayApi {
    if (!this.railwayApi) {
      const token = process.env.RAILWAY_TOKEN;
      if (!token) {
        throw new Error(
          'RAILWAY_TOKEN is not set — the railway environment plugin needs a Railway API token to create run services',
        );
      }
      this.railwayApi = new RailwayClient({ token });
    }
    return this.railwayApi;
  }

  /** Lazily bind the relay listener (one per plugin process, shared by runs). */
  async relay(): Promise<RelayServer> {
    if (!this.relayInstance) {
      if (this.config.relayPublicUrl && !this.config.relayPort) {
        // A fixed public URL implies the edge routes to a KNOWN port; binding
        // an ephemeral one would leave every container dialing a URL that
        // routes nowhere (prepare would always time out and roll back).
        throw new Error(
          'ANIMUS_ENV_RELAY_PUBLIC_URL is set but ANIMUS_ENV_RELAY_PORT is not — bind the relay to the port your public URL routes to',
        );
      }
      this.relayInstance = await RelayServer.listen({
        host: this.config.relayHost ?? '0.0.0.0',
        port: this.config.relayPort ?? 0,
        publicUrl: this.config.relayPublicUrl,
        tls: this.config.tls,
      });
    }
    return this.relayInstance;
  }

  /** `prepare`: mint the run token, create the Railway service from the base
   *  image with the relay coordinates injected, wait (bounded) for the
   *  container to dial home, then clone the planned repos and (optionally)
   *  `animus install`. */
  async prepare(req: PrepareRequest): Promise<{ handle: EnvironmentHandle }> {
    const spec = req.spec;
    const { projectId, environmentId } = resolveTarget(spec, this.config);
    const image = spec.image?.trim() || DEFAULT_IMAGE;
    // A broker-supplied run id names the node deterministically (reconcilable +
    // cold-reapable by run id); otherwise fall back to the per-run random id +
    // per-instance prefix. The relay run id is kept random regardless so a repeat
    // prepare for the same run never collides on an already-registered relay run.
    const runId = specRunId(spec);
    const id = shortId('r');
    const serviceName = runId ? deterministicServiceName(projectId, runId) : `${this.instancePrefix()}${id}`;
    // Reconcile before create: any pre-existing service with this run's
    // deterministic name is a leaked orphan from a failed earlier prepare of the
    // SAME run (nothing else can own that name), so delete it first to keep the
    // invariant "at most one service per run id" and avoid an accumulating leak.
    if (runId) {
      try {
        const existing = (await this.railway().listRunServices(projectId)).find((s) => s.name === serviceName);
        if (existing) await this.railway().deleteService(existing.id, environmentId);
      } catch {
        // Best-effort reconcile; a create below still proceeds.
      }
    }

    const relay = await this.relay();
    const { url, token } = relay.registerRun(id);
    const plan = planWorkspace(spec, WORKSPACE_ROOT);
    // Validate every planned subdir up front (a spec-supplied repo `name` like
    // `../outside` must never escape the workspace root or poison the default
    // cwd metadata). Fail BEFORE creating the service.
    try {
      if (!plan.single) for (const planned of plan.repos) assertSafeSubdir(planned.subdir);
      // A remote container cannot see the daemon host's filesystem: local-path
      // repo urls (the docker plugin bind-mounts these) are unsupported here.
      // Fail loudly rather than returning a workspace missing its checkouts.
      for (const planned of plan.repos) {
        if (planned.repo.url.trim().startsWith('/')) {
          throw new Error(
            `repo '${planned.repo.url}' is a local path — the railway environment can only clone remote urls`,
          );
        }
      }
    } catch (err) {
      relay.releaseRun(id);
      throw err;
    }

    let serviceId: string | null = null;
    let deploymentId: string | null | undefined;
    // Mint a repo-scoped GitHub App token (+ bot commit identity) so the node's
    // harness pushes + opens PRs AS the app. Best-effort; overrides any passthrough
    // GITHUB_TOKEN from runVariables.
    const appVars = await githubAppCredentials(spec, process.env, Math.floor(Date.now() / 1000));
    // Central Claude refresh: inject a short-lived access token (refresh token
    // stripped) so the node can't rotate/corrupt the shared daemon credential.
    const claudeVars = await claudeNodeCredentials(process.env, Date.now());
    try {
      const created = await this.railway().createRunService({
        projectId,
        environmentId,
        name: serviceName,
        image,
        variables: { ...runVariables({ wssUrl: url, token, specEnv: spec.env }), ...claudeVars, ...appVars },
        startCommand: this.config.bridgeCommand ?? DEFAULT_BRIDGE_COMMAND,
      });
      serviceId = created.serviceId;
      deploymentId = created.deploymentId;

      const dialTimeoutMs = (this.config.dialTimeoutSecs ?? 300) * 1000;
      await relay.waitForConnection(id, dialTimeoutMs);

      // The workspace root's existence is guaranteed by the bridge itself
      // (BridgeClient ensures its ANIMUS_ENV_WORKSPACE_ROOT exists on
      // connect), so custom images without /workspace still work.

      // Materialize the planned repos (remote urls only) inside the container.
      for (const command of cloneCommands(plan)) {
        const res = await relay.exec(id, command, { timeoutSecs: 600 });
        if (res.exit_code !== 0) {
          throw new Error(
            `repo clone failed (exit ${res.exit_code}) for ${command.args?.slice(-2)[0] ?? '?'}: ${(res.stderr ?? '').trim()}`,
          );
        }
      }

      // Opt-in provisioning: restore the pinned plugin set from the cloned
      // project's animus.toml/plugins.lock (`spec.metadata.provision_animus`).
      const meta = (spec.metadata ?? {}) as Record<string, unknown>;
      if (meta.provision_animus === true) {
        const cwd = plan.single || !plan.primarySubdir ? WORKSPACE_ROOT : `${WORKSPACE_ROOT}/${plan.primarySubdir}`;
        await provisionAnimus((command) => relay.exec(id, command, { timeoutSecs: 900 }), {
          workspaceRoot: cwd,
        });
      }
    } catch (err) {
      // Roll back the half-prepared run: forget the token, delete the service.
      relay.releaseRun(id);
      if (serviceId) {
        await this.railway()
          .deleteService(serviceId, environmentId)
          .catch(() => undefined);
      }
      throw err;
    }

    const metadata: RailwayHandleMeta = {
      service_id: serviceId,
      service_name: serviceName,
      project_id: projectId,
      environment_id: environmentId,
      deployment_id: deploymentId ?? null,
      image,
      primary_subdir: plan.primarySubdir,
      animus_run_id: runId,
    };
    return { handle: { id, workspace_root: WORKSPACE_ROOT, metadata } };
  }

  /** `exec` / `exec_stream` core: relay the command to the in-container
   *  bridge. When `onChunk` is provided the streamed `exec/output` frames feed
   *  it incrementally (the plugin layer turns them into `environment/output`
   *  notifications). */
  async execCommand(
    handle: EnvironmentHandle,
    command: HarnessCommand,
    stdin: string | null | undefined,
    timeoutSecs: number | null | undefined,
    onChunk?: (stream: 'stdout' | 'stderr', text: string) => void,
  ): Promise<ExecResponse> {
    const relay = await this.relay();
    const meta = handle.metadata as RailwayHandleMeta | undefined;
    // Default a multi-repo workspace's cwd to the primary repo subdir (the
    // bridge itself only knows the workspace root).
    const effective: HarnessCommand =
      !command.cwd && meta?.primary_subdir ? { ...command, cwd: meta.primary_subdir } : command;
    return relay.exec(handle.id, effective, {
      stdin: stdin ?? null,
      timeoutSecs: timeoutSecs ?? null,
      onOutput: onChunk,
    });
  }

  /** `teardown`: delete the Railway service and release the relay run.
   *  Idempotent — a missing service or unknown handle is a successful no-op. */
  async teardown(handle: EnvironmentHandle): Promise<void> {
    if (this.relayInstance) this.relayInstance.releaseRun(handle.id);
    const meta = handle.metadata as RailwayHandleMeta | undefined;
    const environmentId = meta?.environment_id || this.config.environmentId;
    // Fast path: a full handle carries the service_id; delete it directly.
    if (meta?.service_id) {
      if (!environmentId) {
        throw new Error(
          `cannot tear down service '${meta.service_id}': handle metadata has no environment_id and RAILWAY_ENVIRONMENT_ID is unset`,
        );
      }
      await this.railway().deleteService(meta.service_id, environmentId);
      return;
    }
    // Cold path: only a run id (no service_id) — resolve the service by its
    // deterministic name and delete it. This closes the crash window where a
    // service was created but the caller never received the full handle.
    const runId = meta?.animus_run_id?.trim();
    const projectId = meta?.project_id || this.config.projectId;
    if (!runId || !projectId || !environmentId) return;
    const serviceName = deterministicServiceName(projectId, runId);
    const match = (await this.railway().listRunServices(projectId)).find((s) => s.name === serviceName);
    if (match) await this.railway().deleteService(match.id, environmentId);
  }

  /** GC sweep: delete orphaned run services (no live relay registration).
   *
   *  Default scope is THIS instance's services only (name prefix embeds the
   *  per-process `instanceId`), so a sweep can never delete another live
   *  plugin instance's runs in a shared project. Pass
   *  `allInstances: true` — safe ONLY when a single plugin instance manages
   *  the project — to also reap `animus-run-*` services left behind by
   *  crashed previous instances. Returns the deleted service ids. */
  async gcOrphans(
    opts: { projectId?: string; environmentId?: string; allInstances?: boolean } = {},
  ): Promise<string[]> {
    const target = opts.projectId ?? this.config.projectId;
    if (!target) throw new Error('gcOrphans needs a project id (RAILWAY_PROJECT_ID or argument)');
    const environmentId = opts.environmentId ?? this.config.environmentId;
    if (!environmentId) throw new Error('gcOrphans needs an environment id (RAILWAY_ENVIRONMENT_ID or argument)');
    const services = await this.railway().listRunServices(target);
    const liveHandles = new Set(
      (this.relayInstance?.registeredRuns() ?? []).map((h) => `${this.instancePrefix()}${h}`),
    );
    const removed: string[] = [];
    for (const svc of services) {
      if (!svc.name.startsWith(SERVICE_NAME_PREFIX)) continue;
      if (!opts.allInstances && !svc.name.startsWith(this.instancePrefix())) continue;
      if (liveHandles.has(svc.name)) continue;
      await this.railway().deleteService(svc.id, environmentId);
      removed.push(svc.id);
    }
    return removed;
  }

  /** Health: surface missing credentials/config before scheduling work. The
   *  API itself is not probed (keeps preflight fast + quota-free). */
  health(): HealthReport {
    // Only the API token is globally REQUIRED: project/environment ids may
    // arrive per-run via spec.metadata, and the public relay URL is optional
    // for local/dev. Those soft gaps report `degraded` (informational) so a
    // preflight that treats `unhealthy` as fatal does not block valid per-run
    // configurations.
    if (!this.railwayApi && !process.env.RAILWAY_TOKEN) {
      return {
        status: 'unhealthy',
        uptime_ms: null,
        memory_usage_bytes: null,
        last_error: 'railway environment is not configured: missing RAILWAY_TOKEN',
      };
    }
    // A public URL without a fixed relay port makes every real prepare fail
    // (see relay()); that is a hard misconfiguration, surface it at preflight.
    if (this.config.relayPublicUrl && !this.config.relayPort) {
      return {
        status: 'unhealthy',
        uptime_ms: null,
        memory_usage_bytes: null,
        last_error:
          'ANIMUS_ENV_RELAY_PUBLIC_URL is set but ANIMUS_ENV_RELAY_PORT is not — the relay must bind the port the public URL routes to',
      };
    }
    const soft: string[] = [];
    if (!this.config.projectId) soft.push('RAILWAY_PROJECT_ID');
    if (!this.config.environmentId) soft.push('RAILWAY_ENVIRONMENT_ID');
    if (!this.config.relayPublicUrl) soft.push('ANIMUS_ENV_RELAY_PUBLIC_URL');
    if (soft.length > 0) {
      return {
        status: 'degraded',
        uptime_ms: null,
        memory_usage_bytes: null,
        last_error: `railway environment defaults are incomplete: missing ${soft.join(', ')} (spec.metadata can supply project/environment ids per-run; the public relay URL is only needed for real Railway runs)`,
      };
    }
    return { status: 'healthy', uptime_ms: null, memory_usage_bytes: null, last_error: null };
  }

  /** Close the relay listener (tests / shutdown). */
  async close(): Promise<void> {
    if (this.relayInstance) {
      await this.relayInstance.close();
      this.relayInstance = null;
    }
  }
}
