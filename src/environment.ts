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
  type EnvironmentNodeDescriptor,
  type ExecResponse,
  type HarnessCommand,
  type HealthReport,
  type PrepareRequest,
  type WorkspacePlan,
} from '@launchapp-dev/animus-environment-base';
import {
  RelayServer,
  RelayClient,
  PluginClient,
  makeBackendCallHandler,
  type RelayServerOptions,
  type SessionResult,
  type JournalEventParams,
} from '@launchapp-dev/animus-env-transport';

/** The relay surface `prepare`/`exec`/`teardown` drive, satisfied by BOTH the
 *  in-process `RelayServer` (tests) and the `RelayClient` that talks to the
 *  shared singleton (production). `registerRun` may be sync or async — callers
 *  `await` it. */
type RelayTransport = Pick<RelayClient, 'exec' | 'runSession' | 'releaseRun' | 'registeredRuns' | 'close'> & {
  registerRun(handleId?: string): { url: string; token: string } | Promise<{ url: string; token: string }>;
  // RelayServer returns the connection, RelayClient returns void — callers ignore it.
  waitForConnection(handleId: string, timeoutMs: number): Promise<unknown>;
};

import { DEAD_DEPLOYMENT_STATES, RailwayClient, SERVICE_NAME_PREFIX, type RailwayApi } from './railway.js';

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

/** Default parent-side log-storage plugin a node's `log_storage/*` backend/call
 *  is serviced against (its install path in the portal image). */
export const DEFAULT_UPSTREAM_LOG_BIN = '/app/.animus/plugins/animus-log-storage-s3';

/** S3 env vars the parent's log-storage-s3 plugin reads (see its plugin.toml
 *  `env_required`): bucket + credentials are required, the rest optional. */
const LOG_S3_ENV_KEYS = [
  'S3_BUCKET',
  'S3_ACCESS_KEY_ID',
  'S3_SECRET_ACCESS_KEY',
  'S3_ENDPOINT',
  'S3_REGION',
  'S3_PREFIX',
  'S3_FORCE_PATH_STYLE',
] as const;

/** Collect the parent's S3 env (non-empty values only) to forward into the
 *  lazily-spawned log servicer, so a node's proxied log writes land in the same
 *  bucket the daemon itself uses. */
export function logStorageEnv(env: NodeJS.ProcessEnv = process.env): Record<string, string> {
  const out: Record<string, string> = {};
  for (const key of LOG_S3_ENV_KEYS) {
    const value = env[key];
    if (value !== undefined && value !== '') out[key] = value;
  }
  return out;
}

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
  /** Public WSS URL containers dial (TLS terminated at the Railway edge). Now
   *  owned by the SINGLETON relay (animus-env-relay); kept here only for the
   *  legacy in-process `RelayServer` test path. */
  relayPublicUrl?: string;
  /** Port the relay binds. Now owned by the singleton relay; unused by the
   *  client path. */
  relayPort?: number;
  /** Host interface the relay binds. Now owned by the singleton relay. */
  relayHost?: string;
  /** Unix socket the shared singleton relay (animus-env-relay) listens on; the
   *  client dials it. Default {@link DEFAULT_RELAY_SOCK} inside `RelayClient`. */
  relaySocketPath?: string;
  /** In-container start command (default `animus-env-bridge`). */
  bridgeCommand?: string;
  /** Bound wait for the container to dial home (default 300s — a Railway
   *  image pull + deploy is not fast). */
  dialTimeoutSecs?: number;
  /** Pull credentials for a private run image (ghcr et al). Both parts must be
   *  present to be applied; omitted for public images. */
  registryCredentials?: { username: string; password: string };
  /** Parent-side backend plugin binary a node's `backend/call` is serviced
   *  against (transparent passthrough — nested "animus inside animus"). When set
   *  with a DATABASE_URL, the relay spawns it lazily and routes subject/config/
   *  queue/journal role calls from lean nodes to it. */
  upstreamBackendBin?: string;
  /** DATABASE_URL handed to the parent-side backend plugin (kept on the PARENT;
   *  never sent to the node). */
  databaseUrl?: string;
  /** Parent-side log-storage plugin binary a node's `log_storage/*` backend/call
   *  is serviced against (default `/app/.animus/plugins/animus-log-storage-s3`).
   *  Wired only when the parent's S3 env (bucket + credentials) is present;
   *  otherwise log calls fall back to the default backend. */
  upstreamLogBin?: string;
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
    registryCredentials:
      env.ANIMUS_ENV_REGISTRY_USERNAME && env.ANIMUS_ENV_REGISTRY_PASSWORD
        ? { username: env.ANIMUS_ENV_REGISTRY_USERNAME, password: env.ANIMUS_ENV_REGISTRY_PASSWORD }
        : undefined,
    upstreamBackendBin: env.ANIMUS_ENV_UPSTREAM_BACKEND_BIN,
    databaseUrl: env.BASE_DB_URL ?? env.DATABASE_URL,
    upstreamLogBin: env.ANIMUS_ENV_UPSTREAM_LOG_BIN,
    relaySocketPath: env.ANIMUS_ENV_RELAY_SOCK,
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

/** Durable dir the portal Connections flow stores the Codex ChatGPT-subscription
 *  `auth.json` in (the portal's codex wrapper + start.sh default). Used when the
 *  daemon env leaves `CODEX_OAUTH_HOME` unset so the node still gets codex auth
 *  without any extra portal config. */
export const DEFAULT_CODEX_OAUTH_HOME = '/data/animus-state/codex-config';

/** Read the daemon-side Codex auth.json + GitHub token and encode them for the
 *  node bootstrap. Claude is handled separately (async central refresh). */
export function harnessCredentialVars(hostEnv: NodeJS.ProcessEnv): Record<string, string> {
  const vars: Record<string, string> = {};
  // Fall back to the durable portal default so codex works even when the daemon
  // env does not export CODEX_OAUTH_HOME (best-effort: skips when absent).
  const codexHome = (hostEnv.CODEX_OAUTH_HOME ?? DEFAULT_CODEX_OAUTH_HOME).replace(/\/$/, '');
  try {
    vars.ANIMUS_NODE_CODEX_AUTH_B64 = readFileSync(`${codexHome}/auth.json`).toString('base64');
  } catch {
    // no codex login on the daemon; skip
  }
  // Expose the token as BOTH GITHUB_TOKEN (git credential helper) and GH_TOKEN
  // (what the `gh` CLI reads) so `gh pr create` authenticates on the node.
  if (hostEnv.GITHUB_TOKEN) {
    vars.GITHUB_TOKEN = hostEnv.GITHUB_TOKEN;
    vars.GH_TOKEN = hostEnv.GITHUB_TOKEN;
  }
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

/** Resolve the repo slug to SCOPE the App token to: the run's primary repo, else a
 *  broker-supplied `spec.metadata.github_repo` (a clone URL or a bare `owner/repo`).
 *  Null => no specific repo (a bare broker node) — the caller mints an
 *  installation-wide token instead. */
function tokenScopeSlug(spec: {
  repos?: Array<{ url: string; primary?: boolean }>;
  metadata?: unknown;
}): { owner: string; repo: string } | null {
  const fromRepos = parseGithubSlug((spec.repos ?? []).find((r) => r.primary)?.url ?? spec.repos?.[0]?.url);
  if (fromRepos) return fromRepos;
  const meta = (spec.metadata ?? {}) as Record<string, unknown>;
  const raw = typeof meta.github_repo === 'string' ? meta.github_repo.trim() : '';
  if (!raw) return null;
  const fromUrl = parseGithubSlug(raw);
  if (fromUrl) return fromUrl;
  const bare = raw.match(/^([^/\s]+)\/([^/\s]+?)(?:\.git)?$/);
  return bare && bare[1] && bare[2] ? { owner: bare[1], repo: bare[2] } : null;
}

/** Mint a GitHub App installation token so the node's harness can push + open PRs
 *  AS the app (not a personal account), plus the app's bot commit identity.
 *
 *  Scope: when a target repo is known (`spec.repos` primary or
 *  `spec.metadata.github_repo`) the token is scoped to THAT repo. On a BARE broker
 *  node (no repos — cloning is the harness's job) it falls back to an
 *  installation-wide token (all repos the app is installed on), resolved from
 *  `GITHUB_APP_INSTALLATION_ID` or the app's first installation — so a shared
 *  per-run node can still push whatever repo the harness self-clones.
 *
 *  Best-effort: returns {} when the app is not configured or any GitHub call fails
 *  (the node then just has no push credential). */
export async function githubAppCredentials(
  spec: { repos?: Array<{ url: string; primary?: boolean }>; metadata?: unknown },
  hostEnv: NodeJS.ProcessEnv,
  now: number,
  fetchImpl: typeof fetch = fetch,
): Promise<Record<string, string>> {
  const appId = hostEnv.GITHUB_APP_ID;
  const rawKey = hostEnv.GITHUB_APP_PRIVATE_KEY;
  if (!appId || !rawKey) return {};
  const slug = tokenScopeSlug(spec);
  const privateKey = rawKey.includes('\\n') ? rawKey.replace(/\\n/g, '\n') : rawKey;
  try {
    const jwt = githubAppJwt(appId, privateKey, now);
    const gh = async <T = Record<string, unknown>>(path: string, init?: RequestInit): Promise<T> => {
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
      return (await res.json()) as T;
    };
    // Resolve the installation: repo-scoped when a target repo is known, else the
    // app's own installation (env override, else the first one).
    let installId: unknown;
    let installAppId: unknown;
    if (slug) {
      const install = await gh(`/repos/${slug.owner}/${slug.repo}/installation`);
      installId = install.id;
      installAppId = install.app_id;
    } else if (hostEnv.GITHUB_APP_INSTALLATION_ID) {
      const install = await gh(`/app/installations/${hostEnv.GITHUB_APP_INSTALLATION_ID}`);
      installId = install.id;
      installAppId = install.app_id;
    } else {
      const installs = await gh<Array<{ id: unknown; app_id: unknown; account?: { login?: unknown } }>>(
        `/app/installations`,
      );
      const first = Array.isArray(installs) ? installs[0] : undefined;
      if (!first) return {};
      if (Array.isArray(installs) && installs.length > 1) {
        const chosen = typeof first.account?.login === 'string' ? first.account.login : String(first.id);
        process.stderr.write(
          `[animus-environment-railway] GitHub App has ${installs.length} installations; ` +
            `no GITHUB_APP_INSTALLATION_ID and no target repo, so guessing the FIRST one ('${chosen}'). ` +
            `The minted token is scoped to that org and will 403 on any other. ` +
            `Set GITHUB_APP_INSTALLATION_ID or pass a target repo (spec.repos primary or spec.metadata.github_repo) to scope it deterministically.\n`,
        );
      }
      installId = first.id;
      installAppId = first.app_id;
    }
    const minted = await gh(`/app/installations/${installId}/access_tokens`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      // Scope to the one repo when known; otherwise an installation-wide token.
      body: slug ? JSON.stringify({ repositories: [slug.repo] }) : '{}',
    });
    // Expose the minted token as BOTH GITHUB_TOKEN (git credential helper) and
    // GH_TOKEN (what the `gh` CLI reads) so `gh pr create` works on the node.
    const mintedToken = String(minted.token);
    const vars: Record<string, string> = { GITHUB_TOKEN: mintedToken, GH_TOKEN: mintedToken };
    const appSlug = hostEnv.GITHUB_APP_SLUG;
    if (appSlug) {
      let botId: unknown = installAppId;
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
  /** Pre-started in-process relay (tests). Default: connect to the shared
   *  singleton via `RelayClient`. */
  relay?: RelayServer;
  config?: RailwayEnvironmentConfig;
}

/** The substrate implementation behind the plugin's four methods. */
export class RailwayEnvironment {
  private readonly config: RailwayEnvironmentConfig;
  private railwayApi: RailwayApi | null;
  private relayInstance: RelayTransport | null;
  /** Random per-process identity baked into service names, so a GC sweep can
   *  distinguish this instance's runs from another live plugin instance's. */
  readonly instanceId: string = shortId('i');

  private backendClient: PluginClient | null = null;
  private logClient: PluginClient | null = null;

  constructor(deps: RailwayEnvironmentDeps = {}) {
    this.config = deps.config ?? configFromEnv();
    this.railwayApi = deps.railway ?? null;
    this.relayInstance = deps.relay ?? null;
  }

  /** Lazily spawn the parent-side backend plugin a node's `backend/call` is
   *  serviced against. Returns null when upstream proxying isn't configured
   *  (no backend bin + DATABASE_URL) so the relay's default reverse-RPC error
   *  path stays in effect. The parent's DATABASE_URL is injected here and NEVER
   *  sent to the node. */
  private backend(): PluginClient | null {
    if (this.backendClient) return this.backendClient;
    const bin = this.config.upstreamBackendBin;
    const dbUrl = this.config.databaseUrl;
    if (!bin || !dbUrl) return null;
    this.backendClient = new PluginClient(bin, {
      env: { DATABASE_URL: dbUrl, BASE_DB_URL: dbUrl },
    });
    return this.backendClient;
  }

  /** Lazily spawn the parent-side log-storage plugin a node's `log_storage/*`
   *  backend/call is serviced against, so run logs offload to the SAME bucket the
   *  daemon uses (instead of hitting animus-postgres). Returns null when the
   *  parent's S3 env (bucket + credentials) is absent — log calls then fall back
   *  to the default servicer. The S3 env is forwarded here and never sent to the
   *  node. */
  private logBackend(): PluginClient | null {
    if (this.logClient) return this.logClient;
    const bin = this.config.upstreamLogBin ?? DEFAULT_UPSTREAM_LOG_BIN;
    const s3 = logStorageEnv();
    // The plugin hard-requires bucket + credentials on initialize; skip wiring it
    // when they are missing so a misconfigured parent degrades gracefully.
    if (!bin || !s3.S3_BUCKET || !s3.S3_ACCESS_KEY_ID || !s3.S3_SECRET_ACCESS_KEY) return null;
    this.logClient = new PluginClient(bin, { env: s3 });
    return this.logClient;
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
  async relay(): Promise<RelayTransport> {
    if (!this.relayInstance) {
      const backend = this.backend();
      const logBackend = this.logBackend();
      // Connect to the SHARED singleton relay (animus-env-relay) over its unix
      // socket rather than binding the public port here — one relay owns the
      // fixed port; every plugin instance is a client (no EADDRINUSE, no leaked
      // listener, concurrent delegations multiplex by handleId). Reverse-RPC is
      // still serviced HERE against the parent's own animus (transparent
      // passthrough): log_storage/* → the parent's log-storage-s3 (same bucket as
      // the daemon), everything else → animus-postgres. Omitted → reverse RPC
      // stays unwired and nodes fall back to their own local backends.
      this.relayInstance = await RelayClient.connect({
        socketPath: this.config.relaySocketPath,
        ...(backend
          ? {
              onReverseRpc: makeBackendCallHandler(
                logBackend ? { default: backend, log_storage: logBackend } : backend,
              ),
            }
          : {}),
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
    const { url, token } = await relay.registerRun(id);
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
        registryCredentials: this.config.registryCredentials,
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

  /** `exec_session`: dispatch a subject to the container's OWN animus (REQ-052
   *  remote-animus) and stream its journal events back via `onJournal`. */
  async runSession(
    handle: EnvironmentHandle,
    params: {
      subject_id: string;
      workflow_ref?: string | null;
      dispatch_input?: string | null;
      workflow_id?: string | null;
    },
    onJournal?: (event: JournalEventParams) => void,
  ): Promise<SessionResult> {
    const relay = await this.relay();
    return relay.runSession(handle.id, params, onJournal);
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

  /** Node-management (`environment/list`): describe every `animus-run-*` service
   *  in the project with its latest-deployment state. `orphan` is state-based
   *  (a dead deployment is always a reap candidate). */
  async listNodes(): Promise<EnvironmentNodeDescriptor[]> {
    const projectId = this.config.projectId;
    if (!projectId) throw new Error('list needs a project id (RAILWAY_PROJECT_ID)');
    return this.describeNodes(projectId);
  }

  /** Node-management (`environment/get`): one node by service id or name. */
  async getNode(idOrName: string): Promise<EnvironmentNodeDescriptor | null> {
    const nodes = await this.listNodes();
    return nodes.find((n) => n.id === idOrName || n.name === idOrName) ?? null;
  }

  /** Node-management (`environment/teardown_node`): delete one service by id or
   *  name. Idempotent — an unknown/already-gone node returns `[]`. */
  async teardownNode(idOrName: string): Promise<string[]> {
    const projectId = this.config.projectId;
    const environmentId = this.config.environmentId;
    if (!projectId) throw new Error('teardown needs a project id (RAILWAY_PROJECT_ID)');
    if (!environmentId) throw new Error('teardown needs an environment id (RAILWAY_ENVIRONMENT_ID)');
    const match = (await this.railway().listRunServices(projectId)).find(
      (s) => s.id === idOrName || s.name === idOrName,
    );
    if (!match) return [];
    await this.railway().deleteService(match.id, environmentId);
    return [match.id];
  }

  /** Node-management (`environment/reap`): delete orphaned/dead nodes.
   *
   *  Default (no opts): reap ONLY services whose latest deployment is dead
   *  (FAILED/CRASHED/REMOVED) — always safe, since a healthy live node is never
   *  in a dead state. `all` additionally reaps non-dead services that have no
   *  live owning run, but ONLY with `force` (a fresh, non-resident process has
   *  no in-memory liveness, so it must not assume every healthy node is an
   *  orphan). `dry_run` reports the plan without deleting. */
  async reap(
    opts: { all?: boolean; force?: boolean; dryRun?: boolean; olderThanSecs?: number } = {},
  ): Promise<{ deleted: string[]; kept: EnvironmentNodeDescriptor[]; dryRun: boolean }> {
    const projectId = this.config.projectId;
    const environmentId = this.config.environmentId;
    if (!projectId) throw new Error('reap needs a project id (RAILWAY_PROJECT_ID)');
    if (!environmentId) throw new Error('reap needs an environment id (RAILWAY_ENVIRONMENT_ID)');
    const nodes = await this.describeNodes(projectId);
    const liveNames = new Set(
      (this.relayInstance?.registeredRuns() ?? []).map((h) => `${this.instancePrefix()}${h}`),
    );
    const now = Date.now();
    const deleted: string[] = [];
    const kept: EnvironmentNodeDescriptor[] = [];
    for (const node of nodes) {
      const dead = DEAD_DEPLOYMENT_STATES.has(node.state.toUpperCase());
      const live = liveNames.has(node.name);
      const oldEnough =
        opts.olderThanSecs === undefined ||
        (node.created_at ? (now - Date.parse(node.created_at)) / 1000 >= opts.olderThanSecs : true);
      let reapIt = false;
      if (dead && oldEnough) reapIt = true;
      else if (opts.all && opts.force && !live && oldEnough) reapIt = true;
      if (!reapIt) {
        kept.push(node);
        continue;
      }
      if (!opts.dryRun) await this.railway().deleteService(node.id, environmentId);
      deleted.push(node.id);
    }
    return { deleted, kept, dryRun: opts.dryRun === true };
  }

  /** Shared listing used by list/get/reap: prefer the state-aware query, fall
   *  back to id+name only when the substrate can't report deployment state. */
  private async describeNodes(projectId: string): Promise<EnvironmentNodeDescriptor[]> {
    const api = this.railway();
    let rows: Array<{ id: string; name: string; status?: string | null; createdAt?: string | null }>;
    if (typeof api.listRunServicesDetailed === 'function') {
      rows = await api.listRunServicesDetailed(projectId);
    } else {
      rows = await api.listRunServices(projectId);
    }
    return rows.map((r) => {
      const state = (r.status ?? 'unknown').toString();
      return {
        id: r.id,
        name: r.name,
        state,
        run_id: null,
        image: null,
        created_at: r.createdAt ?? null,
        orphan: DEAD_DEPLOYMENT_STATES.has(state.toUpperCase()),
      };
    });
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
    // The public URL + port are now owned by the singleton relay
    // (animus-env-relay), whose own health surfaces a misconfigured port; this
    // plugin only needs to reach that relay's unix socket at prepare time.
    const soft: string[] = [];
    if (!this.config.projectId) soft.push('RAILWAY_PROJECT_ID');
    if (!this.config.environmentId) soft.push('RAILWAY_ENVIRONMENT_ID');
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

  /** Close the relay listener + any parent-side backend plugins (tests / shutdown). */
  async close(): Promise<void> {
    if (this.relayInstance) {
      await this.relayInstance.close();
      this.relayInstance = null;
    }
    this.backendClient?.close();
    this.backendClient = null;
    this.logClient?.close();
    this.logClient = null;
  }
}
