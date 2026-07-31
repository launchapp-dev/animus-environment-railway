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

import {
  createCipheriv,
  createDecipheriv,
  createHash,
  createHmac,
  createSign,
  randomBytes,
  timingSafeEqual,
} from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { mkdir, rm } from 'node:fs/promises';
import { createConnection, createServer, type Server } from 'node:net';
import { join } from 'node:path';

import {
  planWorkspace,
  provisionAnimus,
  type EnvironmentHandle,
  type EnvironmentNodeDescriptor,
  type ExecResponse,
  type HarnessCommand,
  type HealthReport,
  type CallContext,
  type PrepareRequest,
  type WorkspacePlan,
} from '@launchapp-dev/animus-environment-base';
import {
  RelayClient,
  PluginClient,
  makeBackendCallHandler,
  RelayErrorCode,
  RelayRpcError,
  ExecutionFenceSchema,
  type RelayServerOptions,
  type ReverseRpcHandler,
  type SessionResult,
  type JournalEventParams,
  type ExecutionFence,
} from '@launchapp-dev/animus-env-transport';

/** The relay surface `prepare`/`exec`/`teardown` drive, satisfied by BOTH the
 *  in-process `RelayServer` (tests) and the `RelayClient` that talks to the
 *  shared singleton (production). `registerRun` may be sync or async — callers
 *  `await` it. */
export type RelayTransport = Pick<RelayClient, 'exec' | 'runSession' | 'releaseRun' | 'registeredRuns' | 'close'> & {
  registerRun(handleId?: string):
    | { url: string; token: string; ownerToken?: string }
    | Promise<{ url: string; token: string; ownerToken?: string }>;
  attachRun?: (
    handleId: string,
    credentials: RelayCredentials,
  ) => Promise<{ url: string; token: string; ownerToken: string }>;
  // RelayServer returns the connection, RelayClient returns void — callers ignore it.
  waitForConnection(handleId: string, timeoutMs: number): Promise<unknown>;
};

function validateExecutionFence(
  params: {
    subject_id: string;
    workflow_id?: string | null;
    execution_fence?: unknown;
  },
): ExecutionFence | null {
  if (params.execution_fence === undefined || params.execution_fence === null) return null;
  const parsed = ExecutionFenceSchema.safeParse(params.execution_fence);
  if (!parsed.success) {
    throw new Error(`environment exec_session execution_fence is invalid: ${parsed.error.message}`);
  }
  const fence = parsed.data;
  if (params.workflow_id !== fence.workflow_id) {
    throw new Error('environment exec_session workflow_id does not match execution_fence');
  }
  const qualifiedSubject = fence.subject?.qualified_id;
  if (
    qualifiedSubject &&
    qualifiedSubject !== params.subject_id &&
    !qualifiedSubject.endsWith(`:${params.subject_id}`)
  ) {
    throw new Error('environment exec_session subject_id does not match execution_fence');
  }
  return fence;
}

function sameExecutionFence(left: ExecutionFence | null | undefined, right: ExecutionFence): boolean {
  return left !== null && left !== undefined && JSON.stringify(left) === JSON.stringify(right);
}

interface RelayCredentials {
  token: string;
  ownerToken: string;
}

import { DEAD_DEPLOYMENT_STATES, RailwayClient, SERVICE_NAME_PREFIX, type RailwayApi } from './railway.js';
import { makeCredentialsServicer } from './credentials-servicer.js';

// Short, Railway-valid service-name token. This is cosmetic substrate naming,
// never an authorization identifier.
function shortId(prefix: string): string {
  return `${prefix}${randomBytes(3).toString('hex')}`;
}

/** Relay/environment handle ids are bearer-adjacent routing identifiers. Give
 * them 128 random bits so an attacker cannot feasibly guess a live handle. */
function secureHandleId(): string {
  return `r${randomBytes(16).toString('hex')}`;
}

/** Compact, deterministic service-name projection of a secure handle id. The
 * full 128-bit id remains required by the relay; exposing this 40-bit prefix in
 * Railway naming does not make the remaining 88 random bits guessable. */
function handleServiceToken(handleId: string): string {
  return handleId.slice(0, 11);
}

/** Trusted parent-to-node actor handoff consumed by the direct workflow runner.
 * A workflow/environment spec can never set this variable: only the actor the
 * plugin SDK extracted from the JSON-RPC call context may populate it. */
export const ACTOR_ENV = 'ANIMUS_ACTOR_JSON';

type Actor = NonNullable<CallContext['actor']>;

export interface ActorBinding {
  scope: 'actor' | 'system';
  fingerprint: string | null;
  actor: Actor | null;
  actorJson: string | null;
}

/** Normalize optional SDK fields before hashing/forwarding. Extra passthrough
 * properties are deliberately dropped: the Rust Actor contract is exactly
 * user_id + claims + tenant_id. */
export function actorBinding(actor: Actor | null | undefined): ActorBinding {
  if (!actor) return { scope: 'system', fingerprint: null, actor: null, actorJson: null };
  const canonical: Actor = {
    user_id: actor.user_id,
    claims: [...(actor.claims ?? [])],
    tenant_id: actor.tenant_id ?? null,
  };
  const actorJson = JSON.stringify(canonical);
  return {
    scope: 'actor',
    fingerprint: `sha256:${createHash('sha256').update(actorJson).digest('hex')}`,
    actor: canonical,
    actorJson,
  };
}

/** The SDK intentionally extracts actors leniently. This boundary is stricter:
 * a caller that supplied an `actor` key but failed typed extraction must not be
 * silently downgraded to a system run. */
export function validateRawActor(rawActor: unknown, actor: Actor | undefined, present = rawActor !== undefined): void {
  if (present && !actor) {
    throw new Error('environment actor is present but malformed; refusing system-scope downgrade');
  }
  if (actor && !present) {
    throw new Error('environment actor context has no matching request actor');
  }
}

function actorBindingMac(secret: string, handle: EnvironmentHandle, metadata: Record<string, unknown>): string {
  const signed: Record<string, unknown> = {
    version: 1,
    handle_id: handle.id,
    service_id: metadata.service_id,
    environment_id: metadata.environment_id,
    scope: metadata.animus_actor_scope,
    fingerprint: metadata.animus_actor_fingerprint,
  };
  // Preserve compatibility with handles signed before durable relay bindings
  // existed: only new handles include this field in the MAC payload.
  if (typeof metadata.animus_relay_binding === 'string') {
    signed.animus_relay_binding = metadata.animus_relay_binding;
  }
  return createHmac('sha256', secret)
    .update('animus-environment-railway/actor-binding/v1\0')
    .update(JSON.stringify(signed))
    .digest('hex');
}

function relayBindingKey(secret: string): Buffer {
  return createHash('sha256')
    .update('animus-environment-railway/relay-binding/v1\0')
    .update(secret)
    .digest();
}

function sealRelayBinding(secret: string, handleId: string, credentials: RelayCredentials): string {
  const nonce = randomBytes(12);
  const cipher = createCipheriv('aes-256-gcm', relayBindingKey(secret), nonce);
  cipher.setAAD(Buffer.from(handleId, 'utf8'));
  const ciphertext = Buffer.concat([
    cipher.update(JSON.stringify(credentials), 'utf8'),
    cipher.final(),
  ]);
  return [
    'v1',
    nonce.toString('base64url'),
    ciphertext.toString('base64url'),
    cipher.getAuthTag().toString('base64url'),
  ].join('.');
}

function openRelayBinding(secret: string, handle: EnvironmentHandle): RelayCredentials | null {
  const metadata = handle.metadata as Record<string, unknown> | undefined;
  const sealed = metadata?.animus_relay_binding;
  if (sealed === undefined || sealed === null) return null;
  if (typeof sealed !== 'string') {
    throw new Error(`environment handle '${handle.id}' has malformed relay binding metadata`);
  }
  const [version, nonceRaw, ciphertextRaw, tagRaw, ...extra] = sealed.split('.');
  if (version !== 'v1' || !nonceRaw || !ciphertextRaw || !tagRaw || extra.length > 0) {
    throw new Error(`environment handle '${handle.id}' has malformed relay binding metadata`);
  }
  try {
    const decipher = createDecipheriv('aes-256-gcm', relayBindingKey(secret), Buffer.from(nonceRaw, 'base64url'));
    decipher.setAAD(Buffer.from(handle.id, 'utf8'));
    decipher.setAuthTag(Buffer.from(tagRaw, 'base64url'));
    const plaintext = Buffer.concat([
      decipher.update(Buffer.from(ciphertextRaw, 'base64url')),
      decipher.final(),
    ]).toString('utf8');
    const parsed = JSON.parse(plaintext) as Partial<RelayCredentials>;
    if (
      typeof parsed.token !== 'string' ||
      typeof parsed.ownerToken !== 'string' ||
      !/^[A-Za-z0-9_-]{43}$/.test(parsed.token) ||
      !/^[A-Za-z0-9_-]{43}$/.test(parsed.ownerToken)
    ) {
      throw new Error('invalid relay credentials');
    }
    return { token: parsed.token, ownerToken: parsed.ownerToken };
  } catch {
    throw new Error(`environment handle '${handle.id}' relay binding could not be authenticated`);
  }
}

function bindingFromHandle(handle: EnvironmentHandle, secret: string): ActorBinding {
  const metadata = handle.metadata;
  if (!metadata || typeof metadata !== 'object' || Array.isArray(metadata)) {
    throw new Error(`environment handle '${handle.id}' has no actor binding metadata`);
  }
  const record = metadata as Record<string, unknown>;
  const suppliedMac = record.animus_actor_binding_mac;
  if (typeof suppliedMac !== 'string' || !/^[0-9a-f]{64}$/.test(suppliedMac)) {
    throw new Error(`environment handle '${handle.id}' has malformed actor binding metadata`);
  }
  const expectedMac = actorBindingMac(secret, handle, record);
  if (!timingSafeEqual(Buffer.from(suppliedMac, 'hex'), Buffer.from(expectedMac, 'hex'))) {
    throw new Error(`environment handle '${handle.id}' has invalid actor binding signature`);
  }
  const scope = record.animus_actor_scope;
  const fingerprint = record.animus_actor_fingerprint;
  if (scope === 'system' && (fingerprint === null || fingerprint === undefined)) {
    return actorBinding(null);
  }
  if (scope === 'actor' && typeof fingerprint === 'string' && /^sha256:[0-9a-f]{64}$/.test(fingerprint)) {
    return { scope, fingerprint, actor: null, actorJson: null };
  }
  throw new Error(`environment handle '${handle.id}' has malformed actor binding metadata`);
}

function authorizeActor(expected: ActorBinding, handleId: string, actor: Actor | null | undefined): ActorBinding {
  const actual = actorBinding(actor);
  if (expected.scope !== actual.scope || expected.fingerprint !== actual.fingerprint) {
    throw new Error(`environment exec_session actor does not match handle '${handleId}' binding`);
  }
  return actual;
}

function rewriteActor(params: unknown, binding: ActorBinding): unknown {
  if (params === null || params === undefined) {
    if (binding.actor) return { actor: binding.actor };
    return params;
  }
  if (typeof params !== 'object' || Array.isArray(params)) {
    if (binding.actor) {
      throw new RelayRpcError(
        RelayErrorCode.InvalidParams,
        'actor-bound reverse RPC params must be an object',
      );
    }
    return params;
  }
  const rewritten = { ...(params as Record<string, unknown>) };
  if (binding.actor) rewritten.actor = binding.actor;
  else delete rewritten.actor;
  return rewritten;
}

/** Enforce the handle's trusted actor on every node-originated reverse RPC.
 * `backend/call` wraps the role payload under its own `params`, so both the
 * envelope and that forwarded payload are scrubbed. */
export function makeActorBoundReverseHandler(
  bindings: ReadonlyMap<string, ActorBinding>,
  next: ReverseRpcHandler,
): ReverseRpcHandler {
  return async (method, params, ctx) => {
    const binding = bindings.get(ctx.handleId);
    if (!binding) {
      throw new RelayRpcError(
        RelayErrorCode.InvalidRequest,
        `reverse RPC rejected for unknown or unbound environment handle '${ctx.handleId}'`,
      );
    }
    const outer = rewriteActor(params, binding);
    if (
      method === 'backend/call' &&
      outer !== null &&
      typeof outer === 'object' &&
      !Array.isArray(outer)
    ) {
      const envelope = outer as Record<string, unknown>;
      return await next(method, { ...envelope, params: rewriteActor(envelope.params, binding) }, ctx);
    }
    return await next(method, outer, ctx);
  };
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

const DEFAULT_CLIENT_ID = 'animus-environment-railway';

/** Default hard admission limit for one stable environment-plugin client. */
export const DEFAULT_MAX_MANAGED_NODES = 5;
export const DEFAULT_CAPACITY_LOCK_ROOT = '/tmp/animus-environment-railway-capacity';
export const DEFAULT_CAPACITY_CONFIRMATION_TIMEOUT_MS = 30_000;
const CAPACITY_LOCK_WAIT_MS = 90_000;
const CAPACITY_VISIBILITY_POLL_MS = 100;

/** Stable service prefix for one logical plugin client. The fingerprint keeps
 * capacity accounting restart-safe without exposing the configured client id. */
export function clientServicePrefix(projectId: string, clientId: string): string {
  const digest = createHash('sha256').update(JSON.stringify([projectId, clientId])).digest('hex').slice(0, 8);
  return `${SERVICE_NAME_PREFIX}${digest}-`;
}

/** Deterministic, Railway-valid service name for a run, scoped to one stable
 * plugin client. At 30 characters it remains below Railway's name limit. */
export function deterministicServiceName(projectId: string, clientId: string, runId: string): string {
  const digest = createHash('sha256')
    .update(JSON.stringify([projectId, clientId, runId]))
    .digest('hex')
    .slice(0, 10);
  return `${clientServicePrefix(projectId, clientId)}${digest}`;
}

/** Pre-cap deterministic name retained for cold teardown/reconciliation. */
function legacyDeterministicServiceName(projectId: string, runId: string): string {
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

/** Bound on the pre-teardown cleanup hook (e.g. git commit+push) so a slow or
 *  hung cleanup can never block node teardown (TASK-809). */
export const CLEANUP_TIMEOUT_SECS = 120;

/** Owner-known reap grace floor: a node younger than this may be mid-`prepare`
 *  (the service exists before the daemon has leased/recorded the run), so it is
 *  never a reap candidate even when it maps to no live run id. Matches the dial
 *  timeout default. */
export const REAP_LEAKED_GRACE_SECS = 300;

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

/** Anthropic API credentials exposed by the daemon as a fallback when the
 * shared Claude subscription session cannot be materialized in a run node.
 * These values are daemon-authoritative and therefore override spec.env. */
const ANTHROPIC_FALLBACK_ENV_KEYS = [
  'ANTHROPIC_AUTH_TOKEN',
  'ANTHROPIC_API_KEY',
  'ANTHROPIC_BASE_URL',
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
  /** Workflow-declared cleanup script (TASK-809): a `sh -c` command run IN the
   *  node right before it is destroyed, to flush uncommitted work to its branch
   *  (e.g. `git add -A && git commit -m checkpoint && git push`). Sourced from
   *  `spec.metadata.cleanup`; absent for workflows that declare no cleanup. */
  cleanup?: string | null;
  /** Actor authority marker. The actor body is never persisted in the handle;
   * only its canonical SHA-256 fingerprint is retained for restart reattach. */
  animus_actor_scope: 'actor' | 'system';
  animus_actor_fingerprint: string | null;
  /** HMAC over the handle identity, substrate target, and actor marker. Makes
   * persisted metadata safe to use for restart reattachment. */
  animus_actor_binding_mac: string;
  /** AEAD-sealed relay token + owner token. Never contains plaintext bearer
   * credentials and is covered by `animus_actor_binding_mac`. */
  animus_relay_binding?: string | null;
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
  /** Default cleanup script (TASK-809): a `sh -c` command run IN every node
   *  right before teardown, to flush uncommitted work (e.g.
   *  `git add -A && git commit && git push`). Sourced from `ANIMUS_ENV_CLEANUP`;
   *  a per-run `spec.metadata.cleanup` overrides it. */
  cleanup?: string;
  /** Stable HMAC secret for signed actor-binding metadata. Defaults to the
   * Railway token; configure explicitly to decouple secret rotation. */
  actorBindingSecret?: string;
  /** Stable AEAD secret for sealed relay credentials in handle metadata.
   * Defaults to actorBindingSecret/RAILWAY_TOKEN for backwards-compatible
   * deployments, but should be configured explicitly. */
  relayBindingSecret?: string;
  /** Stable trusted control identity asserted to the relay singleton. */
  relayOwnerId?: string;
  /** Shared singleton control-plane credential. */
  relayControlToken?: string;
  /** Stable logical client id used to scope restart-safe node accounting. */
  clientId?: string;
  /** Hard maximum Railway services this client may manage at once. Defaults
   * to five. Zero disables new prepares without affecting teardown/reap. */
  maxManagedNodes?: number;
  /** Shared local directory for cross-process admission locks. Every plugin
   * process belonging to one logical client must see the same directory. */
  capacityLockRoot?: string;
  /** How long admission keeps its cross-process lock while waiting for a new
   * Railway service to become visible in inventory. */
  capacityConfirmationTimeoutMs?: number;
}

/** Read the config from the process env (the plugin's runtime posture). */
export function configFromEnv(env: NodeJS.ProcessEnv = process.env): RailwayEnvironmentConfig {
  return {
    projectId: env.RAILWAY_PROJECT_ID,
    environmentId: env.RAILWAY_ENVIRONMENT_ID,
    cleanup: env.ANIMUS_ENV_CLEANUP,
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
    actorBindingSecret: env.ANIMUS_ENV_ACTOR_BINDING_SECRET ?? env.RAILWAY_TOKEN,
    relayBindingSecret:
      env.ANIMUS_ENV_RELAY_BINDING_SECRET ?? env.ANIMUS_ENV_ACTOR_BINDING_SECRET ?? env.RAILWAY_TOKEN,
    relayOwnerId: env.ANIMUS_ENV_RELAY_OWNER_ID,
    relayControlToken: env.ANIMUS_ENV_RELAY_CONTROL_TOKEN,
    clientId: env.ANIMUS_ENV_CLIENT_ID,
    maxManagedNodes: env.ANIMUS_ENV_MAX_MANAGED_NODES
      ? Number(env.ANIMUS_ENV_MAX_MANAGED_NODES)
      : undefined,
    capacityLockRoot: env.ANIMUS_ENV_CAPACITY_LOCK_DIR,
    capacityConfirmationTimeoutMs: env.ANIMUS_ENV_CAPACITY_CONFIRMATION_TIMEOUT_MS
      ? Number(env.ANIMUS_ENV_CAPACITY_CONFIRMATION_TIMEOUT_MS)
      : undefined,
  };
}

function validatedMaxManagedNodes(value: number | undefined): number {
  const limit = value ?? DEFAULT_MAX_MANAGED_NODES;
  if (!Number.isSafeInteger(limit) || limit < 0) {
    throw new Error(
      `ANIMUS_ENV_MAX_MANAGED_NODES must be a non-negative integer (received ${String(value)})`,
    );
  }
  return limit;
}

function validatedCapacityConfirmationTimeoutMs(value: number | undefined): number {
  const timeout = value ?? DEFAULT_CAPACITY_CONFIRMATION_TIMEOUT_MS;
  if (!Number.isSafeInteger(timeout) || timeout < 0) {
    throw new Error(
      `ANIMUS_ENV_CAPACITY_CONFIRMATION_TIMEOUT_MS must be a non-negative integer (received ${String(value)})`,
    );
  }
  return timeout;
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
 *  cannot rotate anything. Best-effort: returns {} when there is no usable
 *  login or refresh fails, allowing the node to use its Anthropic API fallback. */
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
    process.stderr.write(
      '[animus-environment-railway] claude credential file is unreadable or invalid; using Anthropic API fallback if configured\n',
    );
    return {};
  }
  const expiresAt = Number(oauth.expiresAt ?? oauth.expires_at ?? 0);
  const accessTokenValue = oauth.accessToken ?? oauth.access_token;
  const refreshTokenValue = oauth.refreshToken ?? oauth.refresh_token;
  const accessToken = typeof accessTokenValue === 'string' ? accessTokenValue.trim() : '';
  const refreshToken = typeof refreshTokenValue === 'string' ? refreshTokenValue.trim() : '';
  if (accessToken && expiresAt && expiresAt - now > CLAUDE_REFRESH_SKEW_MS) {
    // Access token still valid: inject it as-is, minus the refresh token.
    return { ANIMUS_NODE_CLAUDE_CREDENTIALS_B64: encodeNodeClaudeCreds(file, oauth) };
  }
  if (!refreshToken) {
    process.stderr.write(
      '[animus-environment-railway] claude subscription credential has no usable access or refresh token; using Anthropic API fallback if configured\n',
    );
    return {};
  }
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
    const t = (await res.json()) as { access_token?: string; refresh_token?: string; expires_in?: number };
    if (typeof t.access_token !== 'string' || t.access_token.trim() === '') {
      process.stderr.write(
        '[animus-environment-railway] claude token refresh returned no access token; using Anthropic API fallback if configured\n',
      );
      return {};
    }
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
  actorJson?: string | null;
}): Record<string, string> {
  const hostEnv = args.hostEnv ?? process.env;
  const vars: Record<string, string> = {};
  // BASE_DB_URL passthrough: cloud deployments hand every run container the
  // shared database endpoint the daemon itself uses.
  if (hostEnv.BASE_DB_URL) vars.BASE_DB_URL = hostEnv.BASE_DB_URL;
  Object.assign(vars, args.specEnv ?? {});
  // Reserved authority channel: a workflow-authored spec can neither spoof an
  // actor nor leave one on a system-scoped node.
  delete vars[ACTOR_ENV];
  // The daemon's API fallback wins over spec.env so a dispatched task cannot
  // replace the configured provider endpoint or credential on its own node.
  for (const key of ANTHROPIC_FALLBACK_ENV_KEYS) {
    const value = hostEnv[key];
    if (value !== undefined && value !== '') vars[key] = value;
  }
  // Subscription creds + GitHub token win over spec env (they are the daemon's
  // authoritative harness auth, base64'd for the run-image bootstrap).
  Object.assign(vars, harnessCredentialVars(hostEnv));
  // Relay coordinates always win (they are the run's identity).
  vars.ANIMUS_ENV_WSS_URL = args.wssUrl;
  vars.ANIMUS_ENV_RUN_TOKEN = args.token;
  vars.ANIMUS_ENV_WORKSPACE_ROOT = WORKSPACE_ROOT;
  if (args.actorJson) vars[ACTOR_ENV] = args.actorJson;
  return vars;
}

/** owner/repo parsed from a github remote url (https or ssh), or null. */
export function parseGithubSlug(url: string | undefined): { owner: string; repo: string } | null {
  if (!url) return null;
  const m = url.match(/github\.com[:/]+([^/]+)\/([^/]+?)(?:\.git)?\/?$/i);
  return m && m[1] && m[2] ? { owner: m[1], repo: m[2] } : null;
}

/** Sign a short-lived (<=10 min) GitHub App JWT (RS256) with the app private key.
 *  Exported for the credentials servicer (on-demand re-minting for nodes). */
export function githubAppJwt(appId: string, privateKeyPem: string, now: number): string {
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
  relay?: RelayTransport;
  config?: RailwayEnvironmentConfig;
}

/** The substrate implementation behind the plugin's four methods. */
export class RailwayEnvironment {
  private readonly config: RailwayEnvironmentConfig;
  private railwayApi: RailwayApi | null;
  private relayInstance: RelayTransport | null;
  /** In-memory authority for live relay handles. Handle metadata is used only
   * to rehydrate this map after a plugin restart, after matching ctx.actor. */
  private readonly actorBindings = new Map<string, ActorBinding>();
  private readonly actorBindingSecret: string;
  private readonly relayBindingSecret: string;
  /** Legacy per-process identity retained so GC can recognize pre-cap random
   * service names during a rolling upgrade. New names use the stable client id. */
  readonly instanceId: string = shortId('i');
  private readonly clientId: string;
  private readonly maxManagedNodes: number;
  private readonly capacityConfirmationTimeoutMs: number;
  /** Services created by this live client. Unioned with Railway's listing so
   * eventual list consistency cannot let concurrent prepares oversubscribe. */
  private readonly locallyManagedServices = new Map<
    string,
    { scope: string; name: string; environmentId: string }
  >();
  /** Serialize list/reconcile/create admission for concurrent prepare RPCs. */
  private admissionTail: Promise<void> = Promise.resolve();

  private backendClient: PluginClient | null = null;
  private logClient: PluginClient | null = null;

  constructor(deps: RailwayEnvironmentDeps = {}) {
    this.config = deps.config ?? configFromEnv();
    this.railwayApi = deps.railway ?? null;
    this.relayInstance = deps.relay ?? null;
    // A process-local fallback keeps live authorization safe in tests or
    // injected deployments without credentials, but intentionally cannot
    // reattach across restart. Production config derives a stable key from
    // RAILWAY_TOKEN unless ANIMUS_ENV_ACTOR_BINDING_SECRET is explicit.
    this.actorBindingSecret = this.config.actorBindingSecret?.trim() || randomBytes(32).toString('hex');
    this.relayBindingSecret = this.config.relayBindingSecret?.trim() || this.actorBindingSecret;
    this.clientId = this.config.clientId?.trim() || this.config.relayOwnerId?.trim() || DEFAULT_CLIENT_ID;
    this.maxManagedNodes = validatedMaxManagedNodes(this.config.maxManagedNodes);
    this.capacityConfirmationTimeoutMs = validatedCapacityConfirmationTimeoutMs(
      this.config.capacityConfirmationTimeoutMs,
    );
  }

  private async capacitySocketIsActive(socketPath: string): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      const socket = createConnection(socketPath);
      let settled = false;
      const finish = (active: boolean): void => {
        if (settled) return;
        settled = true;
        socket.destroy();
        resolve(active);
      };
      socket.once('connect', () => finish(true));
      socket.once('error', (error: NodeJS.ErrnoException) => {
        if (error.code === 'ECONNREFUSED' || error.code === 'ENOENT') {
          finish(false);
          return;
        }
        if (settled) return;
        settled = true;
        socket.destroy();
        reject(error);
      });
    });
  }

  private async tryListenCapacitySocket(socketPath: string): Promise<Server | null> {
    return new Promise<Server | null>((resolve, reject) => {
      const server = createServer((socket) => socket.destroy());
      const onError = (error: NodeJS.ErrnoException): void => {
        server.removeAllListeners();
        if (error.code === 'EADDRINUSE') {
          resolve(null);
          return;
        }
        reject(error);
      };
      server.once('error', onError);
      server.listen(socketPath, () => {
        server.off('error', onError);
        resolve(server);
      });
    });
  }

  private async acquireCapacityLock(scope: string): Promise<() => Promise<void>> {
    const root = this.config.capacityLockRoot?.trim() || DEFAULT_CAPACITY_LOCK_ROOT;
    // Linux abstract sockets are owned entirely by the kernel: they are
    // exclusive across processes and disappear atomically on process death,
    // so no stale pathname can race with a replacement owner. macOS/BSD use a
    // compact filesystem socket for local development and tests.
    const lockId = createHash('sha256').update(JSON.stringify([root, scope])).digest('hex').slice(0, 24);
    const filesystemSocket = process.platform !== 'linux';
    if (filesystemSocket) await mkdir(root, { recursive: true });
    const socketPath = filesystemSocket ? join(root, `${lockId}.sock`) : `\0animus-env-cap-${lockId}`;
    const deadline = Date.now() + CAPACITY_LOCK_WAIT_MS;

    while (true) {
      const server = await this.tryListenCapacitySocket(socketPath);
      if (server) {
        return async () => {
          await new Promise<void>((resolve) => server.close(() => resolve())).catch(() => undefined);
          if (filesystemSocket) await rm(socketPath, { force: true }).catch(() => undefined);
        };
      }

      // A crashed process leaves only a dead socket pathname. Probe before
      // removing it so a slow/old owner can never have its live lock stolen.
      if (!(await this.capacitySocketIsActive(socketPath))) {
        if (filesystemSocket) {
          await rm(socketPath, { force: true }).catch((error: NodeJS.ErrnoException) => {
            if (error.code !== 'ENOENT') throw error;
          });
        }
        continue;
      }
      if (Date.now() >= deadline) {
        throw new Error(
          `timed out waiting for Railway node capacity lock for client '${this.clientId}'; ` +
            'failing closed without creating a node',
        );
      }
      await new Promise((resolve) => setTimeout(resolve, 50 + Math.floor(Math.random() * 50)));
    }
  }

  private async withAdmissionLock<T>(scope: string, operation: () => Promise<T>): Promise<T> {
    const previous = this.admissionTail;
    let release!: () => void;
    this.admissionTail = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    let releaseFileLock: (() => Promise<void>) | undefined;
    try {
      releaseFileLock = await this.acquireCapacityLock(scope);
      return await operation();
    } finally {
      await releaseFileLock?.();
      release();
    }
  }

  private async confirmCreatedServiceVisible(args: {
    projectId: string;
    serviceId: string;
    serviceName: string;
  }): Promise<void> {
    const deadline = Date.now() + this.capacityConfirmationTimeoutMs;
    let lastListError: unknown;
    while (true) {
      try {
        const services = await this.railway().listRunServices(args.projectId);
        if (services.some((service) => service.id === args.serviceId && service.name === args.serviceName)) return;
        lastListError = undefined;
      } catch (error) {
        lastListError = error;
      }

      const remainingMs = deadline - Date.now();
      if (remainingMs <= 0) break;
      await new Promise((resolve) => setTimeout(resolve, Math.min(CAPACITY_VISIBILITY_POLL_MS, remainingMs)));
    }

    const listFailure =
      lastListError instanceof Error ? `; last inventory error: ${lastListError.message}` : '';
    throw new Error(
      `Railway did not confirm created service '${args.serviceName}' (${args.serviceId}) in inventory within ` +
        `${this.capacityConfirmationTimeoutMs}ms; failing closed${listFailure}`,
    );
  }

  private async createWithinCapacity(args: {
    projectId: string;
    environmentId: string;
    serviceName: string;
    legacyServiceName?: string;
    image: string;
    variables: Record<string, string>;
  }): Promise<{ serviceId: string; deploymentId?: string | null }> {
    const admissionScope = JSON.stringify([args.projectId, this.clientId]);
    return this.withAdmissionLock(admissionScope, async () => {
      const railway = this.railway();
      const scope = clientServicePrefix(args.projectId, this.clientId);
      const services = await railway.listRunServices(args.projectId);

      // Same-run reconciliation belongs inside admission: the replaced service
      // must not consume capacity, and concurrent retries must not both create.
      const replaceNames = new Set(
        [args.serviceName, args.legacyServiceName].filter((name): name is string => Boolean(name)),
      );
      for (const existing of services.filter((service) => replaceNames.has(service.name))) {
        await railway.deleteService(existing.id, args.environmentId);
        this.locallyManagedServices.delete(existing.id);
      }
      for (const [serviceId, local] of this.locallyManagedServices) {
        if (local.scope === scope && replaceNames.has(local.name)) {
          await railway.deleteService(serviceId, local.environmentId);
          this.locallyManagedServices.delete(serviceId);
        }
      }

      const remaining = services.filter((service) => !replaceNames.has(service.name));
      const managedIds = new Set(
        remaining
          .filter(
            (service) =>
              service.name.startsWith(scope) ||
              // Pre-cap names do not encode a logical client. Count them
              // conservatively for every client rather than risk exceeding a
              // configured safety limit during a rolling upgrade.
              /^animus-run-[0-9a-f]{12}$/.test(service.name) ||
              /^animus-run-i[0-9a-f]{6}-/.test(service.name),
          )
          .map((service) => service.id),
      );
      for (const [serviceId, local] of this.locallyManagedServices) {
        if (local.scope === scope) managedIds.add(serviceId);
      }

      if (managedIds.size >= this.maxManagedNodes) {
        throw new Error(
          `Railway node capacity exhausted for client '${this.clientId}': ` +
            `${managedIds.size}/${this.maxManagedNodes} managed nodes; wait for teardown or run ` +
            '`animus environment reap`, or raise ANIMUS_ENV_MAX_MANAGED_NODES intentionally',
        );
      }

      const created = await railway.createRunService({
        projectId: args.projectId,
        environmentId: args.environmentId,
        name: args.serviceName,
        image: args.image,
        variables: args.variables,
        startCommand: this.config.bridgeCommand ?? DEFAULT_BRIDGE_COMMAND,
        registryCredentials: this.config.registryCredentials,
      });
      this.locallyManagedServices.set(created.serviceId, {
        scope,
        name: args.serviceName,
        environmentId: args.environmentId,
      });
      try {
        // Keep the cross-process admission lock until Railway's authoritative
        // inventory exposes this create. Without this barrier, a second plugin
        // process can observe stale capacity and oversubscribe the client.
        await this.confirmCreatedServiceVisible({
          projectId: args.projectId,
          serviceId: created.serviceId,
          serviceName: args.serviceName,
        });
      } catch (confirmationError) {
        let rollbackError: unknown;
        try {
          await railway.deleteService(created.serviceId, args.environmentId);
        } catch (error) {
          rollbackError = error;
        } finally {
          this.locallyManagedServices.delete(created.serviceId);
        }
        if (rollbackError instanceof Error) {
          const confirmationMessage =
            confirmationError instanceof Error ? confirmationError.message : String(confirmationError);
          throw new Error(`${confirmationMessage}; rollback delete failed: ${rollbackError.message}`);
        }
        throw confirmationError;
      }
      return created;
    });
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
      // The `credentials` role serves on-demand GitHub token re-mints for the
      // node's git credential helper (TASK-855) — installation-wide scope, App
      // private key never crosses the relay. Wired alongside the backend
      // passthrough only (an unwired reverse RPC keeps its existing semantics:
      // nodes fall back to their own local backends).
      this.relayInstance = await RelayClient.connect({
        socketPath: this.config.relaySocketPath,
        actorId: this.config.relayOwnerId?.trim() || 'animus-environment-railway',
        controlToken: this.config.relayControlToken,
        ...(backend
          ? {
              onReverseRpc: makeActorBoundReverseHandler(
                this.actorBindings,
                makeBackendCallHandler({
                  default: backend,
                  ...(logBackend ? { log_storage: logBackend } : {}),
                  credentials: makeCredentialsServicer(process.env),
                }),
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
  async prepare(
    req: PrepareRequest,
    actor?: Actor,
    rawActor: unknown = (req as Record<string, unknown>).actor,
    rawActorPresent = Object.prototype.hasOwnProperty.call(req, 'actor'),
  ): Promise<{ handle: EnvironmentHandle }> {
    validateRawActor(rawActor, actor, rawActorPresent);
    const spec = req.spec;
    const { projectId, environmentId } = resolveTarget(spec, this.config);
    const image = spec.image?.trim() || DEFAULT_IMAGE;
    // A broker-supplied run id names the node deterministically (reconcilable +
    // cold-reapable by run id); otherwise fall back to the per-run random id +
    // per-instance prefix. The relay run id is kept random regardless so a repeat
    // prepare for the same run never collides on an already-registered relay run.
    const runId = specRunId(spec);
    const id = secureHandleId();
    const binding = actorBinding(actor);
    // Bind before register/create: a node can dial and issue reverse RPC as soon
    // as Railway starts it, so no unbound authorization window may exist.
    this.actorBindings.set(id, binding);
    const serviceName = runId
      ? deterministicServiceName(projectId, this.clientId, runId)
      : `${clientServicePrefix(projectId, this.clientId)}${handleServiceToken(id)}`;
    const legacyServiceName = runId ? legacyDeterministicServiceName(projectId, runId) : undefined;

    let relay: RelayTransport | undefined;
    let url: string;
    let token: string;
    let ownerToken: string | undefined;
    let plan: WorkspacePlan;
    try {
      relay = await this.relay();
      ({ url, token, ownerToken } = await relay.registerRun(id));
      plan = planWorkspace(spec, WORKSPACE_ROOT);
      // Validate every planned subdir up front (a spec-supplied repo `name` like
      // `../outside` must never escape the workspace root or poison the default
      // cwd metadata). Fail BEFORE creating the service.
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
      if (relay) await Promise.resolve(relay.releaseRun(id)).catch(() => undefined);
      this.actorBindings.delete(id);
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
      const created = await this.createWithinCapacity({
        projectId,
        environmentId,
        serviceName,
        legacyServiceName,
        image,
        variables: {
          ...runVariables({ wssUrl: url, token, specEnv: spec.env, actorJson: binding.actorJson }),
          ...claudeVars,
          ...appVars,
        },
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
      await Promise.resolve(relay.releaseRun(id)).catch(() => undefined);
      this.actorBindings.delete(id);
      if (serviceId) {
        await this.railway()
          .deleteService(serviceId, environmentId)
          .catch(() => undefined);
        this.locallyManagedServices.delete(serviceId);
      }
      throw err;
    }

    const specMeta = (spec.metadata ?? {}) as Record<string, unknown>;
    // Per-run `spec.metadata.cleanup` overrides the global `ANIMUS_ENV_CLEANUP`
    // default; either yields a `sh -c` script run in-node just before teardown.
    const cleanup =
      typeof specMeta.cleanup === 'string' && specMeta.cleanup.trim().length > 0
        ? specMeta.cleanup.trim()
        : this.config.cleanup && this.config.cleanup.trim().length > 0
          ? this.config.cleanup.trim()
          : null;
    const unsignedMetadata = {
      service_id: serviceId,
      service_name: serviceName,
      project_id: projectId,
      environment_id: environmentId,
      deployment_id: deploymentId ?? null,
      image,
      primary_subdir: plan.primarySubdir,
      animus_run_id: runId,
      cleanup,
      animus_actor_scope: binding.scope,
      animus_actor_fingerprint: binding.fingerprint,
      animus_relay_binding: ownerToken
        ? sealRelayBinding(this.relayBindingSecret, id, { token, ownerToken })
        : null,
    };
    const metadata: RailwayHandleMeta = {
      ...unsignedMetadata,
      animus_actor_binding_mac: actorBindingMac(
        this.actorBindingSecret,
        { id, workspace_root: WORKSPACE_ROOT, metadata: unsignedMetadata },
        unsignedMetadata,
      ),
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
      execution_fence?: unknown;
    },
    onJournal?: (event: JournalEventParams) => void,
    actor?: Actor,
    rawActor?: unknown,
    rawActorPresent = rawActor !== undefined,
  ): Promise<SessionResult> {
    validateRawActor(rawActor, actor, rawActorPresent);
    // A live map entry is authoritative and can never be overwritten from
    // caller-supplied metadata. A restart may rehydrate only from a handle whose
    // actor marker has a valid server-side HMAC, then only for the matching
    // authenticated call actor.
    const persisted = bindingFromHandle(handle, this.actorBindingSecret);
    const live = this.actorBindings.get(handle.id);
    if (
      live &&
      (live.scope !== persisted.scope || live.fingerprint !== persisted.fingerprint)
    ) {
      throw new Error(`environment handle '${handle.id}' actor binding metadata does not match live authority`);
    }
    const binding = authorizeActor(live ?? persisted, handle.id, actor);
    if (!live) this.actorBindings.set(handle.id, binding);
    const executionFence = validateExecutionFence(params);
    const relay = await this.relay();
    if (!relay.registeredRuns().includes(handle.id)) {
      const credentials = openRelayBinding(this.relayBindingSecret, handle);
      if (!credentials || !relay.attachRun) {
        throw new Error(`environment handle '${handle.id}' cannot reattach to the relay after restart`);
      }
      await relay.attachRun(handle.id, credentials);
      const dialTimeoutMs = (this.config.dialTimeoutSecs ?? 300) * 1000;
      await relay.waitForConnection(handle.id, dialTimeoutMs);
    }
    const result = await relay.runSession(
      handle.id,
      { ...params, execution_fence: executionFence },
      onJournal,
    );
    if (executionFence) {
      if (result.workflow_id !== executionFence.workflow_id) {
        throw new Error('environment exec_session response workflow_id does not match execution_fence');
      }
      if (!sameExecutionFence(result.execution_fence, executionFence)) {
        throw new Error('environment exec_session response did not echo the exact execution_fence');
      }
    } else if (result.execution_fence !== undefined && result.execution_fence !== null) {
      throw new Error('environment exec_session response returned an undelegated execution_fence');
    }
    return result;
  }

  /** `teardown`: delete the Railway service and release the relay run.
   *  Idempotent — a missing service or unknown handle is a successful no-op. */
  async teardown(handle: EnvironmentHandle): Promise<void> {
    const meta = handle.metadata as RailwayHandleMeta | undefined;
    // Pre-teardown cleanup hook (TASK-809): run a workflow-declared command IN the
    // node (e.g. `git add -A && git commit && git push`) to flush uncommitted work
    // to its branch BEFORE the node is destroyed. Best-effort + bounded, attempted
    // only while this instance's relay is still connected (graceful teardown); a
    // failed/slow cleanup never blocks teardown, and a crashed node is covered by
    // incremental pushes during the run.
    const cleanup = meta?.cleanup?.trim() || this.config.cleanup?.trim();
    if (cleanup && this.relayInstance) {
      try {
        await this.execCommand(handle, { program: 'sh', args: ['-c', cleanup] }, null, CLEANUP_TIMEOUT_SECS);
      } catch {
        // best-effort: a failed cleanup must not block node teardown
      }
    }
    // Cleanup may itself need actor-bound reverse RPC (for example an on-demand
    // credential call). Revoke authority immediately after that bounded hook,
    // before releasing the relay registration or deleting the substrate node.
    this.actorBindings.delete(handle.id);
    const credentials = openRelayBinding(this.relayBindingSecret, handle);
    if (this.relayInstance?.registeredRuns().includes(handle.id)) {
      await Promise.resolve(this.relayInstance.releaseRun(handle.id, credentials ?? undefined));
    } else if (credentials) {
      const relay = await this.relay();
      await Promise.resolve(relay.releaseRun(handle.id, credentials));
    }
    const environmentId = meta?.environment_id || this.config.environmentId;
    // Fast path: a full handle carries the service_id; delete it directly.
    if (meta?.service_id) {
      if (!environmentId) {
        throw new Error(
          `cannot tear down service '${meta.service_id}': handle metadata has no environment_id and RAILWAY_ENVIRONMENT_ID is unset`,
        );
      }
      await this.railway().deleteService(meta.service_id, environmentId);
      this.locallyManagedServices.delete(meta.service_id);
      return;
    }
    // Cold path: only a run id (no service_id) — resolve the service by its
    // deterministic name and delete it. This closes the crash window where a
    // service was created but the caller never received the full handle.
    const runId = meta?.animus_run_id?.trim();
    const projectId = meta?.project_id || this.config.projectId;
    if (!runId || !projectId || !environmentId) return;
    const serviceNames = new Set([
      deterministicServiceName(projectId, this.clientId, runId),
      legacyDeterministicServiceName(projectId, runId),
    ]);
    const match = (await this.railway().listRunServices(projectId)).find((s) => serviceNames.has(s.name));
    if (match) {
      await this.railway().deleteService(match.id, environmentId);
      this.locallyManagedServices.delete(match.id);
    }
  }

  /** GC sweep: delete orphaned run services (no live relay registration).
   *
   *  Default scope is only services this process created plus its pre-cap
   *  instance prefix. Stable client names are intentionally not enough proof
   *  of orphanhood because another plugin process may own them. Pass
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
    const clientPrefix = clientServicePrefix(target, this.clientId);
    const liveHandles = new Set(
      (this.relayInstance?.registeredRuns() ?? []).flatMap((handleId) => [
        `${clientPrefix}${handleServiceToken(handleId)}`,
        `${this.instancePrefix()}${handleServiceToken(handleId)}`,
      ]),
    );
    const removed: string[] = [];
    for (const svc of services) {
      if (!svc.name.startsWith(SERVICE_NAME_PREFIX)) continue;
      if (!opts.allInstances && !this.locallyManagedServices.has(svc.id) && !svc.name.startsWith(this.instancePrefix())) {
        continue;
      }
      if (liveHandles.has(svc.name)) continue;
      await this.railway().deleteService(svc.id, environmentId);
      this.locallyManagedServices.delete(svc.id);
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
    // Restart-recoverable teardown: accept a substrate service id, a literal
    // service name, OR a bare animus run id — resolving the run id to its
    // deterministic service name so a daemon that persisted only the run id can
    // drive teardown after losing the in-memory handle (TASK-797/TASK-933).
    const byRunName = deterministicServiceName(projectId, this.clientId, idOrName);
    const byLegacyRunName = legacyDeterministicServiceName(projectId, idOrName);
    const match = (await this.railway().listRunServices(projectId)).find(
      (s) =>
        s.id === idOrName ||
        s.name === idOrName ||
        s.name === byRunName ||
        s.name === byLegacyRunName,
    );
    if (!match) return [];
    await this.railway().deleteService(match.id, environmentId);
    this.locallyManagedServices.delete(match.id);
    return [match.id];
  }

  /** Node-management (`environment/reap`): delete orphaned/dead nodes.
   *
   *  Default (no opts): reap ONLY services whose latest deployment is dead
   *  (FAILED/CRASHED/REMOVED) — always safe, since a healthy live node is never
   *  in a dead state. `all` additionally reaps non-dead services that have no
   *  live owning run, but ONLY with `force` (a fresh, non-resident process has
   *  no in-memory liveness, so it must not assume every healthy node is an
   *  orphan). `dry_run` reports the plan without deleting.
   *
   *  Owner-known mode: when `liveRunIds` (the daemon's authoritative set of live
   *  run ids) is supplied, a healthy node whose name maps to NO live run id is a
   *  leak and is reaped WITHOUT `force` — with a mandatory {@link
   *  REAP_LEAKED_GRACE_SECS} floor so an in-flight `prepare` is never reaped. */
  async reap(
    opts: {
      all?: boolean;
      force?: boolean;
      dryRun?: boolean;
      olderThanSecs?: number;
      /** Daemon's authoritative live-run set. Present => owner-known mode:
       *  healthy nodes mapping to no live run id are reaped WITHOUT `force`. */
      liveRunIds?: string[];
    } = {},
  ): Promise<{ deleted: string[]; kept: EnvironmentNodeDescriptor[]; dryRun: boolean }> {
    const projectId = this.config.projectId;
    const environmentId = this.config.environmentId;
    if (!projectId) throw new Error('reap needs a project id (RAILWAY_PROJECT_ID)');
    if (!environmentId) throw new Error('reap needs an environment id (RAILWAY_ENVIRONMENT_ID)');
    const nodes = await this.describeNodes(projectId);
    // Protected set = names of runs KNOWN to be live. Two authoritative sources,
    // unioned: (1) this process's in-memory relay registrations (only meaningful
    // in the resident daemon), and (2) the daemon-supplied live run ids mapped to
    // their deterministic service names (works from a fresh CLI process too).
    const liveNames = new Set(
      (this.relayInstance?.registeredRuns() ?? []).map(
        (handleId) => `${clientServicePrefix(projectId, this.clientId)}${handleServiceToken(handleId)}`,
      ),
    );
    const ownerKnown = opts.liveRunIds !== undefined;
    for (const rid of opts.liveRunIds ?? []) {
      liveNames.add(deterministicServiceName(projectId, this.clientId, rid));
      liveNames.add(legacyDeterministicServiceName(projectId, rid));
    }
    // In owner-known mode enforce a grace floor so a mid-prepare node (created
    // before the daemon leased its run) is never reaped out from under itself.
    const graceSecs = ownerKnown
      ? Math.max(opts.olderThanSecs ?? 0, REAP_LEAKED_GRACE_SECS)
      : opts.olderThanSecs;
    const now = Date.now();
    const deleted: string[] = [];
    const kept: EnvironmentNodeDescriptor[] = [];
    for (const node of nodes) {
      const dead = DEAD_DEPLOYMENT_STATES.has(node.state.toUpperCase());
      const live = liveNames.has(node.name);
      const oldEnough =
        graceSecs === undefined ||
        (node.created_at ? (now - Date.parse(node.created_at)) / 1000 >= graceSecs : true);
      let reapIt = false;
      if (dead && oldEnough) reapIt = true;
      // Owner-known: a healthy node mapping to no live run id is a leak — reap it
      // WITHOUT `force` (the daemon's live set is authoritative). Legacy path
      // (no live set supplied) keeps the `all`+`force` empty-liveness guard.
      else if (!live && oldEnough && (ownerKnown || (opts.all && opts.force))) reapIt = true;
      if (!reapIt) {
        kept.push(node);
        continue;
      }
      if (!opts.dryRun) {
        await this.railway().deleteService(node.id, environmentId);
        this.locallyManagedServices.delete(node.id);
      }
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
    this.actorBindings.clear();
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
