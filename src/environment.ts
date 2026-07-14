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

import { randomBytes } from 'node:crypto';

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
  // Relay coordinates always win over spec env (they are the run's identity).
  vars.ANIMUS_ENV_WSS_URL = args.wssUrl;
  vars.ANIMUS_ENV_RUN_TOKEN = args.token;
  vars.ANIMUS_ENV_WORKSPACE_ROOT = WORKSPACE_ROOT;
  return vars;
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
    const id = shortId('r');
    const serviceName = `${this.instancePrefix()}${id}`;

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
    try {
      const created = await this.railway().createRunService({
        projectId,
        environmentId,
        name: serviceName,
        image,
        variables: runVariables({ wssUrl: url, token, specEnv: spec.env }),
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
    if (!meta?.service_id) return;
    const environmentId = meta.environment_id || this.config.environmentId;
    if (!environmentId) {
      throw new Error(
        `cannot tear down service '${meta.service_id}': handle metadata has no environment_id and RAILWAY_ENVIRONMENT_ID is unset`,
      );
    }
    await this.railway().deleteService(meta.service_id, environmentId);
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
