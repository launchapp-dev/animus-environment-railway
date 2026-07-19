#!/usr/bin/env node
// animus-environment-railway — Railway ephemeral-container execution-environment
// plugin (v0.7).
//
// One process, one role (`environment`). The JSON-RPC machinery (serve loop,
// `--manifest`, lifecycle, param validation, `environment/output` streaming)
// lives in `animus-environment-base`; the WS relay the containers dial home to
// lives in `animus-env-transport`; this binary maps the four environment
// methods onto the Railway GraphQL API + that relay (see ./environment.ts).

import { defineEnvironmentPlugin } from '@launchapp-dev/animus-environment-base';

import { RailwayEnvironment } from './environment.js';

const env = new RailwayEnvironment();

const plugin = defineEnvironmentPlugin({
  name: 'animus-environment-railway',
  version: '0.4.14',
  description:
    'Railway ephemeral-container execution-environment plugin for Animus (v0.7). Creates a Railway service from the base image, relays harness commands over an outbound WebSocket the container dials home, and deletes the service on teardown.',
  env_required: [
    {
      name: 'RAILWAY_TOKEN',
      description: 'Railway API token used to create/delete run services.',
      required: true,
      sensitive: true,
    },
    {
      name: 'RAILWAY_PROJECT_ID',
      description: 'Railway project run services are created in (spec.metadata.railway_project_id overrides per-run).',
      required: false,
    },
    {
      name: 'RAILWAY_ENVIRONMENT_ID',
      description:
        'Railway environment within the project (spec.metadata.railway_environment_id overrides per-run).',
      required: false,
    },
    {
      name: 'ANIMUS_ENV_RELAY_PUBLIC_URL',
      description:
        'Public WSS URL run containers dial home to (e.g. wss://<daemon-domain>). TLS is terminated at the platform edge.',
      required: false,
    },
    {
      name: 'ANIMUS_ENV_RELAY_PORT',
      description:
        'Fixed port the in-plugin relay binds; must equal the port the public WSS URL routes to. Required whenever ANIMUS_ENV_RELAY_PUBLIC_URL is set.',
      required: false,
    },
    {
      name: 'ANIMUS_ENV_RAILWAY_IMAGE',
      description:
        'Base image for run services (overrides the built-in default). Must ship the animus-env-bridge entrypoint.',
      required: false,
    },
    {
      name: 'ANIMUS_ENV_REGISTRY_USERNAME',
      description:
        'Registry username for pulling a private run image (e.g. a GitHub username for ghcr). Set together with ANIMUS_ENV_REGISTRY_PASSWORD; both required for the pull credentials to apply.',
      required: false,
    },
    {
      name: 'ANIMUS_ENV_REGISTRY_PASSWORD',
      description:
        'Registry password/token for pulling a private run image (e.g. a GitHub PAT with read:packages for ghcr). Injected into the Railway service so a private ANIMUS_ENV_RAILWAY_IMAGE can be pulled.',
      required: false,
      sensitive: true,
    },
    {
      name: 'ANIMUS_ENV_UPSTREAM_BACKEND_BIN',
      description:
        "Path to the parent-side backend plugin (e.g. this project's animus-postgres) a lean node's backend/call is serviced against. Set together with DATABASE_URL to enable nested 'animus inside animus' state proxying; stays on the parent, never sent to the node.",
      required: false,
    },
    {
      name: 'DATABASE_URL',
      description:
        "Parent Postgres URL handed to ANIMUS_ENV_UPSTREAM_BACKEND_BIN so a node's proxied subject/config/queue/journal calls resolve against the parent DB. Never injected into the node.",
      required: false,
      sensitive: true,
    },
    {
      name: 'ANIMUS_ENV_UPSTREAM_LOG_BIN',
      description:
        "Path to the parent-side log-storage plugin (default /app/.animus/plugins/animus-log-storage-s3) a node's log_storage/* backend/call is serviced against, so run logs offload to the SAME bucket the daemon uses instead of animus-postgres. Wired only when the parent's S3 env (S3_BUCKET/S3_ACCESS_KEY_ID/S3_SECRET_ACCESS_KEY, plus optional S3_ENDPOINT/S3_REGION/S3_PREFIX/S3_FORCE_PATH_STYLE) is present; those S3 vars are forwarded to the plugin and never sent to the node.",
      required: false,
    },
    {
      name: 'ANIMUS_ENV_BRIDGE_COMMAND',
      description: 'Start command for the run container (default: animus-env-bridge).',
      required: false,
    },
    {
      name: 'ANIMUS_ENV_DIAL_TIMEOUT_SECS',
      description: 'How long prepare waits for the container to dial home (default 300).',
      required: false,
    },
    {
      name: 'ANIMUS_ENV_CLEANUP',
      description:
        'Default cleanup script (sh -c) run IN every node right before teardown, to flush uncommitted work to its branch (e.g. `git add -A && git commit -m checkpoint && git push origin HEAD || true`). Best-effort + bounded; a per-run spec.metadata.cleanup overrides it.',
      required: false,
    },
    {
      name: 'CLAUDE_CONFIG_DIR',
      description:
        'Daemon-side dir holding the Claude subscription .credentials.json; base64-injected into each node so the claude harness runs on the subscription.',
      required: false,
    },
    {
      name: 'CODEX_OAUTH_HOME',
      description:
        'Daemon-side dir holding the Codex ChatGPT-subscription auth.json; base64-injected into each node so the codex harness runs on the subscription. Defaults to the durable portal path /data/animus-state/codex-config when unset, so codex works on nodes without extra portal config.',
      required: false,
    },
    {
      name: 'GITHUB_TOKEN',
      description: 'GitHub token injected into each node for git push + PR creation from the harness (fallback when no GitHub App is configured).',
      required: false,
      sensitive: true,
    },
    {
      name: 'GITHUB_APP_ID',
      description: 'GitHub App id; when set with GITHUB_APP_PRIVATE_KEY, prepare mints a repo-scoped installation token so the node pushes + opens PRs as the app.',
      required: false,
    },
    {
      name: 'GITHUB_APP_PRIVATE_KEY',
      description: 'GitHub App private key (PEM) used to sign the app JWT that mints the per-run installation token.',
      required: false,
      sensitive: true,
    },
    {
      name: 'GITHUB_APP_SLUG',
      description: 'GitHub App slug; sets the bot commit identity (<slug>[bot]) so commits are attributed to the app.',
      required: false,
    },
  ],

  prepare: (params) => env.prepare(params),

  exec: (params) => env.execCommand(params.handle, params.command, params.stdin, params.timeout_secs),

  execStream: (params, emit) =>
    env.execCommand(params.handle, params.command, params.stdin, params.timeout_secs, (stream, text) => {
      emit({ kind: 'output', handle_id: params.handle.id, stream, text });
    }),

  execSession: async (params, emit) => {
    const result = await env.runSession(
      params.handle,
      {
        subject_id: params.subject_id,
        workflow_ref: params.workflow_ref,
        dispatch_input: params.dispatch_input,
        workflow_id: params.workflow_id,
      },
      (ev) =>
        emit({
          kind: 'journal',
          handle_id: params.handle.id,
          workflow_id: ev.workflow_id ?? null,
          event_kind: ev.kind,
          phase_id: ev.phase_id ?? null,
          status: ev.status ?? null,
          ts: ev.ts,
          payload: ev.payload,
          terminal: ev.terminal ?? false,
        }),
    );
    return { workflow_id: result.workflow_id, status: result.status };
  },

  teardown: async (params) => {
    await env.teardown(params.handle);
    return {};
  },

  listNodes: async () => ({ nodes: await env.listNodes() }),

  getNode: async (params) => ({ node: await env.getNode(params.id) }),

  teardownNode: async (params) => ({ deleted: await env.teardownNode(params.id) }),

  reapNodes: async (params) => {
    const r = await env.reap({
      all: params.all,
      force: params.force,
      dryRun: params.dry_run,
      olderThanSecs: params.older_than_secs,
    });
    return { deleted: r.deleted, kept: r.kept, dry_run: r.dryRun };
  },

  // Surface missing Railway credentials/config at preflight instead of on the
  // first `prepare`.
  health: () => env.health(),
});

// The daemon/runner that spawned us owns our lifecycle over stdio: when it
// closes our stdin (shutdown RPC / eviction / parent exit) `plugin.run()`
// resolves. Our in-process RelayServer's listening socket would otherwise keep
// Node's event loop alive, orphaning this process still holding the fixed relay
// port (reparented to init) so the NEXT run's plugin can't bind it. Close the
// relay and exit explicitly on EOF and on SIGTERM/SIGINT so an orphan can never
// accumulate on the port.
let exiting = false;
const shutdown = (code: number): void => {
  if (exiting) return;
  exiting = true;
  void env
    .close()
    .catch(() => undefined)
    .finally(() => process.exit(code));
};
process.on('SIGTERM', () => shutdown(0));
process.on('SIGINT', () => shutdown(0));
plugin.run().then(
  () => shutdown(0),
  (err) => {
    process.stderr.write(`[animus-environment-railway] fatal: ${String(err)}\n`);
    shutdown(1);
  },
);
