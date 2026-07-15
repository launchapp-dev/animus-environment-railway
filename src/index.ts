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
  version: '0.4.4',
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
      name: 'CLAUDE_CONFIG_DIR',
      description:
        'Daemon-side dir holding the Claude subscription .credentials.json; base64-injected into each node so the claude harness runs on the subscription.',
      required: false,
    },
    {
      name: 'CODEX_OAUTH_HOME',
      description:
        'Daemon-side dir holding the Codex ChatGPT-subscription auth.json; base64-injected into each node so the codex harness runs on the subscription.',
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

  teardown: async (params) => {
    await env.teardown(params.handle);
    return {};
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
