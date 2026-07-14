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
  version: '0.1.2',
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

plugin.run().catch((err) => {
  process.stderr.write(`[animus-environment-railway] fatal: ${String(err)}\n`);
  process.exit(1);
});
