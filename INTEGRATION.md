# Integration checklist — animus-environment-railway

This repo is **standalone-tested** (typecheck clean, unit suite green against a
fake Railway API + a real in-process relay/bridge). The items below are what
the integrating agent must wire before real Railway runs work end to end.

## (a) Base image does not ship `animus-env-bridge` yet

The plugin starts the run service with start command `animus-env-bridge`
(override: `ANIMUS_ENV_BRIDGE_COMMAND`), but
`ghcr.io/launchapp-dev/animus:v0.7.0-rc.2` does **not** include that
entrypoint. Either:

- **Preferred**: add `@launchapp-dev/animus-env-transport` to the base image
  build (`npm install -g @launchapp-dev/animus-env-transport` or copy the
  package and symlink `dist/bridge-cli.js` to `/usr/local/bin/animus-env-bridge`),
  bumping the image tag and `DEFAULT_IMAGE` here; or
- have the plugin inject it at prepare: set `ANIMUS_ENV_BRIDGE_COMMAND` to a
  bootstrap that fetches the bridge (e.g. `npx -y @launchapp-dev/animus-env-transport`
  once it is published to npm) — slower cold start, no image rebuild.

## (b) Reverse MCP / subject RPC channel (166 Phase 2 env-aware-MCP gap)

The transport carries a container→daemon reverse RPC channel
(`BridgeClient.request(method, params)` → `RelayServer.onReverseRpc`). It is
**scaffolded**: this plugin does not yet pass an `onReverseRpc` handler, so
reverse calls answer `ReverseNotWired (-32011)`. To close the gap:

- decide the method namespace (suggested: `mcp/call`, `subject/*`) and
  implement a daemon-side proxy handler in this plugin that forwards to the
  kernel's MCP surface for the run's Actor;
- have the in-container harness point its `animus` MCP endpoint at the bridge
  (the bridge already multiplexes those frames over the same socket).

## (c) Real-Railway integration testing

Unit tests mock fetch / the Railway API. To run the gated integration suite
(`environment.test.ts`, bottom `describe.skipIf`):

- `RAILWAY_TOKEN` — API token with service create/delete on the project
- `RAILWAY_PROJECT_ID`, `RAILWAY_ENVIRONMENT_ID`
- `ANIMUS_ENV_RELAY_PUBLIC_URL` — a `wss://` URL reachable FROM Railway that
  routes to this plugin's relay port (`ANIMUS_ENV_RELAY_PORT`); on Railway this
  is the daemon service's public domain with the relay port exposed.

The GraphQL field shapes (`serviceCreate` / `serviceInstanceUpdate` /
`serviceInstanceDeployV2` / `serviceDelete` / `project.services`) were written
against the public backboard v2 API but are **integration-pending** — verify
against a live project and adjust `src/railway.ts` builders if the schema
drifted.

## (d) Register in `resolve_environment` routing

The kernel (ao-cli v0.7.0-rc.2 `EnvironmentClient` + `resolve_environment`)
routes `EnvironmentSpec.kind` → plugin. Register this plugin for
`kind = "railway"` (subject_kind × harness → "railway" where remote execution
is wanted), and add it to the relevant `animus.toml` / install docs
(`animus plugin install launchapp-dev/animus-environment-railway`).

## (e) Runner phase-exec seam follow-up

Phase 2's exec seam is gated behind `ANIMUS_ENVIRONMENT_EXEC` (166). Once the
workflow runner executes phases through `EnvironmentClient`, the streamed
`environment/output` notifications from this plugin land in the run transcript
unchanged — no plugin-side work expected, but verify the runner honors
`exec_stream` for long-lived agent phases and passes `timeout_secs`.

## Notes for the integrator

- **Teardown/GC**: `teardown` is idempotent (missing service = success);
  `RailwayEnvironment.gcOrphans()` sweeps this instance's orphaned
  `animus-run-<instanceId>-*` services (never another live instance's);
  `gcOrphans({ allInstances: true })` also reaps crashed-instance leftovers —
  safe only when a single plugin instance manages the project. Schedule one of
  these (daemon housekeeping) once routing lands.
- **Secrets**: the per-run relay token is minted daemon-side and injected as
  `ANIMUS_ENV_RUN_TOKEN`; rotate by teardown/prepare. `RAILWAY_TOKEN` belongs
  in the OS keychain via `animus secret set RAILWAY_TOKEN`.
- **Relay lifetime**: one `RelayServer` per plugin process, shared by runs;
  handles die with the process, so a plugin restart orphans running services —
  that is what the GC sweep is for.
