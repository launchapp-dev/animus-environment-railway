# animus-environment-railway

Railway ephemeral-container execution-environment plugin for Animus 0.7.

Mirrors the shape of
[`animus-environment-docker`](https://github.com/launchapp-dev/animus-environment-docker)
(prepare / exec / exec_stream / teardown on
[`animus-environment-base`](https://github.com/launchapp-dev/animus-environment-base)),
but Railway has **no inbound exec/attach** — so the local `docker exec`
primitive is replaced by the
[`animus-env-transport`](https://github.com/launchapp-dev/animus-env-transport)
WebSocket relay: the container dials an **outbound** WSS connection home to a
RelayServer hosted inside this plugin, and every `HarnessCommand` rides that
socket.

## Flow

1. **prepare(spec)** — start/reuse the relay listener, mint a per-run token,
   create a Railway service from the base image (default
   `ghcr.io/launchapp-dev/animus:v0.7.0-rc.2`, override via `spec.image` /
   `ANIMUS_ENV_RAILWAY_IMAGE`) via the Railway GraphQL v2 API
   (`serviceCreate` → `serviceInstanceUpdate(startCommand)` →
   `serviceInstanceDeployV2`), injecting `ANIMUS_ENV_WSS_URL`,
   `ANIMUS_ENV_RUN_TOKEN`, `BASE_DB_URL` passthrough, and `spec.env`. Wait
   (bounded, default 300s) for the container's `animus-env-bridge` to dial
   home, then `git clone` the planned repos (argv arrays, remote urls only)
   and optionally `animus install` (`spec.metadata.provision_animus: true`).
   Returns `handle{ id, workspace_root: /workspace, metadata: { service_id,
   service_name, project_id, environment_id, deployment_id } }`. A failed
   prepare rolls back (service delete + token release).
2. **exec / exec_stream** — relay the command to the in-container bridge;
   streamed frames become `environment/output` notifications; stdin +
   timeout/kill honored bridge-side.
3. **teardown** — `serviceDelete` + release the relay run. Idempotent. A
   name-prefix GC sweep (`gcOrphans`) removes orphaned `animus-run-*`
   services.

## Configuration

See `plugin.toml` for the full env surface. Minimum for real runs:
`RAILWAY_TOKEN`, `RAILWAY_PROJECT_ID`, `RAILWAY_ENVIRONMENT_ID`, and
`ANIMUS_ENV_RELAY_PUBLIC_URL` (a `wss://` URL reachable from Railway that
routes to this plugin's relay port). Per-run overrides:
`spec.metadata.railway_project_id` / `railway_environment_id`.

## Status

- **Unit-tested** (no creds needed): GraphQL request builders + client over a
  mocked fetch; the full prepare→exec/exec_stream→teardown flow against a fake
  Railway API and a REAL in-process relay + bridge; token injection; rollback
  when a container never dials home; GC sweep; `--manifest`.
- **Integration-pending**: real Railway calls (gated tests skip with a clear
  message when creds are absent), the base image shipping the
  `animus-env-bridge` entrypoint, and the reverse MCP/subject RPC daemon
  proxy. **See [INTEGRATION.md](INTEGRATION.md) for the wiring checklist.**

## Publishing note

Dependencies on `animus-environment-base` / `animus-env-transport` (and,
transitively, the SDK) are `file:../` siblings, matching the rest of the TS
plugin family — none are on npm yet. Publish those first and switch to version
ranges before distributing this package outside a sibling checkout.

## Develop

```bash
npm install
npm run typecheck
npm test
node dist/index.js --manifest   # after npm run build
```
