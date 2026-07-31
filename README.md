# animus-environment-railway

Railway ephemeral-container execution-environment plugin for Animus 0.7.

Mirrors the shape of
[`animus-environment-docker`](https://github.com/launchapp-dev/animus-environment-docker)
(prepare / exec / exec_stream / teardown on
[`animus-environment-base`](https://github.com/launchapp-dev/animus-environment-base)),
but Railway has **no inbound exec/attach** — so the local `docker exec`
primitive is replaced by the
[`animus-env-transport`](https://github.com/launchapp-dev/animus-env-transport)
WebSocket relay: the container dials an **outbound** WSS connection home to
the shared `animus-env-relay` singleton, and every `HarnessCommand` rides that
socket.

## Flow

1. **prepare(spec)** — start/reuse the relay listener, mint a per-run token,
   enforce the client's hard managed-node limit (default 5),
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

Every logical client is limited to five managed Railway nodes by default.
`ANIMUS_ENV_CLIENT_ID` gives the client a stable capacity identity across
plugin restarts; it defaults to `ANIMUS_ENV_RELAY_OWNER_ID`, then
`animus-environment-railway`. Set `ANIMUS_ENV_MAX_MANAGED_NODES` to a
non-negative integer to change the limit (`0` disables new prepares while
leaving teardown and reap available). Admission lists Railway first and fails
closed when capacity cannot be verified. Unknown and legacy `animus-run-*`
services count against the client cap unless their name is structurally
attributable to a different cap-aware client. A retry carrying the same stable
run id reconciles its prior service under admission before the cap is checked,
so replacing that service does not consume an additional slot. A cross-process
lock under `ANIMUS_ENV_CAPACITY_LOCK_DIR` (default
`/tmp/animus-environment-railway-capacity`) serializes recount-and-create for
the same project/client; every process for that client must share this path.
The lock remains held until the new service appears in Railway inventory. If
that confirmation exceeds `ANIMUS_ENV_CAPACITY_CONFIRMATION_TIMEOUT_MS`
(default 30 seconds), the plugin deletes the unconfirmed service and fails the
prepare closed. If creation is ambiguous or that cleanup delete fails, the
plugin keeps a durable reservation under the capacity lock directory and
continues charging the slot across restarts until successful inventory or
teardown proves the service absent. Corrupt or incomplete reservation records
remain charged instead of reopening capacity. Service names contain only a
hash of the client id.

`exec_session` actor authority is bound at prepare time. The plugin injects
only the SDK-authenticated actor as `ANIMUS_ACTOR_JSON`, signs the persisted
handle marker for restart-safe reattachment, and enforces the same actor on
node-originated reverse RPC. `ANIMUS_ENV_ACTOR_BINDING_SECRET` may provide a
dedicated stable signing key; when omitted, `RAILWAY_TOKEN` is used.

Relay ownership is restart-safe as well. New handles contain an AEAD-sealed
run token and owner token covered by the actor-binding HMAC. After the Portal,
daemon, or environment-plugin process restarts, the plugin presents those
exact credentials to the durable singleton registration and resumes the same
session ID; it never prepares a second node or workflow. Configure
`ANIMUS_ENV_RELAY_CONTROL_TOKEN` as a 32-byte base64url credential shared with
the singleton. `ANIMUS_ENV_RELAY_BINDING_SECRET` should be stable across
deploys; it defaults to the actor-binding secret or `RAILWAY_TOKEN`.

Claude authentication prefers the shared subscription credential in
`CLAUDE_CONFIG_DIR`. The daemon refreshes that credential centrally and only
passes its short-lived access token to a node. When the subscription login is
missing, expired without a refresh token, or invalid, the plugin instead
passes the daemon's `ANTHROPIC_AUTH_TOKEN` or `ANTHROPIC_API_KEY` and optional
`ANTHROPIC_BASE_URL`. These fallback values are daemon-authoritative and cannot
be replaced by a task's `spec.env`.

## Status

- **Unit-tested** (no creds needed): GraphQL request builders + client over a
  mocked fetch; the full prepare→exec/exec_stream→teardown flow against a fake
  Railway API and a REAL in-process relay + bridge; token injection; rollback
  when a container never dials home; GC sweep; `--manifest`.
- **Integration-pending**: real Railway calls (gated tests skip with a clear
  message when creds are absent), the base image shipping the
  `animus-env-bridge` entrypoint, and the reverse MCP/subject RPC daemon
  proxy. **See [INTEGRATION.md](INTEGRATION.md) for the wiring checklist.**

## Install

Installable as an Animus plugin via `animus install` (git + tag):

```bash
animus plugin install launchapp-dev/animus-environment-railway
```

The install resolver downloads the GitHub Release asset for the host's target
triple (or the `-noarch` fallback), verifies it against its `.sha256` sidecar,
extracts, and execs the contained binary. `npm run release` builds a
self-contained esbuild bundle (`dist/animus-environment-railway`, single JS file
with a node shebang — it inlines `animus-env-transport`,
`animus-environment-base`, the SDK, `ws` and `zod`, so it runs on plain node
with no `node_modules`) and stages the correctly-named assets under
`dist/release/`:

```
animus-environment-railway-v<version>-<target>.tar.gz          (the archive)
animus-environment-railway-v<version>-<target>.tar.gz.sha256   (per-asset checksum)
animus-environment-railway-v<version>-noarch.tar.gz            (universal fallback)
animus-environment-railway-v<version>-noarch.tar.gz.sha256
```

The script prints the `gh release create` command to run by hand once the
assets look right; it does not cut the release itself.

## Publishing note

Dependencies on `animus-environment-base` and `animus-env-transport` are pinned
to immutable Git revisions. Both dependency trees build themselves during Git
installation, so a clean checkout needs no mutable sibling worktrees. The
`npm run bundle` esbuild step still inlines them into the single published
binary, and the release asset carries no Git or `file:../` dependency.

The bundle is deliberately fail-closed on the transport boundary: this source
release pins `@launchapp-dev/animus-env-transport` to the immutable `v0.3.2` Git
tag and rejects any different installed package version before writing release
assets. Private-repository builders must have GitHub read access when running
`npm ci`; the published environment binary remains self-contained.

GitHub Actions uses a short-lived installation token from a dedicated GitHub
App. Configure `ANIMUS_DEPENDENCY_APP_ID` and
`ANIMUS_DEPENDENCY_APP_PRIVATE_KEY` as repository secrets, install the App on
only `animus-environment-base` and `animus-env-transport`, and grant it only
Contents: read. The workflow additionally restricts each minted token to those
two repositories, removes its temporary Git URL mappings on every exit path,
and relies on the token action's post-step revocation. Do not substitute a
personal token or long-lived deploy keys.

## Develop

```bash
npm install
npm run typecheck
npm test
node dist/index.js --manifest   # after npm run build (tsc)
npm run bundle                  # esbuild self-contained dist/animus-environment-railway
npm run release                 # stage GitHub Release assets under dist/release/
```
