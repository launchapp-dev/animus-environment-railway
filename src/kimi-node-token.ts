import {
  RelayErrorCode,
  RelayRpcError,
  type BackendServicer,
} from '@launchapp-dev/animus-env-transport';
import { DEFAULT_KIMI_CODE_HOME, freshKimiNodeCredential } from './environment.js';

/** Reverse-RPC method a run node calls through the bridge upstream socket to
 *  pull a fresh Kimi access token mid-run (TASK-1420). The node never holds a
 *  refresh token; the daemon-side store remains the single writer. */
export const KIMI_TOKEN_METHOD = 'kimi/token';

/** Minimum remaining life a served token must have. The node-side kimi CLI
 *  treats a token as stale once less than max(300s, expires_in/2) remains
 *  (450s for the usual 900s token) and then has no refresh path of its own,
 *  so a served token must clear that bar with margin — otherwise the node
 *  would write back a credential its CLI immediately tries (and fails) to
 *  refresh itself. 600s = 450s CLI threshold + 150s clock/skew margin. */
export const KIMI_NODE_TOKEN_MIN_TTL_MS = 600_000;

/** Wrap the `credentials` role servicer with the Kimi access-token issuer.
 *  `git/token` (and any future methods) delegate to the inner servicer;
 *  `kimi/token` is served from the same central refresh prepare uses
 *  (freshKimiNodeCredential — in-process single-flight, rotation written back
 *  to the daemon store), so the node always receives exactly what a fresh
 *  prepare would inject: the current credential MINUS the refresh token. The
 *  scoped backend-call policy (backend-call-policy.ts) binds the call to the
 *  run's handle; the run/relay token never leaves the bridge, and no
 *  credential material is logged. */
export function makeKimiNodeTokenServicer(
  inner: BackendServicer,
  hostEnv: NodeJS.ProcessEnv = process.env,
): BackendServicer {
  return {
    async call(method: string, params: unknown): Promise<unknown> {
      if (method !== KIMI_TOKEN_METHOD) return inner.call(method, params);
      const credential = await freshKimiNodeCredential(
        hostEnv,
        Date.now(),
        fetch,
        DEFAULT_KIMI_CODE_HOME,
        KIMI_NODE_TOKEN_MIN_TTL_MS,
      );
      if (!credential) {
        // Fail the call, not the run: the node keeps its current (possibly
        // still-valid) token and retries on its next poll.
        throw new RelayRpcError(
          RelayErrorCode.InternalError,
          'kimi credential unavailable on the daemon (no usable login or refresh failed)',
        );
      }
      const { refresh_token: _snake, refreshToken: _camel, ...accessOnly } = credential;
      return { credential: accessOnly };
    },
  };
}
