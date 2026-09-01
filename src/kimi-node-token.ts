import {
  RelayErrorCode,
  RelayRpcError,
  type BackendServicer,
} from '@launchapp-dev/animus-env-transport';
import { freshKimiNodeCredential } from './environment.js';

/** Reverse-RPC method a run node calls through the bridge upstream socket to
 *  pull a fresh Kimi access token mid-run (TASK-1420). The node never holds a
 *  refresh token; the daemon-side store remains the single writer. */
export const KIMI_TOKEN_METHOD = 'kimi/token';

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
      const credential = await freshKimiNodeCredential(hostEnv, Date.now());
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
