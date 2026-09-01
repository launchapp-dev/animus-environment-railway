import {
  RelayErrorCode,
  RelayRpcError,
  type BackendServicer,
} from '@launchapp-dev/animus-env-transport';
import { DEFAULT_KIMI_CODE_HOME, freshKimiNodeCredential } from './environment.js';
import { capacityNodeBundle } from './capacity-broker.js';

/** Reverse-RPC method a run node calls through the bridge upstream socket to
 *  pull a fresh Kimi access token mid-run (TASK-1420). The node never holds a
 *  refresh token; the daemon-side store remains the single writer. */
export const KIMI_TOKEN_METHOD = 'kimi/token';

/** Minimum remaining life a served token must have. The node-side kimi CLI
 *  treats a token as stale once less than max(300s, expires_in/2) remains
 *  (450s for the usual 900s token) and then has no refresh path of its own,
 *  so a served token must clear that bar with margin — otherwise the node
 *  would write back a credential its CLI immediately tries (and fails) to
 *  refresh itself. 600s = 450s CLI threshold + 150s clock/skew margin.
 *  NOTE: the broker path inherits the Portal's own refresh skew; if the
 *  Portal's KIMI_OAUTH_REFRESH_SKEW_MS is below this floor, the Portal skew
 *  should be raised so broker-served tokens clear the CLI threshold. */
export const KIMI_NODE_TOKEN_MIN_TTL_MS = 600_000;

/** Extract the `credentials/kimi-code.json` record from a broker-served kimi
 *  files archive (already refresh-stripped server-side). Returns null when the
 *  archive does not carry a usable access token. */
function credentialFromBrokerBundle(bundle: string): Record<string, unknown> | null {
  try {
    const archive = JSON.parse(bundle) as { files?: Record<string, unknown> };
    const content = archive?.files?.['credentials/kimi-code.json'];
    if (typeof content !== 'string') return null;
    const cred = JSON.parse(content) as Record<string, unknown>;
    if (!cred || typeof cred !== 'object' || Array.isArray(cred)) return null;
    const accessToken = cred.access_token ?? cred.accessToken;
    if (typeof accessToken !== 'string' || accessToken.trim() === '') return null;
    delete cred.refresh_token;
    delete cred.refreshToken;
    return cred;
  } catch {
    return null;
  }
}

/** Wrap the `credentials` role servicer with the Kimi access-token issuer.
 *  `git/token` (and any future methods) delegate to the inner servicer;
 *  `kimi/token` is served from the same source prepare uses: the Portal
 *  capacity broker when CAPACITY_BROKER_URL/TOKEN are injected (locked
 *  single-writer refresh, refresh-stripped server-side), otherwise the
 *  daemon-side store via freshKimiNodeCredential (in-process single-flight,
 *  rotation written back). The node always receives the current credential
 *  MINUS the refresh token. The scoped backend-call policy
 *  (backend-call-policy.ts) binds the call to the run's handle; the run/relay
 *  token never leaves the bridge, and no credential material is logged. */
export function makeKimiNodeTokenServicer(
  inner: BackendServicer,
  hostEnv: NodeJS.ProcessEnv = process.env,
): BackendServicer {
  return {
    async call(method: string, params: unknown): Promise<unknown> {
      if (method !== KIMI_TOKEN_METHOD) return inner.call(method, params);
      const brokerBundle = await capacityNodeBundle(hostEnv, 'kimi');
      const fromBroker = brokerBundle ? credentialFromBrokerBundle(brokerBundle) : null;
      const credential =
        fromBroker ??
        (await freshKimiNodeCredential(hostEnv, Date.now(), fetch, DEFAULT_KIMI_CODE_HOME, KIMI_NODE_TOKEN_MIN_TTL_MS));
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
