// Parent-side `credentials` role servicer for the node→parent backend/call
// reverse RPC (TASK-855 part 2). The node's git credential helper
// (`git-credential-animus`, shipped in the run-image) asks for a FRESH GitHub
// token at every push, so the 1h installation-token lifetime no longer bounds
// how long a run may live before it can push.
//
// Scope: mints INSTALLATION-WIDE tokens (the same scope a bare broker node
// receives at spawn) rather than per-run repo-scoped ones — the relay handler
// is shared by every run in the plugin process, and an installation-wide token
// is correct for all of them; per-run repo scoping at refresh time would
// require handle-identity threading through the reverse RPC and buys nothing
// for a single-org fleet. The App PRIVATE KEY never crosses the relay; only
// the minted short-lived token does. Token values are never logged.
import { githubAppJwt } from './environment.js';

interface CredentialsCache {
  installId?: string;
}

export interface CredentialsServicer {
  call(method: string, params: unknown): Promise<unknown>;
}

export function makeCredentialsServicer(
  hostEnv: NodeJS.ProcessEnv,
  fetchImpl: typeof fetch = fetch,
  nowFn: () => number = () => Math.floor(Date.now() / 1000),
): CredentialsServicer {
  const cache: CredentialsCache = {};
  return {
    async call(method: string, _params: unknown): Promise<unknown> {
      if (method !== 'git/token') {
        throw new Error(`credentials servicer: unknown method '${method}'`);
      }
      const appId = hostEnv.GITHUB_APP_ID;
      const rawKey = hostEnv.GITHUB_APP_PRIVATE_KEY;
      if (!appId || !rawKey) {
        // No App configured: fall back to the static host token when present
        // (same value the node already holds — better than a hard failure).
        if (hostEnv.GITHUB_TOKEN) {
          return { token: hostEnv.GITHUB_TOKEN, expires_at: null, source: 'static' };
        }
        throw new Error('credentials servicer: GitHub App not configured and no GITHUB_TOKEN fallback');
      }
      const privateKey = rawKey.includes('\\n') ? rawKey.replace(/\\n/g, '\n') : rawKey;
      const jwt = githubAppJwt(appId, privateKey, nowFn());
      const gh = async <T = Record<string, unknown>>(path: string, init?: RequestInit): Promise<T> => {
        const res = await fetchImpl(`https://api.github.com${path}`, {
          ...init,
          headers: {
            authorization: `Bearer ${jwt}`,
            accept: 'application/vnd.github+json',
            'user-agent': 'animus-environment-railway',
            ...init?.headers,
          },
        });
        if (!res.ok) {
          throw new Error(`credentials servicer: GitHub ${path} -> HTTP ${res.status}`);
        }
        return (await res.json()) as T;
      };
      // Resolve the installation once per servicer lifetime; every subsequent
      // mint is a single POST to access_tokens.
      if (cache.installId === undefined) {
        if (hostEnv.GITHUB_APP_INSTALLATION_ID) {
          cache.installId = hostEnv.GITHUB_APP_INSTALLATION_ID;
        } else {
          const installs = await gh<Array<{ id: unknown }>>('/app/installations');
          const first = Array.isArray(installs) ? installs[0] : undefined;
          if (!first) throw new Error('credentials servicer: GitHub App has no installations');
          cache.installId = String(first.id);
        }
      }
      const minted = await gh<{ token?: unknown; expires_at?: unknown }>(
        `/app/installations/${cache.installId}/access_tokens`,
        { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' },
      );
      if (typeof minted.token !== 'string' || minted.token.length === 0) {
        throw new Error('credentials servicer: GitHub returned no token');
      }
      return {
        token: minted.token,
        expires_at: typeof minted.expires_at === 'string' ? minted.expires_at : null,
        source: 'github_app',
      };
    },
  };
}
