// Parent-side `credentials` role servicer for the node→parent backend/call
// reverse RPC (TASK-855 part 2). The node's git credential helper
// (`git-credential-animus`, shipped in the run-image) asks for a FRESH GitHub
// token at every push, so the 1h installation-token lifetime no longer bounds
// how long a run may live before it can push.
//
// Scope: every request MUST carry the repository selected by the trusted
// handle policy. The servicer resolves that repository's installation and asks
// GitHub for a one-repository token. It never returns an installation-wide App
// token or a static host PAT to the least-trusted node. The App PRIVATE KEY
// never crosses the relay; only the minted short-lived token does. Token values
// are never logged.
import { githubAppJwt } from './environment.js';

interface CredentialsCache {
  installationByRepository: Map<string, string>;
}

export interface CredentialsServicer {
  call(method: string, params: unknown): Promise<unknown>;
}

function repositoryScope(params: unknown): { owner: string; repo: string } {
  if (!params || typeof params !== 'object' || Array.isArray(params)) {
    throw new Error('credentials servicer: git/token requires repository scope');
  }
  const record = params as Record<string, unknown>;
  const owner = typeof record.owner === 'string' ? record.owner.trim() : '';
  const repo = typeof record.repo === 'string' ? record.repo.trim() : '';
  if (!/^[A-Za-z0-9_.-]+$/.test(owner) || !/^[A-Za-z0-9_.-]+$/.test(repo)) {
    throw new Error('credentials servicer: git/token repository scope is invalid');
  }
  return { owner, repo };
}

export function makeCredentialsServicer(
  hostEnv: NodeJS.ProcessEnv,
  fetchImpl: typeof fetch = fetch,
  nowFn: () => number = () => Math.floor(Date.now() / 1000),
): CredentialsServicer {
  const cache: CredentialsCache = { installationByRepository: new Map() };
  return {
    async call(method: string, params: unknown): Promise<unknown> {
      if (method !== 'git/token') {
        throw new Error(`credentials servicer: unknown method '${method}'`);
      }
      const scope = repositoryScope(params);
      const appId = hostEnv.GITHUB_APP_ID;
      const rawKey = hostEnv.GITHUB_APP_PRIVATE_KEY;
      if (!appId || !rawKey) {
        throw new Error('credentials servicer: repository-scoped GitHub App credentials are not configured');
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
      const key = `${scope.owner.toLowerCase()}/${scope.repo.toLowerCase()}`;
      let installId = cache.installationByRepository.get(key);
      if (!installId) {
        const installation = await gh<{ id?: unknown }>(
          `/repos/${encodeURIComponent(scope.owner)}/${encodeURIComponent(scope.repo)}/installation`,
        );
        if (installation.id === undefined || installation.id === null) {
          throw new Error('credentials servicer: GitHub returned no repository installation id');
        }
        installId = String(installation.id);
        cache.installationByRepository.set(key, installId);
      }
      const minted = await gh<{ token?: unknown; expires_at?: unknown }>(
        `/app/installations/${installId}/access_tokens`,
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ repositories: [scope.repo] }),
        },
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
