/** Portal capacity-broker client for node credential issue (TASK-1420 follow-up).
 *
 *  The Portal owns OAuth refresh for the shared Claude/Kimi logins (locked,
 *  single-writer, Postgres row-fenced). The env plugin's local file refresh
 *  races that writer (single-use refresh-token rotation) and its Claude
 *  endpoint is dead from Railway (console.anthropic.com is Cloudflare
 *  challenged from datacenter IPs). When the daemon injects
 *  CAPACITY_BROKER_URL + CAPACITY_BROKER_TOKEN (manifest env_required), node
 *  credentials are sourced from the broker's family-only credential-refresh
 *  endpoint instead of the local file:
 *
 *    POST {CAPACITY_BROKER_URL}/credential-refresh  {family: "claude"|"kimi"}
 *    Authorization: Bearer <CAPACITY_BROKER_TOKEN>
 *    -> { ok: true, data: { value: <string>, generation, refreshed } }
 *
 *  `value` is the current bundle as a JSON STRING with refresh material
 *  ALREADY stripped server-side (family-only path). Shapes: claude = the full
 *  `.credentials.json` (claudeAiOauth wrapper); kimi = the files archive
 *  `{files:{"config.toml":...,"credentials/kimi-code.json":"..."}}`.
 *
 *  Any failure (broker env unset, HTTP error, malformed body) returns null so
 *  callers fall back to the legacy daemon-file behavior (older Portals).
 *  Never logs the token, the URL credentials, or bundle contents. */

export interface CapacityBroker {
  url: string;
  token: string;
}

/** Resolve the broker coordinates from the plugin env. Both must be set;
 *  either absent means "older Portal / no broker" and callers use the file
 *  fallback. */
export function capacityBrokerFromEnv(hostEnv: NodeJS.ProcessEnv): CapacityBroker | null {
  const url = (hostEnv.CAPACITY_BROKER_URL ?? '').trim().replace(/\/+$/, '');
  const token = (hostEnv.CAPACITY_BROKER_TOKEN ?? '').trim();
  return url && token ? { url, token } : null;
}

/** Recursively drop refresh material from the known bundle shapes — defense in
 *  depth on top of the server-side strip, so a node never receives a refresh
 *  token even if a broker build regresses. Returns null when the bundle is not
 *  parseable object JSON. */
export function stripRefreshMaterial(value: string): string | null {
  let root: Record<string, unknown>;
  try {
    root = JSON.parse(value) as Record<string, unknown>;
  } catch {
    return null;
  }
  if (!root || typeof root !== 'object' || Array.isArray(root)) return null;
  const strip = (obj: Record<string, unknown>): Record<string, unknown> => {
    const copy = { ...obj };
    delete copy.refresh_token;
    delete copy.refreshToken;
    return copy;
  };
  const oauth = root.claudeAiOauth;
  if (oauth && typeof oauth === 'object' && !Array.isArray(oauth)) {
    return JSON.stringify({ ...root, claudeAiOauth: strip(oauth as Record<string, unknown>) });
  }
  const files = root.files;
  if (files && typeof files === 'object' && !Array.isArray(files)) {
    const out: Record<string, unknown> = {};
    for (const [name, content] of Object.entries(files as Record<string, unknown>)) {
      if (typeof content !== 'string') {
        out[name] = content;
        continue;
      }
      try {
        const inner = JSON.parse(content) as Record<string, unknown>;
        out[name] = inner && typeof inner === 'object' && !Array.isArray(inner) ? JSON.stringify(strip(inner)) : content;
      } catch {
        out[name] = content; // non-JSON bundle files (e.g. config.toml) carry no refresh token
      }
    }
    return JSON.stringify({ ...root, files: out });
  }
  return JSON.stringify(strip(root));
}

/** Fetch the current node-issuable bundle for `family` from the Portal
 *  capacity broker. Returns the refresh-stripped bundle JSON string, or null
 *  on any failure (caller falls back to the daemon-side file). */
export async function capacityNodeBundle(
  hostEnv: NodeJS.ProcessEnv,
  family: 'claude' | 'kimi',
  fetchImpl: typeof fetch = fetch,
): Promise<string | null> {
  const broker = capacityBrokerFromEnv(hostEnv);
  if (!broker) return null;
  try {
    const res = await fetchImpl(`${broker.url}/credential-refresh`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${broker.token}` },
      body: JSON.stringify({ family }),
    });
    if (!res.ok) {
      process.stderr.write(
        `[animus-environment-railway] capacity broker credential-refresh for family '${family}' failed (HTTP ${res.status}); falling back to the daemon-side credential file\n`,
      );
      return null;
    }
    const body = (await res.json()) as { ok?: boolean; data?: { value?: unknown } };
    const value = body?.ok === true ? body.data?.value : undefined;
    if (typeof value !== 'string' || value.length === 0) {
      process.stderr.write(
        `[animus-environment-railway] capacity broker returned no usable bundle for family '${family}'; falling back to the daemon-side credential file\n`,
      );
      return null;
    }
    const stripped = stripRefreshMaterial(value);
    if (!stripped) {
      process.stderr.write(
        `[animus-environment-railway] capacity broker bundle for family '${family}' is unparseable; falling back to the daemon-side credential file\n`,
      );
      return null;
    }
    return stripped;
  } catch (err) {
    process.stderr.write(
      `[animus-environment-railway] capacity broker credential-refresh for family '${family}' errored (${String(err)}); falling back to the daemon-side credential file\n`,
    );
    return null;
  }
}
