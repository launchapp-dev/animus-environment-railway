// Unit tests for the Railway GraphQL request builders + client, over a mocked
// fetch. Real Railway calls are integration-pending (see INTEGRATION.md and
// environment.test.ts's gated suite).

import { describe, expect, it } from 'vitest';

import {
  buildProjectServicesRequest,
  buildServiceCreateRequest,
  buildServiceDeleteRequest,
  buildServiceInstanceDeployRequest,
  buildServiceInstanceUpdateRequest,
  RAILWAY_GQL_ENDPOINT,
  RailwayApiError,
  RailwayClient,
  SERVICE_NAME_PREFIX,
} from './railway.js';

interface Captured {
  url: string;
  init: RequestInit;
  body: { query: string; variables: Record<string, unknown> };
}

function mockFetch(responses: Array<{ status?: number; json?: unknown; text?: string }>): {
  fetchImpl: typeof fetch;
  calls: Captured[];
} {
  const calls: Captured[] = [];
  let i = 0;
  const fetchImpl = (async (url: unknown, init?: RequestInit) => {
    const spec = responses[Math.min(i, responses.length - 1)];
    i += 1;
    calls.push({
      url: String(url),
      init: init ?? {},
      body: JSON.parse(String(init?.body ?? '{}')) as Captured['body'],
    });
    const status = spec?.status ?? 200;
    return {
      ok: status >= 200 && status < 300,
      status,
      json: async () => spec?.json ?? {},
      text: async () => spec?.text ?? JSON.stringify(spec?.json ?? {}),
    } as Response;
  }) as typeof fetch;
  return { fetchImpl, calls };
}

describe('request builders', () => {
  it('serviceCreate carries project/environment/name/image/variables', () => {
    const req = buildServiceCreateRequest({
      projectId: 'proj-1',
      environmentId: 'env-1',
      name: `${SERVICE_NAME_PREFIX}abc`,
      image: 'ghcr.io/launchapp-dev/animus:v0.7.0-rc.2',
      variables: { ANIMUS_ENV_WSS_URL: 'wss://relay/relay/abc', ANIMUS_ENV_RUN_TOKEN: 't' },
    });
    expect(req.query).toContain('mutation serviceCreate');
    expect(req.variables).toEqual({
      input: {
        projectId: 'proj-1',
        environmentId: 'env-1',
        name: 'animus-run-abc',
        source: { image: 'ghcr.io/launchapp-dev/animus:v0.7.0-rc.2' },
        variables: { ANIMUS_ENV_WSS_URL: 'wss://relay/relay/abc', ANIMUS_ENV_RUN_TOKEN: 't' },
      },
    });
  });

  it('serviceInstanceUpdate sets the bridge start command', () => {
    const req = buildServiceInstanceUpdateRequest({
      serviceId: 'svc-1',
      environmentId: 'env-1',
      startCommand: 'animus-env-bridge',
    });
    expect(req.query).toContain('serviceInstanceUpdate');
    expect(req.variables).toMatchObject({
      serviceId: 'svc-1',
      environmentId: 'env-1',
      input: { startCommand: 'animus-env-bridge' },
    });
  });

  it('deploy / delete / list carry their ids', () => {
    expect(buildServiceInstanceDeployRequest({ serviceId: 's', environmentId: 'e' }).variables).toEqual({
      serviceId: 's',
      environmentId: 'e',
    });
    expect(buildServiceDeleteRequest({ serviceId: 's', environmentId: 'e' }).variables).toEqual({
      id: 's',
      environmentId: 'e',
    });
    expect(buildProjectServicesRequest({ projectId: 'p' }).variables).toEqual({ id: 'p' });
  });
});

describe('RailwayClient', () => {
  it('POSTs to the v2 endpoint with the bearer token', async () => {
    const { fetchImpl, calls } = mockFetch([
      { json: { data: { serviceCreate: { id: 'svc-9', name: 'animus-run-x' } } } },
      { json: { data: { serviceInstanceUpdate: true } } },
      { json: { data: { serviceInstanceDeployV2: 'dep-1' } } },
    ]);
    const client = new RailwayClient({ token: 'tok-123', fetchImpl });
    const created = await client.createRunService({
      projectId: 'p',
      environmentId: 'e',
      name: 'animus-run-x',
      image: 'img',
      variables: {},
      startCommand: 'animus-env-bridge',
    });

    expect(created).toEqual({ serviceId: 'svc-9', deploymentId: 'dep-1' });
    expect(calls).toHaveLength(3);
    expect(calls[0]?.url).toBe(RAILWAY_GQL_ENDPOINT);
    const headers = calls[0]?.init.headers as Record<string, string>;
    expect(headers.authorization).toBe('Bearer tok-123');
    expect(headers['content-type']).toBe('application/json');
    expect(calls[0]?.body.query).toContain('serviceCreate');
    expect(calls[1]?.body.query).toContain('serviceInstanceUpdate');
    expect(calls[2]?.body.query).toContain('serviceInstanceDeployV2');
  });

  it('deletes the half-configured service when update/deploy fails after create', async () => {
    const { fetchImpl, calls } = mockFetch([
      { json: { data: { serviceCreate: { id: 'svc-9', name: 'animus-run-x' } } } },
      { json: { errors: [{ message: 'startCommand rejected' }] } },
      { json: { data: { serviceDelete: true } } },
    ]);
    const client = new RailwayClient({ token: 't', fetchImpl });
    await expect(
      client.createRunService({
        projectId: 'p',
        environmentId: 'e',
        name: 'animus-run-x',
        image: 'img',
        variables: {},
        startCommand: 'animus-env-bridge',
      }),
    ).rejects.toThrow(/startCommand rejected/);
    expect(calls).toHaveLength(3);
    expect(calls[2]?.body.query).toContain('serviceDelete');
    expect(calls[2]?.body.variables).toMatchObject({ id: 'svc-9' });
  });

  it('throws RailwayApiError on GraphQL errors', async () => {
    const { fetchImpl } = mockFetch([{ json: { errors: [{ message: 'Not Authorized' }] } }]);
    const client = new RailwayClient({ token: 't', fetchImpl });
    await expect(client.listRunServices('p')).rejects.toThrow(/Not Authorized/);
  });

  it('throws RailwayApiError on HTTP failures', async () => {
    const { fetchImpl } = mockFetch([{ status: 502, text: 'bad gateway' }]);
    const client = new RailwayClient({ token: 't', fetchImpl });
    await expect(client.listRunServices('p')).rejects.toMatchObject({ status: 502 });
  });

  it('treats deleting an already-gone service as success (idempotent teardown)', async () => {
    const { fetchImpl } = mockFetch([{ json: { errors: [{ message: 'Service not found' }] } }]);
    const client = new RailwayClient({ token: 't', fetchImpl });
    await expect(client.deleteService('svc-gone', 'e')).resolves.toBeUndefined();
  });

  it('filters the project service listing to the run-name prefix', async () => {
    const { fetchImpl } = mockFetch([
      {
        json: {
          data: {
            project: {
              services: {
                edges: [
                  { node: { id: '1', name: 'animus-run-aaa' } },
                  { node: { id: '2', name: 'postgres' } },
                  { node: { id: '3', name: 'animus-run-bbb' } },
                ],
              },
            },
          },
        },
      },
    ]);
    const client = new RailwayClient({ token: 't', fetchImpl });
    const services = await client.listRunServices('p');
    expect(services.map((s) => s.id)).toEqual(['1', '3']);
  });

  it('requires a token', () => {
    expect(() => new RailwayClient({ token: '' })).toThrow(RailwayApiError);
  });
});
