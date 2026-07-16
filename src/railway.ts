// Railway GraphQL API client (v2 backboard endpoint).
//
// The plugin drives four operations:
//   - serviceCreate            — create a service from the base image with
//                                per-run variables (relay URL + token, spec env)
//   - serviceInstanceUpdate    — set the start command (`animus-env-bridge`)
//   - serviceInstanceDeployV2  — kick the first deploy of the instance
//   - serviceDelete            — teardown
// plus a `project.services` listing used by the orphan GC sweep.
//
// Request BUILDERS are pure (unit-tested with a mocked fetch); the exact
// GraphQL field shapes are integration-pending until run against a real
// Railway project with RAILWAY_TOKEN (see INTEGRATION.md).

export const RAILWAY_GQL_ENDPOINT = 'https://backboard.railway.com/graphql/v2';

/** Every prepared service is named with this prefix so orphans can be swept by
 *  name (Railway has no free-form service labels). */
export const SERVICE_NAME_PREFIX = 'animus-run-';

export interface GraphQLRequest {
  query: string;
  variables: Record<string, unknown>;
}

/** Private-registry pull credentials for the run image (ghcr et al). When the
 *  base image is private, Railway needs these to pull it; omitted for public
 *  images. */
export interface RegistryCredentials {
  username: string;
  password: string;
}

export interface ServiceCreateInput {
  projectId: string;
  environmentId: string;
  name: string;
  image: string;
  variables: Record<string, string>;
  /** Set when `image` lives in a private registry that needs auth to pull. */
  registryCredentials?: RegistryCredentials;
}

export function buildServiceCreateRequest(input: ServiceCreateInput): GraphQLRequest {
  return {
    query: `mutation serviceCreate($input: ServiceCreateInput!) {
  serviceCreate(input: $input) { id name }
}`,
    variables: {
      input: {
        projectId: input.projectId,
        environmentId: input.environmentId,
        name: input.name,
        source: { image: input.image },
        variables: input.variables,
      },
    },
  };
}

export function buildServiceInstanceUpdateRequest(args: {
  serviceId: string;
  environmentId: string;
  startCommand: string;
  registryCredentials?: RegistryCredentials;
}): GraphQLRequest {
  const input: Record<string, unknown> = { startCommand: args.startCommand };
  if (args.registryCredentials) input.registryCredentials = args.registryCredentials;
  return {
    query: `mutation serviceInstanceUpdate($serviceId: String!, $environmentId: String, $input: ServiceInstanceUpdateInput!) {
  serviceInstanceUpdate(serviceId: $serviceId, environmentId: $environmentId, input: $input)
}`,
    variables: {
      serviceId: args.serviceId,
      environmentId: args.environmentId,
      input,
    },
  };
}

export function buildServiceInstanceDeployRequest(args: { serviceId: string; environmentId: string }): GraphQLRequest {
  return {
    query: `mutation serviceInstanceDeploy($serviceId: String!, $environmentId: String!) {
  serviceInstanceDeployV2(serviceId: $serviceId, environmentId: $environmentId)
}`,
    variables: { serviceId: args.serviceId, environmentId: args.environmentId },
  };
}

export function buildServiceDeleteRequest(args: { serviceId: string; environmentId: string }): GraphQLRequest {
  return {
    query: `mutation serviceDelete($id: String!, $environmentId: String!) {
  serviceDelete(id: $id, environmentId: $environmentId)
}`,
    variables: { id: args.serviceId, environmentId: args.environmentId },
  };
}

export function buildProjectServicesRequest(args: { projectId: string }): GraphQLRequest {
  return {
    query: `query projectServices($id: String!) {
  project(id: $id) { services { edges { node { id name } } } }
}`,
    variables: { id: args.projectId },
  };
}

// ---------------------------------------------------------------------------
// Substrate-facing API surface (mockable seam for the environment tests)

export interface CreatedService {
  serviceId: string;
  /** Deployment id when the deploy mutation returns one (informational). */
  deploymentId?: string | null;
}

export interface RailwayApi {
  /** Create + configure + deploy a run service. Returns its ids. */
  createRunService(input: ServiceCreateInput & { startCommand: string }): Promise<CreatedService>;
  /** Delete a service. MUST be idempotent (a missing service is a no-op). */
  deleteService(serviceId: string, environmentId: string): Promise<void>;
  /** List `animus-run-*` services in the project (GC sweep input). */
  listRunServices(projectId: string): Promise<Array<{ id: string; name: string }>>;
}

export class RailwayApiError extends Error {
  readonly status?: number;
  readonly errors?: unknown;
  constructor(message: string, opts: { status?: number; errors?: unknown } = {}) {
    super(message);
    this.name = 'RailwayApiError';
    this.status = opts.status;
    this.errors = opts.errors;
  }
}

export interface RailwayClientOptions {
  token: string;
  endpoint?: string;
  /** Injectable fetch (tests). */
  fetchImpl?: typeof fetch;
}

/** Thin GraphQL-over-fetch client for the Railway v2 API. */
export class RailwayClient implements RailwayApi {
  private readonly token: string;
  private readonly endpoint: string;
  private readonly fetchImpl: typeof fetch;

  constructor(opts: RailwayClientOptions) {
    if (!opts.token) throw new RailwayApiError('RAILWAY_TOKEN is required to talk to the Railway API');
    this.token = opts.token;
    this.endpoint = opts.endpoint ?? RAILWAY_GQL_ENDPOINT;
    this.fetchImpl = opts.fetchImpl ?? fetch;
  }

  /** POST one GraphQL request and return `data`, throwing on transport or
   *  GraphQL-level errors. */
  async execute<T = unknown>(request: GraphQLRequest): Promise<T> {
    let res: Response;
    try {
      res = await this.fetchImpl(this.endpoint, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          // Railway PROJECT tokens (scoped to one project+environment — the
          // preferred, least-privilege choice for a per-project ephemeral runner)
          // authenticate via the `Project-Access-Token` header, NOT
          // `Authorization: Bearer` (that returns "Not Authorized" for a project
          // token; Bearer is for account/team tokens). Verified live against
          // backboard.railway.com: serviceCreate/serviceDelete succeed with this
          // header. Set RAILWAY_TOKEN to a project token from Project Settings → Tokens.
          'project-access-token': this.token,
        },
        body: JSON.stringify(request),
      });
    } catch (err) {
      throw new RailwayApiError(`Railway API request failed: ${String(err)}`);
    }
    if (!res.ok) {
      const body = await res.text().catch(() => '');
      throw new RailwayApiError(`Railway API returned HTTP ${res.status}: ${body.slice(0, 500)}`, {
        status: res.status,
      });
    }
    const payload = (await res.json()) as { data?: T; errors?: Array<{ message?: string }> };
    if (payload.errors && payload.errors.length > 0) {
      const msg = payload.errors.map((e) => e.message ?? 'unknown').join('; ');
      throw new RailwayApiError(`Railway GraphQL error: ${msg}`, { errors: payload.errors });
    }
    if (payload.data === undefined) {
      throw new RailwayApiError('Railway GraphQL response had no data');
    }
    return payload.data;
  }

  async createRunService(input: ServiceCreateInput & { startCommand: string }): Promise<CreatedService> {
    const created = await this.execute<{ serviceCreate: { id: string; name: string } }>(
      buildServiceCreateRequest(input),
    );
    const serviceId = created.serviceCreate?.id;
    if (!serviceId) throw new RailwayApiError('serviceCreate returned no service id');

    try {
      // The bridge entrypoint is set via the service instance, then deployed.
      await this.execute(
        buildServiceInstanceUpdateRequest({
          serviceId,
          environmentId: input.environmentId,
          startCommand: input.startCommand,
          registryCredentials: input.registryCredentials,
        }),
      );
      const deployed = await this.execute<{ serviceInstanceDeployV2?: string | null }>(
        buildServiceInstanceDeployRequest({ serviceId, environmentId: input.environmentId }),
      );
      return { serviceId, deploymentId: deployed.serviceInstanceDeployV2 ?? null };
    } catch (err) {
      // The service exists but is half-configured: don't leak it.
      await this.deleteService(serviceId, input.environmentId).catch(() => undefined);
      throw err;
    }
  }

  async deleteService(serviceId: string, environmentId: string): Promise<void> {
    try {
      await this.execute(buildServiceDeleteRequest({ serviceId, environmentId }));
    } catch (err) {
      // Idempotent teardown: a service that is already gone is a success.
      if (err instanceof RailwayApiError && /not found|does not exist/i.test(err.message)) return;
      throw err;
    }
  }

  async listRunServices(projectId: string): Promise<Array<{ id: string; name: string }>> {
    const data = await this.execute<{
      project: { services: { edges: Array<{ node: { id: string; name: string } }> } };
    }>(buildProjectServicesRequest({ projectId }));
    const edges = data.project?.services?.edges ?? [];
    return edges.map((e) => e.node).filter((n) => n.name.startsWith(SERVICE_NAME_PREFIX));
  }
}
