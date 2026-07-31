import {
  RelayErrorCode,
  RelayRpcError,
  type BackendCallAuthorizer,
  type BackendCallAuthorizationRequest,
} from '@launchapp-dev/animus-env-transport';

export interface BackendCallRepositoryScope {
  owner: string;
  repo: string;
}

/** Parent-owned authority for one remote environment handle. The node never
 * supplies or mutates this structure. */
export interface BackendCallScope {
  subjectId: string | null;
  workflowId: string | null;
  repository: BackendCallRepositoryScope | null;
}

type JsonRecord = Record<string, unknown>;

function deny(message: string): never {
  throw new RelayRpcError(RelayErrorCode.BackendCallForbidden, `reverse backend call denied: ${message}`);
}

function record(value: unknown, label: string): JsonRecord {
  if (!value || typeof value !== 'object' || Array.isArray(value)) deny(`${label} must be an object`);
  return value as JsonRecord;
}

function requireRole(request: BackendCallAuthorizationRequest, allowed: readonly string[]): void {
  if (!allowed.includes(request.role)) {
    deny(`role '${request.role}' cannot call '${request.method}'`);
  }
}

function requireExactId(actual: unknown, expected: string, label: string): void {
  if (typeof actual !== 'string' || actual !== expected) {
    deny(`${label} must match the bound workflow '${expected}'`);
  }
}

function subjectParts(subjectId: string): { kind: string; nativeId: string } {
  const separator = subjectId.indexOf(':');
  if (separator <= 0 || separator === subjectId.length - 1) {
    deny(`bound subject '${subjectId}' is not qualified as '<kind>:<id>'`);
  }
  return { kind: subjectId.slice(0, separator), nativeId: subjectId.slice(separator + 1) };
}

function subjectIdMatches(value: unknown, subjectId: string, nativeId: string): boolean {
  return value === subjectId || value === nativeId;
}

function authorizeSubject(request: BackendCallAuthorizationRequest, scope: BackendCallScope): { params: unknown } {
  requireRole(request, ['subject', 'subject_backend']);
  const parts = request.method.split('/');
  let routeKind: string | null = null;
  let verb: string;
  if (parts.length === 2 && parts[0] === 'subject') {
    verb = parts[1] ?? '';
  } else if (parts.length === 2) {
    routeKind = parts[0] ?? null;
    verb = parts[1] ?? '';
  } else if (parts.length === 3 && parts[1] === 'v2') {
    // V2 authorization also requires binding the nested request context actor.
    // The current reverse-call contract exposes only handle/role/method/params,
    // so fail closed instead of trusting a node-authored context.actor.
    deny(`subject v2 method '${request.method}' requires trusted actor-context binding`);
  } else {
    deny(`unknown subject method '${request.method}'`);
  }

  if (verb === 'schema') {
    if (routeKind && scope.subjectId && routeKind !== subjectParts(scope.subjectId).kind) {
      deny(`subject schema kind '${routeKind}' is outside the bound subject`);
    }
    return { params: request.params };
  }
  if (!['get', 'update', 'status'].includes(verb)) {
    deny(`subject method '${request.method}' is not allowlisted`);
  }
  if (!scope.subjectId) deny(`subject method '${request.method}' arrived before a subject was bound`);

  const { kind, nativeId } = subjectParts(scope.subjectId);
  if (routeKind && routeKind !== kind) {
    deny(`subject route kind '${routeKind}' does not match bound kind '${kind}'`);
  }
  const params = record(request.params, `${request.method} params`);
  if (!subjectIdMatches(params.id, scope.subjectId, nativeId)) {
    deny(`subject id does not match bound subject '${scope.subjectId}'`);
  }
  if (params.kind !== undefined && params.kind !== kind) {
    deny(`subject kind does not match bound kind '${kind}'`);
  }
  return {
    params: {
      ...params,
      id: nativeId,
      ...(routeKind ? {} : { kind }),
    },
  };
}

function requireWorkflow(scope: BackendCallScope, method: string): string {
  if (!scope.workflowId) deny(`workflow method '${method}' arrived before a workflow was bound`);
  return scope.workflowId;
}

function authorizeJournal(request: BackendCallAuthorizationRequest, scope: BackendCallScope): { params: unknown } {
  requireRole(request, ['journal', 'workflow_journal']);
  if (request.method === 'journal/schema') return { params: request.params };
  const workflowId = requireWorkflow(scope, request.method);
  const params = record(request.params, `${request.method} params`);

  switch (request.method) {
    case 'journal/save': {
      const run = record(params.run, 'journal/save params.run');
      requireExactId(run.workflow_id, workflowId, 'journal/save run.workflow_id');
      const blob = run.blob === undefined || run.blob === null ? {} : record(run.blob, 'journal/save params.run.blob');
      return {
        params: {
          ...params,
          run: {
            ...run,
            workflow_id: workflowId,
            blob: {
              ...blob,
              ...(scope.subjectId ? { subject_id: scope.subjectId } : {}),
            },
          },
        },
      };
    }
    case 'journal/load':
      requireExactId(params.workflow_id, workflowId, 'journal/load workflow_id');
      return { params: { ...params, workflow_id: workflowId } };
    case 'journal/record': {
      const event = record(params.event, 'journal/record params.event');
      requireExactId(event.run_id, workflowId, 'journal/record event.run_id');
      return { params: { ...params, event: { ...event, run_id: workflowId } } };
    }
    case 'journal/events':
      if (params.run_id !== undefined && params.run_id !== null) {
        requireExactId(params.run_id, workflowId, 'journal/events run_id');
      }
      return { params: { ...params, run_id: workflowId } };
    case 'journal/checkpoint_save':
    case 'journal/checkpoint_load':
    case 'journal/checkpoint_list':
    case 'journal/checkpoint_prune':
      requireExactId(params.workflow_id, workflowId, `${request.method} workflow_id`);
      return { params: { ...params, workflow_id: workflowId } };
    default:
      deny(`journal method '${request.method}' is not allowlisted`);
  }
}

function authorizeLogs(request: BackendCallAuthorizationRequest, scope: BackendCallScope): { params: unknown } {
  requireRole(request, ['log', 'log_storage', 'log_storage_backend']);
  if (request.method === 'log_storage/schema') return { params: request.params };
  const workflowId = requireWorkflow(scope, request.method);
  const params = record(request.params, `${request.method} params`);
  if (request.method === 'log_storage/store') {
    if (!Array.isArray(params.entries) || params.entries.length > 1000) {
      deny('log_storage/store entries must be an array of at most 1000 records');
    }
    const entries = params.entries.map((value, index) => {
      const entry = record(value, `log_storage/store entries[${index}]`);
      const fields = entry.fields === undefined ? {} : record(entry.fields, `log_storage/store entries[${index}].fields`);
      return {
        ...entry,
        source_name: workflowId,
        fields: {
          ...fields,
          workflow_id: workflowId,
          ...(scope.subjectId ? { subject_id: scope.subjectId } : {}),
        },
      };
    });
    return { params: { ...params, entries } };
  }
  if (request.method === 'log_storage/query' || request.method === 'log_storage/tail') {
    return { params: { ...params, source_name: workflowId } };
  }
  deny(`log storage method '${request.method}' is not allowlisted`);
}

function authorizeConfig(request: BackendCallAuthorizationRequest): { params: unknown } {
  requireRole(request, ['config', 'config_source']);
  if (request.method !== 'config/load' && request.method !== 'config/validate') {
    deny(`config method '${request.method}' is not allowlisted; node config is read-only`);
  }
  return { params: request.params };
}

function authorizeCredentials(request: BackendCallAuthorizationRequest, scope: BackendCallScope): { params: unknown } {
  requireRole(request, ['credentials']);
  if (request.method !== 'git/token') deny(`credentials method '${request.method}' is not allowlisted`);
  if (!scope.repository) deny('git/token requires a repository-bound handle');
  return { params: { owner: scope.repository.owner, repo: scope.repository.repo } };
}

/** Build the concrete Railway node policy. It is intentionally a closed
 * dispatch table: new backend methods fail until their trust and row semantics
 * are reviewed and added here. */
export function makeScopedBackendCallAuthorizer(
  scopes: ReadonlyMap<string, BackendCallScope>,
): BackendCallAuthorizer {
  return (request) => {
    const scope = scopes.get(request.handleId);
    if (!scope) deny(`unknown or unbound environment handle '${request.handleId}'`);

    if (request.method.startsWith('config/')) return authorizeConfig(request);
    if (request.method.startsWith('journal/')) return authorizeJournal(request, scope);
    if (request.method.startsWith('log_storage/')) return authorizeLogs(request, scope);
    if (request.method === 'git/token' || request.role === 'credentials') {
      return authorizeCredentials(request, scope);
    }
    if (request.method.startsWith('queue/')) deny('the remote node queue is local and cannot call the parent queue');
    return authorizeSubject(request, scope);
  };
}
