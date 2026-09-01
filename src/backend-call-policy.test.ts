import { describe, expect, it } from 'vitest';

import { RelayErrorCode } from '@launchapp-dev/animus-env-transport';

import {
  makeScopedBackendCallAuthorizer,
  type BackendCallScope,
} from './backend-call-policy.js';

const BOUND: BackendCallScope = {
  subjectId: 'task:TASK-724',
  workflowId: 'wf-724',
  repository: { owner: 'launchapp-dev', repo: 'animus-env-transport' },
};

function authorizer(scope: BackendCallScope = BOUND) {
  return makeScopedBackendCallAuthorizer(new Map([['h1', scope]]));
}

function invoke(
  authorize: ReturnType<typeof makeScopedBackendCallAuthorizer>,
  request: Parameters<ReturnType<typeof makeScopedBackendCallAuthorizer>>[0],
) {
  return Promise.resolve().then(() => authorize(request));
}

function call(role: string, method: string, params: unknown = {}) {
  return invoke(authorizer(), { handleId: 'h1', role, method, params });
}

describe('makeScopedBackendCallAuthorizer', () => {
  it('allows only read-only config methods', async () => {
    await expect(call('config_source', 'config/load', { project_root: '/workspace' })).resolves.toEqual({
      params: { project_root: '/workspace' },
    });
    await expect(call('config_source', 'config/validate')).resolves.toEqual({ params: {} });
    await expect(call('config_source', 'config/write', { workflows: [] })).rejects.toMatchObject({
      code: RelayErrorCode.BackendCallForbidden,
      message: expect.stringContaining('read-only'),
    });
  });

  it('binds subject reads and writes to the one trusted subject', async () => {
    await expect(call('subject_backend', 'subject/get', { kind: 'task', id: 'task:TASK-724' })).resolves.toEqual({
      params: { kind: 'task', id: 'TASK-724' },
    });
    await expect(call('subject_backend', 'task/status', { id: 'TASK-724', status: 'in-progress' })).resolves.toEqual({
      params: { id: 'TASK-724', status: 'in-progress' },
    });
    await expect(call('subject_backend', 'task/update', { id: 'task:TASK-999', patch: {} })).rejects.toMatchObject({
      code: RelayErrorCode.BackendCallForbidden,
    });
    await expect(call('subject_backend', 'task/list')).rejects.toMatchObject({
      code: RelayErrorCode.BackendCallForbidden,
    });
    await expect(call('subject_backend', 'requirement/get', { id: 'TASK-724' })).rejects.toMatchObject({
      code: RelayErrorCode.BackendCallForbidden,
    });
    await expect(
      call('subject_backend', 'subject/v2/get', {
        id: 'TASK-724',
        context: { actor: { user_id: 'attacker' } },
      }),
    ).rejects.toMatchObject({
      code: RelayErrorCode.BackendCallForbidden,
      message: expect.stringContaining('trusted actor-context binding'),
    });
  });

  it('permits schema bootstrap but denies subject rows before exec_session binds one', async () => {
    const authorize = authorizer({ ...BOUND, subjectId: null });
    await expect(
      invoke(authorize, { handleId: 'h1', role: 'subject_backend', method: 'subject/schema', params: {} }),
    ).resolves.toEqual({ params: {} });
    await expect(
      invoke(authorize, {
        handleId: 'h1',
        role: 'subject_backend',
        method: 'subject/get',
        params: { id: 'TASK-724' },
      }),
    ).rejects.toMatchObject({ code: RelayErrorCode.BackendCallForbidden });
  });

  it('binds every journal row operation to the one workflow', async () => {
    await expect(call('workflow_journal', 'journal/load', { workflow_id: 'wf-724' })).resolves.toEqual({
      params: { workflow_id: 'wf-724' },
    });
    await expect(call('workflow_journal', 'journal/events', { run_id: null, limit: 10 })).resolves.toEqual({
      params: { run_id: 'wf-724', limit: 10 },
    });
    await expect(call('workflow_journal', 'journal/save', {
      run: {
        workflow_id: 'wf-724',
        status: 'running',
        blob: { subject_id: 'task:TASK-999', machine_state: 'running' },
      },
    })).resolves.toEqual({
      params: {
        run: {
          workflow_id: 'wf-724',
          status: 'running',
          blob: { subject_id: 'task:TASK-724', machine_state: 'running' },
        },
      },
    });
    await expect(call('workflow_journal', 'journal/load', { workflow_id: 'wf-other' })).rejects.toMatchObject({
      code: RelayErrorCode.BackendCallForbidden,
    });
    await expect(call('workflow_journal', 'journal/list', { status: 'running' })).rejects.toMatchObject({
      code: RelayErrorCode.BackendCallForbidden,
    });
    await expect(call('workflow_journal', 'journal/delete', { workflow_id: 'wf-724' })).rejects.toMatchObject({
      code: RelayErrorCode.BackendCallForbidden,
    });
  });

  it('forces log writes and reads into the bound workflow namespace', async () => {
    await expect(call('log_storage_backend', 'log_storage/store', {
      entries: [{ id: 'e1', source: 'daemon', source_name: 'forged', fields: { workflow_id: 'wf-other' } }],
    })).resolves.toEqual({
      params: {
        entries: [{
          id: 'e1',
          source: 'daemon',
          source_name: 'wf-724',
          fields: { workflow_id: 'wf-724', subject_id: 'task:TASK-724' },
        }],
      },
    });
    await expect(call('log_storage_backend', 'log_storage/query', { source_name: 'wf-other', limit: 25 })).resolves.toEqual({
      params: { source_name: 'wf-724', limit: 25 },
    });
  });

  it('forces credential refresh to the handle repository', async () => {
    await expect(call('credentials', 'git/token', { owner: 'attacker', repo: 'other' })).resolves.toEqual({
      params: { owner: 'launchapp-dev', repo: 'animus-env-transport' },
    });
    const authorize = authorizer({ ...BOUND, repository: null });
    await expect(
      invoke(authorize, { handleId: 'h1', role: 'credentials', method: 'git/token', params: {} }),
    ).rejects.toMatchObject({ code: RelayErrorCode.BackendCallForbidden });
  });

  it('allows kimi/token with no node-controlled params and rejects other credential methods', async () => {
    // The parent serves the bound run a fresh access-token-only Kimi
    // credential; attacker-supplied params are dropped, not forwarded.
    await expect(call('credentials', 'kimi/token', { family: 'other', credentialId: 'x' })).resolves.toEqual({
      params: {},
    });
    await expect(call('credentials', 'kimi/token')).resolves.toEqual({ params: {} });
    await expect(call('credentials', 'kimi/refresh_token')).rejects.toMatchObject({
      code: RelayErrorCode.BackendCallForbidden,
    });
    await expect(call('credentials', 'admin/dump')).rejects.toMatchObject({
      code: RelayErrorCode.BackendCallForbidden,
    });
    // Role confusion stays closed: kimi/token under a non-credentials role is
    // not routed to the credentials authorizer.
    await expect(call('subject_backend', 'kimi/token', {})).rejects.toMatchObject({
      code: RelayErrorCode.BackendCallForbidden,
    });
  });

  it('denies role confusion, the parent queue, unknown methods, and unknown handles', async () => {
    await expect(call('config_source', 'subject/get', { id: 'TASK-724' })).rejects.toMatchObject({
      code: RelayErrorCode.BackendCallForbidden,
    });
    await expect(call('queue', 'queue/lease')).rejects.toMatchObject({ code: RelayErrorCode.BackendCallForbidden });
    await expect(call('subject_backend', 'admin/drop_everything')).rejects.toMatchObject({
      code: RelayErrorCode.BackendCallForbidden,
    });
    await expect(
      invoke(authorizer(), { handleId: 'missing', role: 'subject_backend', method: 'subject/schema', params: {} }),
    ).rejects.toMatchObject({ code: RelayErrorCode.BackendCallForbidden });
  });
});
