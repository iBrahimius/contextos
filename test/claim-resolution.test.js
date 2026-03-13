import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  SOURCE_TYPE_RANK, CLAIM_SOURCE_TYPES,
  computeResolutionKey, computeFacetKey,
  buildClaimFromObservation, resolveSupersession,
  buildClaimResolutionKey, buildClaimFacetKey, dedupeClaimsByFacet,
  selectPreferredClaimsByResolution, analyzeClaimsTruthSet,
} from '../src/core/claim-resolution.js';

const baseObservation = {
  id: 'obs_1',
  conversation_id: 'conv_1',
  message_id: 'msg_1',
  actor_id: 'user',
  category: 'task',
  predicate: 'status',
  subject_entity_id: 'ent_1',
  detail: 'Build claims table',
  confidence: 0.9,
  scope_kind: 'private',
  created_at: '2026-03-07T10:00:00.000Z',
};

function makeClaim(overrides = {}) {
  const timestamp = overrides.updated_at ?? overrides.created_at ?? '2026-03-07T10:00:00.000Z';
  return {
    ...overrides,
    id: overrides.id ?? 'claim_new',
    resolution_key: overrides.resolution_key ?? 'task:ent_1:status',
    facet_key: overrides.facet_key ?? 'status',
    source_type: overrides.source_type ?? 'implicit',
    confidence: overrides.confidence ?? 0.8,
    lifecycle_state: overrides.lifecycle_state ?? 'active',
    created_at: overrides.created_at ?? timestamp,
    updated_at: timestamp,
  };
}

describe('computeResolutionKey', () => {
  it('returns type:subject:predicate format', () => {
    assert.equal(computeResolutionKey('fact', 'entity_1', 'status'), 'fact:entity_1:status');
  });

  it('returns null when subject missing', () => {
    assert.equal(computeResolutionKey('fact', null, 'status'), null);
  });

  it('returns null when predicate missing', () => {
    assert.equal(computeResolutionKey('fact', 'entity_1', null), null);
  });
});

describe('buildClaimFromObservation', () => {
  it('maps category to claim_type', () => {
    const claim = buildClaimFromObservation(baseObservation);

    assert.equal(claim.claim_type, 'task');
    assert.equal(claim.facet_key, computeFacetKey(baseObservation));
  });

  it('infers source_type explicit for user actor', () => {
    const claim = buildClaimFromObservation(baseObservation);

    assert.equal(claim.source_type, 'explicit');
  });

  it('infers source_type implicit for agent actor', () => {
    const claim = buildClaimFromObservation({
      ...baseObservation,
      actor_id: 'agent',
    });

    assert.equal(claim.source_type, 'implicit');
  });

  it('sets lifecycle active when confidence >= 0.5', () => {
    const claim = buildClaimFromObservation(baseObservation);

    assert.equal(claim.lifecycle_state, 'active');
  });

  it('sets lifecycle candidate when confidence < 0.5', () => {
    const claim = buildClaimFromObservation({
      ...baseObservation,
      confidence: 0.49,
    });

    assert.equal(claim.lifecycle_state, 'candidate');
  });
});

describe('resolveSupersession', () => {
  it('returns activate when no existing claims', () => {
    assert.deepEqual(resolveSupersession(makeClaim(), []), {
      action: 'activate',
      supersedes: null,
    });
  });

  it('returns supersede when explicit > implicit', () => {
    const existing = makeClaim({
      id: 'claim_existing',
      source_type: 'implicit',
      confidence: 0.8,
    });
    const newer = makeClaim({
      id: 'claim_new',
      source_type: 'explicit',
      confidence: 0.8,
    });

    assert.ok(SOURCE_TYPE_RANK.explicit > SOURCE_TYPE_RANK.implicit);
    assert.deepEqual(resolveSupersession(newer, [existing]), {
      action: 'supersede',
      supersedes: 'claim_existing',
    });
  });

  it('returns supersede when higher confidence', () => {
    const existing = makeClaim({
      id: 'claim_existing',
      source_type: 'implicit',
      confidence: 0.7,
    });
    const newer = makeClaim({
      id: 'claim_new',
      source_type: 'implicit',
      confidence: 0.95,
    });

    assert.deepEqual(resolveSupersession(newer, [existing]), {
      action: 'supersede',
      supersedes: 'claim_existing',
    });
  });

  it('returns dispute when all equal', () => {
    const existing = makeClaim({
      id: 'claim_existing',
      source_type: 'implicit',
      confidence: 0.8,
      updated_at: '2026-03-07T10:00:00.000Z',
    });
    const newer = makeClaim({
      id: 'claim_new',
      source_type: 'implicit',
      confidence: 0.8,
      updated_at: '2026-03-07T10:00:00.000Z',
    });

    assert.deepEqual(resolveSupersession(newer, [existing]), {
      action: 'dispute',
      conflictsWith: 'claim_existing',
      conflictsWithIds: ['claim_existing'],
    });
  });

  it('prefers one current path per resolution key even when facet keys differ', () => {
    const existing = makeClaim({
      id: 'claim_existing',
      resolution_key: 'task:ent_1:status',
      facet_key: 'task|ent_1|status|none|stale',
      source_type: 'implicit',
      confidence: 0.7,
    });
    const newer = makeClaim({
      id: 'claim_new',
      resolution_key: 'task:ent_1:status',
      facet_key: 'task|ent_1|status|none|fresh',
      source_type: 'explicit',
      confidence: 0.9,
      updated_at: '2026-03-07T11:00:00.000Z',
    });

    assert.deepEqual(resolveSupersession(newer, [existing]), {
      action: 'supersede',
      supersedes: 'claim_existing',
    });
  });
});

describe('selectPreferredClaimsByResolution', () => {
  it('prefers active claims over superseded or disputed history in current-truth views', () => {
    const selected = selectPreferredClaimsByResolution([
      makeClaim({
        id: 'claim_superseded',
        resolution_key: 'task:ent_1:status',
        facet_key: 'task|ent_1|status|none|old',
        lifecycle_state: 'superseded',
        confidence: 0.95,
      }),
      makeClaim({
        id: 'claim_disputed',
        resolution_key: 'task:ent_1:status',
        facet_key: 'task|ent_1|status|none|conflict',
        lifecycle_state: 'disputed',
        confidence: 0.99,
      }),
      makeClaim({
        id: 'claim_active',
        resolution_key: 'task:ent_1:status',
        facet_key: 'task|ent_1|status|none|current',
        lifecycle_state: 'active',
        confidence: 0.8,
      }),
    ]);

    assert.deepEqual(selected.map((claim) => claim.id), ['claim_active']);
  });
});

const CLAIM_RESOLUTION_TEST_TIMESTAMP = '2026-03-07T10:00:00.000Z';

let claimResolutionDbDepsPromise;

async function getClaimResolutionDbDeps() {
  if (!claimResolutionDbDepsPromise) {
    claimResolutionDbDepsPromise = Promise.all([
      import('node:fs/promises'),
      import('node:os'),
      import('node:path'),
      import('../src/db/database.js'),
      import('../src/core/claim-resolution.js'),
    ]).then(([fs, os, path, dbModule, claimResolutionModule]) => ({
      fs,
      os,
      path,
      ContextDatabase: dbModule.ContextDatabase,
      ensureClaimForObservation: claimResolutionModule.ensureClaimForObservation,
    }));
  }

  return claimResolutionDbDepsPromise;
}

async function makeClaimResolutionDb() {
  const deps = await getClaimResolutionDbDeps();
  const root = await deps.fs.mkdtemp(deps.path.join(deps.os.tmpdir(), 'contextos-claim-resolution-test-'));
  await deps.fs.mkdir(deps.path.join(root, 'data'), { recursive: true });
  const dbPath = deps.path.join(root, 'data', 'contextos.db');
  const db = new deps.ContextDatabase(dbPath);

  return {
    ...deps,
    db,
    root,
  };
}

async function destroyClaimResolutionDb(runtime) {
  runtime.db.close();
  await runtime.fs.rm(runtime.root, { recursive: true, force: true });
}

function seedClaimResolutionData(db) {
  const conv = db.createConversation('Claim Resolution Test Conversation');
  const msg = db.insertMessage({
    conversationId: conv.id,
    role: 'user',
    direction: 'inbound',
    content: 'claim resolution test message',
    tokenCount: 10,
    capturedAt: CLAIM_RESOLUTION_TEST_TIMESTAMP,
  });
  const ent1 = db.insertEntity({ label: 'Claim Subject', kind: 'concept' });
  const ent2 = db.insertEntity({ label: 'Claim Object', kind: 'concept' });
  const obs = db.insertObservation({
    conversationId: conv.id,
    messageId: msg.id,
    actorId: 'user',
    category: 'fact',
    detail: 'seed observation',
    confidence: 0.9,
    createdAt: CLAIM_RESOLUTION_TEST_TIMESTAMP,
  });

  return { conv, msg, obs, ent1, ent2 };
}

function insertClaimResolutionObservation(db, context, overrides = {}) {
  const hasSubjectEntityId = Object.prototype.hasOwnProperty.call(overrides, 'subjectEntityId');
  const hasObjectEntityId = Object.prototype.hasOwnProperty.call(overrides, 'objectEntityId');
  const actorId = overrides.actorId ?? 'user';
  const category = overrides.category ?? 'task';
  const predicate = overrides.predicate ?? 'status';
  const subjectEntityId = hasSubjectEntityId ? overrides.subjectEntityId : context.ent1.id;
  const objectEntityId = hasObjectEntityId ? overrides.objectEntityId : null;
  const detail = overrides.detail ?? 'claim observation';
  const confidence = overrides.confidence ?? 0.9;
  const scopeKind = overrides.scopeKind ?? 'private';
  const scopeId = overrides.scopeId ?? null;
  const createdAt = overrides.createdAt ?? CLAIM_RESOLUTION_TEST_TIMESTAMP;
  const inserted = db.insertObservation({
    conversationId: context.conv.id,
    messageId: context.msg.id,
    actorId,
    category,
    predicate,
    subjectEntityId,
    objectEntityId,
    detail,
    confidence,
    scopeKind,
    scopeId,
  });

  return {
    id: inserted.id,
    conversation_id: context.conv.id,
    message_id: context.msg.id,
    actor_id: actorId,
    category,
    predicate,
    subject_entity_id: subjectEntityId,
    object_entity_id: objectEntityId,
    detail,
    confidence,
    scope_kind: scopeKind,
    scope_id: scopeId,
    created_at: createdAt,
  };
}

function insertClaimResolutionFixture(db, context, overrides = {}) {
  const claimType = overrides.claim_type ?? 'task';
  const hasSubjectEntityId = Object.prototype.hasOwnProperty.call(overrides, 'subject_entity_id');
  const hasObjectEntityId = Object.prototype.hasOwnProperty.call(overrides, 'object_entity_id');
  const subjectEntityId = hasSubjectEntityId ? overrides.subject_entity_id : context.ent1.id;
  const objectEntityId = hasObjectEntityId ? overrides.object_entity_id : null;
  const predicate = overrides.predicate ?? 'status';
  const timestamp = overrides.created_at ?? overrides.createdAt ?? CLAIM_RESOLUTION_TEST_TIMESTAMP;
  const observation = overrides.observation ?? insertClaimResolutionObservation(db, context, {
    actorId: overrides.actor_id ?? 'user',
    category: claimType,
    predicate,
    subjectEntityId,
    objectEntityId,
    detail: overrides.value_text ?? 'claim fixture',
    confidence: overrides.confidence ?? 0.9,
    scopeKind: overrides.scope_kind ?? 'private',
    scopeId: overrides.scope_id ?? null,
    createdAt: timestamp,
  });

  return db.insertClaim({
    observation_id: overrides.observation_id ?? observation.id,
    conversation_id: context.conv.id,
    message_id: context.msg.id,
    actor_id: overrides.actor_id ?? 'user',
    claim_type: claimType,
    subject_entity_id: subjectEntityId,
    predicate,
    object_entity_id: objectEntityId,
    value_text: overrides.value_text ?? 'claim fixture',
    confidence: overrides.confidence ?? 0.9,
    source_type: overrides.source_type ?? 'explicit',
    lifecycle_state: overrides.lifecycle_state ?? 'active',
    valid_from: overrides.valid_from ?? timestamp,
    valid_to: overrides.valid_to ?? null,
    resolution_key: overrides.resolution_key ?? `${claimType}:${subjectEntityId}:${predicate}`,
    facet_key: overrides.facet_key ?? predicate,
    supersedes_claim_id: overrides.supersedes_claim_id ?? null,
    superseded_by_claim_id: overrides.superseded_by_claim_id ?? null,
    scope_kind: overrides.scope_kind ?? 'private',
    scope_id: overrides.scope_id ?? null,
    created_at: timestamp,
    updated_at: overrides.updated_at ?? overrides.updatedAt ?? timestamp,
  });
}

describe('ensureClaimForObservation', () => {
  it('inserts claim with active state when no conflicts', async () => {
    const runtime = await makeClaimResolutionDb();
    const context = seedClaimResolutionData(runtime.db);

    try {
      const observation = insertClaimResolutionObservation(runtime.db, context, {
        actorId: 'user',
        category: 'task',
        predicate: 'status',
        subjectEntityId: context.ent1.id,
        detail: 'Ship conflict resolution',
      });
      const claim = runtime.ensureClaimForObservation(runtime.db, observation);

      assert.equal(claim.lifecycle_state, 'active');
      assert.equal(claim.resolution_key, `task:${context.ent1.id}:status`);
      assert.deepEqual(
        runtime.db.listClaimsByResolutionKey(claim.resolution_key).map((entry) => entry.id),
        [claim.id],
      );
    } finally {
      await destroyClaimResolutionDb(runtime);
    }
  });

  it('supersedes existing claim with same resolution key (explicit > implicit)', async () => {
    const runtime = await makeClaimResolutionDb();
    const context = seedClaimResolutionData(runtime.db);

    try {
      const existingClaim = insertClaimResolutionFixture(runtime.db, context, {
        actor_id: 'agent',
        source_type: 'implicit',
        lifecycle_state: 'active',
        value_text: 'Assistant inferred status',
      });
      const observation = insertClaimResolutionObservation(runtime.db, context, {
        actorId: 'user',
        category: 'task',
        predicate: 'status',
        subjectEntityId: context.ent1.id,
        detail: 'User confirmed status',
        createdAt: '2026-03-07T11:00:00.000Z',
      });
      const claim = runtime.ensureClaimForObservation(runtime.db, observation);
      const refreshedExisting = runtime.db.getClaim(existingClaim.id);

      assert.equal(claim.lifecycle_state, 'active');
      assert.equal(claim.supersedes_claim_id, existingClaim.id);
      assert.equal(refreshedExisting?.lifecycle_state, 'superseded');
      assert.equal(refreshedExisting?.valid_to, claim.valid_from);
      assert.deepEqual(
        runtime.db.listClaimsByResolutionKey(claim.resolution_key).map((entry) => entry.id),
        [claim.id, existingClaim.id],
      );
    } finally {
      await destroyClaimResolutionDb(runtime);
    }
  });

  it('marks both as disputed when equal strength', async () => {
    const runtime = await makeClaimResolutionDb();
    const context = seedClaimResolutionData(runtime.db);

    try {
      const existingClaim = insertClaimResolutionFixture(runtime.db, context, {
        actor_id: 'user',
        source_type: 'explicit',
        confidence: 0.9,
        created_at: CLAIM_RESOLUTION_TEST_TIMESTAMP,
        updated_at: CLAIM_RESOLUTION_TEST_TIMESTAMP,
        value_text: 'Existing user claim',
      });
      const observation = insertClaimResolutionObservation(runtime.db, context, {
        actorId: 'user',
        category: 'task',
        predicate: 'status',
        subjectEntityId: context.ent1.id,
        detail: 'Conflicting user claim',
        confidence: 0.9,
        createdAt: CLAIM_RESOLUTION_TEST_TIMESTAMP,
      });
      const claim = runtime.ensureClaimForObservation(runtime.db, observation);
      const refreshedExisting = runtime.db.getClaim(existingClaim.id);

      assert.equal(claim.lifecycle_state, 'disputed');
      assert.equal(refreshedExisting?.lifecycle_state, 'disputed');
    } finally {
      await destroyClaimResolutionDb(runtime);
    }
  });

  it('marks contradictory comparable claims disputed instead of superseding them', async () => {
    const runtime = await makeClaimResolutionDb();
    const context = seedClaimResolutionData(runtime.db);

    try {
      const existingClaim = insertClaimResolutionFixture(runtime.db, context, {
        actor_id: 'user',
        source_type: 'explicit',
        confidence: 0.92,
        predicate: 'provider',
        object_entity_id: context.ent2.id,
        value_text: 'Cloudflare',
        facet_key: `fact|${context.ent1.id}|provider|${context.ent2.id}|cloudflare`,
        resolution_key: `fact:${context.ent1.id}:provider`,
      });
      const alternativeEntity = runtime.db.insertEntity({ label: 'Route53', kind: 'service' });
      const observation = insertClaimResolutionObservation(runtime.db, context, {
        actorId: 'user',
        category: 'fact',
        predicate: 'provider',
        subjectEntityId: context.ent1.id,
        objectEntityId: alternativeEntity.id,
        detail: 'Route53',
        confidence: 0.95,
      });
      const claim = runtime.ensureClaimForObservation(runtime.db, observation);
      const refreshedExisting = runtime.db.getClaim(existingClaim.id);

      assert.equal(claim.lifecycle_state, 'disputed');
      assert.equal(refreshedExisting?.lifecycle_state, 'disputed');
      assert.equal(claim.supersedes_claim_id, null);
      assert.equal(refreshedExisting?.superseded_by_claim_id, null);
    } finally {
      await destroyClaimResolutionDb(runtime);
    }
  });

  it('handles null resolution key (no supersession check)', async () => {
    const runtime = await makeClaimResolutionDb();
    const context = seedClaimResolutionData(runtime.db);

    try {
      const observation = insertClaimResolutionObservation(runtime.db, context, {
        actorId: 'user',
        category: 'task',
        predicate: 'status',
        subjectEntityId: null,
        detail: 'Task without subject',
      });
      const claim = runtime.ensureClaimForObservation(runtime.db, observation);

      assert.equal(claim.lifecycle_state, 'active');
      assert.equal(claim.resolution_key, null);
    } finally {
      await destroyClaimResolutionDb(runtime);
    }
  });

  it('sets bidirectional links on supersession', async () => {
    const runtime = await makeClaimResolutionDb();
    const context = seedClaimResolutionData(runtime.db);

    try {
      const existingClaim = insertClaimResolutionFixture(runtime.db, context, {
        actor_id: 'agent',
        source_type: 'implicit',
        lifecycle_state: 'active',
        value_text: 'Assistant inferred preference',
      });
      const observation = insertClaimResolutionObservation(runtime.db, context, {
        actorId: 'user',
        category: 'task',
        predicate: 'status',
        subjectEntityId: context.ent1.id,
        detail: 'User confirmed preference',
        createdAt: '2026-03-07T11:30:00.000Z',
      });
      const claim = runtime.ensureClaimForObservation(runtime.db, observation);
      const refreshedClaim = runtime.db.getClaim(claim.id);
      const refreshedExisting = runtime.db.getClaim(existingClaim.id);

      assert.equal(refreshedClaim?.supersedes_claim_id, existingClaim.id);
      assert.equal(refreshedExisting?.superseded_by_claim_id, refreshedClaim?.id);
    } finally {
      await destroyClaimResolutionDb(runtime);
    }
  });
});

describe('buildClaimResolutionKey (4-level fallback cascade)', () => {
  it('Level 1: returns metadata-based explicit key when present', () => {
    const key = buildClaimResolutionKey(
      'fact',
      'entity_1',
      'status',
      null,
      null,
      { resolutionKey: 'Custom Key' }
    );
    assert.equal(key, 'fact|custom key');
  });

  it('Level 1: normalizes metadata key to lowercase', () => {
    const key = buildClaimResolutionKey(
      'fact',
      'entity_1',
      'status',
      null,
      null,
      { resolution_key: '  EXPLICIT   Key  ' }
    );
    assert.equal(key, 'fact|explicit key');
  });

  it('Level 2: relationship key with subject and object', () => {
    const key = buildClaimResolutionKey(
      'relationship',
      'entity_1',
      'knows',
      'entity_2',
      null,
      null
    );
    assert.equal(key, 'relationship|entity_1|knows|entity_2');
  });

  it('Level 2: relationship fills in default predicate', () => {
    const key = buildClaimResolutionKey(
      'relationship',
      'entity_1',
      null,
      'entity_2',
      null,
      null
    );
    assert.equal(key, 'relationship|entity_1|related_to|entity_2');
  });

  it('Level 3: predicate-based key with subject', () => {
    const key = buildClaimResolutionKey(
      'fact',
      'entity_1',
      'color',
      null,
      null,
      null
    );
    assert.equal(key, 'fact|entity_1|color|none');
  });

  it('Level 3: predicate-based key with object', () => {
    const key = buildClaimResolutionKey(
      'fact',
      null,
      'type',
      'entity_2',
      null,
      null
    );
    assert.equal(key, 'fact|none|type|entity_2');
  });

  it('Level 4: value-text based fallback', () => {
    const key = buildClaimResolutionKey(
      'fact',
      'entity_1',
      null,
      null,
      'This is the value text',
      null
    );
    assert.equal(key, 'fact|entity_1|this is the value text');
  });

  it('Level 4: truncates long value text to 160 chars', () => {
    const longValue = 'a'.repeat(200);
    const key = buildClaimResolutionKey(
      'fact',
      'entity_1',
      null,
      null,
      longValue,
      null
    );
    assert.equal(key.length <= 'fact|entity_1|'.length + 160, true);
  });

  it('returns null when all fallback levels fail', () => {
    const key = buildClaimResolutionKey(
      'fact',
      null,
      null,
      null,
      null,
      null
    );
    assert.equal(key, null);
  });

  it('normalizes whitespace in claim text', () => {
    const key = buildClaimResolutionKey(
      'fact',
      'entity_1',
      '  Status   Is   Active  ',
      null,
      null,
      null
    );
    assert.equal(key, 'fact|entity_1|status is active|none');
  });
});

describe('buildClaimFacetKey (5-component key)', () => {
  it('constructs 5-component facet key', () => {
    const key = buildClaimFacetKey(
      'fact',
      'entity_1',
      'color',
      'entity_2',
      'red'
    );
    assert.equal(key, 'fact|entity_1|color|entity_2|red');
  });

  it('defaults null subject to "none"', () => {
    const key = buildClaimFacetKey(
      'fact',
      null,
      'type',
      'entity_2',
      'value'
    );
    assert.equal(key, 'fact|none|type|entity_2|value');
  });

  it('defaults null object to "none"', () => {
    const key = buildClaimFacetKey(
      'fact',
      'entity_1',
      'property',
      null,
      'value'
    );
    assert.equal(key, 'fact|entity_1|property|none|value');
  });

  it('uses claim type as default predicate', () => {
    const key = buildClaimFacetKey(
      'task',
      'entity_1',
      null,
      'entity_2',
      'value'
    );
    assert.equal(key, 'task|entity_1|task|entity_2|value');
  });

  it('normalizes value text to lowercase', () => {
    const key = buildClaimFacetKey(
      'fact',
      'entity_1',
      'status',
      null,
      'COMPLETED'
    );
    assert.equal(key, 'fact|entity_1|status|none|completed');
  });

  it('truncates long value text to 160 chars', () => {
    const longValue = 'a'.repeat(200);
    const key = buildClaimFacetKey(
      'fact',
      'entity_1',
      'text',
      null,
      longValue
    );
    const parts = key.split('|');
    assert.equal(parts[4].length, 160);
  });

  it('prevents collision between different entities with same predicate', () => {
    const key1 = buildClaimFacetKey('fact', 'entity_1', 'color', null, 'red');
    const key2 = buildClaimFacetKey('fact', 'entity_2', 'color', null, 'red');
    assert.notEqual(key1, key2);
  });
});

describe('dedupeClaimsByFacet (retrieval-time deduplication)', () => {
  it('removes duplicate facet keys, keeps highest strength (confidence + freshness)', () => {
    const claims = [
      { id: 'c1', facet_key: 'key_1', lifecycle_state: 'active', confidence: 0.7, created_at: '2026-03-07T10:00:00Z' },
      { id: 'c2', facet_key: 'key_1', lifecycle_state: 'active', confidence: 0.8, created_at: '2026-03-07T11:00:00Z' },
    ];
    const deduped = dedupeClaimsByFacet(claims);
    assert.equal(deduped.length, 1);
    assert.equal(deduped[0].id, 'c2');
  });

  it('prefers active claim over non-active', () => {
    const claims = [
      { id: 'c1', facet_key: 'key_1', lifecycle_state: 'disputed', confidence: 0.9, created_at: '2026-03-07T10:00:00Z' },
      { id: 'c2', facet_key: 'key_1', lifecycle_state: 'active', confidence: 0.7, created_at: '2026-03-07T11:00:00Z' },
    ];
    const deduped = dedupeClaimsByFacet(claims);
    assert.equal(deduped.length, 1);
    assert.equal(deduped[0].id, 'c2');
  });

  it('prefers higher confidence when both active', () => {
    const claims = [
      { id: 'c1', facet_key: 'key_1', lifecycle_state: 'active', confidence: 0.6, created_at: '2026-03-07T10:00:00Z' },
      { id: 'c2', facet_key: 'key_1', lifecycle_state: 'active', confidence: 0.9, created_at: '2026-03-07T09:00:00Z' },
    ];
    const deduped = dedupeClaimsByFacet(claims);
    assert.equal(deduped.length, 1);
    assert.equal(deduped[0].id, 'c2');
  });

  it('prefers newer timestamp when equal confidence', () => {
    const claims = [
      { id: 'c1', facet_key: 'key_1', lifecycle_state: 'active', confidence: 0.8, created_at: '2026-03-07T10:00:00Z' },
      { id: 'c2', facet_key: 'key_1', lifecycle_state: 'active', confidence: 0.8, created_at: '2026-03-07T11:00:00Z' },
    ];
    const deduped = dedupeClaimsByFacet(claims);
    assert.equal(deduped.length, 1);
    assert.equal(deduped[0].id, 'c2');
  });

  it('handles null facet keys by using claim ID as unique key', () => {
    const claims = [
      { id: 'c1', facet_key: null, lifecycle_state: 'active', confidence: 0.8 },
      { id: 'c2', facet_key: null, lifecycle_state: 'active', confidence: 0.7 },
    ];
    const deduped = dedupeClaimsByFacet(claims);
    // Null facet keys use ID as key, so each is unique
    assert.equal(deduped.length, 2);
  });

  it('preserves claims with different facet keys', () => {
    const claims = [
      { id: 'c1', facet_key: 'key_1', lifecycle_state: 'active', confidence: 0.8, created_at: '2026-03-07T10:00:00Z' },
      { id: 'c2', facet_key: 'key_2', lifecycle_state: 'active', confidence: 0.7, created_at: '2026-03-07T11:00:00Z' },
      { id: 'c3', facet_key: 'key_3', lifecycle_state: 'active', confidence: 0.9, created_at: '2026-03-07T09:00:00Z' },
    ];
    const deduped = dedupeClaimsByFacet(claims);
    assert.equal(deduped.length, 3);
  });
});

describe('Source Type Inference with Confidence Thresholds', () => {
  it('uses explicit metadata when present', () => {
    const observation = {
      confidence: 0.5,
      origin_kind: 'user',
      metadata: { sourceType: 'derived' },
    };
    const claim = buildClaimFromObservation({
      ...baseObservation,
      ...observation,
    });
    assert.equal(claim.source_type, 'derived');
  });

  it('user origin: confidence >= 0.85 → explicit', () => {
    const claim = buildClaimFromObservation({
      ...baseObservation,
      origin_kind: 'user',
      confidence: 0.85,
    });
    assert.equal(claim.source_type, 'explicit');
  });

  it('user origin: 0.65 <= confidence < 0.85 → implicit', () => {
    const claim = buildClaimFromObservation({
      ...baseObservation,
      origin_kind: 'user',
      confidence: 0.75,
    });
    assert.equal(claim.source_type, 'implicit');
  });

  it('user origin: confidence < 0.65 → inference', () => {
    const claim = buildClaimFromObservation({
      ...baseObservation,
      origin_kind: 'user',
      confidence: 0.5,
    });
    assert.equal(claim.source_type, 'inference');
  });

  it('agent origin: confidence >= 0.8 → derived', () => {
    const claim = buildClaimFromObservation({
      ...baseObservation,
      origin_kind: 'agent',
      confidence: 0.85,
    });
    assert.equal(claim.source_type, 'derived');
  });

  it('agent origin: confidence < 0.8 → inference', () => {
    const claim = buildClaimFromObservation({
      ...baseObservation,
      origin_kind: 'agent',
      confidence: 0.7,
    });
    assert.equal(claim.source_type, 'inference');
  });
});

describe('CLAIM_SOURCE_TYPES set', () => {
  it('contains exactly 5 source types', () => {
    assert.equal(CLAIM_SOURCE_TYPES.size, 5);
  });

  it('includes explicit', () => {
    assert.equal(CLAIM_SOURCE_TYPES.has('explicit'), true);
  });

  it('includes implicit', () => {
    assert.equal(CLAIM_SOURCE_TYPES.has('implicit'), true);
  });

  it('includes inference', () => {
    assert.equal(CLAIM_SOURCE_TYPES.has('inference'), true);
  });

  it('includes derived', () => {
    assert.equal(CLAIM_SOURCE_TYPES.has('derived'), true);
  });

  it('includes unknown', () => {
    assert.equal(CLAIM_SOURCE_TYPES.has('unknown'), true);
  });
});


describe('analyzeClaimsTruthSet', () => {
  it('aggregates independent support without double-counting duplicate evidence', () => {
    const claims = [
      makeClaim({
        id: 'claim_support_1',
        observation_id: 'obs_same',
        message_id: 'msg_same',
        object_entity_id: 'ent_cloudflare',
        facet_key: 'fact|ent_1|provider|ent_cloudflare|cloudflare',
        confidence: 0.7,
      }),
      makeClaim({
        id: 'claim_support_2',
        observation_id: 'obs_same',
        message_id: 'msg_same',
        object_entity_id: 'ent_cloudflare',
        facet_key: 'fact|ent_1|provider|ent_cloudflare|cloudflare',
        confidence: 0.95,
      }),
      makeClaim({
        id: 'claim_support_3',
        observation_id: 'obs_other',
        message_id: 'msg_other',
        object_entity_id: 'ent_cloudflare',
        facet_key: 'fact|ent_1|provider|ent_cloudflare|cloudflare',
        confidence: 0.8,
      }),
    ];

    const analysis = analyzeClaimsTruthSet(claims);
    const truth = analysis.byClaimId.get('claim_support_1');

    assert.equal(truth.support_count, 2);
    assert.deepEqual(truth.support_keys, ['observation:obs_same', 'observation:obs_other']);
    assert.equal(truth.aggregated_confidence, 0.99);
    assert.equal(truth.effective_confidence, 0.99);
    assert.equal(truth.has_conflict, false);
  });

  it('detects conflicts only among current comparable variants and keeps superseded rows out of the conflict set', () => {
    const analysis = analyzeClaimsTruthSet([
      makeClaim({
        id: 'claim_cloudflare',
        object_entity_id: 'ent_cloudflare',
        facet_key: 'fact|ent_1|provider|ent_cloudflare|cloudflare',
        confidence: 0.9,
      }),
      makeClaim({
        id: 'claim_route53',
        object_entity_id: 'ent_route53',
        facet_key: 'fact|ent_1|provider|ent_route53|route53',
        confidence: 0.85,
        lifecycle_state: 'disputed',
      }),
      makeClaim({
        id: 'claim_old',
        object_entity_id: 'ent_legacy',
        facet_key: 'fact|ent_1|provider|ent_legacy|legacy',
        confidence: 0.95,
        lifecycle_state: 'superseded',
        superseded_by_claim_id: 'claim_cloudflare',
      }),
    ]);

    assert.equal(analysis.conflicts.length, 1);
    assert.deepEqual(analysis.conflicts[0].claim_ids.sort(), ['claim_cloudflare', 'claim_route53']);
    assert.equal(analysis.byClaimId.get('claim_cloudflare').has_conflict, true);
    assert.equal(analysis.byClaimId.get('claim_old').has_conflict, false);
  });

  it('keeps stale superseded truth visible but out of active truth, with explainability fields intact', () => {
    const analysis = analyzeClaimsTruthSet([
      makeClaim({
        id: 'claim_old',
        object_entity_id: 'ent_legacy',
        facet_key: 'fact|ent_1|provider|ent_legacy|legacy',
        confidence: 0.91,
        lifecycle_state: 'superseded',
        superseded_by_claim_id: 'claim_cloudflare',
      }),
      makeClaim({
        id: 'claim_cloudflare',
        object_entity_id: 'ent_cloudflare',
        facet_key: 'fact|ent_1|provider|ent_cloudflare|cloudflare',
        confidence: 0.88,
        lifecycle_state: 'active',
      }),
    ]);

    const stale = analysis.byClaimId.get('claim_old');
    const fresh = analysis.byClaimId.get('claim_cloudflare');

    assert.equal(analysis.conflicts.length, 0);
    assert.equal(fresh.is_current, true);
    assert.equal(fresh.is_superseded, false);
    assert.deepEqual(fresh.current_claim_ids, ['claim_cloudflare']);
    assert.deepEqual(fresh.support_claim_ids, ['claim_cloudflare']);
    assert.equal(fresh.representative_claim_id, 'claim_cloudflare');
    assert.equal(fresh.effective_confidence, 0.88);

    assert.equal(stale.is_current, false);
    assert.equal(stale.is_superseded, true);
    assert.deepEqual(stale.current_claim_ids, []);
    assert.deepEqual(stale.support_claim_ids, ['claim_old']);
    assert.equal(stale.has_conflict, false);
  });
});
