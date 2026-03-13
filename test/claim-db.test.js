import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { ContextDatabase } from '../src/db/database.js';

async function makeTempDb() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'contextos-claim-test-'));
  await fs.mkdir(path.join(root, 'data'), { recursive: true });
  const dbPath = path.join(root, 'data', 'contextos.db');
  const db = new ContextDatabase(dbPath);
  return { db, root };
}

function seedData(db) {
  const conv = db.createConversation('Test Conversation');
  const msg = db.insertMessage({
    conversationId: conv.id, role: 'user', direction: 'inbound',
    content: 'test message', tokenCount: 10, capturedAt: new Date().toISOString(),
  });
  // Create entities so FK constraints on observations are satisfied
  const ent1 = db.insertEntity({ label: 'Test Entity 1', kind: 'concept' });
  const ent2 = db.insertEntity({ label: 'Test Entity 2', kind: 'concept' });
  const obs = db.insertObservation({
    conversationId: conv.id, messageId: msg.id, actorId: 'user',
    category: 'fact', detail: 'test observation', confidence: 0.9,
    createdAt: new Date().toISOString(),
  });
  return { conv, msg, obs, ent1, ent2 };
}

function insertExtraObservation(db, context, overrides = {}) {
  return db.insertObservation({
    conversationId: context.conv.id,
    messageId: context.msg.id,
    actorId: overrides.actorId ?? 'user',
    category: overrides.category ?? 'fact',
    predicate: overrides.predicate ?? 'status',
    subjectEntityId: overrides.subjectEntityId ?? context.ent1.id,
    objectEntityId: overrides.objectEntityId ?? null,
    detail: overrides.detail ?? 'extra observation',
    confidence: overrides.confidence ?? 0.9,
    scopeKind: overrides.scopeKind ?? 'private',
    scopeId: overrides.scopeId ?? null,
  });
}

function insertClaimFixture(db, context, overrides = {}) {
  const claimType = overrides.claim_type ?? 'fact';
  const subjectEntityId = overrides.subject_entity_id ?? context.ent1.id;
  const predicate = overrides.predicate ?? 'status';
  const timestamp = overrides.created_at ?? overrides.createdAt ?? '2000-01-01T00:00:00.000Z';
  const observationId = overrides.observation_id
    ?? overrides.observationId
    ?? insertExtraObservation(db, context, {
      actorId: overrides.actor_id ?? 'user',
      category: claimType,
      predicate,
      subjectEntityId,
      objectEntityId: overrides.object_entity_id ?? null,
      detail: overrides.value_text ?? 'test fact',
      confidence: overrides.confidence ?? 0.9,
      scopeKind: overrides.scope_kind ?? 'private',
      scopeId: overrides.scope_id ?? null,
    }).id;

  return db.insertClaim({
    observation_id: observationId,
    conversation_id: context.conv.id,
    message_id: context.msg.id,
    actor_id: overrides.actor_id ?? 'user',
    claim_type: claimType,
    subject_entity_id: subjectEntityId,
    predicate,
    object_entity_id: overrides.object_entity_id ?? null,
    value_text: overrides.value_text ?? 'test fact',
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

describe('Claim Database Methods', () => {
  it('insertClaim creates a claim and returns it', async () => {
    const { db, root } = await makeTempDb();
    const { conv, msg, obs } = seedData(db);
    try {
      const claim = db.insertClaim({
        observation_id: obs.id,
        conversation_id: conv.id,
        message_id: msg.id,
        actor_id: 'user',
        claim_type: 'fact',
        value_text: 'test fact',
        confidence: 0.9,
        source_type: 'explicit',
        lifecycle_state: 'active',
      });
      assert.ok(claim.id);
      assert.equal(claim.claim_type, 'fact');
      assert.equal(claim.lifecycle_state, 'active');
    } finally { db.close(); await fs.rm(root, { recursive: true, force: true }); }
  });

  it('getClaim returns claim by ID', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);
    try {
      const claim = insertClaimFixture(db, context, { observation_id: context.obs.id });
      const fetched = db.getClaim(claim.id);
      assert.equal(fetched?.id, claim.id);
      assert.equal(fetched?.observation_id, context.obs.id);
      assert.equal(fetched?.value_text, 'test fact');
    } finally { db.close(); await fs.rm(root, { recursive: true, force: true }); }
  });

  it('getClaim returns null for missing ID', async () => {
    const { db, root } = await makeTempDb();
    seedData(db);
    try {
      assert.equal(db.getClaim('claim_missing'), null);
    } finally { db.close(); await fs.rm(root, { recursive: true, force: true }); }
  });

  it('getClaimsByIds returns matching claims', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);
    try {
      const first = insertClaimFixture(db, context, { observation_id: context.obs.id });
      const second = insertClaimFixture(db, context, { value_text: 'second fact' });
      const third = insertClaimFixture(db, context, { value_text: 'third fact' });
      const claims = db.getClaimsByIds([third.id, 'claim_missing', first.id]);

      assert.equal(second.id !== third.id, true);
      assert.deepEqual(claims.map((claim) => claim.id), [third.id, first.id]);
    } finally { db.close(); await fs.rm(root, { recursive: true, force: true }); }
  });

  it('getClaimByObservationId returns linked claim', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);
    try {
      const claim = insertClaimFixture(db, context, { observation_id: context.obs.id });
      const fetched = db.getClaimByObservationId(context.obs.id);
      assert.equal(fetched?.id, claim.id);
    } finally { db.close(); await fs.rm(root, { recursive: true, force: true }); }
  });

  it('getClaimByObservationId returns null if none', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);
    try {
      assert.equal(db.getClaimByObservationId(context.obs.id), null);
    } finally { db.close(); await fs.rm(root, { recursive: true, force: true }); }
  });

  it('listCurrentClaims returns only active claims', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);
    try {
      const active = insertClaimFixture(db, context, { observation_id: context.obs.id });
      const candidate = insertClaimFixture(db, context, {
        lifecycle_state: 'candidate',
        value_text: 'candidate fact',
      });
      const claims = db.listCurrentClaims();

      assert.deepEqual(claims.map((claim) => claim.id), [active.id]);
      assert.equal(claims.some((claim) => claim.id === candidate.id), false);
    } finally { db.close(); await fs.rm(root, { recursive: true, force: true }); }
  });

  it('listCurrentClaims filters by types', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);
    try {
      insertClaimFixture(db, context, { observation_id: context.obs.id, claim_type: 'fact' });
      const taskClaim = insertClaimFixture(db, context, {
        claim_type: 'task',
        value_text: 'open task',
      });
      const claims = db.listCurrentClaims({ types: ['task'] });

      assert.deepEqual(claims.map((claim) => claim.id), [taskClaim.id]);
      assert.equal(claims[0].claim_type, 'task');
    } finally { db.close(); await fs.rm(root, { recursive: true, force: true }); }
  });

  it('listCurrentClaims excludes superseded and archived claims', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);
    try {
      const active = insertClaimFixture(db, context, { observation_id: context.obs.id });
      const superseded = insertClaimFixture(db, context, {
        lifecycle_state: 'superseded',
        value_text: 'old fact',
      });
      const archived = insertClaimFixture(db, context, {
        lifecycle_state: 'archived',
        value_text: 'archived fact',
      });
      const claims = db.listCurrentClaims();
      const ids = claims.map((claim) => claim.id);

      assert.equal(ids.includes(active.id), true);
      assert.equal(ids.includes(superseded.id), false);
      assert.equal(ids.includes(archived.id), false);
    } finally { db.close(); await fs.rm(root, { recursive: true, force: true }); }
  });

  it('listClaimsByResolutionKey returns ordered results', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);
    try {
      const older = insertClaimFixture(db, context, {
        observation_id: context.obs.id,
        resolution_key: 'fact:ent_1:status',
        created_at: '2000-01-01T00:00:00.000Z',
        updated_at: '2000-01-01T00:00:00.000Z',
      });
      const newer = insertClaimFixture(db, context, {
        resolution_key: 'fact:ent_1:status',
        created_at: '2000-01-02T00:00:00.000Z',
        updated_at: '2000-01-02T00:00:00.000Z',
      });
      const claims = db.listClaimsByResolutionKey('fact:ent_1:status');

      assert.deepEqual(claims.map((claim) => claim.id), [newer.id, older.id]);
    } finally { db.close(); await fs.rm(root, { recursive: true, force: true }); }
  });

  it('updateClaim changes lifecycle_state and updated_at', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);
    try {
      const claim = insertClaimFixture(db, context, {
        observation_id: context.obs.id,
        created_at: '2000-01-01T00:00:00.000Z',
        updated_at: '2000-01-01T00:00:00.000Z',
      });
      const updated = db.updateClaim(claim.id, { lifecycle_state: 'archived' });

      assert.equal(updated?.lifecycle_state, 'archived');
      assert.notEqual(updated?.updated_at, claim.updated_at);
      assert.ok(Date.parse(updated.updated_at) > Date.parse(claim.updated_at));
    } finally { db.close(); await fs.rm(root, { recursive: true, force: true }); }
  });

  it('getClaimStateStats returns counts by type x state', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);
    try {
      insertClaimFixture(db, context, {
        observation_id: context.obs.id,
        claim_type: 'fact',
        lifecycle_state: 'active',
      });
      insertClaimFixture(db, context, {
        claim_type: 'fact',
        lifecycle_state: 'archived',
        value_text: 'archived fact',
      });
      insertClaimFixture(db, context, {
        claim_type: 'task',
        lifecycle_state: 'candidate',
        value_text: 'candidate task',
      });

      assert.deepEqual(db.getClaimStateStats(), {
        fact: {
          active: 1,
          archived: 1,
        },
        task: {
          candidate: 1,
        },
      });
    } finally { db.close(); await fs.rm(root, { recursive: true, force: true }); }
  });

  it('rejects invalid lifecycle transitions', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);
    try {
      const claim = insertClaimFixture(db, context, {
        observation_id: context.obs.id,
        lifecycle_state: 'active',
      });

      assert.throws(
        () => db.updateClaim(claim.id, { lifecycle_state: 'candidate' }),
        /Invalid claim lifecycle transition: active -> candidate/,
      );
      assert.equal(db.getClaim(claim.id)?.lifecycle_state, 'active');
    } finally { db.close(); await fs.rm(root, { recursive: true, force: true }); }
  });

  it('normalizes linked active claims to superseded and keeps them out of current truth', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);
    try {
      const newer = insertClaimFixture(db, context, {
        observation_id: context.obs.id,
        lifecycle_state: 'active',
        value_text: 'new fact',
      });
      const older = insertClaimFixture(db, context, {
        lifecycle_state: 'active',
        superseded_by_claim_id: newer.id,
        value_text: 'old fact',
      });

      assert.equal(older.lifecycle_state, 'superseded');
      assert.ok(older.valid_to);
      assert.deepEqual(db.listCurrentClaims().map((claim) => claim.id), [newer.id]);
    } finally { db.close(); await fs.rm(root, { recursive: true, force: true }); }
  });

  it('tracks honest backfill coverage states', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);
    try {
      const withClaim = insertExtraObservation(db, context, { detail: 'with claim' });
      const noClaim = insertExtraObservation(db, context, { detail: 'no claim yet' });
      const failed = insertExtraObservation(db, context, { detail: 'failed claim' });
      const pending = insertExtraObservation(db, context, { detail: 'still pending' });

      db.insertClaim({
        observation_id: withClaim.id,
        conversation_id: context.conv.id,
        message_id: context.msg.id,
        actor_id: 'user',
        claim_type: 'fact',
        subject_entity_id: context.ent1.id,
        predicate: 'status',
        value_text: 'backfilled claim',
        lifecycle_state: 'active',
        source_type: 'explicit',
      });
      db.upsertClaimBackfillStatus({ observationId: noClaim.id, status: 'no_claim' });
      db.upsertClaimBackfillStatus({ observationId: failed.id, status: 'failed', errorMessage: 'boom' });

      assert.deepEqual(db.getClaimBackfillCoverage(), {
        total_observations: 5,
        not_yet_processed: 2,
        processed_with_claims: 1,
        processed_with_no_claim: 1,
        failed: 1,
        processed: 3,
        remaining: 2,
        completion_ratio: 0.6,
      });
      assert.equal(db.listObservationsForClaimBackfill().map((row) => row.id).includes(noClaim.id), false);
      assert.equal(db.listObservationsForClaimBackfill().map((row) => row.id).includes(failed.id), true);
      assert.equal(db.listObservationsForClaimBackfill().map((row) => row.id).includes(pending.id), true);
    } finally { db.close(); await fs.rm(root, { recursive: true, force: true }); }
  });
});
