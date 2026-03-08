import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { ContextDatabase } from '../src/db/database.js';
import {
  createCachedRegistry,
  getDecisionRegistry,
  getGoalRegistry,
  getRegistrySnapshot,
  getRuleRegistry,
  getTaskRegistry,
} from '../src/core/claim-registries.js';

async function makeTempDb() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'contextos-claim-registries-test-'));
  await fs.mkdir(path.join(root, 'data'), { recursive: true });
  const dbPath = path.join(root, 'data', 'contextos.db');
  const db = new ContextDatabase(dbPath);
  return { db, root };
}

function seedData(db) {
  const conv = db.createConversation('Registry Test Conversation');
  const msg = db.insertMessage({
    conversationId: conv.id,
    role: 'user',
    direction: 'inbound',
    content: 'registry test message',
    tokenCount: 12,
    capturedAt: new Date().toISOString(),
  });
  const ent1 = db.insertEntity({ label: 'Entity One', kind: 'concept' });
  const ent2 = db.insertEntity({ label: 'Entity Two', kind: 'concept' });
  const ent3 = db.insertEntity({ label: 'Entity Three', kind: 'concept' });

  return { conv, msg, ent1, ent2, ent3 };
}

function insertObservationFixture(db, context, overrides = {}) {
  return db.insertObservation({
    conversationId: context.conv.id,
    messageId: context.msg.id,
    actorId: overrides.actorId ?? 'user',
    category: overrides.category ?? 'fact',
    predicate: overrides.predicate ?? 'status',
    subjectEntityId: overrides.subjectEntityId ?? context.ent1.id,
    objectEntityId: overrides.objectEntityId ?? null,
    detail: overrides.detail ?? 'registry observation',
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
  const observation = insertObservationFixture(db, context, {
    actorId: overrides.actor_id ?? 'user',
    category: claimType,
    predicate,
    subjectEntityId,
    objectEntityId: overrides.object_entity_id ?? null,
    detail: overrides.value_text ?? 'registry claim',
    confidence: overrides.confidence ?? 0.9,
    scopeKind: overrides.scope_kind ?? 'private',
    scopeId: overrides.scope_id ?? null,
  });

  return db.insertClaim({
    observation_id: observation.id,
    conversation_id: context.conv.id,
    message_id: context.msg.id,
    actor_id: overrides.actor_id ?? 'user',
    claim_type: claimType,
    subject_entity_id: subjectEntityId,
    predicate,
    object_entity_id: overrides.object_entity_id ?? null,
    value_text: overrides.value_text ?? 'registry claim',
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

function getGroup(registry, entityId) {
  return registry.find((group) => group.entityId === entityId) ?? null;
}

describe('Claim Registry Methods', () => {
  it('getTaskRegistry returns only task claims grouped by entity', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);

    try {
      insertClaimFixture(db, context, {
        claim_type: 'task',
        subject_entity_id: context.ent1.id,
        predicate: 'task',
        value_text: 'pending',
        created_at: '2000-01-01T00:00:00.000Z',
      });
      insertClaimFixture(db, context, {
        claim_type: 'task',
        subject_entity_id: context.ent1.id,
        predicate: 'follow_up',
        value_text: 'blocked',
        created_at: '2000-01-02T00:00:00.000Z',
      });
      insertClaimFixture(db, context, {
        claim_type: 'task',
        subject_entity_id: context.ent2.id,
        predicate: 'task',
        value_text: 'active',
        created_at: '2000-01-03T00:00:00.000Z',
      });
      insertClaimFixture(db, context, {
        claim_type: 'decision',
        subject_entity_id: context.ent3.id,
        predicate: 'decision',
        value_text: 'accepted',
        created_at: '2000-01-04T00:00:00.000Z',
      });

      const registry = getTaskRegistry(db);
      const ent1Group = getGroup(registry, context.ent1.id);
      const ent2Group = getGroup(registry, context.ent2.id);

      assert.equal(registry.length, 2);
      assert.deepEqual(ent1Group?.claims.map((claim) => claim.status), ['blocked', 'pending']);
      assert.deepEqual(ent1Group?.claims.map((claim) => claim.predicate), ['follow_up', 'task']);
      assert.deepEqual(ent2Group?.claims.map((claim) => claim.status), ['active']);
      assert.equal(registry.some((group) => group.entityId === context.ent3.id), false);
      assert.ok(ent1Group?.claims.every((claim) => claim.claimType === 'task'));
    } finally {
      db.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it('getDecisionRegistry returns only decision claims', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);

    try {
      insertClaimFixture(db, context, {
        claim_type: 'decision',
        subject_entity_id: context.ent1.id,
        predicate: 'architecture',
        value_text: 'accepted',
      });
      insertClaimFixture(db, context, {
        claim_type: 'goal',
        subject_entity_id: context.ent2.id,
        predicate: 'launch',
        value_text: 'active',
      });

      const registry = getDecisionRegistry(db);

      assert.equal(registry.length, 1);
      assert.equal(registry[0].entityId, context.ent1.id);
      assert.deepEqual(registry[0].claims.map((claim) => claim.status), ['accepted']);
      assert.ok(registry[0].claims.every((claim) => claim.claimType === 'decision'));
    } finally {
      db.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it('getGoalRegistry returns only goal claims', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);

    try {
      insertClaimFixture(db, context, {
        claim_type: 'goal',
        subject_entity_id: context.ent2.id,
        predicate: 'milestone',
        value_text: 'on_hold',
      });
      insertClaimFixture(db, context, {
        claim_type: 'task',
        subject_entity_id: context.ent1.id,
        predicate: 'task',
        value_text: 'active',
      });

      const registry = getGoalRegistry(db);

      assert.equal(registry.length, 1);
      assert.equal(registry[0].entityId, context.ent2.id);
      assert.deepEqual(registry[0].claims.map((claim) => claim.status), ['on_hold']);
      assert.ok(registry[0].claims.every((claim) => claim.claimType === 'goal'));
    } finally {
      db.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it('getRuleRegistry returns both rule and constraint claims', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);

    try {
      insertClaimFixture(db, context, {
        claim_type: 'rule',
        subject_entity_id: context.ent1.id,
        predicate: 'always',
        value_text: 'active',
        created_at: '2000-01-01T00:00:00.000Z',
      });
      insertClaimFixture(db, context, {
        claim_type: 'constraint',
        subject_entity_id: context.ent2.id,
        predicate: 'limit',
        value_text: 'active',
        created_at: '2000-01-02T00:00:00.000Z',
      });
      insertClaimFixture(db, context, {
        claim_type: 'goal',
        subject_entity_id: context.ent3.id,
        predicate: 'ship',
        value_text: 'active',
        created_at: '2000-01-03T00:00:00.000Z',
      });

      const registry = getRuleRegistry(db);
      const claimTypes = registry.flatMap((group) => group.claims.map((claim) => claim.claimType)).sort();

      assert.equal(registry.length, 2);
      assert.deepEqual(claimTypes, ['constraint', 'rule']);
    } finally {
      db.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it('getRegistrySnapshot returns all four registries', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);

    try {
      insertClaimFixture(db, context, {
        claim_type: 'task',
        subject_entity_id: context.ent1.id,
        predicate: 'task',
        value_text: 'pending',
      });
      insertClaimFixture(db, context, {
        claim_type: 'decision',
        subject_entity_id: context.ent2.id,
        predicate: 'architecture',
        value_text: 'accepted',
      });
      insertClaimFixture(db, context, {
        claim_type: 'goal',
        subject_entity_id: context.ent3.id,
        predicate: 'launch',
        value_text: 'active',
      });
      insertClaimFixture(db, context, {
        claim_type: 'rule',
        subject_entity_id: context.ent1.id,
        predicate: 'guardrail',
        value_text: 'active',
      });
      insertClaimFixture(db, context, {
        claim_type: 'constraint',
        subject_entity_id: context.ent2.id,
        predicate: 'budget',
        value_text: 'active',
      });

      const snapshot = getRegistrySnapshot(db);

      assert.deepEqual(Object.keys(snapshot), ['tasks', 'decisions', 'goals', 'rules']);
      assert.equal(snapshot.tasks.length, 1);
      assert.equal(snapshot.decisions.length, 1);
      assert.equal(snapshot.goals.length, 1);
      assert.equal(snapshot.rules.length, 2);
    } finally {
      db.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it('createCachedRegistry returns cached results within TTL', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);

    try {
      insertClaimFixture(db, context, {
        claim_type: 'task',
        subject_entity_id: context.ent1.id,
        predicate: 'task',
        value_text: 'pending',
      });

      const cachedRegistry = createCachedRegistry(60_000);
      const first = cachedRegistry.getTaskRegistry(db);

      insertClaimFixture(db, context, {
        claim_type: 'task',
        subject_entity_id: context.ent2.id,
        predicate: 'task',
        value_text: 'active',
        created_at: '2000-01-02T00:00:00.000Z',
      });

      const second = cachedRegistry.getTaskRegistry(db);

      assert.strictEqual(second, first);
      assert.equal(second.length, 1);
      assert.equal(second[0].entityId, context.ent1.id);
    } finally {
      db.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it('createCachedRegistry invalidate() forces re-fetch', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);

    try {
      insertClaimFixture(db, context, {
        claim_type: 'task',
        subject_entity_id: context.ent1.id,
        predicate: 'task',
        value_text: 'pending',
      });

      const cachedRegistry = createCachedRegistry(60_000);
      const first = cachedRegistry.getTaskRegistry(db);

      insertClaimFixture(db, context, {
        claim_type: 'task',
        subject_entity_id: context.ent2.id,
        predicate: 'task',
        value_text: 'active',
        created_at: '2000-01-02T00:00:00.000Z',
      });
      cachedRegistry.invalidate();

      const second = cachedRegistry.getTaskRegistry(db);

      assert.notStrictEqual(second, first);
      assert.equal(second.length, 2);
      assert.ok(second.some((group) => group.entityId === context.ent2.id));
    } finally {
      db.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it('createCachedRegistry re-fetches after TTL expiry', async () => {
    const { db, root } = await makeTempDb();
    const context = seedData(db);
    const originalNow = Date.now;
    let now = 1000;

    try {
      Date.now = () => now;
      insertClaimFixture(db, context, {
        claim_type: 'decision',
        subject_entity_id: context.ent1.id,
        predicate: 'architecture',
        value_text: 'accepted',
      });

      const cachedRegistry = createCachedRegistry(5_000);
      const first = cachedRegistry.getDecisionRegistry(db);

      insertClaimFixture(db, context, {
        claim_type: 'decision',
        subject_entity_id: context.ent2.id,
        predicate: 'database',
        value_text: 'deferred',
        created_at: '2000-01-02T00:00:00.000Z',
      });
      now += 5_001;

      const second = cachedRegistry.getDecisionRegistry(db);

      assert.notStrictEqual(second, first);
      assert.equal(second.length, 2);
      assert.ok(second.some((group) => group.entityId === context.ent2.id));
    } finally {
      Date.now = originalNow;
      db.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});
