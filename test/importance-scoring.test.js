import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import {
  applyImportanceWeighting,
  messageImportanceProxy,
  recencyBoost,
  RetrievalEngine,
} from "../src/core/retrieval.js";
import { ContextDatabase } from "../src/db/database.js";

async function makeTempDb() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-importance-scoring-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  const dbPath = path.join(root, "data", "contextos.db");
  const db = new ContextDatabase(dbPath);
  return { db, root };
}

function isoDaysAgo(daysOld) {
  return new Date(Date.now() - daysOld * 24 * 60 * 60 * 1000).toISOString();
}

function seedContext(db) {
  const conversation = db.createConversation("Importance Scoring");
  const message = db.insertMessage({
    conversationId: conversation.id,
    role: "user",
    direction: "inbound",
    content: "importance scoring fixture",
    tokenCount: 4,
  });
  const entity = db.insertEntity({ label: "Importance Entity", kind: "concept" });

  return { conversation, message, entity };
}

function insertObservation(db, context, overrides = {}) {
  return db.insertObservation({
    conversationId: context.conversation.id,
    messageId: context.message.id,
    actorId: "user:test",
    category: overrides.category ?? "fact",
    predicate: overrides.predicate ?? "status",
    subjectEntityId: overrides.subjectEntityId ?? context.entity.id,
    objectEntityId: overrides.objectEntityId ?? null,
    detail: overrides.detail ?? "importance observation",
    confidence: overrides.confidence ?? 0.9,
    scopeKind: overrides.scopeKind ?? "private",
    scopeId: overrides.scopeId ?? null,
  });
}

function insertClaim(db, context, overrides = {}) {
  const observation = overrides.observation
    ?? insertObservation(db, context, {
      category: overrides.claim_type ?? "fact",
      predicate: overrides.predicate ?? "status",
      subjectEntityId: overrides.subject_entity_id ?? context.entity.id,
      objectEntityId: overrides.object_entity_id ?? null,
      detail: overrides.value_text ?? "importance claim",
    });

  return db.insertClaim({
    observation_id: observation.id,
    conversation_id: context.conversation.id,
    message_id: context.message.id,
    actor_id: "user:test",
    claim_type: overrides.claim_type ?? "fact",
    subject_entity_id: overrides.subject_entity_id ?? context.entity.id,
    predicate: overrides.predicate ?? "status",
    object_entity_id: overrides.object_entity_id ?? null,
    value_text: overrides.value_text ?? "importance claim",
    confidence: overrides.confidence ?? 0.9,
    source_type: "explicit",
    lifecycle_state: overrides.lifecycle_state ?? "active",
    resolution_key: overrides.resolution_key ?? "fact:importance-entity:status",
    facet_key: overrides.facet_key ?? "status",
    scope_kind: "private",
    importance_score: overrides.importance_score,
  });
}

test("messageImportanceProxy returns 1.0 for today's message", () => {
  const nowMs = Date.parse("2026-03-09T12:00:00.000Z");

  assert.equal(messageImportanceProxy("2026-03-09T12:00:00.000Z", nowMs), 1.0);
});

test("messageImportanceProxy returns 0.3 for a 60-day-old message", () => {
  const nowMs = Date.parse("2026-03-09T12:00:00.000Z");

  assert.equal(messageImportanceProxy("2026-01-08T12:00:00.000Z", nowMs), 0.3);
});

test("messageImportanceProxy returns about 0.5 for a 15-day-old message", () => {
  const nowMs = Date.parse("2026-03-09T12:00:00.000Z");
  const score = messageImportanceProxy("2026-02-22T12:00:00.000Z", nowMs);

  assert.ok(Math.abs(score - 0.5) < 0.001, `expected ~0.5, got ${score}`);
});

test("recencyBoost returns 1.15 for 0-day-old items", () => {
  assert.equal(recencyBoost(0), 1.15);
});

test("recencyBoost returns 1.0 for 10-day-old items", () => {
  assert.equal(recencyBoost(10), 1.0);
});

test("retrieval reranks results so a high-importance old result beats a low-importance new result", async () => {
  const entity = {
    id: "ent_importance",
    label: "importance retrieval",
    kind: "concept",
    summary: "importance retrieval",
    complexityScore: 1,
  };
  const importanceByObservationId = new Map([
    ["obs_fact_old", 1.0],
    ["obs_constraint_new", 0.3],
  ]);
  const graph = {
    matchQuery(queryText) {
      return queryText ? [{ entity, score: 1 }] : [];
    },
    getEntity(entityId) {
      return entityId === entity.id ? entity : null;
    },
    findEntityByLabel(label) {
      return String(label ?? "").toLowerCase() === entity.label ? entity : null;
    },
    neighbors() {
      return [];
    },
    findPath() {
      return null;
    },
    registerMiss() {
      return null;
    },
  };
  const database = {
    listObservationsForEntities() {
      return [];
    },
    listTasksForEntities() {
      return [];
    },
    listDecisionsForEntities() {
      return [];
    },
    listConstraintsForEntities() {
      return [{
        id: "constraint_new",
        observation_id: "obs_constraint_new",
        entity_id: entity.id,
        detail: "fresh low-importance constraint",
        created_at: isoDaysAgo(0.1),
        message_captured_at: isoDaysAgo(0.1),
        origin_kind: "user",
      }];
    },
    listFactsForEntities() {
      return [{
        id: "fact_old",
        observation_id: "obs_fact_old",
        entity_id: entity.id,
        detail: "old high-importance fact",
        created_at: isoDaysAgo(45),
        message_captured_at: isoDaysAgo(45),
        origin_kind: "user",
      }];
    },
    listClaimsForEntities() {
      return [];
    },
    listChunksForEntities() {
      return [];
    },
    searchChunks() {
      return [];
    },
    searchObservations() {
      return [];
    },
    prepare() {
      return { all: () => [] };
    },
    listEmbeddedMessages() {
      return [];
    },
    listEmbeddedObservations() {
      return [];
    },
    getObservationImportanceScore(observationId) {
      return importanceByObservationId.get(observationId) ?? 1.0;
    },
    getGraphVersion() {
      return 1;
    },
  };
  const telemetry = {
    listActiveRetrievalHints() {
      return [];
    },
    logRetrieval() {
      return "query_importance";
    },
  };
  const classifier = {
    classifyText() {
      return { entities: [] };
    },
  };
  const engine = new RetrievalEngine({ graph, database, telemetry, classifier });

  const result = await engine.retrieve({ queryText: "importance retrieval" });
  const factResult = result.items.find((item) => item.id === "fact_old");
  const constraintResult = result.items.find((item) => item.id === "constraint_new");

  assert.ok(factResult);
  assert.ok(constraintResult);
  assert.equal(factResult.importanceScore, 1.0);
  assert.equal(constraintResult.importanceScore, 0.3);
  assert.ok(constraintResult.recencyBoost > 1.0);
  assert.ok(factResult.score > constraintResult.score);
  assert.ok(
    result.items.findIndex((item) => item.id === "fact_old")
      < result.items.findIndex((item) => item.id === "constraint_new"),
  );
});

test("getObservationImportanceScore returns 1.0 when no claim exists", async () => {
  const { db, root } = await makeTempDb();

  try {
    const context = seedContext(db);
    const observation = insertObservation(db, context);

    assert.equal(db.getObservationImportanceScore(observation.id), 1.0);
  } finally {
    db.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("getObservationImportanceScore falls back to the highest related active claim importance", async () => {
  const { db, root } = await makeTempDb();

  try {
    const context = seedContext(db);
    const targetObservation = insertObservation(db, context, {
      detail: "observation without direct claim",
    });

    insertClaim(db, context, {
      claim_type: "fact",
      value_text: "active fallback claim",
      importance_score: 0.82,
    });
    insertClaim(db, context, {
      claim_type: "fact",
      value_text: "candidate claim should not win fallback",
      lifecycle_state: "candidate",
      importance_score: 0.97,
    });

    assert.equal(db.getObservationImportanceScore(targetObservation.id), 0.82);
  } finally {
    db.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("applyImportanceWeighting treats null importance scores as 1.0", () => {
  const nowMs = Date.parse("2026-03-09T12:00:00.000Z");
  const [result] = applyImportanceWeighting([{
    type: "fact",
    id: "fact_null_importance",
    score: 0.8,
    summary: "null importance should not penalize",
    payload: {
      observation_id: "obs_null_importance",
      created_at: "2026-02-27T12:00:00.000Z",
    },
    tokenCount: 5,
    hintIds: [],
  }], {
    nowMs,
    database: {
      getObservationImportanceScore() {
        return null;
      },
    },
  });

  assert.equal(result.importanceScore, 1.0);
  assert.equal(result.recencyBoost, 1.0);
  assert.equal(result.score, 0.8);
});
