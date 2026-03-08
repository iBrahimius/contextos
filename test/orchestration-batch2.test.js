import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-orchestration-batch2-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  return root;
}

async function createHarness() {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({
    rootDir,
    autoBackfillEmbeddings: false,
  });

  return {
    rootDir,
    contextOS,
    async close() {
      await contextOS.close();
      await fs.rm(rootDir, { recursive: true, force: true });
    },
  };
}

async function createMessage(harness, overrides = {}) {
  return harness.contextOS.ingestMessage({
    conversationId: overrides.conversationId ?? null,
    conversationTitle: overrides.conversationTitle ?? "Batch 2 Orchestration",
    role: overrides.role ?? "user",
    direction: overrides.direction ?? "inbound",
    actorId: overrides.actorId ?? "user:test",
    originKind: overrides.originKind ?? null,
    ingestId: overrides.ingestId ?? null,
    content: overrides.content ?? "batch 2 orchestration test message",
    scopeKind: overrides.scopeKind ?? "project",
    scopeId: overrides.scopeId ?? "proj-batch2",
  });
}

function buildMessageContext(capture) {
  return {
    conversationId: capture.conversationId,
    messageId: capture.message.id,
    actorId: capture.message.actorId,
    scopeKind: capture.message.scopeKind,
    scopeId: capture.message.scopeId,
  };
}

function insertClaimFixture(harness, context, overrides = {}) {
  const claimType = overrides.claim_type ?? "fact";
  const predicate = overrides.predicate ?? "status";
  const timestamp = overrides.created_at ?? "2026-03-07T10:00:00.000Z";

  const observation = harness.contextOS.database.insertObservation({
    conversationId: context.conversationId,
    messageId: context.messageId,
    actorId: overrides.actor_id ?? context.actorId ?? "user:test",
    category: claimType,
    predicate,
    subjectEntityId: overrides.subject_entity_id ?? null,
    objectEntityId: overrides.object_entity_id ?? null,
    detail: overrides.detail ?? overrides.value_text ?? `${claimType} claim`,
    confidence: overrides.confidence ?? 0.9,
    sourceSpan: overrides.detail ?? `${claimType} claim`,
    scopeKind: overrides.scope_kind ?? context.scopeKind ?? "private",
    scopeId: overrides.scope_id ?? context.scopeId ?? null,
  });

  harness.contextOS.database.prepare(`
    UPDATE observations
    SET created_at = ?
    WHERE id = ?
  `).run(timestamp, observation.id);

  return harness.contextOS.database.insertClaim({
    observation_id: observation.id,
    conversation_id: context.conversationId,
    message_id: context.messageId,
    actor_id: overrides.actor_id ?? context.actorId ?? "user:test",
    claim_type: claimType,
    subject_entity_id: overrides.subject_entity_id ?? null,
    predicate,
    object_entity_id: overrides.object_entity_id ?? null,
    value_text: overrides.value_text ?? `${claimType} claim`,
    confidence: overrides.confidence ?? 0.9,
    source_type: overrides.source_type ?? "explicit",
    lifecycle_state: overrides.lifecycle_state ?? "active",
    valid_from: overrides.valid_from ?? timestamp,
    valid_to: overrides.valid_to ?? null,
    resolution_key: overrides.resolution_key ?? `${claimType}:${overrides.subject_entity_id ?? "missing"}:${predicate}`,
    facet_key: overrides.facet_key ?? predicate,
    supersedes_claim_id: overrides.supersedes_claim_id ?? null,
    superseded_by_claim_id: overrides.superseded_by_claim_id ?? null,
    scope_kind: overrides.scope_kind ?? context.scopeKind ?? "private",
    scope_id: overrides.scope_id ?? context.scopeId ?? null,
    created_at: timestamp,
    updated_at: overrides.updated_at ?? timestamp,
  });
}

test("dreamCycle returns Batch 2 metrics and episode summaries", async () => {
  const harness = await createHarness();

  try {
    const capture = await createMessage(harness);
    const context = buildMessageContext(capture);
    const entity = harness.contextOS.database.insertEntity({ label: "DreamCluster", kind: "concept" });
    const baseTime = Date.now() - 5 * 60 * 1000;

    for (let index = 0; index < 3; index += 1) {
      const observation = harness.contextOS.database.insertObservation({
        conversationId: context.conversationId,
        messageId: context.messageId,
        actorId: context.actorId,
        category: "fact",
        predicate: "status",
        subjectEntityId: entity.id,
        detail: `Dream cluster observation ${index}`,
        confidence: 0.9,
        sourceSpan: `Dream cluster observation ${index}`,
        scopeKind: context.scopeKind,
        scopeId: context.scopeId,
      });

      harness.contextOS.database.prepare(`
        UPDATE observations
        SET created_at = ?
        WHERE id = ?
      `).run(new Date(baseTime + index * 60 * 1000).toISOString(), observation.id);
    }

    const report = await harness.contextOS.dreamCycle({
      dry_run: true,
      detect_patterns: false,
    });

    assert.ok(report.metrics);
    assert.equal(typeof report.metrics.claims_archived, "number");
    assert.equal(typeof report.metrics.episodes_detected, "number");
    assert.equal(typeof report.metrics.atoms_extracted, "number");
    assert.equal(typeof report.metrics.levels_generated, "number");
    assert.ok(Array.isArray(report.episode_summaries));
    assert.equal(Array.isArray(report.new_patterns), true);
    assert.equal(Array.isArray(report.errors), true);
    assert.ok(report.metrics.episodes_detected >= 1);
    assert.ok(report.metrics.clusters_created >= 1);
    assert.ok(report.metrics.atoms_extracted >= 3);
    assert.ok(report.metrics.levels_generated >= 3);
  } finally {
    await harness.close();
  }
});

test("annotateResultsWithClaims rescales retrieval scores by claim importance", async () => {
  const harness = await createHarness();

  try {
    const capture = await createMessage(harness);
    const context = buildMessageContext(capture);
    const entity = harness.contextOS.database.insertEntity({ label: "ImportanceEntity", kind: "concept" });
    const claim = insertClaimFixture(harness, context, {
      claim_type: "fact",
      subject_entity_id: entity.id,
      predicate: "priority",
      value_text: "importance weighted",
      resolution_key: `fact:${entity.id}:priority`,
      facet_key: "priority",
    });

    harness.contextOS.database.prepare(`
      UPDATE claims
      SET importance_score = ?, updated_at = ?
      WHERE id = ?
    `).run(0.8, new Date().toISOString(), claim.id);

    const annotated = harness.contextOS.annotateResultsWithClaims([{
      type: "fact",
      id: claim.observation_id,
      summary: "importance test",
      score: 1,
      payload: { observation_id: claim.observation_id },
    }]);

    assert.equal(annotated.length, 1);
    assert.equal(annotated[0].claim?.id, claim.id);
    assert.equal(annotated[0].score, 0.8);
  } finally {
    await harness.close();
  }
});

test("sessionRecovery returns fallback packet within token budget", async () => {
  const harness = await createHarness();

  try {
    const capture = await createMessage(harness);
    const context = buildMessageContext(capture);
    const entity = harness.contextOS.database.insertEntity({ label: "RecoveryFallback", kind: "concept" });

    insertClaimFixture(harness, context, {
      claim_type: "task",
      subject_entity_id: entity.id,
      predicate: "workstream",
      value_text: "active",
    });

    const packet = await harness.contextOS.sessionRecovery({ tokenBudget: 400 });

    assert.ok(packet.timestamp);
    assert.ok(packet.since_datetime);
    assert.ok(Array.isArray(packet.new_claims));
    assert.ok(Array.isArray(packet.updated_claims));
    assert.ok(Array.isArray(packet.new_conflicts));
    assert.ok(Array.isArray(packet.active_entities));
    assert.ok(packet.summary);
    assert.ok(packet.token_count <= 400, `expected token_count <= 400, got ${packet.token_count}`);
    assert.ok(packet.changes_summary.some((line) => /No checkpoint found/.test(line)));
  } finally {
    await harness.close();
  }
});

test("sessionRecovery detects active-state changes from the last checkpoint", async () => {
  const harness = await createHarness();

  try {
    const capture = await createMessage(harness);
    const context = buildMessageContext(capture);
    const entity = harness.contextOS.database.insertEntity({ label: "RecoveryDiff", kind: "concept" });
    const taskClaim = insertClaimFixture(harness, context, {
      claim_type: "task",
      subject_entity_id: entity.id,
      predicate: "status",
      value_text: "active",
      metadata_json: { status: "active" },
    });

    harness.contextOS.database.saveSessionCheckpoint({
      graphVersion: harness.contextOS.graph.getGraphVersion(),
      activeTaskIds: [taskClaim.id],
      activeDecisionIds: [],
      activeGoalIds: [],
    });
    harness.contextOS.database.updateClaim(taskClaim.id, {
      lifecycle_state: "superseded",
    });

    const packet = await harness.contextOS.sessionRecovery({});

    assert.ok(packet.updated_claims.some((claim) => (
      claim.id === taskClaim.id
      && claim.previous_state === "active"
      && claim.current_state !== "active"
    )));
  } finally {
    await harness.close();
  }
});
