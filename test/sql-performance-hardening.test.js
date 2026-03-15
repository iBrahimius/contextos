import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { ContextDatabase } from "../src/db/database.js";

async function createTestDB() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-sql-perf-"));
  const dbPath = path.join(root, "data", "contextos.db");
  await fs.mkdir(path.dirname(dbPath), { recursive: true });
  return { db: new ContextDatabase(dbPath), root };
}

async function cleanupTestDB(db, root) {
  db.close();
  await fs.rm(root, { recursive: true, force: true });
}

function seedContext(db) {
  const conversation = db.createConversation("SQL perf test");
  const message = db.insertMessage({
    conversationId: conversation.id,
    role: "user",
    direction: "inbound",
    content: "Testing SQL performance hardening.",
    tokenCount: 5,
    capturedAt: new Date().toISOString(),
  });
  const entity = db.insertEntity({ label: "ContextOS", kind: "system" });

  return { conversation, message, entity };
}

function insertObservation(db, context, overrides = {}) {
  return db.insertObservation({
    conversationId: context.conversation.id,
    messageId: context.message.id,
    actorId: overrides.actorId ?? "user",
    category: overrides.category ?? "fact",
    predicate: overrides.predicate ?? "describes",
    subjectEntityId: overrides.subjectEntityId ?? context.entity.id,
    objectEntityId: overrides.objectEntityId ?? null,
    detail: overrides.detail ?? "Observation detail",
    confidence: overrides.confidence ?? 0.9,
    scopeKind: overrides.scopeKind ?? "private",
    scopeId: overrides.scopeId ?? null,
  });
}

function insertClaim(db, context, overrides = {}) {
  const claimType = overrides.claimType ?? overrides.claim_type ?? "fact";
  const claimValueText = Object.hasOwn(overrides, "valueText")
    ? overrides.valueText
    : Object.hasOwn(overrides, "value_text")
      ? overrides.value_text
      : "Claim detail";
  const observation = overrides.observationId
    ? { id: overrides.observationId }
    : insertObservation(db, context, {
      category: claimType,
      detail: overrides.detail ?? (claimValueText ?? "Claim detail"),
      subjectEntityId: overrides.subjectEntityId ?? overrides.subject_entity_id ?? context.entity.id,
    });

  return db.insertClaim({
    observation_id: observation.id,
    conversation_id: context.conversation.id,
    message_id: context.message.id,
    actor_id: overrides.actorId ?? overrides.actor_id ?? "user",
    claim_type: claimType,
    subject_entity_id: overrides.subjectEntityId ?? overrides.subject_entity_id ?? context.entity.id,
    predicate: overrides.predicate ?? "describes",
    object_entity_id: overrides.objectEntityId ?? overrides.object_entity_id ?? null,
    value_text: claimValueText,
    confidence: overrides.confidence ?? 0.9,
    source_type: overrides.sourceType ?? overrides.source_type ?? "explicit",
    lifecycle_state: overrides.lifecycleState ?? overrides.lifecycle_state ?? "active",
    resolution_key: overrides.resolutionKey ?? overrides.resolution_key ?? `fact:${context.entity.id}:describes`,
    facet_key: overrides.facetKey ?? overrides.facet_key ?? "describes",
    scope_kind: overrides.scopeKind ?? overrides.scope_kind ?? "private",
    scope_id: overrides.scopeId ?? overrides.scope_id ?? null,
  });
}

function countRows(db, tableName) {
  return Number(db.sqlite.prepare(`SELECT COUNT(*) AS count FROM ${tableName}`).get()?.count ?? 0);
}

function insertProposal(db, overrides = {}) {
  const stored = db.insertGraphProposal({
    proposalType: overrides.proposalType ?? "fact",
    detail: overrides.detail ?? "Proposal detail",
    confidence: overrides.confidence ?? 0.5,
    status: overrides.status ?? "proposed",
    writeClass: overrides.writeClass ?? "ai_proposed",
    payload: overrides.payload ?? { type: overrides.proposalType ?? "fact", detail: overrides.detail ?? "Proposal detail" },
  });

  if (overrides.createdAt) {
    const createdAt = overrides.createdAt instanceof Date ? overrides.createdAt.toISOString() : String(overrides.createdAt);
    db.prepare(`
      UPDATE graph_proposals
      SET created_at = ?
      WHERE id = ?
    `).run(createdAt, stored.id);
  }

  return stored;
}

test("FTS backfill restores missing observation and claim entries from shadow-table lookups", async () => {
  const { db, root } = await createTestDB();
  const context = seedContext(db);

  try {
    const observation = insertObservation(db, context, {
      detail: "Observation without FTS row after manual deletion",
    });
    const claim = insertClaim(db, context, {
      claimType: "fact",
      valueText: "Claim without FTS row after manual deletion",
    });

    db.sqlite.prepare(`DELETE FROM observation_fts WHERE observation_id = ?`).run(observation.id);
    db.sqlite.prepare(`DELETE FROM claims_fts WHERE claim_id = ?`).run(claim.id);

    assert.equal(db.sqlite.prepare(`SELECT c0 FROM observation_fts_content WHERE c0 = ?`).get(observation.id), undefined);
    assert.equal(db.sqlite.prepare(`SELECT c0 FROM claims_fts_content WHERE c0 = ?`).get(claim.id), undefined);

    db.backfillObservationFts();
    db.backfillClaimsFts();

    const observationFtsRow = db.sqlite.prepare(`SELECT c0, c1, c2 FROM observation_fts_content WHERE c0 = ?`).get(observation.id);
    const claimFtsRow = db.sqlite.prepare(`SELECT c0, c1, c2 FROM claims_fts_content WHERE c0 = ?`).get(claim.id);

    assert.equal(observationFtsRow?.c0, observation.id);
    assert.equal(observationFtsRow?.c1, "fact");
    assert.equal(observationFtsRow?.c2, "Observation without FTS row after manual deletion");
    assert.equal(claimFtsRow?.c0, claim.id);
    assert.equal(claimFtsRow?.c1, "fact");
    assert.equal(claimFtsRow?.c2, "Claim without FTS row after manual deletion");
  } finally {
    await cleanupTestDB(db, root);
  }
});

test("FTS backfill is a no-op when all eligible rows are already present", async () => {
  const { db, root } = await createTestDB();
  const context = seedContext(db);

  try {
    insertObservation(db, context, { detail: "Observation already present in FTS" });
    insertClaim(db, context, {
      claimType: "fact",
      valueText: "Claim already present in FTS",
    });
    insertClaim(db, context, {
      claimType: "fact",
      valueText: null,
      detail: "Claim with null text should not require an FTS row",
      resolutionKey: `fact:${context.entity.id}:null-text`,
      facetKey: "null-text",
    });

    const observationCountBefore = countRows(db, "observation_fts_content");
    const claimCountBefore = countRows(db, "claims_fts_content");
    const originalPrepare = db.prepare.bind(db);
    let attemptedInsert = false;

    db.prepare = (sql) => {
      if (sql.includes("INSERT INTO observation_fts") || sql.includes("INSERT INTO claims_fts")) {
        attemptedInsert = true;
        throw new Error("backfill insert should be skipped when all eligible FTS rows already exist");
      }

      return originalPrepare(sql);
    };

    db.backfillObservationFts();
    db.backfillClaimsFts();

    assert.equal(attemptedInsert, false);
    assert.equal(countRows(db, "observation_fts_content"), observationCountBefore);
    assert.equal(countRows(db, "claims_fts_content"), claimCountBefore);
  } finally {
    await cleanupTestDB(db, root);
  }
});

test("listGraphProposals filters queue buckets at the SQL level", async () => {
  const { db, root } = await createTestDB();

  try {
    const parked = insertProposal(db, {
      proposalType: "fact",
      detail: "Parked AI proposal",
      confidence: 0.4,
      status: "proposed",
      writeClass: "ai_proposed",
    });
    const actionableAi = insertProposal(db, {
      proposalType: "fact",
      detail: "Actionable AI proposal",
      confidence: 0.92,
      status: "proposed",
      writeClass: "ai_proposed",
    });
    const actionableCanonical = insertProposal(db, {
      proposalType: "add_decision",
      detail: "Canonical proposal",
      confidence: 0.2,
      status: "pending",
      writeClass: "canonical",
    });
    insertProposal(db, {
      proposalType: "fact",
      detail: "Already reviewed proposal",
      confidence: 0.99,
      status: "accepted",
      writeClass: "ai_proposed",
    });

    assert.deepEqual(
      db.listGraphProposals({ queueBucket: "parked", limit: null }).map((row) => row.id),
      [parked.id],
    );
    assert.deepEqual(
      new Set(db.listGraphProposals({ queueBucket: "actionable", sort: "oldest", limit: null }).map((row) => row.id)),
      new Set([actionableAi.id, actionableCanonical.id]),
    );
  } finally {
    await cleanupTestDB(db, root);
  }
});

test("listGraphProposals filters by createdBefore", async () => {
  const { db, root } = await createTestDB();

  try {
    const oldProposal = insertProposal(db, {
      proposalType: "fact",
      detail: "Old proposal",
      createdAt: "2026-01-01T00:00:00.000Z",
    });
    insertProposal(db, {
      proposalType: "fact",
      detail: "New proposal",
      createdAt: "2026-02-01T00:00:00.000Z",
    });

    const rows = db.listGraphProposals({
      createdBefore: "2026-01-15T00:00:00.000Z",
      sort: "oldest",
      limit: null,
    });

    assert.deepEqual(rows.map((row) => row.id), [oldProposal.id]);
  } finally {
    await cleanupTestDB(db, root);
  }
});

test("countGraphProposals returns count without loading rows", async () => {
  const { db, root } = await createTestDB();

  try {
    insertProposal(db, { confidence: 0.4, writeClass: "ai_proposed", status: "proposed" });
    insertProposal(db, { confidence: 0.3, writeClass: "ai_proposed", status: "proposed" });
    insertProposal(db, { confidence: 0.9, writeClass: "ai_proposed", status: "proposed" });
    insertProposal(db, { confidence: 0.2, writeClass: "canonical", status: "pending" });

    const parkedCount = db.countGraphProposals({ statuses: ["pending", "proposed"], queueBucket: "parked" });
    const actionableCount = db.countGraphProposals({ statuses: ["pending", "proposed"], queueBucket: "actionable" });
    const totalCount = db.countGraphProposals({});

    assert.equal(parkedCount, 2);
    assert.equal(actionableCount, 2);
    assert.equal(totalCount, 4);
    assert.equal(typeof parkedCount, "number");
  } finally {
    await cleanupTestDB(db, root);
  }
});
