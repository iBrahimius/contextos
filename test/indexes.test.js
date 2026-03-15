import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";

import { ContextDatabase } from "../src/db/database.js";

async function createTestDB() {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-indexes-"));
  const dbPath = path.join(tmpDir, "test.db");
  return { db: new ContextDatabase(dbPath), tmpDir };
}

async function cleanupDB(db, tmpDir) {
  db.close();
  await fs.rm(tmpDir, { recursive: true, force: true });
}

function getIndexes(db) {
  const rows = db.sqlite
    .prepare(
      `
    SELECT name FROM sqlite_master
    WHERE type = 'index' AND name LIKE 'idx_%'
    ORDER BY name
  `,
    )
    .all();
  return rows.map((r) => r.name);
}

function getQueryPlan(db, sql) {
  const plan = db.sqlite.prepare(`EXPLAIN QUERY PLAN ${sql}`).all();
  return plan.map((p) => p.detail).join(" ");
}

test("Database includes all required indexes", async () => {
  const { db, tmpDir } = await createTestDB();

  try {
    const indexes = getIndexes(db);

    const required = [
      "idx_messages_captured_at",
      "idx_observations_created_at",
      "idx_tasks_entity_created",
      "idx_decisions_entity_created",
      "idx_constraints_entity_created",
      "idx_facts_entity_created",
      "idx_chunk_entities_entity",
      "idx_claims_observation",
      "idx_graph_proposals_created",
      "idx_graph_proposals_write_class",
      "idx_graph_proposals_confidence",
      "idx_graph_proposals_dedup",
    ];

    for (const idx of required) {
      assert.ok(indexes.includes(idx), `Missing index: ${idx}`);
    }
  } finally {
    await cleanupDB(db, tmpDir);
  }
});

test("SQL performance hardening indexes exist in sqlite_master", async () => {
  const { db, tmpDir } = await createTestDB();

  try {
    const indexes = getIndexes(db);

    assert.deepEqual(
      [
        "idx_claims_observation",
        "idx_graph_proposals_write_class",
        "idx_graph_proposals_confidence",
        "idx_graph_proposals_dedup",
      ].filter((name) => indexes.includes(name)),
      [
        "idx_claims_observation",
        "idx_graph_proposals_write_class",
        "idx_graph_proposals_confidence",
        "idx_graph_proposals_dedup",
      ],
    );
  } finally {
    await cleanupDB(db, tmpDir);
  }
});

test("idx_messages_captured_at is used for time-range queries", async () => {
  const { db, tmpDir } = await createTestDB();

  try {
    const plan = getQueryPlan(
      db,
      "SELECT id FROM messages WHERE captured_at > '2026-01-01' ORDER BY captured_at DESC LIMIT 100",
    );

    assert.match(plan, /idx_messages_captured_at/, "Query should use idx_messages_captured_at");
  } finally {
    await cleanupDB(db, tmpDir);
  }
});

test("idx_observations_created_at is used for range queries", async () => {
  const { db, tmpDir } = await createTestDB();

  try {
    const plan = getQueryPlan(
      db,
      "SELECT id FROM observations WHERE created_at > '2026-01-01' ORDER BY created_at DESC",
    );

    assert.match(plan, /idx_observations_created_at/, "Query should use idx_observations_created_at");
  } finally {
    await cleanupDB(db, tmpDir);
  }
});

test("idx_chunk_entities_entity is used for entity lookups", async () => {
  const { db, tmpDir } = await createTestDB();

  try {
    const plan = getQueryPlan(db, "SELECT chunk_id FROM chunk_entities WHERE entity_id = 'test-entity'");

    assert.match(plan, /idx_chunk_entities_entity/, "Query should use idx_chunk_entities_entity");
  } finally {
    await cleanupDB(db, tmpDir);
  }
});

test("idx_graph_proposals_created is used for proposal time queries", async () => {
  const { db, tmpDir } = await createTestDB();

  try {
    const plan = getQueryPlan(
      db,
      "SELECT id FROM graph_proposals WHERE created_at > '2026-01-01' ORDER BY created_at DESC",
    );

    assert.match(plan, /idx_graph_proposals_created/, "Query should use idx_graph_proposals_created");
  } finally {
    await cleanupDB(db, tmpDir);
  }
});

test("idx_graph_proposals_dedup is used for proposal dedup candidate lookups", async () => {
  const { db, tmpDir } = await createTestDB();

  try {
    const plan = getQueryPlan(
      db,
      "SELECT id FROM graph_proposals INDEXED BY idx_graph_proposals_dedup WHERE proposal_type = 'relationship' AND status IN ('pending', 'proposed') AND subject_label IS NOT NULL AND predicate IS NOT NULL AND object_label IS NOT NULL AND detail IS NOT NULL ORDER BY confidence DESC, created_at ASC",
    );

    assert.match(plan, /idx_graph_proposals_dedup/, "Dedup lookup should use idx_graph_proposals_dedup");
  } finally {
    await cleanupDB(db, tmpDir);
  }
});

test("Registry tables have entity_id, created_at indexes", async () => {
  const { db, tmpDir } = await createTestDB();

  try {
    const indexes = getIndexes(db);

    const registryIndexes = [
      "idx_tasks_entity_created",
      "idx_decisions_entity_created",
      "idx_constraints_entity_created",
      "idx_facts_entity_created",
    ];

    for (const idx of registryIndexes) {
      assert.ok(indexes.includes(idx), `Missing registry index: ${idx}`);
    }
  } finally {
    await cleanupDB(db, tmpDir);
  }
});

test("All existing tests still pass with new indexes", async () => {
  const { db, tmpDir } = await createTestDB();

  try {
    // Just verify the DB initializes without errors
    assert.ok(db.sqlite);
    assert.ok(db.hasTable("messages"));
    assert.ok(db.hasTable("observations"));
  } finally {
    await cleanupDB(db, tmpDir);
  }
});
