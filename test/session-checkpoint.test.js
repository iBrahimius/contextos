import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { ContextDatabase } from "../src/db/database.js";

async function makeDb() {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-checkpoint-"));
  const dbPath = path.join(dir, "test.db");
  const db = new ContextDatabase(dbPath);
  return { db, dir };
}

async function cleanup({ db, dir }) {
  db.close();
  await fs.rm(dir, { recursive: true, force: true });
}

// ── saveSessionCheckpoint ────────────────────────────────────────────

test("saveSessionCheckpoint — stores and returns checkpoint data", async () => {
  const fixture = await makeDb();
  try {
    const checkpoint = fixture.db.saveSessionCheckpoint({
      graphVersion: 42,
      activeTaskIds: ["claim-1", "claim-2"],
      activeDecisionIds: ["claim-3"],
      activeGoalIds: [],
    });

    assert.ok(checkpoint, "should return a checkpoint object");
    assert.equal(checkpoint.graphVersion, 42);
    assert.deepEqual(checkpoint.activeTaskIds, ["claim-1", "claim-2"]);
    assert.deepEqual(checkpoint.activeDecisionIds, ["claim-3"]);
    assert.deepEqual(checkpoint.activeGoalIds, []);
    assert.ok(typeof checkpoint.savedAt === "string");
    assert.ok(checkpoint.id > 0);
  } finally {
    await cleanup(fixture);
  }
});

test("saveSessionCheckpoint — defaults activeIds to empty arrays", async () => {
  const fixture = await makeDb();
  try {
    const checkpoint = fixture.db.saveSessionCheckpoint({ graphVersion: 1 });

    assert.deepEqual(checkpoint.activeTaskIds, []);
    assert.deepEqual(checkpoint.activeDecisionIds, []);
    assert.deepEqual(checkpoint.activeGoalIds, []);
  } finally {
    await cleanup(fixture);
  }
});

// ── loadLatestCheckpoint ─────────────────────────────────────────────

test("loadLatestCheckpoint — returns null when no checkpoints exist", async () => {
  const fixture = await makeDb();
  try {
    const result = fixture.db.loadLatestCheckpoint();
    assert.equal(result, null);
  } finally {
    await cleanup(fixture);
  }
});

test("loadLatestCheckpoint — returns the most recently saved checkpoint", async () => {
  const fixture = await makeDb();
  try {
    fixture.db.saveSessionCheckpoint({ graphVersion: 1 });
    fixture.db.saveSessionCheckpoint({ graphVersion: 2 });
    fixture.db.saveSessionCheckpoint({ graphVersion: 3, activeTaskIds: ["t-1"] });

    const latest = fixture.db.loadLatestCheckpoint();
    assert.equal(latest.graphVersion, 3);
    assert.deepEqual(latest.activeTaskIds, ["t-1"]);
  } finally {
    await cleanup(fixture);
  }
});

// ── Rolling window: max 10 checkpoints ───────────────────────────────

test("saveSessionCheckpoint — keeps at most 10 checkpoints", async () => {
  const fixture = await makeDb();
  try {
    // Insert 12 checkpoints
    for (let i = 1; i <= 12; i++) {
      fixture.db.saveSessionCheckpoint({ graphVersion: i });
    }

    // Count rows directly
    const count = fixture.db.sqlite.prepare(
      `SELECT COUNT(*) AS count FROM session_checkpoints`,
    ).get().count;

    assert.equal(count, 10);
  } finally {
    await cleanup(fixture);
  }
});

test("saveSessionCheckpoint — rolling window keeps the most recent 10", async () => {
  const fixture = await makeDb();
  try {
    for (let i = 1; i <= 15; i++) {
      fixture.db.saveSessionCheckpoint({ graphVersion: i });
    }

    // Oldest surviving should have graphVersion = 6 (15 - 10 + 1)
    const rows = fixture.db.sqlite.prepare(
      `SELECT graph_version FROM session_checkpoints ORDER BY id ASC`,
    ).all();

    assert.equal(rows.length, 10);
    assert.equal(rows[0].graph_version, 6);
    assert.equal(rows[9].graph_version, 15);
  } finally {
    await cleanup(fixture);
  }
});

test("saveSessionCheckpoint — exactly 10 checkpoints are all kept", async () => {
  const fixture = await makeDb();
  try {
    for (let i = 1; i <= 10; i++) {
      fixture.db.saveSessionCheckpoint({ graphVersion: i });
    }

    const count = fixture.db.sqlite.prepare(
      `SELECT COUNT(*) AS count FROM session_checkpoints`,
    ).get().count;

    assert.equal(count, 10);
    const latest = fixture.db.loadLatestCheckpoint();
    assert.equal(latest.graphVersion, 10);
  } finally {
    await cleanup(fixture);
  }
});
