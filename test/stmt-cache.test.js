import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";

async function createTestDB() {
  const rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-stmt-"));
  await fs.mkdir(path.join(rootDir, "data"), { recursive: true });
  const contextOS = new ContextOS({ rootDir });
  return {
    db: contextOS.database,
    async close() {
      contextOS.database.close();
      contextOS.telemetry.close();
      await fs.rm(rootDir, { recursive: true, force: true });
    },
  };
}

test("statement cache has a max size property", async () => {
  const { db, close } = await createTestDB();
  try {
    assert.equal(db._stmtMaxSize, 500);
  } finally {
    await close();
  }
});

test("prepare returns the same statement for the same SQL", async () => {
  const { db, close } = await createTestDB();
  try {
    const stmt1 = db.prepare("SELECT 1");
    const stmt2 = db.prepare("SELECT 1");
    assert.strictEqual(stmt1, stmt2);
  } finally {
    await close();
  }
});

test("cache evicts oldest entry when exceeding max size", async () => {
  const { db, close } = await createTestDB();
  try {
    // Clear existing cache from schema setup to isolate eviction behavior
    db.statements.clear();
    db._stmtMaxSize = 5;

    // Fill cache with 5 statements
    for (let i = 0; i < 5; i++) {
      db.prepare(`SELECT ${i + 1}`);
    }
    assert.equal(db.statements.size, 5);

    // Add a 6th — should evict the oldest (SELECT 1)
    db.prepare("SELECT 6");
    assert.equal(db.statements.size, 5, "Cache should not exceed max size");
    assert.ok(!db.statements.has("SELECT 1"), "Oldest entry should be evicted");
    assert.ok(db.statements.has("SELECT 6"), "Newest entry should exist");
  } finally {
    await close();
  }
});

test("accessing a cached statement refreshes its LRU position", async () => {
  const { db, close } = await createTestDB();
  try {
    // Clear existing cache from schema setup to isolate LRU behavior
    db.statements.clear();
    db._stmtMaxSize = 3;

    db.prepare("SELECT 1");
    db.prepare("SELECT 2");
    db.prepare("SELECT 3");

    // Access SELECT 1 to refresh it (move to end)
    db.prepare("SELECT 1");

    // Add a new one — should evict SELECT 2 (now oldest), not SELECT 1
    db.prepare("SELECT 4");
    assert.equal(db.statements.size, 3);
    assert.ok(!db.statements.has("SELECT 2"), "SELECT 2 should be evicted (oldest)");
    assert.ok(db.statements.has("SELECT 1"), "SELECT 1 should survive (recently accessed)");
    assert.ok(db.statements.has("SELECT 4"), "SELECT 4 should exist (newest)");
  } finally {
    await close();
  }
});

test("cache works correctly under normal operation", async () => {
  const { db, close } = await createTestDB();
  try {
    // Use the real max size (500) — normal operation shouldn't hit the limit
    const initialSize = db.statements.size;

    // Run some typical queries
    db.prepare("SELECT COUNT(*) FROM messages");
    db.prepare("SELECT * FROM messages WHERE id = ?");
    db.prepare("SELECT * FROM conversations LIMIT ?");

    assert.ok(db.statements.size >= initialSize + 3,
      "Cache should grow with new statements");
    assert.ok(db.statements.size <= 500,
      "Cache should not exceed max size");
  } finally {
    await close();
  }
});

test("evicted statements do not break re-preparation", async () => {
  const { db, close } = await createTestDB();
  try {
    db.statements.clear();
    db._stmtMaxSize = 2;

    const stmt1 = db.prepare("SELECT 1");
    db.prepare("SELECT 2");
    db.prepare("SELECT 3"); // evicts SELECT 1

    // Re-prepare SELECT 1 — should work without crashing
    const stmt1b = db.prepare("SELECT 1");
    assert.ok(stmt1b, "Re-prepared statement should exist");

    // It should still work
    const result = stmt1b.get();
    assert.equal(result["1"], 1);
  } finally {
    await close();
  }
});
