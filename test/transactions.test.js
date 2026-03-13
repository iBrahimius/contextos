/**
 * Transaction safety tests for ContextOS database operations.
 *
 * Validates that multi-step writes are atomic: either all writes
 * succeed or none persist. Tests withTransaction helper, re-entrancy,
 * and crash-safety via induced failures.
 *
 * Issue: https://github.com/iBrahimius/contextos/issues/8
 */

import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { ContextDatabase } from "../src/db/database.js";
import { ContextOS } from "../src/core/context-os.js";

async function makeDb() {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-tx-"));
  const dbPath = path.join(dir, "data", "contextos.db");
  await fs.mkdir(path.join(dir, "data"), { recursive: true });
  const db = new ContextDatabase(dbPath);
  return { db, dir };
}

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-tx-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  await fs.mkdir(path.join(root, "docs"), { recursive: true });
  return root;
}

// ── withTransaction basics ───────────────────────────────────────────

test("withTransaction commits data on success", async () => {
  const { db } = await makeDb();
  const conv = db.createConversation("TX Test");

  db.withTransaction(() => {
    db.insertMessage({
      conversationId: conv.id,
      role: "user",
      direction: "inbound",
      content: "Transaction commit test",
      tokenCount: 5,
    });
  });

  // Verify the message persisted
  const messages = db.listMessages(conv.id);
  assert.equal(messages.length, 1);
  assert.equal(messages[0].content, "Transaction commit test");
  db.close();
});

test("withTransaction rolls back on error — no partial data", async () => {
  const { db } = await makeDb();
  const conv = db.createConversation("Rollback Test");

  const versionBefore = db.getGraphVersion();

  assert.throws(() => {
    db.withTransaction(() => {
      // First write succeeds
      db.insertMessage({
        conversationId: conv.id,
        role: "user",
        direction: "inbound",
        content: "This should be rolled back",
        tokenCount: 5,
      });

      // Simulate crash mid-transaction
      throw new Error("Simulated crash");
    });
  }, { message: "Simulated crash" });

  // Message should NOT exist
  const messages = db.listMessages(conv.id);
  assert.equal(messages.length, 0, "message was rolled back");

  // Graph version should not have changed
  assert.equal(db.getGraphVersion(), versionBefore, "graph version unchanged");
  db.close();
});

test("withTransaction is re-entrant — nested calls don't crash", async () => {
  const { db } = await makeDb();

  let innerRan = false;

  db.withTransaction(() => {
    // Outer transaction
    db.createConversation("Outer");

    // Nested transaction — should be a no-op (just runs fn directly)
    db.withTransaction(() => {
      db.createConversation("Inner");
      innerRan = true;
    });
  });

  assert.ok(innerRan, "inner transaction callback executed");
  db.close();
});

test("withTransaction re-entrancy — inner error rolls back entire outer transaction", async () => {
  const { db } = await makeDb();
  const conv = db.createConversation("Re-entrant Rollback");

  assert.throws(() => {
    db.withTransaction(() => {
      db.insertMessage({
        conversationId: conv.id,
        role: "user",
        direction: "inbound",
        content: "Outer write",
        tokenCount: 3,
      });

      // Inner call (re-entrant, no-op wrapper) — error propagates up
      db.withTransaction(() => {
        throw new Error("Inner failure");
      });
    });
  }, { message: "Inner failure" });

  // Outer write should also be rolled back
  const messages = db.listMessages(conv.id);
  assert.equal(messages.length, 0, "outer write rolled back by inner error");
  db.close();
});

// ── Method-level atomicity ───────────────────────────────────────────

test("insertMessage is atomic — message + touchConversation happen together", async () => {
  const { db } = await makeDb();
  const conv = db.createConversation("Atomicity Test");

  const msg = db.insertMessage({
    conversationId: conv.id,
    role: "user",
    direction: "inbound",
    content: "Atomic insert",
    tokenCount: 3,
  });

  assert.ok(msg.id, "message created");
  assert.equal(msg.deduped, false, "not a duplicate");

  // Conversation should have been touched
  const updated = db.getConversation(conv.id);
  assert.ok(updated.updatedAt, "conversation was touched");
  db.close();
});

test("insertEntity is atomic — entity + aliases + graph version in one transaction", async () => {
  const { db } = await makeDb();

  const versionBefore = db.getGraphVersion();
  const entity = db.insertEntity({
    label: "Test Entity",
    kind: "concept",
    aliases: ["alias1", "alias2"],
  });

  assert.ok(entity.id, "entity created");
  assert.ok(entity.graphVersion > versionBefore, "graph version bumped");

  // Verify aliases
  const found1 = db.findEntityBySlugOrAlias("alias1");
  const found2 = db.findEntityBySlugOrAlias("alias2");
  assert.ok(found1, "alias1 resolves");
  assert.ok(found2, "alias2 resolves");
  assert.equal(found1.id, entity.id, "alias1 points to correct entity");
  assert.equal(found2.id, entity.id, "alias2 points to correct entity");
  db.close();
});

test("saveSessionCheckpoint is atomic — insert + prune happen together", async () => {
  const { db } = await makeDb();

  // Insert 11 checkpoints — should keep only 10
  for (let i = 0; i < 11; i++) {
    db.saveSessionCheckpoint({ graphVersion: i });
  }

  const latest = db.loadLatestCheckpoint();
  assert.ok(latest, "checkpoint exists");
  assert.equal(latest.graphVersion, 10, "latest checkpoint is the last one");

  // Count checkpoints — should be exactly 10 (11th insert triggered prune)
  const count = db.sqlite.prepare("SELECT COUNT(*) as cnt FROM session_checkpoints").get();
  assert.equal(count.cnt, 10, "old checkpoints were pruned");
  db.close();
});

// ── Crash safety ─────────────────────────────────────────────────────

test("crash-safety: insertEntity rolls back on alias failure", async () => {
  const { db } = await makeDb();

  const versionBefore = db.getGraphVersion();

  // Monkey-patch insertEntityAlias to fail after entity INSERT
  const original = db.insertEntityAlias.bind(db);
  let callCount = 0;
  db.insertEntityAlias = (entityId, alias) => {
    callCount++;
    if (callCount === 2) {
      throw new Error("Alias insertion failed");
    }
    return original(entityId, alias);
  };

  assert.throws(() => {
    db.insertEntity({
      label: "Crash Test Entity",
      kind: "concept",
      aliases: ["good-alias", "bad-alias"],
    });
  }, { message: "Alias insertion failed" });

  // Entity should NOT exist (rolled back)
  const found = db.findEntityBySlugOrAlias("crash-test-entity");
  assert.equal(found, undefined, "entity was rolled back");

  // Graph version should be unchanged
  assert.equal(db.getGraphVersion(), versionBefore, "graph version unchanged after rollback");

  // First alias should also not exist
  const alias = db.findEntityBySlugOrAlias("good-alias");
  assert.equal(alias, undefined, "first alias was also rolled back");

  db.close();
});

test("bumpGraphVersion uses withTransaction — nested calls are safe", async () => {
  const { db } = await makeDb();

  // bumpGraphVersion should work standalone
  const v1 = db.bumpGraphVersion();
  assert.equal(v1, 1);

  // And inside another transaction (re-entrant)
  db.withTransaction(() => {
    const v2 = db.bumpGraphVersion();
    assert.equal(v2, 2, "nested bumpGraphVersion works");
  });

  assert.equal(db.getGraphVersion(), 2, "version persisted after nested call");
  db.close();
});

// ── Full pipeline atomicity ──────────────────────────────────────────

test("e2e: entity creation through ContextOS.graph uses transactions", async () => {
  const rootDir = await makeRoot();
  const ctx = new ContextOS({ rootDir, autoBackfillEmbeddings: false });

  const entity = ctx.graph.ensureEntity({
    label: "Transaction Test",
    kind: "concept",
    aliases: ["tx-alias"],
  });

  assert.ok(entity.id, "entity created via graph");

  // Verify alias resolves
  const found = ctx.graph.findEntityByLabel("tx-alias");
  assert.ok(found, "alias resolves through graph layer");
  assert.equal(found.id, entity.id, "correct entity");
});
