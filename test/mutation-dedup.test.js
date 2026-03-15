import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { ContextDatabase } from "../src/db/database.js";

async function createTestDB() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-mutation-dedup-"));
  const dbPath = path.join(root, "data", "contextos.db");
  await fs.mkdir(path.dirname(dbPath), { recursive: true });
  return { db: new ContextDatabase(dbPath), root };
}

async function cleanupTestDB(db, root) {
  db.close();
  await fs.rm(root, { recursive: true, force: true });
}

function countGraphProposals(db) {
  return Number(db.sqlite.prepare(`SELECT COUNT(*) AS count FROM graph_proposals`).get()?.count ?? 0);
}

function getGraphProposalRow(db, id) {
  return db.sqlite.prepare(`SELECT * FROM graph_proposals WHERE id = ? LIMIT 1`).get(id);
}

function insertProposal(db, overrides = {}) {
  const proposalType = overrides.proposalType ?? "relationship";
  const detail = Object.hasOwn(overrides, "detail") ? overrides.detail : "ContextOS depends on SQLite";

  return db.insertGraphProposal({
    conversationId: overrides.conversationId ?? null,
    messageId: overrides.messageId ?? null,
    actorId: overrides.actorId ?? "test",
    scopeKind: overrides.scopeKind ?? "private",
    scopeId: overrides.scopeId ?? null,
    proposalType,
    subjectLabel: Object.hasOwn(overrides, "subjectLabel") ? overrides.subjectLabel : "ContextOS",
    predicate: Object.hasOwn(overrides, "predicate") ? overrides.predicate : "depends_on",
    objectLabel: Object.hasOwn(overrides, "objectLabel") ? overrides.objectLabel : "SQLite",
    detail,
    confidence: overrides.confidence ?? 0.5,
    status: overrides.status ?? "proposed",
    reason: overrides.reason ?? null,
    payload: overrides.payload ?? { type: proposalType, detail },
    writeClass: overrides.writeClass ?? "ai_proposed",
  });
}

test("exact duplicate proposals are skipped with normalized matching", async () => {
  const { db, root } = await createTestDB();

  try {
    const first = insertProposal(db, {
      subjectLabel: "  ContextOS  ",
      predicate: "DEPENDS_ON",
      objectLabel: "  SQLite  ",
      detail: "  Uses SQLite for persistence  ",
      confidence: 0.65,
    });
    const versionAfterFirst = db.getGraphVersion();

    const duplicate = insertProposal(db, {
      subjectLabel: "contextos",
      predicate: "depends_on",
      objectLabel: "sqlite",
      detail: "uses sqlite for persistence",
      confidence: 0.65,
    });

    assert.equal(first.deduplicated, false);
    assert.equal(duplicate.id, first.id);
    assert.equal(duplicate.deduplicated, true);
    assert.equal(duplicate.skipped, true);
    assert.equal(countGraphProposals(db), 1);
    assert.equal(db.getGraphVersion(), versionAfterFirst);
  } finally {
    await cleanupTestDB(db, root);
  }
});

test("higher-confidence duplicate updates the existing proposal confidence", async () => {
  const { db, root } = await createTestDB();

  try {
    const first = insertProposal(db, {
      detail: "Ship mutation deduplication",
      confidence: 0.4,
    });
    const versionAfterFirst = db.getGraphVersion();

    const duplicate = insertProposal(db, {
      subjectLabel: "contextos",
      predicate: "depends_on",
      objectLabel: "sqlite",
      detail: "ship mutation deduplication",
      confidence: 0.9,
    });
    const stored = getGraphProposalRow(db, first.id);

    assert.equal(duplicate.id, first.id);
    assert.equal(duplicate.deduplicated, true);
    assert.equal(duplicate.skipped, undefined);
    assert.equal(Number(stored.confidence), 0.9);
    assert.equal(countGraphProposals(db), 1);
    assert.equal(db.getGraphVersion(), versionAfterFirst);
  } finally {
    await cleanupTestDB(db, root);
  }
});

test("lower-confidence duplicate is skipped without updating confidence", async () => {
  const { db, root } = await createTestDB();

  try {
    const first = insertProposal(db, {
      detail: "Keep the stronger confidence",
      confidence: 0.92,
    });

    const duplicate = insertProposal(db, {
      subjectLabel: "contextos",
      predicate: "depends_on",
      objectLabel: "sqlite",
      detail: "keep the stronger confidence",
      confidence: 0.41,
    });
    const stored = getGraphProposalRow(db, first.id);

    assert.equal(duplicate.id, first.id);
    assert.equal(duplicate.deduplicated, true);
    assert.equal(duplicate.skipped, true);
    assert.equal(Number(stored.confidence), 0.92);
    assert.equal(countGraphProposals(db), 1);
  } finally {
    await cleanupTestDB(db, root);
  }
});

test("different detail inserts a new proposal", async () => {
  const { db, root } = await createTestDB();

  try {
    const first = insertProposal(db, {
      detail: "First mutation detail",
    });

    const second = insertProposal(db, {
      detail: "Second mutation detail",
    });

    assert.notEqual(second.id, first.id);
    assert.equal(second.deduplicated, false);
    assert.equal(countGraphProposals(db), 2);
  } finally {
    await cleanupTestDB(db, root);
  }
});

test("accepted and rejected proposals are not considered duplicates", async () => {
  const { db, root } = await createTestDB();

  try {
    for (const status of ["accepted", "rejected"]) {
      const first = insertProposal(db, {
        detail: `Lifecycle ${status}`,
        status,
      });

      const second = insertProposal(db, {
        detail: `Lifecycle ${status}`,
        status: "proposed",
      });

      assert.notEqual(second.id, first.id, `${status} proposals should not deduplicate`);
      assert.equal(second.deduplicated, false);
    }

    assert.equal(countGraphProposals(db), 4);
  } finally {
    await cleanupTestDB(db, root);
  }
});

test("null fields are handled correctly during deduplication", async () => {
  const { db, root } = await createTestDB();

  try {
    const first = insertProposal(db, {
      detail: null,
    });

    const duplicate = insertProposal(db, {
      subjectLabel: "contextos",
      predicate: "depends_on",
      objectLabel: "sqlite",
      detail: null,
    });

    assert.equal(duplicate.id, first.id);
    assert.equal(duplicate.deduplicated, true);
    assert.equal(duplicate.skipped, true);
    assert.equal(countGraphProposals(db), 1);
  } finally {
    await cleanupTestDB(db, root);
  }
});

test("fact proposals deduplicate on type and detail when subject, predicate, and object are null", async () => {
  const { db, root } = await createTestDB();

  try {
    const first = insertProposal(db, {
      proposalType: "fact",
      subjectLabel: null,
      predicate: null,
      objectLabel: null,
      detail: "  ContextOS stores graph proposals  ",
    });

    const duplicate = insertProposal(db, {
      proposalType: "fact",
      subjectLabel: null,
      predicate: null,
      objectLabel: null,
      detail: "contextos stores graph proposals",
      confidence: 0.75,
    });
    const stored = getGraphProposalRow(db, first.id);

    assert.equal(duplicate.id, first.id);
    assert.equal(duplicate.deduplicated, true);
    assert.equal(Number(stored.confidence), 0.75);
    assert.equal(countGraphProposals(db), 1);
  } finally {
    await cleanupTestDB(db, root);
  }
});

test("graph version is not bumped for deduplicated inserts", async () => {
  const { db, root } = await createTestDB();

  try {
    const first = insertProposal(db, {
      detail: "Graph version should stay stable on dedup",
      confidence: 0.55,
    });
    const versionAfterFirst = db.getGraphVersion();

    const skippedDuplicate = insertProposal(db, {
      detail: "graph version should stay stable on dedup",
      confidence: 0.4,
    });
    assert.equal(skippedDuplicate.graphVersion, versionAfterFirst);
    assert.equal(db.getGraphVersion(), versionAfterFirst);

    const updatedDuplicate = insertProposal(db, {
      detail: "graph version should stay stable on dedup",
      confidence: 0.91,
    });
    assert.equal(updatedDuplicate.graphVersion, versionAfterFirst);
    assert.equal(db.getGraphVersion(), versionAfterFirst);

    const distinctProposal = insertProposal(db, {
      detail: "Graph version should increase for new proposals",
      confidence: 0.2,
    });

    assert.equal(first.deduplicated, false);
    assert.equal(distinctProposal.deduplicated, false);
    assert.equal(db.getGraphVersion(), versionAfterFirst + 1);
  } finally {
    await cleanupTestDB(db, root);
  }
});
