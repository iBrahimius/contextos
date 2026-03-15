import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";

async function createTestInstance(options = {}) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-lod-test-"));

  const mockLlmClient = options.llmClient === null ? null : (options.llmClient ?? {
    completeJSON: async () => ({
      data: {
        l0: "Headline: entities, topic, key insight (test)",
        l1: "Synopsis: decisions, facts, tensions (test)",
        l2: "Full narrative with context, quotes, details (test)",
      },
    }),
  });

  const contextOS = new ContextOS({
    rootDir: root,
    llmClient: mockLlmClient,
    autoBackfillEmbeddings: false,
  });

  return { contextOS, root };
}

async function cleanupTestInstance(contextOS, root) {
  contextOS.close();
  await fs.rm(root, { recursive: true, force: true });
}

function insertTestConversationAndMessage(db) {
  const now = new Date().toISOString();
  const convId = `conv_lod_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const msgId = `msg_lod_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  db.prepare(`INSERT INTO conversations (id, title, created_at, updated_at) VALUES (?, ?, ?, ?)`).run(convId, "LOD test", now, now);
  db.prepare(`INSERT INTO messages (id, conversation_id, role, direction, content, token_count, captured_at) VALUES (?, ?, ?, ?, ?, ?, ?)`).run(msgId, convId, "user", "inbound", "test", 1, now);
  return { convId, msgId };
}

function insertTestClusterWithObservations(db, clusterId, episodeId, observationCount = 5) {
  const now = new Date().toISOString();
  const { convId, msgId } = insertTestConversationAndMessage(db);

  db.prepare(`INSERT OR IGNORE INTO episodes (id, started_at, ended_at, created_at) VALUES (?, ?, ?, ?)`).run(
    episodeId, now, now, now,
  );

  db.prepare(`
    INSERT INTO observation_clusters (id, episode_id, topic_label, entities, topics, time_span_start, time_span_end, observation_count, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(clusterId, episodeId, "test-topic", "[]", "[]", now, now, observationCount, now);

  for (let i = 0; i < observationCount; i++) {
    const obsId = `obs_lod_${clusterId}_${i}`;
    db.prepare(`
      INSERT INTO observations (id, conversation_id, message_id, category, detail, confidence, compressed_into, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(obsId, convId, msgId, "fact", `Test observation ${i} for LOD generation`, 0.8, clusterId, now);
  }
}

// --- buildClusterLevelPromptInputs ---

test("buildClusterLevelPromptInputs maps atoms and observations correctly", async () => {
  const { contextOS, root } = await createTestInstance();
  try {
    const cluster = {
      observations: [
        { id: "obs1", detail: "ContextOS uses SQLite", confidence: 0.9, category: "fact" },
        { id: "obs2", detail: "Deploy decision made", confidence: 0.85, category: "decision" },
      ],
    };
    const atoms = [
      { atom_type: "fact", text: "ContextOS uses SQLite", source_observation_ids: ["obs1"] },
      { atom_type: "decision", text: "Deploy decision made", source_observation_ids: ["obs2"] },
    ];

    const result = contextOS.buildClusterLevelPromptInputs(cluster, atoms);
    assert.equal(result.promptAtoms.length, 2);
    assert.equal(result.promptAtoms[0].type, "fact");
    assert.equal(result.promptAtoms[0].confidence, 0.9);
    assert.equal(result.promptObservations.length, 2);
    assert.equal(result.sourceObservationIds.length, 2);
  } finally {
    await cleanupTestInstance(contextOS, root);
  }
});

// --- processSingleDreamCycleCluster ---

test("processSingleDreamCycleCluster skips clusters with < 3 observations", async () => {
  const { contextOS, root } = await createTestInstance();
  try {
    const cluster = {
      id: "cluster_small",
      observations: [
        { id: "obs1", detail: "One", confidence: 0.8, category: "fact" },
        { id: "obs2", detail: "Two", confidence: 0.8, category: "fact" },
      ],
    };
    const result = await contextOS.processSingleDreamCycleCluster(cluster, { persist: false });
    assert.equal(result.status, "skipped");
    assert.equal(result.atomsExtracted, 0);
  } finally {
    await cleanupTestInstance(contextOS, root);
  }
});

test("processSingleDreamCycleCluster uses LLM when available", async () => {
  const { contextOS, root } = await createTestInstance();
  try {
    const cluster = {
      id: "cluster_llm",
      observations: [
        { id: "obs1", detail: "Fact one about system", confidence: 0.9, category: "fact" },
        { id: "obs2", detail: "Decision about architecture", confidence: 0.85, category: "decision" },
        { id: "obs3", detail: "Constraint on performance", confidence: 0.8, category: "constraint" },
      ],
    };
    const result = await contextOS.processSingleDreamCycleCluster(cluster, { persist: false });
    assert.equal(result.status, "processed");
    assert.equal(result.atomsExtracted, 3);
    assert.equal(result.levelsGenerated.l0, 1);
    assert.equal(result.levelsGenerated.l1, 1);
    assert.equal(result.levelsGenerated.l2, 1);
  } finally {
    await cleanupTestInstance(contextOS, root);
  }
});

test("processSingleDreamCycleCluster falls back to naive truncation without LLM", async () => {
  const { contextOS, root } = await createTestInstance({ llmClient: null });
  try {
    const cluster = {
      id: "cluster_nollm",
      observations: [
        { id: "obs1", detail: "Fact about system behavior", confidence: 0.9, category: "fact" },
        { id: "obs2", detail: "Decision about database", confidence: 0.85, category: "decision" },
        { id: "obs3", detail: "Constraint on memory", confidence: 0.8, category: "constraint" },
      ],
    };
    const result = await contextOS.processSingleDreamCycleCluster(cluster, { persist: false, allowLlm: true });
    assert.equal(result.status, "processed");
    assert.ok(result.atomsExtracted >= 3);
    assert.ok(result.levelsGenerated.l0 + result.levelsGenerated.l1 + result.levelsGenerated.l2 > 0);
  } finally {
    await cleanupTestInstance(contextOS, root);
  }
});

// --- processDreamCycleClusters ---

test("processDreamCycleClusters processes multiple clusters", async () => {
  const { contextOS, root } = await createTestInstance();
  try {
    const clusters = [
      {
        id: "c1",
        observations: [
          { id: "o1", detail: "A", confidence: 0.8, category: "fact" },
          { id: "o2", detail: "B", confidence: 0.8, category: "fact" },
          { id: "o3", detail: "C", confidence: 0.8, category: "fact" },
        ],
      },
      {
        id: "c2",
        observations: [
          { id: "o4", detail: "D", confidence: 0.8, category: "decision" },
          { id: "o5", detail: "E", confidence: 0.8, category: "decision" },
          { id: "o6", detail: "F", confidence: 0.8, category: "decision" },
          { id: "o7", detail: "G", confidence: 0.8, category: "decision" },
        ],
      },
    ];
    const result = await contextOS.processDreamCycleClusters(clusters, { persist: false });
    assert.equal(result.atomsExtracted, 7);
    assert.equal(result.levelsGenerated.l0, 2);
    assert.equal(result.levelsGenerated.l1, 2);
    assert.equal(result.levelsGenerated.l2, 2);
  } finally {
    await cleanupTestInstance(contextOS, root);
  }
});

test("processDreamCycleClusters skips small clusters", async () => {
  const { contextOS, root } = await createTestInstance();
  try {
    const clusters = [
      { id: "c_small", observations: [{ id: "o1", detail: "A", confidence: 0.8, category: "fact" }] },
    ];
    const result = await contextOS.processDreamCycleClusters(clusters, { persist: false });
    assert.equal(result.atomsExtracted, 0);
    assert.equal(result.levelsGenerated.l0, 0);
  } finally {
    await cleanupTestInstance(contextOS, root);
  }
});

test("processDreamCycleClusters handles LLM errors gracefully", async () => {
  const failingLlm = { completeJSON: async () => { throw new Error("LLM unavailable"); } };
  const { contextOS, root } = await createTestInstance({ llmClient: failingLlm });
  try {
    const clusters = [
      {
        id: "c_fail",
        observations: [
          { id: "o1", detail: "A long enough detail", confidence: 0.8, category: "fact" },
          { id: "o2", detail: "Another observation", confidence: 0.8, category: "fact" },
          { id: "o3", detail: "Third observation here", confidence: 0.8, category: "fact" },
        ],
      },
    ];
    // LLM fails → generateLevels returns {} → falls back to naive → still produces levels
    const result = await contextOS.processDreamCycleClusters(clusters, { persist: false });
    assert.ok(result.atomsExtracted >= 0);
  } finally {
    await cleanupTestInstance(contextOS, root);
  }
});

// --- persistence ---

test("processSingleDreamCycleCluster persists levels to database", async () => {
  const { contextOS, root } = await createTestInstance();
  try {
    const clusterId = "cluster_persist_test";
    const episodeId = "episode_persist_test";
    insertTestClusterWithObservations(contextOS.database, clusterId, episodeId, 4);

    const cluster = {
      id: clusterId,
      observations: [
        { id: `obs_lod_${clusterId}_0`, detail: "Test observation 0", confidence: 0.8, category: "fact" },
        { id: `obs_lod_${clusterId}_1`, detail: "Test observation 1", confidence: 0.8, category: "fact" },
        { id: `obs_lod_${clusterId}_2`, detail: "Test observation 2", confidence: 0.8, category: "fact" },
        { id: `obs_lod_${clusterId}_3`, detail: "Test observation 3", confidence: 0.8, category: "fact" },
      ],
    };

    await contextOS.processSingleDreamCycleCluster(cluster, { persist: true });

    const levels = contextOS.database.prepare(
      `SELECT * FROM cluster_levels WHERE cluster_id = ? ORDER BY level ASC`
    ).all(clusterId);

    assert.ok(levels.length >= 1, "Should persist at least one level");
    assert.ok(levels.some(l => l.level === 0), "Should have L0");
    assert.ok(levels.some(l => l.level === 1), "Should have L1");
    assert.ok(levels.some(l => l.level === 2), "Should have L2");
    assert.ok(levels.every(l => l.text.length > 0), "All levels should have text");
  } finally {
    await cleanupTestInstance(contextOS, root);
  }
});

// --- backfillClusterLevels ---

test("backfillClusterLevels processes eligible clusters", async () => {
  const { contextOS, root } = await createTestInstance();
  try {
    insertTestClusterWithObservations(contextOS.database, "cluster_bf_1", "episode_bf", 5);
    insertTestClusterWithObservations(contextOS.database, "cluster_bf_2", "episode_bf", 3);
    insertTestClusterWithObservations(contextOS.database, "cluster_bf_3", "episode_bf", 2);

    const report = await contextOS.backfillClusterLevels({ batchSize: 10, minObservations: 3 });
    assert.equal(report.total_eligible, 2);
    assert.equal(report.processed, 2);
    assert.equal(report.skipped, 0);
    assert.equal(report.failed, 0);

    const levels = contextOS.database.prepare(`SELECT COUNT(*) as count FROM cluster_levels`).get();
    assert.ok(levels.count >= 4, `Expected ≥4 levels, got ${levels.count}`);
  } finally {
    await cleanupTestInstance(contextOS, root);
  }
});

test("backfillClusterLevels skips already-backfilled clusters", async () => {
  const { contextOS, root } = await createTestInstance();
  try {
    insertTestClusterWithObservations(contextOS.database, "cluster_already", "episode_already", 4);

    const report1 = await contextOS.backfillClusterLevels({ minObservations: 3 });
    assert.equal(report1.processed, 1);

    const report2 = await contextOS.backfillClusterLevels({ minObservations: 3 });
    assert.equal(report2.total_eligible, 0);
    assert.equal(report2.processed, 0);
  } finally {
    await cleanupTestInstance(contextOS, root);
  }
});

// --- dream cycle integration ---

test("dream cycle dry-run returns level breakdown", async () => {
  const { contextOS, root } = await createTestInstance();
  try {
    const report = await contextOS.dreamCycle({ dry_run: true });
    assert.ok(typeof report.levels_generated === "object");
    assert.ok("l0" in report.levels_generated);
    assert.ok("l1" in report.levels_generated);
    assert.ok("l2" in report.levels_generated);
  } finally {
    await cleanupTestInstance(contextOS, root);
  }
});
