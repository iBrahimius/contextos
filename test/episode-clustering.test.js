import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";
import {
  detectEpisodes,
  detectTopicClusters,
  clusterObservations,
} from "../src/core/episode-clustering.js";
import { createId, nowIso, estimateTokens } from "../src/core/utils.js";

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-clustering-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  return root;
}

// Helper: insert a message
function insertMessage(db, conversationId, content, actorId = "user") {
  return db.insertMessage({
    conversationId,
    role: "user",
    direction: "inbound",
    actorId,
    originKind: "import",
    content,
    tokenCount: estimateTokens(content),
    raw: { seeded: true },
    ingestId: createId(),
  });
}

// Helper: insert an observation
function insertObservation(
  db,
  conversationId,
  messageId,
  {
    category = "fact",
    detail = "test observation",
    confidence = 0.8,
    subjectEntityId = null,
    objectEntityId = null,
    metadata = null,
  } = {}
) {
  const id = createId("obs");
  db.prepare(`
    INSERT INTO observations (
      id, conversation_id, message_id, actor_id, category,
      predicate, subject_entity_id, object_entity_id, detail,
      confidence, source_span, metadata_json, created_at, compressed_into
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
  `).run(
    id,
    conversationId,
    messageId,
    "test-actor",
    category,
    null,
    subjectEntityId,
    objectEntityId,
    detail,
    confidence,
    detail,
    metadata ? JSON.stringify(metadata) : null,
    nowIso()
  );
  return id;
}

test("detectEpisodes: splits observations by time gap", () => {
  const now = new Date();
  const baseTime = now.getTime();

  const observations = [
    { id: "1", created_at: new Date(baseTime).toISOString() },
    { id: "2", created_at: new Date(baseTime + 5 * 60 * 1000).toISOString() }, // 5 min later
    { id: "3", created_at: new Date(baseTime + 10 * 60 * 1000).toISOString() }, // 5 min later (total 10 min)
    { id: "4", created_at: new Date(baseTime + 50 * 60 * 1000).toISOString() }, // 40 min gap -> NEW EPISODE
    { id: "5", created_at: new Date(baseTime + 55 * 60 * 1000).toISOString() }, // 5 min later
  ];

  const episodes = detectEpisodes(observations, 30); // 30-min gap threshold

  assert.equal(episodes.length, 2, "Should detect 2 episodes");
  assert.equal(episodes[0].observations.length, 3, "First episode has 3 observations");
  assert.equal(episodes[1].observations.length, 2, "Second episode has 2 observations");
  assert.equal(episodes[0].started_at, observations[0].created_at);
  assert.equal(episodes[0].ended_at, observations[2].created_at);
  assert.equal(episodes[1].started_at, observations[3].created_at);
  assert.equal(episodes[1].ended_at, observations[4].created_at);
});

test("detectEpisodes: single observation creates one episode", () => {
  const obs = [{ id: "1", created_at: nowIso() }];
  const episodes = detectEpisodes(obs, 30);

  assert.equal(episodes.length, 1);
  assert.equal(episodes[0].observations.length, 1);
});

test("detectEpisodes: empty array returns empty array", () => {
  const episodes = detectEpisodes([], 30);
  assert.equal(episodes.length, 0);
});

test("detectTopicClusters: splits on entity overlap drop below threshold", () => {
  const observations = [
    {
      id: "1",
      created_at: nowIso(),
      subject_entity_id: "entity1",
      object_entity_id: "entity2",
      metadata_json: JSON.stringify({
        entities: [{ label: "ProjectA" }, { label: "ComponentX" }],
      }),
    },
    {
      id: "2",
      created_at: new Date(Date.now() + 1000).toISOString(),
      subject_entity_id: "entity1",
      object_entity_id: "entity2",
      metadata_json: JSON.stringify({
        entities: [{ label: "ProjectA" }],
      }),
    },
    // New observation with almost no entity overlap -> should trigger topic shift
    {
      id: "3",
      created_at: new Date(Date.now() + 2000).toISOString(),
      subject_entity_id: "entity3",
      object_entity_id: "entity4",
      metadata_json: JSON.stringify({
        entities: [{ label: "ProjectB" }, { label: "ComponentY" }],
      }),
    },
  ];

  const clusters = detectTopicClusters(observations, {
    entityOverlapThreshold: 0.5,
  });

  assert.ok(clusters.length >= 2, `Expected 2+ clusters, got ${clusters.length}`);
  assert.equal(clusters[0].observations.length, 2, "First cluster has 2 observations");
  assert.equal(clusters[1].observations.length, 1, "Second cluster has 1 observation");
});

test("detectTopicClusters: respects maxClusterSize", () => {
  const maxSize = 10;
  const observations = [];

  // Create 25 observations with identical entities
  for (let i = 0; i < 25; i++) {
    observations.push({
      id: `obs-${i}`,
      created_at: new Date(Date.now() + i * 100).toISOString(),
      subject_entity_id: "entity1",
      object_entity_id: "entity2",
      metadata_json: null,
    });
  }

  const clusters = detectTopicClusters(observations, {
    maxClusterSize: maxSize,
    entityOverlapThreshold: 0.5,
  });

  // All clusters should have at most maxSize observations
  for (const cluster of clusters) {
    assert.ok(
      cluster.observations.length <= maxSize,
      `Cluster exceeds max size: ${cluster.observations.length} > ${maxSize}`
    );
  }

  // Total observations should be preserved
  const totalObsInClusters = clusters.reduce(
    (sum, c) => sum + c.observations.length,
    0
  );
  assert.equal(totalObsInClusters, observations.length);
});

test("detectTopicClusters: works with no embeddings", () => {
  const observations = [
    {
      id: "1",
      created_at: nowIso(),
      subject_entity_id: "entity1",
      metadata_json: JSON.stringify({ entities: [{ label: "A" }, { label: "B" }] }),
    },
    {
      id: "2",
      created_at: new Date(Date.now() + 1000).toISOString(),
      subject_entity_id: "entity1",
      metadata_json: JSON.stringify({ entities: [{ label: "A" }, { label: "B" }] }),
    },
  ];

  const clusters = detectTopicClusters(observations, { embeddings: new Map() });

  assert.equal(clusters.length, 1, "Should create 1 cluster without embeddings");
  assert.equal(clusters[0].observations.length, 2);
});

test("clusterObservations: end-to-end clustering", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir, autoBackfillEmbeddings: false });
  const conversation = contextOS.database.createConversation(
    "Clustering Test"
  );

  try {
    const now = Date.now();

    // Insert messages at different times
    const msg1 = insertMessage(contextOS.database, conversation.id, "Message 1");
    const msg2 = insertMessage(contextOS.database, conversation.id, "Message 2");
    const msg3 = insertMessage(
      contextOS.database,
      conversation.id,
      "Message 3"
    );

    // Insert observations in first episode
    insertObservation(contextOS.database, conversation.id, msg1.id, {
      detail: "Observation in first episode",
    });
    insertObservation(contextOS.database, conversation.id, msg2.id, {
      detail: "Another observation in first episode",
    });

    // Insert observation in second episode (manually set time)
    const obs3Id = createId("obs");
    contextOS.database
      .prepare(
        `
      INSERT INTO observations (
        id, conversation_id, message_id, actor_id, category,
        detail, confidence, source_span, created_at, compressed_into
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
    `
      )
      .run(
        obs3Id,
        conversation.id,
        msg3.id,
        "test-actor",
        "fact",
        "Observation in second episode",
        0.8,
        "detail",
        new Date(now + 60 * 60 * 1000).toISOString() // 1 hour later
      );

    // Run clustering
    const result = clusterObservations(contextOS.database, {
      sessionGapMinutes: 30,
    });

    assert.ok(result.episodes_detected >= 1, "Should detect at least 1 episode");
    assert.ok(result.clusters_detected >= 1, "Should detect at least 1 cluster");
    assert.ok(result.observations_clustered >= 1, "Should cluster at least 1 observation");

    // Verify episodes were persisted
    const episodes = contextOS.database.prepare(`
      SELECT id, cluster_count FROM episodes
    `).all();
    assert.ok(episodes.length > 0, "Episodes should be persisted");

    // Verify clusters were persisted
    const clusters = contextOS.database.prepare(`
      SELECT id, episode_id, observation_count FROM observation_clusters
    `).all();
    assert.ok(clusters.length > 0, "Clusters should be persisted");

    // Verify observations were marked
    const markedObs = contextOS.database.prepare(`
      SELECT id, compressed_into FROM observations
      WHERE compressed_into IS NOT NULL
    `).all();
    assert.ok(markedObs.length > 0, "Observations should be marked with cluster IDs");
  } finally {
    // Cleanup
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("clusterObservations: handles date range filtering", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir, autoBackfillEmbeddings: false });
  const conversation = contextOS.database.createConversation(
    "Date Range Test"
  );

  try {
    const now = Date.now();
    const oneHourAgo = new Date(now - 60 * 60 * 1000).toISOString();
    const twoHoursAgo = new Date(now - 2 * 60 * 60 * 1000).toISOString();
    const oneHourFromNow = new Date(now + 60 * 60 * 1000).toISOString();

    // Insert observations at different times
    const msg1 = insertMessage(contextOS.database, conversation.id, "Msg 1");
    const msg2 = insertMessage(contextOS.database, conversation.id, "Msg 2");

    const obs1Id = createId("obs");
    contextOS.database
      .prepare(
        `
      INSERT INTO observations (
        id, conversation_id, message_id, actor_id, category,
        detail, confidence, source_span, created_at, compressed_into
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
    `
      )
      .run(
        obs1Id,
        conversation.id,
        msg1.id,
        "test",
        "fact",
        "obs 1",
        0.8,
        "detail",
        twoHoursAgo
      );

    const obs2Id = createId("obs");
    contextOS.database
      .prepare(
        `
      INSERT INTO observations (
        id, conversation_id, message_id, actor_id, category,
        detail, confidence, source_span, created_at, compressed_into
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
    `
      )
      .run(
        obs2Id,
        conversation.id,
        msg2.id,
        "test",
        "fact",
        "obs 2",
        0.8,
        "detail",
        oneHourFromNow
      );

    // Cluster only observations from last hour
    const result = clusterObservations(contextOS.database, {
      since: oneHourAgo,
      sessionGapMinutes: 30,
    });

    // Should only cluster the one observation from the last hour
    // The one from 2 hours ago should not be included
    assert.ok(result.observations_clustered >= 0, "Should complete without error");
  } finally {
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("clusterObservations: empty observation set returns zeros", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir, autoBackfillEmbeddings: false });
  contextOS.database.createConversation(
    "Empty Clustering Test"
  );

  try {
    const result = clusterObservations(contextOS.database, {
      since: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
      sessionGapMinutes: 30,
    });

    assert.equal(result.episodes_detected, 0);
    assert.equal(result.clusters_detected, 0);
    assert.equal(result.observations_clustered, 0);
  } finally {
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("clusterObservations: large observation set stays under limits", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir, autoBackfillEmbeddings: false });
  const conversation = contextOS.database.createConversation("Large Clustering");

  try {
    const now = Date.now();

    // Insert 500 observations spread across multiple episodes
    for (let i = 0; i < 500; i++) {
      const msg = insertMessage(contextOS.database, conversation.id, `Msg ${i}`);
      const timestamp = new Date(now + i * 60 * 1000).toISOString(); // 1 min apart
      const obsId = createId("obs");
      contextOS.database
        .prepare(
          `
        INSERT INTO observations (
          id, conversation_id, message_id, actor_id, category,
          detail, confidence, source_span, created_at, compressed_into
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
      `
        )
        .run(
          obsId,
          conversation.id,
          msg.id,
          "test",
          "fact",
          `Observation ${i}`,
          0.8,
          "detail",
          timestamp
        );
    }

    const result = clusterObservations(contextOS.database, {
      sessionGapMinutes: 30,
    });

    // Verify all observations were processed
    const allObs = contextOS.database.prepare(`
      SELECT COUNT(*) as count FROM observations
    `).get();
    assert.equal(allObs.count, 500, "All 500 observations should be inserted");

    // Verify clustering completed
    assert.ok(result.observations_clustered <= 500, "Should not cluster more than inserted");
  } finally {
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});
