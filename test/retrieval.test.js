import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";
import { persistHeuristicPatchForMessage } from "./test-helpers.js";

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  await fs.mkdir(path.join(root, "docs"), { recursive: true });
  return root;
}

test("graph-aware retrieval expands related components", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const conversation = contextOS.database.createConversation("Retrieval Test");

  await contextOS.ingestMessage({
    conversationId: conversation.id,
    conversationTitle: conversation.title,
    role: "user",
    direction: "inbound",
    content:
      "Querying the memory system should also surface the embedding engine, retrieval pipeline, and storage layer because they are related components.",
  });

  persistHeuristicPatchForMessage(contextOS, contextOS.database.listMessages(conversation.id).at(-1));

  const result = await contextOS.retrieve({
    conversationId: conversation.id,
    queryText: "memory system",
  });

  const labels = result.expandedEntities.map((entity) => entity.label.toLowerCase());
  assert.ok(labels.includes("memory system"));
  assert.ok(labels.includes("embedding engine"));
  assert.ok(labels.includes("retrieval pipeline"));
  assert.ok(labels.includes("storage layer"));
});

test("retrieval hints widen expansion even without a stored graph edge", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const conversation = contextOS.database.createConversation("Hint Expansion");
  const memorySystem = contextOS.graph.ensureEntity({ label: "memory system", kind: "component" });
  const retrievalPipeline = contextOS.graph.ensureEntity({ label: "retrieval pipeline", kind: "component" });

  contextOS.telemetry.insertRetrievalHint({
    conversationId: conversation.id,
    seedEntityId: memorySystem.id,
    seedLabel: memorySystem.label,
    expandEntityId: retrievalPipeline.id,
    expandLabel: retrievalPipeline.label,
    reason: "depends on edge observed live",
    weight: 1.2,
    ttlTurns: 4,
  });

  const result = await contextOS.retrieve({
    conversationId: conversation.id,
    queryText: "memory system",
  });

  const labels = result.expandedEntities.map((entity) => entity.label.toLowerCase());
  assert.ok(labels.includes("memory system"));
  assert.ok(labels.includes("retrieval pipeline"));
  assert.ok(result.expansionPath.some((edge) => edge.source === "hint" && edge.to === retrievalPipeline.id));
  assert.ok(result.hintOutcomes.some((outcome) => outcome.hintId && outcome.reward > 0));

  const activeHints = contextOS.telemetry.listActiveRetrievalHints(10);
  const hinted = activeHints.find((hint) => hint.seed_label === "memory system" && hint.expand_label === "retrieval pipeline");
  assert.ok(hinted);
  assert.ok(Number(hinted.weight) > 1.2);

  const stats = contextOS.telemetry.listRetrievalHintStats(10);
  const stat = stats.find((row) => row.id === hinted.id);
  assert.equal(stat.times_considered, 1);
  assert.equal(stat.times_applied, 1);
  assert.equal(stat.times_rewarded, 1);

  const events = contextOS.telemetry.listRecentRetrievalHintEvents(10);
  assert.ok(events.some((event) => event.hint_id === hinted.id && event.event_type === "rewarded"));
});

test("unused hints decay faster when the graph already covers the path", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const conversation = contextOS.database.createConversation("Hint TTL");
  const memorySystem = contextOS.graph.ensureEntity({ label: "memory system", kind: "component" });
  const retrievalPipeline = contextOS.graph.ensureEntity({ label: "retrieval pipeline", kind: "component" });
  contextOS.graph.connect({
    subjectEntityId: memorySystem.id,
    predicate: "depends_on",
    objectEntityId: retrievalPipeline.id,
    weight: 1,
  });

  const hintId = contextOS.telemetry.insertRetrievalHint({
    conversationId: conversation.id,
    seedEntityId: memorySystem.id,
    seedLabel: memorySystem.label,
    expandEntityId: retrievalPipeline.id,
    expandLabel: retrievalPipeline.label,
    reason: "depends on edge observed live",
    weight: 0.2,
    ttlTurns: 4,
  }).id;

  const result = await contextOS.retrieve({
    conversationId: conversation.id,
    queryText: "memory system",
  });
  assert.ok(result.expandedEntities.some((entity) => entity.id === retrievalPipeline.id));

  const stats = contextOS.telemetry.listRetrievalHintStats(10);
  const stat = stats.find((row) => row.id === hintId);
  assert.equal(stat.times_considered, 1);
  assert.equal(stat.times_applied, 0);
  assert.equal(stat.times_unused, 1);
  assert.ok(Number(stat.last_reward) < 0);

  const activeHints = contextOS.telemetry.listActiveRetrievalHints(10);
  const hinted = activeHints.find((hint) => hint.id === hintId);
  assert.ok(hinted);
  assert.ok(Number(hinted.weight) < 0.2);
  assert.ok(Number(hinted.ttl_turns) < 4);

  const events = contextOS.telemetry.listRecentRetrievalHintEvents(10);
  assert.ok(events.some((event) => event.hint_id === hintId && event.event_type === "decayed"));
});

test("query seed matching ignores generic question words and favors specific entities", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });

  contextOS.graph.ensureEntity({ label: "What", kind: "concept" });
  contextOS.graph.ensureEntity({ label: "hosting", kind: "concept" });
  const vercel = contextOS.graph.ensureEntity({ label: "Vercel", kind: "vendor" });
  const rhi = contextOS.graph.ensureEntity({ label: "Rumor Has It", kind: "project" });

  const hostingSeeds = contextOS.retrieval.findSeedEntities("What hosting platform do we use?");
  assert.deepEqual(hostingSeeds.map((entity) => entity.label), []);

  const brandedSeeds = contextOS.retrieval.findSeedEntities("What is the price of Rumor Has It?");
  assert.equal(brandedSeeds[0]?.id, rhi.id);
  assert.ok(!brandedSeeds.some((entity) => entity.label === "What"));
  assert.ok(!brandedSeeds.some((entity) => entity.id === vercel.id));
});

test("lexical registry search can surface factual hits without seed entity coverage", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const conversation = contextOS.database.createConversation("Registry lexical retrieval");
  const ledger = contextOS.graph.ensureEntity({ label: "pricing ledger", kind: "system" });
  const message = contextOS.database.insertMessage({
    conversationId: conversation.id,
    role: "user",
    direction: "inbound",
    content: "pricing note",
    tokenCount: 2,
  });
  const observation = contextOS.database.insertObservation({
    messageId: message.id,
    conversationId: conversation.id,
    category: "fact",
    detail: "Pricing note",
    subjectEntityId: ledger.id,
    scopeKind: "private",
  });

  contextOS.database.insertFact({
    observationId: observation.id,
    entityId: ledger.id,
    detail: "RHI pricing remains €34.99 per unit plus shipping.",
  });

  const result = await contextOS.retrieve({
    conversationId: conversation.id,
    queryText: "What is the price of Rumor Has It?",
  });

  assert.ok(
    result.items.slice(0, 5).some((item) => item.type === "fact" && item.summary.includes("34.99")),
    "expected top-5 results to include the pricing fact",
  );
});
