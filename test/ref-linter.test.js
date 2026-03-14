import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";
import {
  createRefRegistrySnapshot,
  extractRefTags,
  lintRefTags,
  lintRefTagsPath,
} from "../src/core/ref-linter.js";
import { ContextDatabase } from "../src/db/database.js";
import { handleRequest } from "../src/http/router.js";

async function makeRoot(prefix = "contextos-ref-linter-") {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), prefix));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  return root;
}

async function makeTempDb() {
  const root = await makeRoot("contextos-ref-linter-db-");
  const db = new ContextDatabase(path.join(root, "data", "contextos.db"));
  return { db, root };
}

function createSeedMessage(db, conversationId, content) {
  return db.insertMessage({
    conversationId,
    role: "user",
    direction: "inbound",
    actorId: "seed",
    content,
    tokenCount: Math.max(1, Math.ceil(content.length / 4)),
    capturedAt: new Date().toISOString(),
  });
}

function seedRegistry(db) {
  const conversation = db.createConversation("Ref Linter Test");
  const message = createSeedMessage(db, conversation.id, "Seed registry data for ref linter tests.");

  const ibrahim = db.insertEntity({ label: "Ibrahim", kind: "person" });
  const dns = db.insertEntity({ label: "DNS on Cloudflare", kind: "project" });
  const ibrahimId = ibrahim.id;
  const dnsId = dns.id;

  const decisionObservation = db.insertObservation({
    conversationId: conversation.id,
    messageId: message.id,
    actorId: "seed",
    category: "decision",
    subjectEntityId: dnsId,
    detail: "DNS on Cloudflare is the chosen DNS migration path.",
    confidence: 0.95,
  });
  const decisionId = db.insertDecision({
    observationId: decisionObservation.id,
    entityId: dnsId,
    title: "DNS on Cloudflare",
    rationale: "Better edge routing.",
  });
  db.insertClaim({
    observation_id: decisionObservation.id,
    conversation_id: conversation.id,
    message_id: message.id,
    actor_id: "seed",
    claim_type: "decision",
    subject_entity_id: dnsId,
    predicate: "decision",
    value_text: "accepted",
    confidence: 0.95,
    source_type: "explicit",
    lifecycle_state: "active",
    resolution_key: `decision:${dnsId}:decision`,
    facet_key: "decision",
  });

  const taskObservation = db.insertObservation({
    conversationId: conversation.id,
    messageId: message.id,
    actorId: "seed",
    category: "task",
    subjectEntityId: dnsId,
    detail: "Update the cutover checklist.",
    confidence: 0.92,
  });
  const taskId = db.insertTask({
    observationId: taskObservation.id,
    entityId: dnsId,
    title: "Update the cutover checklist",
    status: "done",
    priority: "high",
  });
  db.insertClaim({
    observation_id: taskObservation.id,
    conversation_id: conversation.id,
    message_id: message.id,
    actor_id: "seed",
    claim_type: "task",
    subject_entity_id: dnsId,
    predicate: "task",
    value_text: "done",
    confidence: 0.92,
    source_type: "explicit",
    lifecycle_state: "active",
    resolution_key: `task:${dnsId}:task`,
    facet_key: "task",
  });

  const constraintObservation = db.insertObservation({
    conversationId: conversation.id,
    messageId: message.id,
    actorId: "seed",
    category: "constraint",
    subjectEntityId: dnsId,
    detail: "Keep TTL above 60 seconds.",
    confidence: 0.88,
  });
  const constraintId = db.insertConstraint({
    observationId: constraintObservation.id,
    entityId: dnsId,
    detail: "Keep TTL above 60 seconds.",
    severity: "medium",
  });
  db.insertClaim({
    observation_id: constraintObservation.id,
    conversation_id: conversation.id,
    message_id: message.id,
    actor_id: "seed",
    claim_type: "constraint",
    subject_entity_id: dnsId,
    predicate: "constraint",
    value_text: "expired",
    confidence: 0.88,
    source_type: "explicit",
    lifecycle_state: "active",
    resolution_key: `constraint:${dnsId}:constraint`,
    facet_key: "constraint",
  });

  return {
    conversation,
    message,
    entityIds: { ibrahimId, dnsId },
    registryIds: { decisionId, taskId, constraintId },
  };
}

function getSnapshotEntries(snapshot) {
  return {
    entity: snapshot.entity.find((entry) => entry.label === "Ibrahim"),
    decision: snapshot.decision.find((entry) => entry.title === "DNS on Cloudflare"),
    task: snapshot.task.find((entry) => entry.title === "Update the cutover checklist"),
    constraint: snapshot.constraint.find((entry) => entry.detail === "Keep TTL above 60 seconds."),
  };
}

async function withTempDb(callback) {
  const { db, root } = await makeTempDb();
  try {
    seedRegistry(db);
    return await callback({ db, root });
  } finally {
    db.close();
    await fs.rm(root, { recursive: true, force: true });
  }
}

async function createHarness() {
  const rootDir = await makeRoot("contextos-ref-linter-api-");
  const contextOS = new ContextOS({ rootDir });
  seedRegistry(contextOS.database);
  const server = http.createServer((request, response) => handleRequest(contextOS, rootDir, request, response));

  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();

  return {
    rootDir,
    contextOS,
    baseUrl: `http://127.0.0.1:${port}`,
    async close() {
      await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
      contextOS.database.close();
      contextOS.telemetry.close();
      await fs.rm(rootDir, { recursive: true, force: true });
    },
  };
}

async function withHarness(callback) {
  const harness = await createHarness();
  try {
    return await callback(harness);
  } finally {
    await harness.close();
  }
}

test("extractRefTags ignores code fences and parses status assertions", () => {
  const tags = extractRefTags([
    "Visible tag <!-- ref:entity/ibrahim -->",
    "```md",
    "Hidden tag <!-- ref:decision/ignored =active -->",
    "```",
    "Another one <!-- ref:task/update_cutover_checklist =done -->",
  ].join("\n"), { filePath: "MEMORY.md" });

  assert.equal(tags.length, 2);
  assert.deepEqual(tags.map((tag) => ({ type: tag.type, id: tag.id, status: tag.status, line: tag.line })), [
    { type: "entity", id: "ibrahim", status: null, line: 1 },
    { type: "task", id: "update_cutover_checklist", status: "done", line: 5 },
  ]);
});

test("lintRefTags reports conflicts, stale refs, orphans, and suggestions", async () => {
  await withTempDb(async ({ db }) => {
    const snapshot = createRefRegistrySnapshot(db);
    const entries = getSnapshotEntries(snapshot);

    assert.ok(entries.entity);
    assert.ok(entries.decision);
    assert.ok(entries.task);
    assert.ok(entries.constraint);

    const content = [
      `${entries.entity.label} owns the rollout.`,
      `<!-- ref:entity/${entries.entity.preferredRefId} -->`,
      "",
      `We are following ${entries.decision.title} for the DNS migration.`,
      `The checklist is complete. <!-- ref:task/${entries.task.preferredRefId} =active -->`,
      `Constraint note: ${entries.constraint.detail} <!-- ref:constraint/${entries.constraint.preferredRefId} =active -->`,
      "Broken pointer <!-- ref:decision/missing_entry =active -->",
    ].join("\n");

    const result = lintRefTags({
      content,
      database: db,
      filePath: "MEMORY.md",
      snapshot,
    });

    assert.equal(result.ok, false);
    assert.equal(result.counts.tags, 4);
    assert.equal(result.conflicts.length, 1);
    assert.match(result.conflicts[0].description, /does not exist/);

    assert.equal(result.staleRefs.length, 2);
    assert.deepEqual(result.staleRefs.map((entry) => entry.registryStatus).sort(), ["done", "expired"]);

    assert.equal(result.orphans.length, 1);
    assert.equal(result.orphans[0].type, "decision");
    assert.equal(result.orphans[0].id, entries.decision.preferredRefId);

    assert.equal(result.suggestions.length, 1);
    assert.equal(result.suggestions[0].matchType, "decision");
    assert.equal(
      result.suggestions[0].suggestedTag,
      `<!-- ref:decision/${entries.decision.preferredRefId} -->`,
    );
  });
});

test("lintRefTagsPath reads markdown from disk", async () => {
  await withTempDb(async ({ db, root }) => {
    const snapshot = createRefRegistrySnapshot(db);
    const entries = getSnapshotEntries(snapshot);
    const filePath = path.join(root, "MEMORY.md");
    await fs.writeFile(filePath, `Tracking ${entries.decision.title} rollout.`, "utf8");

    const result = await lintRefTagsPath({ path: filePath, database: db, snapshot });

    assert.equal(result.file, filePath);
    assert.equal(result.orphans.length, 1);
    assert.equal(result.suggestions[0].matchId, entries.decision.preferredRefId);
  });
});

test("POST /api/lint/refs accepts raw content", async () => {
  await withHarness(async ({ baseUrl, contextOS }) => {
    const snapshot = createRefRegistrySnapshot(contextOS.database);
    const entries = getSnapshotEntries(snapshot);
    const response = await fetch(`${baseUrl}/api/lint/refs`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        content: `Track ${entries.decision.title} rollout.`,
      }),
    });

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.counts.orphans, 1);
    assert.equal(payload.suggestions[0].matchId, entries.decision.preferredRefId);
    assert.ok(Number.isFinite(payload.graph_version));
  });
});

test("POST /api/lint/refs accepts file paths", async () => {
  await withHarness(async ({ baseUrl, contextOS, rootDir }) => {
    const snapshot = createRefRegistrySnapshot(contextOS.database);
    const entries = getSnapshotEntries(snapshot);
    const relativePath = "notes.md";
    await fs.writeFile(path.join(rootDir, relativePath), `Reminder: ${entries.decision.title}.`, "utf8");

    const response = await fetch(`${baseUrl}/api/lint/refs`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ path: relativePath }),
    });

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.file, path.join(rootDir, relativePath));
    assert.equal(payload.counts.orphans, 1);
    assert.equal(payload.suggestions[0].matchType, "decision");
  });
});
