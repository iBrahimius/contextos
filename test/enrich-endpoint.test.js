import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";

// AI_AUTO_APPLY_CONFIDENCE_THRESHOLD is 0.85 — observations below this
// become graph proposals instead of claims. Test data must stay above it.
import { handleRequest } from "../src/http/router.js";

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-enrich-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  return root;
}

async function createHarness() {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({
    rootDir,
    autoBackfillEmbeddings: false,
    
  });
  const server = http.createServer((request, response) => handleRequest(contextOS, rootDir, request, response));

  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();

  return {
    rootDir,
    contextOS,
    baseUrl: `http://127.0.0.1:${port}`,
    async close() {
      await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
      await contextOS.close();
      await fs.rm(rootDir, { recursive: true, force: true });
    },
  };
}

async function ingestEnrichableMessage(harness, overrides = {}) {
  const conversation = harness.contextOS.database.createConversation(overrides.title ?? "Enrich Endpoint");

  return harness.contextOS.ingestMessage({
    conversationId: conversation.id,
    conversationTitle: conversation.title,
    role: "user",
    direction: "inbound",
    actorId: "user:test",
    ingestId: overrides.ingestId ?? "enrich_ingest_001",
    content: overrides.content ?? "",
    scopeKind: overrides.scopeKind ?? "project",
    scopeId: overrides.scopeId ?? "proj-enrich",
  });
}

test("POST /api/ingest/enrich stores entities and observations for an ingested message", async () => {
  const harness = await createHarness();

  try {
    const ingested = await ingestEnrichableMessage(harness);

    const enrichPayload = {
      ingestId: ingested.message.ingestId,
      source: "haiku-classifier",
      entities: [
        {
          label: "Scribe v3",
          kind: "product",
          summary: "The upstream system that posts enrich payloads.",
        },
        {
          label: "haiku-classifier",
          kind: "service",
          summary: "Classifier that emits enrich patches.",
        },
      ],
      observations: [
        {
          category: "relationship",
          detail: "Scribe v3 uses haiku-classifier for enrich processing.",
          subjectLabel: "Scribe v3",
          objectLabel: "haiku-classifier",
          predicate: "uses",
          confidence: 0.91,
        },
      ],
      graphProposals: [],
    };

    const response = await fetch(`${harness.baseUrl}/api/ingest/enrich`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(enrichPayload),
    });

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.entities.length, 2);
    assert.equal(payload.observations.length, 1);
    assert.equal(payload.claimStats.created, 1);
    assert.deepEqual(payload.claimStats.errors, []);
    assert.ok(payload.graph_version > 0);

    const labels = harness.contextOS.graph.listEntities().map((entity) => entity.label);
    assert.ok(labels.includes("Scribe v3"));
    assert.ok(labels.includes("haiku-classifier"));

    const storedObservation = harness.contextOS.database.prepare(`
      SELECT
        o.category,
        o.predicate,
        o.detail,
        o.actor_id AS actorId,
        o.scope_kind AS scopeKind,
        o.scope_id AS scopeId,
        subject.label AS subjectLabel,
        object.label AS objectLabel
      FROM observations o
      LEFT JOIN entities subject ON subject.id = o.subject_entity_id
      LEFT JOIN entities object ON object.id = o.object_entity_id
      WHERE o.message_id = ?
      ORDER BY o.created_at ASC, o.id ASC
      LIMIT 1
    `).get(ingested.message.id);

    assert.deepEqual({ ...storedObservation }, {
      category: "relationship",
      predicate: "depends_on", // "uses" normalized to "depends_on" by normalize-enrichment
      detail: "Scribe v3 uses haiku-classifier for enrich processing.",
      actorId: "haiku-classifier",
      scopeKind: "project",
      scopeId: "proj-enrich",
      subjectLabel: "Scribe v3",
      objectLabel: "haiku-classifier",
    });

    const storedObservationId = harness.contextOS.database.prepare(`
      SELECT id
      FROM observations
      WHERE message_id = ?
      ORDER BY created_at ASC, id ASC
      LIMIT 1
    `).get(ingested.message.id)?.id;
    const storedClaim = harness.contextOS.database.getClaimByObservationId(storedObservationId);

    assert.equal(storedClaim?.claim_type, "relationship");
    assert.equal(storedClaim?.predicate, "depends_on");
    assert.equal(storedClaim?.value_text, "Scribe v3 uses haiku-classifier for enrich processing.");
    assert.equal(storedClaim?.actor_id, "haiku-classifier");
    assert.equal(storedClaim?.scope_kind, "project");
    assert.equal(storedClaim?.scope_id, "proj-enrich");

    const entitiesBeforeReplay = harness.contextOS.database.prepare(`
      SELECT COUNT(*) AS count
      FROM entities
    `).get().count;

    const replayResponse = await fetch(`${harness.baseUrl}/api/ingest/enrich`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(enrichPayload),
    });

    assert.equal(replayResponse.status, 200);

    const entitiesAfterReplay = harness.contextOS.database.prepare(`
      SELECT COUNT(*) AS count
      FROM entities
    `).get().count;

    assert.equal(entitiesAfterReplay, entitiesBeforeReplay);
  } finally {
    await harness.close();
  }
});

test("POST /api/ingest/enrich creates claims for each stored observation and reports the count", async () => {
  const harness = await createHarness();

  try {
    const ingested = await ingestEnrichableMessage(harness, {
      ingestId: "enrich_ingest_claims_002",
      scopeId: "proj-claims",
    });

    const enrichPayload = {
      ingestId: ingested.message.ingestId,
      source: "haiku-classifier",
      entities: [
        {
          label: "ContextOS",
          kind: "product",
          summary: "Memory system under test.",
        },
        {
          label: "Claim Pipeline",
          kind: "service",
          summary: "Projection layer for persisted observations.",
        },
      ],
      observations: [
        {
          category: "relationship",
          detail: "ContextOS uses Claim Pipeline during enrichment.",
          subjectLabel: "ContextOS",
          objectLabel: "Claim Pipeline",
          predicate: "uses",
          confidence: 0.91,
        },
        {
          category: "fact",
          detail: "ContextOS runs claim projection after observation persistence.",
          subjectLabel: "ContextOS",
          predicate: "status",
          confidence: 0.90,
        },
      ],
      graphProposals: [],
    };

    const response = await fetch(`${harness.baseUrl}/api/ingest/enrich`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(enrichPayload),
    });

    assert.equal(response.status, 200);
    const payload = await response.json();

    assert.equal(payload.observations.length, 2);
    assert.equal(payload.claimStats.created, 2);
    assert.deepEqual(payload.claimStats.errors, []);

    const counts = harness.contextOS.database.prepare(`
      SELECT
        (SELECT COUNT(*) FROM observations WHERE message_id = ?) AS observationCount,
        (SELECT COUNT(*) FROM claims WHERE message_id = ?) AS claimCount
    `).get(ingested.message.id, ingested.message.id);

    assert.equal(counts.observationCount, 2);
    assert.equal(counts.claimCount, 2);
  } finally {
    await harness.close();
  }
});

test("POST /api/ingest/enrich keeps observations when claim creation fails", async () => {
  const harness = await createHarness();
  const originalWarn = console.warn;
  const warnings = [];

  try {
    const ingested = await ingestEnrichableMessage(harness, {
      ingestId: "enrich_ingest_claim_fail_003",
      scopeId: "proj-claim-fail",
    });

    harness.contextOS.database.insertClaim = () => {
      throw new Error("claim insert failed");
    };
    console.warn = (...args) => {
      warnings.push(args);
    };

    const response = await fetch(`${harness.baseUrl}/api/ingest/enrich`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        ingestId: ingested.message.ingestId,
        source: "haiku-classifier",
        entities: [
          {
            label: "ContextOS",
            kind: "product",
            summary: "Memory system under test.",
          },
          {
            label: "Claims",
            kind: "concept",
            summary: "Structured projections of observations.",
          },
        ],
        observations: [
          {
            category: "relationship",
            detail: "ContextOS uses claims after enrich persistence.",
            subjectLabel: "ContextOS",
            objectLabel: "Claims",
            predicate: "uses",
            confidence: 0.87,
          },
        ],
        graphProposals: [],
      }),
    });

    assert.equal(response.status, 200);
    const payload = await response.json();

    assert.equal(payload.observations.length, 1);
    assert.equal(payload.claimStats.created, 0);
    assert.equal(payload.claimStats.errors.length, 1);
    assert.equal(payload.claimStats.errors[0].message, "claim insert failed");
    assert.equal(warnings.length, 1);

    const counts = harness.contextOS.database.prepare(`
      SELECT
        (SELECT COUNT(*) FROM observations WHERE message_id = ?) AS observationCount,
        (SELECT COUNT(*) FROM claims WHERE message_id = ?) AS claimCount
    `).get(ingested.message.id, ingested.message.id);

    assert.equal(counts.observationCount, 1);
    assert.equal(counts.claimCount, 0);
  } finally {
    console.warn = originalWarn;
    await harness.close();
  }
});

test("POST /api/ingest/enrich returns 404 for an unknown ingestId", async () => {
  const harness = await createHarness();

  try {
    const response = await fetch(`${harness.baseUrl}/api/ingest/enrich`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        ingestId: "missing_ingest_id",
        source: "haiku-classifier",
        entities: [],
        observations: [],
        graphProposals: [],
      }),
    });

    assert.equal(response.status, 404);
    const payload = await response.json();
    assert.equal(payload.error, "Message not found");
  } finally {
    await harness.close();
  }
});
