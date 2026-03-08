import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";
import { estimateTokens } from "../src/core/utils.js";
import { handleRequest } from "../src/http/router.js";

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-api-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  return root;
}

function insertSeedMessage(contextOS, conversationId, {
  ingestId,
  role,
  direction,
  actorId,
  originKind = "import",
  content,
}) {
  return contextOS.database.insertMessage({
    conversationId,
    role,
    direction,
    actorId,
    originKind,
    content,
    tokenCount: estimateTokens(content),
    raw: { seeded: true },
    ingestId,
  });
}

function seedApiData(contextOS) {
  const conversation = contextOS.database.createConversation("API Endpoints");
  const beforeMessage = insertSeedMessage(contextOS, conversation.id, {
    ingestId: "event_before",
    role: "user",
    direction: "inbound",
    actorId: "user:seed",
    content: "We need to settle the DNS migration plan today.",
  });
  const decisionMessage = insertSeedMessage(contextOS, conversation.id, {
    ingestId: "cortex_e_20260305_121405_001",
    role: "assistant",
    direction: "outbound",
    actorId: "assistant:seed",
    content: "We decided to move DNS to Cloudflare for the edge network.",
  });
  const taskMessage = insertSeedMessage(contextOS, conversation.id, {
    ingestId: "event_after",
    role: "assistant",
    direction: "outbound",
    actorId: "assistant:seed",
    content: "Next task is updating the DNS cutover checklist and monitoring plan.",
  });
  const constraintMessage = insertSeedMessage(contextOS, conversation.id, {
    ingestId: "event_constraint",
    role: "user",
    direction: "inbound",
    actorId: "user:seed",
    content: "Constraint: keep TTL above 60 seconds during migration.",
  });
  const mutationSourceMessage = insertSeedMessage(contextOS, conversation.id, {
    ingestId: "e_20260306_135035_001",
    role: "user",
    direction: "inbound",
    actorId: "user:seed",
    content: "Please add a follow-up task for DNS monitoring.",
  });

  const orderedMessages = [
    [beforeMessage, "2026-03-05T12:14:00.000Z"],
    [decisionMessage, "2026-03-05T12:14:05.000Z"],
    [taskMessage, "2026-03-05T12:14:10.000Z"],
    [constraintMessage, "2026-03-05T12:14:15.000Z"],
    [mutationSourceMessage, "2026-03-06T13:50:35.000Z"],
  ];
  for (const [message, capturedAt] of orderedMessages) {
    contextOS.database.prepare(`
      UPDATE messages
      SET captured_at = ?
      WHERE id = ?
    `).run(capturedAt, message.id);
    message.capturedAt = capturedAt;
  }

  const dns = contextOS.graph.ensureEntity({ label: "DNS", kind: "component" });
  const cloudflare = contextOS.graph.ensureEntity({ label: "Cloudflare", kind: "component" });

  const relationship = contextOS.graph.connect({
    subjectEntityId: cloudflare.id,
    predicate: "related_to",
    objectEntityId: dns.id,
    weight: 0.9,
    provenanceMessageId: decisionMessage.id,
    metadata: { seeded: true },
  });

  const relationshipObservation = contextOS.database.insertObservation({
    conversationId: conversation.id,
    messageId: decisionMessage.id,
    actorId: "seed",
    category: "relationship",
    predicate: "related_to",
    subjectEntityId: cloudflare.id,
    objectEntityId: dns.id,
    detail: "Cloudflare is the DNS provider for the edge network.",
    confidence: 0.92,
    sourceSpan: "Cloudflare is the DNS provider for the edge network.",
    metadata: { tags: ["infra"] },
  });
  contextOS.graph.updateGraphVersion(relationshipObservation.graphVersion);

  const decisionObservation = contextOS.database.insertObservation({
    conversationId: conversation.id,
    messageId: decisionMessage.id,
    actorId: "seed",
    category: "decision",
    subjectEntityId: dns.id,
    detail: "Move DNS to Cloudflare for the edge network.",
    confidence: 0.95,
    sourceSpan: "Move DNS to Cloudflare for the edge network.",
    metadata: { tags: ["infra"], rationale: "Better edge routing" },
  });
  contextOS.graph.updateGraphVersion(decisionObservation.graphVersion);
  const decisionId = contextOS.database.insertDecision({
    observationId: decisionObservation.id,
    entityId: dns.id,
    title: "Move DNS to Cloudflare",
    rationale: "Better edge routing",
  });

  const taskObservation = contextOS.database.insertObservation({
    conversationId: conversation.id,
    messageId: taskMessage.id,
    actorId: "seed",
    category: "task",
    subjectEntityId: dns.id,
    detail: "Update the DNS cutover checklist.",
    confidence: 0.9,
    sourceSpan: "Update the DNS cutover checklist.",
    metadata: { tags: ["infra"], priority: "high" },
  });
  contextOS.graph.updateGraphVersion(taskObservation.graphVersion);
  const taskId = contextOS.database.insertTask({
    observationId: taskObservation.id,
    entityId: dns.id,
    title: "Update the DNS cutover checklist.",
    status: "open",
    priority: "high",
  });

  const constraintObservation = contextOS.database.insertObservation({
    conversationId: conversation.id,
    messageId: constraintMessage.id,
    actorId: "seed",
    category: "constraint",
    subjectEntityId: dns.id,
    detail: "Keep TTL above 60 seconds during migration.",
    confidence: 0.88,
    sourceSpan: "Keep TTL above 60 seconds during migration.",
    metadata: { tags: ["infra"], severity: "medium" },
  });
  contextOS.graph.updateGraphVersion(constraintObservation.graphVersion);
  const constraintId = contextOS.database.insertConstraint({
    observationId: constraintObservation.id,
    entityId: dns.id,
    detail: "Keep TTL above 60 seconds during migration.",
    severity: "medium",
  });

  const decisionClaim = contextOS.database.insertClaim({
    observation_id: decisionObservation.id,
    conversation_id: conversation.id,
    message_id: decisionMessage.id,
    actor_id: "seed",
    claim_type: "decision",
    subject_entity_id: dns.id,
    predicate: "decision",
    value_text: "accepted",
    confidence: 0.95,
    source_type: "explicit",
    lifecycle_state: "active",
    resolution_key: `decision:${dns.id}:decision`,
    facet_key: "decision",
  });

  const taskClaim = contextOS.database.insertClaim({
    observation_id: taskObservation.id,
    conversation_id: conversation.id,
    message_id: taskMessage.id,
    actor_id: "seed",
    claim_type: "task",
    subject_entity_id: dns.id,
    predicate: "task",
    value_text: "blocked",
    confidence: 0.9,
    source_type: "explicit",
    lifecycle_state: "disputed",
    resolution_key: `task:${dns.id}:task`,
    facet_key: "task",
  });

  const constraintClaim = contextOS.database.insertClaim({
    observation_id: constraintObservation.id,
    conversation_id: conversation.id,
    message_id: constraintMessage.id,
    actor_id: "seed",
    claim_type: "constraint",
    subject_entity_id: dns.id,
    predicate: "constraint",
    value_text: "active",
    confidence: 0.88,
    source_type: "explicit",
    lifecycle_state: "superseded",
    resolution_key: `constraint:${dns.id}:constraint`,
    facet_key: "constraint",
  });

  return {
    conversation,
    beforeMessage,
    decisionMessage,
    taskMessage,
    constraintMessage,
    mutationSourceMessage,
    dns,
    cloudflare,
    relationship,
    decisionId,
    taskId,
    constraintId,
    decisionClaim,
    taskClaim,
    constraintClaim,
  };
}

async function createHarness() {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const seeded = seedApiData(contextOS);
  const server = http.createServer((request, response) => handleRequest(contextOS, rootDir, request, response));

  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();

  return {
    rootDir,
    contextOS,
    seeded,
    baseUrl: `http://127.0.0.1:${port}`,
    async close() {
      await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
      contextOS.database.close();
      contextOS.telemetry.close();
      await fs.rm(rootDir, { recursive: true, force: true });
    },
  };
}

test("POST /api/recall returns token-budgeted evidence", async () => {
  const harness = await createHarness();

  try {
    const response = await fetch(`${harness.baseUrl}/api/recall`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        query: "what did we decide about DNS?",
        scope: "decisions",
        token_budget: 12,
      }),
    });

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.ok(payload.total_results >= 1);
    assert.ok(payload.token_count <= 12);
    assert.equal(payload.evidence[0].event_id, harness.seeded.decisionMessage.ingestId);
    assert.match(payload.evidence[0].content, /move dns to cloudflare/i);
  } finally {
    await harness.close();
  }
});

test("POST /api/recall/context-window returns surrounding messages", async () => {
  const harness = await createHarness();

  try {
    const response = await fetch(`${harness.baseUrl}/api/recall/context-window`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        event_id: harness.seeded.decisionMessage.ingestId,
        before: 1,
        after: 1,
      }),
    });

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.center.event_id, harness.seeded.decisionMessage.ingestId);
    assert.equal(payload.before.length, 1);
    assert.equal(payload.after.length, 1);
    assert.equal(payload.before[0].event_id, harness.seeded.beforeMessage.ingestId);
    assert.equal(payload.after[0].event_id, harness.seeded.taskMessage.ingestId);
  } finally {
    await harness.close();
  }
});

test("GET /api/registries/open-items lists active tasks, decisions, and constraints", async () => {
  const harness = await createHarness();

  try {
    const response = await fetch(`${harness.baseUrl}/api/registries/open-items?kind=all`);

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.tasks[0].status, "active");
    assert.equal(payload.decisions[0].status, "active");
    assert.equal(payload.constraints[0].status, "active");
    assert.match(payload.tasks[0].title, /dns cutover checklist/i);
  } finally {
    await harness.close();
  }
});

test("POST /api/registries/query filters registry entries", async () => {
  const harness = await createHarness();

  try {
    const response = await fetch(`${harness.baseUrl}/api/registries/query`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        name: "tasks",
        query: "DNS",
        filters: {
          status: "active",
          tags: ["infra"],
        },
      }),
    });

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.total, 1);
    assert.equal(payload.results[0].status, "active");
    assert.match(payload.results[0].title, /dns cutover checklist/i);
  } finally {
    await harness.close();
  }
});

test("GET /api/entities/:name supports label lookup with relationships and recent events", async () => {
  const harness = await createHarness();

  try {
    const response = await fetch(`${harness.baseUrl}/api/entities/${encodeURIComponent("Cloudflare")}?include_recent_events=true`);

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.entity.label, "Cloudflare");
    assert.ok(payload.entity.relationships.some((relationship) => relationship.target === "DNS"));
    assert.ok(payload.recent_events.some((event) => event.event_id === harness.seeded.decisionMessage.ingestId));
  } finally {
    await harness.close();
  }
});

test("POST /api/mutations/propose creates a proposed graph proposal", async () => {
  const harness = await createHarness();

  try {
    const response = await fetch(`${harness.baseUrl}/api/mutations/propose`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        type: "add_task",
        payload: {
          title: "Add DNS monitoring alert",
          status: "active",
          tags: ["infra"],
        },
        confidence: 0.8,
        source_event_id: harness.seeded.mutationSourceMessage.ingestId,
      }),
    });

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.status, "proposed");

    const stored = harness.contextOS.database.getGraphProposal(payload.proposal_id);
    assert.equal(stored.status, "proposed");
    assert.equal(stored.message_id, harness.seeded.mutationSourceMessage.id);
  } finally {
    await harness.close();
  }
});

test("POST /api/mutations/review lists, applies, and rejects proposals", async () => {
  const harness = await createHarness();

  try {
    const firstProposal = await fetch(`${harness.baseUrl}/api/mutations/propose`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        type: "add_task",
        payload: {
          title: "Audit DNS failover runbook",
          entity: "DNS",
          status: "active",
          priority: "high",
          tags: ["infra"],
        },
        confidence: 0.86,
        source_event_id: harness.seeded.mutationSourceMessage.ingestId,
      }),
    }).then((response) => response.json());

    const secondProposal = await fetch(`${harness.baseUrl}/api/mutations/propose`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        type: "add_task",
        payload: {
          title: "Archive the old DNS worksheet",
          status: "active",
        },
        confidence: 0.7,
        source_event_id: harness.seeded.mutationSourceMessage.ingestId,
      }),
    }).then((response) => response.json());

    const listResponse = await fetch(`${harness.baseUrl}/api/mutations/review`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action: "list" }),
    });
    assert.equal(listResponse.status, 200);
    const listed = await listResponse.json();
    assert.ok(listed.mutations.some((mutation) => mutation.mutation_id === firstProposal.proposal_id));

    const applyResponse = await fetch(`${harness.baseUrl}/api/mutations/review`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        action: "apply",
        mutation_id: firstProposal.proposal_id,
      }),
    });
    assert.equal(applyResponse.status, 200);
    const applied = await applyResponse.json();
    assert.equal(applied.status, "accepted");

    const rejectResponse = await fetch(`${harness.baseUrl}/api/mutations/review`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        action: "reject",
        mutation_id: secondProposal.proposal_id,
        reason: "No longer needed",
      }),
    });
    assert.equal(rejectResponse.status, 200);
    const rejected = await rejectResponse.json();
    assert.equal(rejected.status, "rejected");

    const acceptedProposal = harness.contextOS.database.getGraphProposal(firstProposal.proposal_id);
    const rejectedProposal = harness.contextOS.database.getGraphProposal(secondProposal.proposal_id);
    assert.equal(acceptedProposal.status, "accepted");
    assert.equal(rejectedProposal.status, "rejected");

    const createdTask = harness.contextOS.database.prepare(`
      SELECT title
      FROM tasks
      WHERE title = ?
      LIMIT 1
    `).get("Audit DNS failover runbook");
    assert.ok(createdTask);
  } finally {
    await harness.close();
  }
});

test("GET /api/status and /api/health expose registry and observation counts", async () => {
  const harness = await createHarness();

  try {
    const statusResponse = await fetch(`${harness.baseUrl}/api/status`);
    assert.equal(statusResponse.status, 200);
    const statusPayload = await statusResponse.json();
    assert.equal(statusPayload.cortex_up, true);
    assert.equal(statusPayload.registries.tasks.active, 1);
    assert.equal(statusPayload.registries.decisions.total, 1);
    assert.equal(statusPayload.registries.constraints.total, 1);
    assert.equal(statusPayload.observation_categories.decision, 1);
    assert.deepEqual(statusPayload.claims, {
      total: 3,
      by_state: {
        candidate: 0,
        active: 1,
        superseded: 1,
        disputed: 1,
        archived: 0,
      },
      by_type: {
        fact: 0,
        decision: 1,
        task: 1,
        constraint: 1,
        preference: 0,
        goal: 0,
        habit: 0,
        rule: 0,
        event: 0,
        state_change: 0,
        relationship: 0,
      },
      coverage_ratio: 0.75,
      disputed_count: 1,
    });

    const healthResponse = await fetch(`${harness.baseUrl}/api/health`);
    assert.equal(healthResponse.status, 200);
    const healthPayload = await healthResponse.json();
    assert.equal(healthPayload.status, "ok");
    assert.equal(healthPayload.observation_categories.task, 1);
    assert.equal(healthPayload.registries.constraints.total, 1);

    const dashboardResponse = await fetch(`${harness.baseUrl}/api/dashboard`);
    assert.equal(dashboardResponse.status, 200);
    const dashboardPayload = await dashboardResponse.json();
    assert.deepEqual(dashboardPayload.claims, statusPayload.claims);
  } finally {
    await harness.close();
  }
});
