import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";
import { estimateTokens } from "../src/core/utils.js";
import { handleRequest } from "../src/http/router.js";
import { persistPatchForMessage } from "./test-helpers.js";

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

function seedReviewProposal(contextOS, {
  messageId,
  proposalType,
  detail,
  payload = null,
  confidence,
  status = "proposed",
  writeClass = "ai_proposed",
  createdAt,
}) {
  const stored = contextOS.database.insertGraphProposal({
    conversationId: null,
    messageId,
    actorId: "seed",
    scopeKind: "private",
    scopeId: null,
    proposalType,
    detail,
    confidence,
    status,
    payload: payload ?? { title: detail, type: proposalType },
    writeClass,
  });

  if (createdAt) {
    contextOS.database.prepare(`
      UPDATE graph_proposals
      SET created_at = ?
      WHERE id = ?
    `).run(createdAt, stored.id);
  }

  return contextOS.database.getGraphProposal(stored.id);
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
  const contextOS = new ContextOS({
    rootDir,
    autoBackfillEmbeddings: false,
    reviewManagerOptions: {
      setTimeout() {
        return {
          unref() {},
        };
      },
      clearTimeout() {},
    },
  });
  contextOS.reviewManager.start();
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
    assert.ok(payload.diagnostics.cache_status.startsWith("miss:"));
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
    assert.ok(payload.diagnostics.cache_status.startsWith("miss:"));
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
    assert.equal(payload.queue_bucket, "actionable");
    assert.equal(payload.triage, "ai_review");
    assert.equal(payload.policy_decision, "queue_ai_review");

    const stored = harness.contextOS.database.getGraphProposal(payload.proposal_id);
    assert.equal(stored.status, "proposed");
    assert.equal(stored.message_id, harness.seeded.mutationSourceMessage.id);
  } finally {
    await harness.close();
  }
});

test("POST /api/mutations/propose parks low-confidence ai proposals without auto-applying them", async () => {
  const harness = await createHarness();

  try {
    const response = await fetch(`${harness.baseUrl}/api/mutations/propose`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        type: "add_task",
        payload: {
          title: "Weak DNS monitoring suggestion",
          status: "active",
        },
        confidence: 0.3,
        source_event_id: harness.seeded.mutationSourceMessage.ingestId,
      }),
    });

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.status, "proposed");
    assert.equal(payload.write_class, "ai_proposed");
    assert.equal(payload.queue_bucket, "parked");
    assert.equal(payload.actionable, false);
    assert.equal(payload.triage, "parked_backlog");
    assert.equal(payload.queue_reason, "low_confidence_ai_proposed_parked");
    assert.equal(payload.policy_decision, "park_low_confidence_ai_proposed");

    const stored = harness.contextOS.database.getGraphProposal(payload.proposal_id);
    assert.equal(stored.status, "proposed");

    const createdTask = harness.contextOS.database.prepare(`
      SELECT title
      FROM tasks
      WHERE title = ?
      LIMIT 1
    `).get("Weak DNS monitoring suggestion");
    assert.equal(createdTask, undefined);
  } finally {
    await harness.close();
  }
});

test("POST /api/mutations/review supports deterministic queue filters and summaries", async () => {
  const harness = await createHarness();

  try {
    const parked = seedReviewProposal(harness.contextOS, {
      messageId: harness.seeded.mutationSourceMessage.id,
      proposalType: "add_task",
      detail: "Parked weak task",
      confidence: 0.31,
      status: "pending",
      writeClass: "ai_proposed",
      createdAt: "2026-03-06T13:50:35.000Z",
    });
    const oldest = seedReviewProposal(harness.contextOS, {
      messageId: harness.seeded.mutationSourceMessage.id,
      proposalType: "add_task",
      detail: "Old queued task",
      confidence: 0.61,
      status: "pending",
      writeClass: "ai_proposed",
      createdAt: "2026-03-06T13:50:36.000Z",
    });
    const middle = seedReviewProposal(harness.contextOS, {
      messageId: harness.seeded.mutationSourceMessage.id,
      proposalType: "add_decision",
      detail: "Canonical queue item",
      confidence: 0.93,
      status: "proposed",
      writeClass: "canonical",
      createdAt: "2026-03-06T13:50:37.000Z",
    });
    seedReviewProposal(harness.contextOS, {
      messageId: harness.seeded.mutationSourceMessage.id,
      proposalType: "add_constraint",
      detail: "Already reviewed item",
      confidence: 0.4,
      status: "rejected",
      writeClass: "canonical",
      createdAt: "2026-03-06T13:50:38.000Z",
    });
    const newest = seedReviewProposal(harness.contextOS, {
      messageId: harness.seeded.mutationSourceMessage.id,
      proposalType: "add_task",
      detail: "Newest queued task",
      confidence: 0.88,
      status: "proposed",
      writeClass: "ai_proposed",
      createdAt: "2026-03-06T13:50:39.000Z",
    });

    const listResponse = await fetch(`${harness.baseUrl}/api/mutations/review`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action: "list" }),
    });
    assert.equal(listResponse.status, 200);
    const listed = await listResponse.json();
    assert.equal(listed.total, 3);
    assert.equal(listed.returned, 3);
    assert.deepEqual(listed.summary.by_status, { proposed: 2, pending: 1 });
    assert.deepEqual(listed.summary.by_write_class, { ai_proposed: 2, canonical: 1 });
    assert.deepEqual(listed.summary.by_triage, { ai_review: 2, human_canonical: 1 });
    assert.deepEqual(listed.summary.by_queue_bucket, { actionable: 3 });
    assert.deepEqual(listed.summary.by_policy_decision, {
      queue_ai_review: 2,
      queue_canonical_review: 1,
    });
    assert.equal(listed.summary.queue_total, 3);
    assert.equal(listed.summary.parked_total, 1);
    assert.equal(listed.applied_filters.include_parked, false);
    assert.equal(listed.mutations[0].mutation_id, newest.id);
    assert.equal(listed.mutations[2].mutation_id, oldest.id);
    assert.ok(listed.mutations.every((mutation) => mutation.queue_bucket === "actionable"));

    const parkedResponse = await fetch(`${harness.baseUrl}/api/mutations/review`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        action: "list",
        triage: "parked_backlog",
      }),
    });
    assert.equal(parkedResponse.status, 200);
    const parkedList = await parkedResponse.json();
    assert.equal(parkedList.total, 1);
    assert.equal(parkedList.returned, 1);
    assert.equal(parkedList.applied_filters.include_parked, true);
    assert.equal(parkedList.mutations[0].mutation_id, parked.id);
    assert.equal(parkedList.mutations[0].queue_bucket, "parked");
    assert.equal(parkedList.mutations[0].policy_decision, "park_low_confidence_ai_proposed");
    assert.deepEqual(parkedList.summary.by_triage, { parked_backlog: 1 });
    assert.deepEqual(parkedList.summary.by_queue_bucket, { parked: 1 });

    const canonicalResponse = await fetch(`${harness.baseUrl}/api/mutations/review`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        action: "list",
        write_class: "canonical",
      }),
    });
    assert.equal(canonicalResponse.status, 200);
    const canonicalList = await canonicalResponse.json();
    assert.equal(canonicalList.total, 1);
    assert.equal(canonicalList.mutations[0].mutation_id, middle.id);
    assert.deepEqual(canonicalList.summary.by_write_class, { canonical: 1 });

    const decisionResponse = await fetch(`${harness.baseUrl}/api/mutations/review`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        action: "list",
        proposal_type: "add_decision",
      }),
    });
    assert.equal(decisionResponse.status, 200);
    const decisionList = await decisionResponse.json();
    assert.equal(decisionList.total, 1);
    assert.equal(decisionList.mutations[0].mutation_id, middle.id);

    const confidenceResponse = await fetch(`${harness.baseUrl}/api/mutations/review`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        action: "list",
        min_confidence: 0.9,
      }),
    });
    assert.equal(confidenceResponse.status, 200);
    const confidenceList = await confidenceResponse.json();
    assert.equal(confidenceList.total, 1);
    assert.equal(confidenceList.mutations[0].mutation_id, middle.id);

    const oldestResponse = await fetch(`${harness.baseUrl}/api/mutations/review`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        action: "list",
        sort: "oldest",
        limit: 2,
      }),
    });
    assert.equal(oldestResponse.status, 200);
    const oldestList = await oldestResponse.json();
    assert.equal(oldestList.total, 3);
    assert.equal(oldestList.returned, 2);
    assert.deepEqual(
      oldestList.mutations.map((mutation) => mutation.mutation_id),
      [oldest.id, middle.id],
    );
    assert.equal(oldestList.applied_filters.sort, "oldest");
    assert.equal(oldestList.applied_filters.limit, 2);
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


test("POST /api/mutations/review applies add_decision proposals that only provide payload.choice", async () => {
  const harness = await createHarness();

  try {
    const proposal = seedReviewProposal(harness.contextOS, {
      messageId: harness.seeded.mutationSourceMessage.id,
      proposalType: "add_decision",
      detail: null,
      payload: {
        type: "add_decision",
        entity: "DNS",
        choice: "Move DNS to Cloudflare",
        rationale: "Cloudflare simplifies cutover and monitoring.",
      },
      confidence: 0.93,
      status: "proposed",
      writeClass: "canonical",
    });

    assert.equal(proposal.detail, null);

    const applyResponse = await fetch(`${harness.baseUrl}/api/mutations/review`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        action: "apply",
        mutation_id: proposal.id,
      }),
    });
    assert.equal(applyResponse.status, 200);
    const applied = await applyResponse.json();
    assert.equal(applied.status, "accepted");

    const acceptedProposal = harness.contextOS.database.getGraphProposal(proposal.id);
    assert.equal(acceptedProposal.status, "accepted");

    const createdDecision = harness.contextOS.database.prepare(`
      SELECT d.title, d.rationale, o.detail AS observation_detail
      FROM decisions d
      JOIN observations o ON o.id = d.observation_id
      WHERE d.id = ?
      LIMIT 1
    `).get(applied.applied.decision_id);
    assert.equal(createdDecision.title, "Move DNS to Cloudflare");
    assert.equal(createdDecision.rationale, "Cloudflare simplifies cutover and monitoring.");
    assert.equal(createdDecision.observation_detail, "Move DNS to Cloudflare");
  } finally {
    await harness.close();
  }
});

test("POST /api/mutations/review applies add_constraint proposals that only provide payload.content", async () => {
  const harness = await createHarness();

  try {
    const proposal = seedReviewProposal(harness.contextOS, {
      messageId: harness.seeded.mutationSourceMessage.id,
      proposalType: "add_constraint",
      detail: null,
      payload: {
        type: "add_constraint",
        entity: "DNS",
        content: "Keep TTL above 60 seconds during migration.",
        severity: "medium",
      },
      confidence: 0.89,
      status: "proposed",
      writeClass: "canonical",
    });

    assert.equal(proposal.detail, null);

    const applyResponse = await fetch(`${harness.baseUrl}/api/mutations/review`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        action: "apply",
        mutation_id: proposal.id,
      }),
    });
    assert.equal(applyResponse.status, 200);
    const applied = await applyResponse.json();
    assert.equal(applied.status, "accepted");

    const acceptedProposal = harness.contextOS.database.getGraphProposal(proposal.id);
    assert.equal(acceptedProposal.status, "accepted");

    const createdConstraint = harness.contextOS.database.prepare(`
      SELECT c.detail, c.severity, o.detail AS observation_detail
      FROM constraints c
      JOIN observations o ON o.id = c.observation_id
      WHERE c.id = ?
      LIMIT 1
    `).get(applied.applied.constraint_id);
    assert.equal(createdConstraint.detail, "Keep TTL above 60 seconds during migration");
    assert.equal(createdConstraint.severity, "medium");
    assert.equal(createdConstraint.observation_detail, "Keep TTL above 60 seconds during migration");
  } finally {
    await harness.close();
  }
});

test("POST /api/mutations/review supports explicit-id batch apply and reject with audit fields", async () => {
  const harness = await createHarness();

  try {
    const applyFirst = await fetch(`${harness.baseUrl}/api/mutations/propose`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        type: "add_task",
        payload: {
          title: "Batch apply DNS monitoring alert",
          entity: "DNS",
          status: "active",
        },
        confidence: 0.82,
        source_event_id: harness.seeded.mutationSourceMessage.ingestId,
      }),
    }).then((response) => response.json());

    const applySecond = await fetch(`${harness.baseUrl}/api/mutations/propose`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        type: "add_fact",
        payload: {
          title: "DNS provider is Cloudflare",
          entity: "DNS",
          detail: "Cloudflare provides DNS and CDN services",
        },
        confidence: 0.88,
        source_event_id: harness.seeded.mutationSourceMessage.ingestId,
      }),
    }).then((response) => response.json());

    const applyResponse = await fetch(`${harness.baseUrl}/api/mutations/review`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        action: "apply_batch",
        mutation_ids: [applyFirst.proposal_id, applySecond.proposal_id],
        reason: "Batch approved",
      }),
    });
    assert.equal(applyResponse.status, 200);
    const applied = await applyResponse.json();
    assert.equal(applied.ok, true);
    assert.equal(applied.status, "accepted");
    assert.equal(applied.count, 2);
    assert.deepEqual(applied.mutation_ids, [applyFirst.proposal_id, applySecond.proposal_id]);
    assert.deepEqual(
      applied.results.map((result) => result.mutation_id),
      [applyFirst.proposal_id, applySecond.proposal_id],
    );
    assert.ok(applied.results.every((result) => result.reviewed_by_actor === "api"));
    assert.ok(applied.results.every((result) => result.reason === "Batch approved"));
    assert.ok(applied.mutations.every((mutation) => mutation.status === "accepted"));
    assert.ok(applied.mutations.every((mutation) => mutation.reviewed_by_actor === "api"));
    assert.ok(applied.mutations.every((mutation) => mutation.reason === "Batch approved"));

    const appliedTask = harness.contextOS.database.prepare(`
      SELECT title
      FROM tasks
      WHERE title = ?
      LIMIT 1
    `).get("Batch apply DNS monitoring alert");
    assert.ok(appliedTask, "Task was created in database");

    const rejectFirst = await fetch(`${harness.baseUrl}/api/mutations/propose`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        type: "add_task",
        payload: {
          title: "Batch reject duplicate DNS task",
          status: "active",
        },
        confidence: 0.63,
        source_event_id: harness.seeded.mutationSourceMessage.ingestId,
      }),
    }).then((response) => response.json());

    const rejectSecond = await fetch(`${harness.baseUrl}/api/mutations/propose`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        type: "add_goal",
        payload: {
          title: "Batch reject redundant DNS goal",
          detail: "Achieve DNS redundancy",
        },
        confidence: 0.58,
        source_event_id: harness.seeded.mutationSourceMessage.ingestId,
      }),
    }).then((response) => response.json());

    const rejectResponse = await fetch(`${harness.baseUrl}/api/mutations/review`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        action: "reject_batch",
        mutation_ids: [rejectFirst.proposal_id, rejectSecond.proposal_id],
        reason: "Duplicate backlog items",
      }),
    });
    assert.equal(rejectResponse.status, 200);
    const rejected = await rejectResponse.json();
    assert.equal(rejected.ok, true);
    assert.equal(rejected.status, "rejected");
    assert.equal(rejected.count, 2);
    assert.ok(rejected.results.every((result) => result.status === "rejected"));
    assert.ok(rejected.results.every((result) => result.reviewed_by_actor === "api"));
    assert.ok(rejected.results.every((result) => result.reason === "Duplicate backlog items"));
    assert.ok(rejected.mutations.every((mutation) => mutation.status === "rejected"));
    assert.ok(rejected.mutations.every((mutation) => mutation.reason === "Duplicate backlog items"));

    const rejectedFirstProposal = harness.contextOS.database.getGraphProposal(rejectFirst.proposal_id);
    const rejectedSecondProposal = harness.contextOS.database.getGraphProposal(rejectSecond.proposal_id);
    assert.equal(rejectedFirstProposal.status, "rejected");
    assert.equal(rejectedSecondProposal.status, "rejected");
    assert.equal(rejectedFirstProposal.reviewed_by_actor, "api");
    assert.equal(rejectedSecondProposal.reviewed_by_actor, "api");
    assert.equal(rejectedFirstProposal.reason, "Duplicate backlog items");
    assert.equal(rejectedSecondProposal.reason, "Duplicate backlog items");
  } finally {
    await harness.close();
  }
});

test("POST /api/mutations/review rejects unsafe explicit-id batch payloads deterministically", async () => {
  const harness = await createHarness();

  try {
    const firstProposal = await fetch(`${harness.baseUrl}/api/mutations/propose`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        type: "add_task",
        payload: {
          title: "Validation batch task one",
          status: "active",
        },
        confidence: 0.74,
        source_event_id: harness.seeded.mutationSourceMessage.ingestId,
      }),
    }).then((response) => response.json());

    const secondProposal = await fetch(`${harness.baseUrl}/api/mutations/propose`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        type: "add_task",
        payload: {
          title: "Validation batch task two",
          status: "active",
        },
        confidence: 0.71,
        source_event_id: harness.seeded.mutationSourceMessage.ingestId,
      }),
    }).then((response) => response.json());

    const assertRejected = async (body, expectedMessage) => {
      const response = await fetch(`${harness.baseUrl}/api/mutations/review`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });

      assert.equal(response.status, 400);
      const payload = await response.json();
      assert.match(payload.error, expectedMessage);
      return payload;
    };

    await assertRejected({
      action: "apply_batch",
    }, /mutation_ids must be an array/i);

    await assertRejected({
      action: "apply_batch",
      mutation_ids: [],
    }, /mutation_ids must contain at least one id/i);

    await assertRejected({
      action: "apply_batch",
      mutation_ids: [firstProposal.proposal_id, firstProposal.proposal_id],
    }, /contains duplicates/i);

    await assertRejected({
      action: "apply_batch",
      mutation_ids: [firstProposal.proposal_id, "gp_missing"],
    }, /unknown mutation_ids/i);

    const acceptedResponse = await fetch(`${harness.baseUrl}/api/mutations/review`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        action: "apply",
        mutation_id: secondProposal.proposal_id,
      }),
    });
    assert.equal(acceptedResponse.status, 200);

    await assertRejected({
      action: "reject_batch",
      mutation_ids: [secondProposal.proposal_id],
      reason: "Too late",
    }, /already reviewed mutation_ids/i);

    await assertRejected({
      action: "apply_batch",
      mutation_id: firstProposal.proposal_id,
      mutation_ids: [firstProposal.proposal_id],
    }, /ambiguous review payload/i);

    await assertRejected({
      action: "apply",
      mutation_id: firstProposal.proposal_id,
      mutation_ids: [firstProposal.proposal_id],
    }, /ambiguous review payload/i);

    const untouchedProposal = harness.contextOS.database.getGraphProposal(firstProposal.proposal_id);
    assert.equal(untouchedProposal.status, "proposed");
  } finally {
    await harness.close();
  }
});

test("GET /api/review/status and POST /api/review/trigger expose automated review state", async () => {
  const harness = await createHarness();

  try {
    const proposal = await fetch(`${harness.baseUrl}/api/mutations/propose`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        type: "add_task",
        payload: {
          title: "Review manager endpoint task",
          status: "active",
        },
        confidence: 0.79,
        source_event_id: harness.seeded.mutationSourceMessage.ingestId,
      }),
    }).then((response) => response.json());

    assert.equal(proposal.status, "proposed");

    const statusResponse = await fetch(`${harness.baseUrl}/api/review/status`);
    assert.equal(statusResponse.status, 200);
    const statusPayload = await statusResponse.json();
    assert.equal(statusPayload.started, true);
    assert.equal(statusPayload.pending_queue_total >= 1, true);
    assert.equal(statusPayload.mutations_since_last_review >= 1, true);
    assert.equal(statusPayload.review_in_progress, false);

    const triggerResponse = await fetch(`${harness.baseUrl}/api/review/trigger`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        reason: "api_manual_review",
      }),
    });
    assert.equal(triggerResponse.status, 200);
    const triggerPayload = await triggerResponse.json();
    assert.equal(triggerPayload.status, "completed");
    assert.equal(triggerPayload.trigger.reason, "api_manual_review");
    assert.equal(triggerPayload.review.action, "auto_review_policy");
    assert.equal(triggerPayload.review.reviewed_total >= 1, true);
    assert.equal(triggerPayload.review.auto_applied.count, 0);
    assert.equal(triggerPayload.review.auto_expired.count, 0);
    assert.equal(triggerPayload.review.remaining_total >= 1, true);
    assert.ok(triggerPayload.review_state.last_review_at);
    assert.equal(triggerPayload.review_state.review_in_progress, false);
    assert.equal(triggerPayload.review_state.mutations_since_last_review, 0);
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
      backfill: {
        total_observations: 4,
        not_yet_processed: 1,
        processed_with_claims: 3,
        processed_with_no_claim: 0,
        failed: 0,
        processed: 3,
        remaining: 1,
        completion_ratio: 0.75,
      },
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

test("GET /api/aggregator exposes incremental aggregation state for live observations", async () => {
  const harness = await createHarness();

  try {
    const capture = await harness.contextOS.ingestMessage({
      conversationId: harness.seeded.conversation.id,
      conversationTitle: harness.seeded.conversation.title,
      role: "user",
      direction: "inbound",
      actorId: "user:aggregator",
      content: "Alice is active, then Alice is inactive.",
      scopeKind: "project",
      scopeId: "proj-aggregator",
    });
    const storedPatch = persistPatchForMessage(harness.contextOS, capture, {
      entities: [
        { label: "Alice", kind: "person" },
      ],
      observations: [
        {
          category: "fact",
          subjectLabel: "Alice",
          detail: "Alice is active",
          confidence: 0.9,
          sourceSpan: "Alice is active",
          metadata: { tags: ["status"] },
        },
        {
          category: "fact",
          subjectLabel: "Alice",
          detail: "Alice is inactive",
          confidence: 0.86,
          sourceSpan: "Alice is inactive",
          metadata: { tags: ["status"] },
        },
      ],
      retrieveHints: [],
      graphProposals: [],
      complexityAdjustments: [],
    });

    const response = await fetch(`${harness.baseUrl}/api/aggregator`);
    assert.equal(response.status, 200);
    const payload = await response.json();

    assert.equal(payload.clusterCount, 1);
    assert.equal(payload.observationCount, 2);
    assert.equal(payload.clusters.length, 1);
    assert.deepEqual(
      payload.clusters[0].observationIds.slice().sort(),
      storedPatch.observations.map((observation) => observation.id).sort(),
    );
    assert.ok(payload.clusters[0].entities.includes("Alice"));
    assert.ok(payload.clusters[0].topics.includes("fact"));
    assert.ok(payload.clusters[0].topics.includes("status"));
  } finally {
    await harness.close();
  }
});
