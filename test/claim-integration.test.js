import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";

import { ensureClaimForObservation } from "../src/core/claim-resolution.js";
import { getValidTransitions } from "../src/core/claim-types.js";
import { ContextOS } from "../src/core/context-os.js";
import { handleRequest } from "../src/http/router.js";
import { persistPatchForMessage } from "./test-helpers.js";

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-claim-integration-"));
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

async function createMessage(harness, overrides = {}) {
  return harness.contextOS.ingestMessage({
    conversationId: overrides.conversationId ?? null,
    conversationTitle: overrides.conversationTitle ?? "Claim Integration",
    role: overrides.role ?? "user",
    direction: overrides.direction ?? "inbound",
    actorId: overrides.actorId ?? "user:test",
    originKind: overrides.originKind ?? null,
    ingestId: overrides.ingestId ?? null,
    content: overrides.content ?? "claim integration test message",
    scopeKind: overrides.scopeKind ?? "project",
    scopeId: overrides.scopeId ?? "proj-claim-integration",
  });
}

function buildMessageContext(capture) {
  return {
    conversationId: capture.conversationId,
    messageId: capture.message.id,
    actorId: capture.message.actorId,
    scopeKind: capture.message.scopeKind,
    scopeId: capture.message.scopeId,
  };
}

function insertObservationRecord(harness, context, overrides = {}) {
  const conversationId = overrides.conversationId ?? context.conversationId;
  const messageId = overrides.messageId ?? context.messageId;
  const actorId = overrides.actorId ?? context.actorId ?? "user:test";
  const category = overrides.category ?? "fact";
  const predicate = overrides.predicate ?? "status";
  const subjectEntityId = overrides.subjectEntityId ?? null;
  const objectEntityId = overrides.objectEntityId ?? null;
  const detail = overrides.detail ?? "claim integration observation";
  const confidence = overrides.confidence ?? 0.9;
  const scopeKind = overrides.scopeKind ?? context.scopeKind ?? "private";
  const scopeId = overrides.scopeId ?? context.scopeId ?? null;
  const createdAt = overrides.createdAt ?? null;
  const inserted = harness.contextOS.database.insertObservation({
    conversationId,
    messageId,
    actorId,
    category,
    predicate,
    subjectEntityId,
    objectEntityId,
    detail,
    confidence,
    sourceSpan: overrides.sourceSpan ?? detail,
    metadata: overrides.metadata ?? null,
    scopeKind,
    scopeId,
  });

  return {
    id: inserted.id,
    conversation_id: conversationId,
    message_id: messageId,
    actor_id: actorId,
    category,
    predicate,
    subject_entity_id: subjectEntityId,
    object_entity_id: objectEntityId,
    detail,
    confidence,
    scope_kind: scopeKind,
    scope_id: scopeId,
    created_at: createdAt ?? inserted.createdAt,
  };
}

function ensureClaimRecord(harness, context, overrides = {}) {
  const observation = insertObservationRecord(harness, context, overrides);
  const claim = ensureClaimForObservation(harness.contextOS.database, observation);
  return { observation, claim };
}

function insertClaimFixture(harness, context, overrides = {}) {
  const claimType = overrides.claim_type ?? "fact";
  const predicate = overrides.predicate ?? "status";
  const timestamp = overrides.created_at ?? overrides.createdAt ?? "2026-03-07T10:00:00.000Z";
  const observation = insertObservationRecord(harness, context, {
    actorId: overrides.actor_id ?? context.actorId ?? "user:test",
    category: claimType,
    predicate,
    subjectEntityId: overrides.subject_entity_id ?? null,
    objectEntityId: overrides.object_entity_id ?? null,
    detail: overrides.detail ?? overrides.value_text ?? `${claimType} claim`,
    confidence: overrides.confidence ?? 0.9,
    scopeKind: overrides.scope_kind ?? context.scopeKind ?? "private",
    scopeId: overrides.scope_id ?? context.scopeId ?? null,
    createdAt: timestamp,
  });

  return harness.contextOS.database.insertClaim({
    observation_id: observation.id,
    conversation_id: context.conversationId,
    message_id: context.messageId,
    actor_id: overrides.actor_id ?? context.actorId ?? "user:test",
    claim_type: claimType,
    subject_entity_id: overrides.subject_entity_id ?? null,
    predicate,
    object_entity_id: overrides.object_entity_id ?? null,
    value_text: overrides.value_text ?? `${claimType} claim`,
    confidence: overrides.confidence ?? 0.9,
    source_type: overrides.source_type ?? "explicit",
    lifecycle_state: overrides.lifecycle_state ?? "active",
    valid_from: overrides.valid_from ?? timestamp,
    valid_to: overrides.valid_to ?? null,
    resolution_key: overrides.resolution_key ?? `${claimType}:${overrides.subject_entity_id ?? "missing"}:${predicate}`,
    facet_key: overrides.facet_key ?? predicate,
    supersedes_claim_id: overrides.supersedes_claim_id ?? null,
    superseded_by_claim_id: overrides.superseded_by_claim_id ?? null,
    scope_kind: overrides.scope_kind ?? context.scopeKind ?? "private",
    scope_id: overrides.scope_id ?? context.scopeId ?? null,
    created_at: timestamp,
    updated_at: overrides.updated_at ?? overrides.updatedAt ?? timestamp,
  });
}

test("full pipeline creates claims from a persisted patch and exposes them in registries", async () => {
  const harness = await createHarness();

  try {
    const capture = await createMessage(harness, {
      ingestId: "claim_pipeline_001",
      content: "Use Cloudflare for DNS and update the cutover checklist.",
      scopeId: "proj-claim-pipeline",
    });

    const persisted = persistPatchForMessage(harness.contextOS, capture, {
      entities: [
        { label: "DNS", kind: "component" },
        { label: "Cloudflare", kind: "service" },
      ],
      observations: [
        {
          category: "decision",
          detail: "Use Cloudflare for DNS.",
          subjectLabel: "DNS",
          predicate: "dns_provider",
          confidence: 0.95,
          metadata: { rationale: "Edge routing" },
        },
        {
          category: "task",
          detail: "Update the DNS cutover checklist.",
          subjectLabel: "DNS",
          predicate: "task",
          confidence: 0.9,
          metadata: { priority: "high" },
        },
      ],
      graphProposals: [],
      retrieveHints: [],
      complexityAdjustments: [],
    }, {
      actorId: "user:test",
      scopeKind: "project",
      scopeId: "proj-claim-pipeline",
    });

    assert.equal(persisted.observations.length, 2);
    assert.equal(persisted.claimStats.created, 2);
    assert.deepEqual(persisted.claimStats.errors, []);

    const dns = harness.contextOS.database.findEntityByName("DNS");
    assert.ok(dns);

    const storedClaims = harness.contextOS.database.prepare(`
      SELECT claim_type, lifecycle_state
      FROM claims
      WHERE message_id = ?
      ORDER BY claim_type ASC
    `).all(capture.message.id);

    assert.deepEqual(storedClaims.map((claim) => claim.claim_type), ["decision", "task"]);
    assert.ok(storedClaims.every((claim) => claim.lifecycle_state === "active"));

    const decisionsResponse = await fetch(`${harness.baseUrl}/api/registries/decisions?entity_id=${dns.id}`);
    assert.equal(decisionsResponse.status, 200);
    const decisionsPayload = await decisionsResponse.json();
    assert.equal(decisionsPayload.decisions.length, 1);
    assert.equal(decisionsPayload.decisions[0].entityId, dns.id);
    assert.equal(decisionsPayload.decisions[0].claims.length, 1);
    assert.equal(decisionsPayload.decisions[0].claims[0].claimType, "decision");
    assert.match(decisionsPayload.decisions[0].claims[0].status, /cloudflare/i);

    const tasksResponse = await fetch(`${harness.baseUrl}/api/registries/tasks?entity_id=${dns.id}`);
    assert.equal(tasksResponse.status, 200);
    const tasksPayload = await tasksResponse.json();
    assert.equal(tasksPayload.tasks.length, 1);
    assert.equal(tasksPayload.tasks[0].entityId, dns.id);
    assert.equal(tasksPayload.tasks[0].claims.length, 1);
    assert.equal(tasksPayload.tasks[0].claims[0].claimType, "task");
    assert.match(tasksPayload.tasks[0].claims[0].status, /cutover checklist/i);
  } finally {
    await harness.close();
  }
});

test("contradictory claims supersede the older claim and registries return only the active winner", async () => {
  const harness = await createHarness();

  try {
    const capture = await createMessage(harness, {
      ingestId: "claim_supersession_001",
      content: "We are changing the DNS provider.",
      scopeId: "proj-claim-supersession",
    });
    const context = buildMessageContext(capture);
    const dns = harness.contextOS.database.insertEntity({ label: "DNS", kind: "component" });
    const cloudflare = harness.contextOS.database.insertEntity({ label: "Cloudflare", kind: "service" });
    const route53 = harness.contextOS.database.insertEntity({ label: "Route53", kind: "service" });

    const first = ensureClaimRecord(harness, context, {
      actorId: "user:test",
      category: "decision",
      predicate: "dns_provider",
      subjectEntityId: dns.id,
      objectEntityId: cloudflare.id,
      detail: "Use Cloudflare for DNS.",
      confidence: 0.9,
      scopeKind: "project",
      scopeId: "proj-claim-supersession",
      createdAt: "2026-03-07T10:00:00.000Z",
    });
    const second = ensureClaimRecord(harness, context, {
      actorId: "user:test",
      category: "decision",
      predicate: "dns_provider",
      subjectEntityId: dns.id,
      objectEntityId: route53.id,
      detail: "Switch to Route53 for DNS.",
      confidence: 0.9,
      scopeKind: "project",
      scopeId: "proj-claim-supersession",
      createdAt: "2026-03-07T10:05:00.000Z",
    });

    const supersededClaim = harness.contextOS.database.getClaim(first.claim.id);
    const activeClaim = harness.contextOS.database.getClaim(second.claim.id);

    assert.equal(supersededClaim?.lifecycle_state, "superseded");
    assert.equal(activeClaim?.lifecycle_state, "active");
    assert.equal(activeClaim?.supersedes_claim_id, supersededClaim?.id);
    assert.equal(supersededClaim?.superseded_by_claim_id, activeClaim?.id);

    const registryResponse = await fetch(`${harness.baseUrl}/api/registries/decisions?entity_id=${dns.id}`);
    assert.equal(registryResponse.status, 200);
    const registryPayload = await registryResponse.json();

    assert.equal(registryPayload.decisions.length, 1);
    assert.equal(registryPayload.decisions[0].claims.length, 1);
    assert.equal(registryPayload.decisions[0].claims[0].claimId, activeClaim?.id);
    assert.match(registryPayload.decisions[0].claims[0].status, /route53/i);
  } finally {
    await harness.close();
  }
});

test("task claims transition through the API state machine and reject invalid rollbacks", async () => {
  const harness = await createHarness();

  try {
    const capture = await createMessage(harness, {
      ingestId: "claim_transition_001",
      content: "Track the cutover checklist task state.",
      scopeId: "proj-claim-transition",
    });
    const context = buildMessageContext(capture);
    const checklist = harness.contextOS.database.insertEntity({ label: "DNS Cutover Checklist", kind: "task" });
    const taskClaim = insertClaimFixture(harness, context, {
      claim_type: "task",
      subject_entity_id: checklist.id,
      predicate: "workflow",
      value_text: "pending",
      lifecycle_state: "active",
      scope_kind: "project",
      scope_id: "proj-claim-transition",
    });

    assert.deepEqual(getValidTransitions(taskClaim.claim_type, taskClaim.value_text), ["active", "cancelled"]);

    const activateResponse = await fetch(`${harness.baseUrl}/api/claims/${taskClaim.id}/transition`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ to_state: "active" }),
    });
    assert.equal(activateResponse.status, 200);
    const activatedPayload = await activateResponse.json();
    assert.equal(activatedPayload.claim.value_text, "active");
    assert.deepEqual(
      getValidTransitions(activatedPayload.claim.claim_type, activatedPayload.claim.value_text),
      ["blocked", "done", "cancelled"],
    );

    const doneResponse = await fetch(`${harness.baseUrl}/api/claims/${taskClaim.id}/transition`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ to_state: "done" }),
    });
    assert.equal(doneResponse.status, 200);
    const donePayload = await doneResponse.json();
    assert.equal(donePayload.claim.value_text, "done");
    assert.deepEqual(getValidTransitions(donePayload.claim.claim_type, donePayload.claim.value_text), []);

    const invalidResponse = await fetch(`${harness.baseUrl}/api/claims/${taskClaim.id}/transition`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ to_state: "pending" }),
    });
    assert.equal(invalidResponse.status, 400);
    const invalidPayload = await invalidResponse.json();
    assert.equal(invalidPayload.error, "Invalid transition");
    assert.deepEqual(invalidPayload.valid_transitions, []);

    const refreshed = harness.contextOS.database.getClaim(taskClaim.id);
    assert.equal(refreshed?.value_text, "done");
  } finally {
    await harness.close();
  }
});

test("equal-strength conflicting claims become disputed and surface through the disputed claims API", async () => {
  const harness = await createHarness();

  try {
    const capture = await createMessage(harness, {
      ingestId: "claim_disputed_001",
      content: "We have conflicting statements about the DNS provider.",
      scopeId: "proj-claim-disputed",
    });
    const context = buildMessageContext(capture);
    const dns = harness.contextOS.database.insertEntity({ label: "DNS", kind: "component" });
    const cloudflare = harness.contextOS.database.insertEntity({ label: "Cloudflare", kind: "service" });
    const route53 = harness.contextOS.database.insertEntity({ label: "Route53", kind: "service" });

    const first = ensureClaimRecord(harness, context, {
      actorId: "assistant:test",
      category: "fact",
      predicate: "dns_provider",
      subjectEntityId: dns.id,
      objectEntityId: cloudflare.id,
      detail: "DNS provider is Cloudflare.",
      confidence: 0.8,
      scopeKind: "project",
      scopeId: "proj-claim-disputed",
      createdAt: "2026-03-07T11:00:00.000Z",
    });
    const second = ensureClaimRecord(harness, context, {
      actorId: "assistant:test",
      category: "fact",
      predicate: "dns_provider",
      subjectEntityId: dns.id,
      objectEntityId: route53.id,
      detail: "DNS provider is Route53.",
      confidence: 0.8,
      scopeKind: "project",
      scopeId: "proj-claim-disputed",
      createdAt: "2026-03-07T11:00:00.000Z",
    });

    const firstClaim = harness.contextOS.database.getClaim(first.claim.id);
    const secondClaim = harness.contextOS.database.getClaim(second.claim.id);

    assert.equal(firstClaim?.lifecycle_state, "disputed");
    assert.equal(secondClaim?.lifecycle_state, "disputed");

    const disputedResponse = await fetch(`${harness.baseUrl}/api/claims/disputed?limit=10`);
    assert.equal(disputedResponse.status, 200);
    const disputedPayload = await disputedResponse.json();
    const disputedIds = disputedPayload.claims.map((claim) => claim.id).sort();

    assert.deepEqual(disputedIds, [first.claim.id, second.claim.id].sort());
  } finally {
    await harness.close();
  }
});

test("registry endpoints stay consistent across task, decision, goal, and rule claim snapshots", async () => {
  const harness = await createHarness();

  try {
    const capture = await createMessage(harness, {
      ingestId: "claim_registry_001",
      content: "Populate all registries with active claims.",
      scopeId: "proj-claim-registries",
    });
    const context = buildMessageContext(capture);
    const taskEntity = harness.contextOS.database.insertEntity({ label: "Launch Checklist", kind: "task" });
    const decisionEntity = harness.contextOS.database.insertEntity({ label: "DNS Migration", kind: "project" });
    const goalEntity = harness.contextOS.database.insertEntity({ label: "Ship ContextOS v2.1", kind: "goal" });
    const ruleEntity = harness.contextOS.database.insertEntity({ label: "Production Guardrail", kind: "rule" });

    const taskClaim = insertClaimFixture(harness, context, {
      claim_type: "task",
      subject_entity_id: taskEntity.id,
      predicate: "workflow",
      value_text: "active",
      scope_kind: "project",
      scope_id: "proj-claim-registries",
    });
    const decisionClaim = insertClaimFixture(harness, context, {
      claim_type: "decision",
      subject_entity_id: decisionEntity.id,
      predicate: "architecture",
      value_text: "accepted",
      scope_kind: "project",
      scope_id: "proj-claim-registries",
    });
    const goalClaim = insertClaimFixture(harness, context, {
      claim_type: "goal",
      subject_entity_id: goalEntity.id,
      predicate: "milestone",
      value_text: "active",
      scope_kind: "project",
      scope_id: "proj-claim-registries",
    });
    const ruleClaim = insertClaimFixture(harness, context, {
      claim_type: "rule",
      subject_entity_id: ruleEntity.id,
      predicate: "policy",
      value_text: "active",
      scope_kind: "project",
      scope_id: "proj-claim-registries",
    });

    const tasksResponse = await fetch(`${harness.baseUrl}/api/registries/tasks?entity_id=${taskEntity.id}`);
    assert.equal(tasksResponse.status, 200);
    const tasksPayload = await tasksResponse.json();
    assert.equal(tasksPayload.tasks.length, 1);
    assert.equal(tasksPayload.tasks[0].claims[0].claimId, taskClaim.id);

    const decisionsResponse = await fetch(`${harness.baseUrl}/api/registries/decisions?entity_id=${decisionEntity.id}`);
    assert.equal(decisionsResponse.status, 200);
    const decisionsPayload = await decisionsResponse.json();
    assert.equal(decisionsPayload.decisions.length, 1);
    assert.equal(decisionsPayload.decisions[0].claims[0].claimId, decisionClaim.id);

    const goalsResponse = await fetch(`${harness.baseUrl}/api/registries/goals?entity_id=${goalEntity.id}`);
    assert.equal(goalsResponse.status, 200);
    const goalsPayload = await goalsResponse.json();
    assert.equal(goalsPayload.goals.length, 1);
    assert.equal(goalsPayload.goals[0].claims[0].claimId, goalClaim.id);

    const rulesResponse = await fetch(`${harness.baseUrl}/api/registries/rules?entity_id=${ruleEntity.id}`);
    assert.equal(rulesResponse.status, 200);
    const rulesPayload = await rulesResponse.json();
    assert.equal(rulesPayload.rules.length, 1);
    assert.equal(rulesPayload.rules[0].claims[0].claimId, ruleClaim.id);
    assert.equal(rulesPayload.rules[0].claims[0].claimType, "rule");
  } finally {
    await harness.close();
  }
});

test("GET /api/status includes aggregated claim metrics", async () => {
  const harness = await createHarness();

  try {
    const capture = await createMessage(harness, {
      ingestId: "claim_status_001",
      content: "Status endpoint should report claim totals.",
      scopeId: "proj-claim-status",
    });
    const context = buildMessageContext(capture);
    const taskEntity = harness.contextOS.database.insertEntity({ label: "Status Task", kind: "task" });
    const decisionEntity = harness.contextOS.database.insertEntity({ label: "Status Decision", kind: "project" });
    const ruleEntity = harness.contextOS.database.insertEntity({ label: "Status Rule", kind: "rule" });

    insertClaimFixture(harness, context, {
      claim_type: "task",
      subject_entity_id: taskEntity.id,
      predicate: "workflow",
      value_text: "active",
      scope_kind: "project",
      scope_id: "proj-claim-status",
    });
    insertClaimFixture(harness, context, {
      claim_type: "decision",
      subject_entity_id: decisionEntity.id,
      predicate: "architecture",
      value_text: "accepted",
      scope_kind: "project",
      scope_id: "proj-claim-status",
    });
    insertClaimFixture(harness, context, {
      claim_type: "rule",
      subject_entity_id: ruleEntity.id,
      predicate: "policy",
      value_text: "active",
      scope_kind: "project",
      scope_id: "proj-claim-status",
    });

    const response = await fetch(`${harness.baseUrl}/api/status`);
    assert.equal(response.status, 200);
    const payload = await response.json();

    assert.deepEqual(payload.claims, {
      total: 3,
      by_state: {
        candidate: 0,
        active: 3,
        superseded: 0,
        disputed: 0,
        archived: 0,
      },
      by_type: {
        fact: 0,
        decision: 1,
        task: 1,
        constraint: 0,
        preference: 0,
        goal: 0,
        habit: 0,
        rule: 1,
        event: 0,
        state_change: 0,
        relationship: 0,
      },
      coverage_ratio: 1,
      disputed_count: 0,
    });
  } finally {
    await harness.close();
  }
});

test("GET /api/claims filters by type and paginates results", async () => {
  const harness = await createHarness();

  try {
    const capture = await createMessage(harness, {
      ingestId: "claim_list_001",
      content: "List claims with pagination.",
      scopeId: "proj-claim-list",
    });
    const context = buildMessageContext(capture);
    const taskEntity = harness.contextOS.database.insertEntity({ label: "Paginated Task", kind: "task" });
    const decisionEntity = harness.contextOS.database.insertEntity({ label: "Paginated Decision", kind: "project" });

    const olderTask = insertClaimFixture(harness, context, {
      claim_type: "task",
      subject_entity_id: taskEntity.id,
      predicate: "workflow",
      value_text: "pending",
      scope_kind: "project",
      scope_id: "proj-claim-list",
      created_at: "2026-03-07T12:00:00.000Z",
    });
    insertClaimFixture(harness, context, {
      claim_type: "decision",
      subject_entity_id: decisionEntity.id,
      predicate: "architecture",
      value_text: "accepted",
      scope_kind: "project",
      scope_id: "proj-claim-list",
      created_at: "2026-03-07T12:01:00.000Z",
    });
    const newerTask = insertClaimFixture(harness, context, {
      claim_type: "task",
      subject_entity_id: taskEntity.id,
      predicate: "follow_up",
      value_text: "active",
      scope_kind: "project",
      scope_id: "proj-claim-list",
      created_at: "2026-03-07T12:02:00.000Z",
    });

    const firstPageResponse = await fetch(`${harness.baseUrl}/api/claims?types=task&limit=1&offset=0`);
    assert.equal(firstPageResponse.status, 200);
    const firstPage = await firstPageResponse.json();

    assert.equal(firstPage.count, 2);
    assert.equal(firstPage.claims.length, 1);
    assert.equal(firstPage.claims[0].id, newerTask.id);
    assert.equal(firstPage.claims[0].claim_type, "task");

    const secondPageResponse = await fetch(`${harness.baseUrl}/api/claims?types=task&limit=1&offset=1`);
    assert.equal(secondPageResponse.status, 200);
    const secondPage = await secondPageResponse.json();

    assert.equal(secondPage.count, 2);
    assert.equal(secondPage.claims.length, 1);
    assert.equal(secondPage.claims[0].id, olderTask.id);
    assert.equal(secondPage.claims[0].claim_type, "task");
  } finally {
    await harness.close();
  }
});

test("GET /api/claims/:id returns a claim and 404s for missing IDs", async () => {
  const harness = await createHarness();

  try {
    const capture = await createMessage(harness, {
      ingestId: "claim_get_001",
      content: "Fetch a single claim.",
      scopeId: "proj-claim-get",
    });
    const context = buildMessageContext(capture);
    const entity = harness.contextOS.database.insertEntity({ label: "Single Claim Entity", kind: "concept" });
    const claim = insertClaimFixture(harness, context, {
      claim_type: "fact",
      subject_entity_id: entity.id,
      predicate: "status",
      value_text: "active",
      scope_kind: "project",
      scope_id: "proj-claim-get",
    });

    const existingResponse = await fetch(`${harness.baseUrl}/api/claims/${claim.id}`);
    assert.equal(existingResponse.status, 200);
    const existingPayload = await existingResponse.json();
    assert.equal(existingPayload.claim.id, claim.id);
    assert.equal(existingPayload.claim.subject_entity_id, entity.id);

    const missingResponse = await fetch(`${harness.baseUrl}/api/claims/claim_missing`);
    assert.equal(missingResponse.status, 404);
    const missingPayload = await missingResponse.json();
    assert.equal(missingPayload.error, "Claim not found");
  } finally {
    await harness.close();
  }
});

test("GET /api/claims/stats returns the claim type by lifecycle matrix", async () => {
  const harness = await createHarness();

  try {
    const capture = await createMessage(harness, {
      ingestId: "claim_stats_001",
      content: "Aggregate claim stats.",
      scopeId: "proj-claim-stats",
    });
    const context = buildMessageContext(capture);
    const taskEntity = harness.contextOS.database.insertEntity({ label: "Stats Task", kind: "task" });
    const decisionEntity = harness.contextOS.database.insertEntity({ label: "Stats Decision", kind: "project" });
    const goalEntity = harness.contextOS.database.insertEntity({ label: "Stats Goal", kind: "goal" });

    insertClaimFixture(harness, context, {
      claim_type: "task",
      subject_entity_id: taskEntity.id,
      predicate: "workflow",
      value_text: "active",
      lifecycle_state: "active",
      scope_kind: "project",
      scope_id: "proj-claim-stats",
    });
    insertClaimFixture(harness, context, {
      claim_type: "task",
      subject_entity_id: taskEntity.id,
      predicate: "follow_up",
      value_text: "blocked",
      lifecycle_state: "disputed",
      scope_kind: "project",
      scope_id: "proj-claim-stats",
    });
    insertClaimFixture(harness, context, {
      claim_type: "decision",
      subject_entity_id: decisionEntity.id,
      predicate: "architecture",
      value_text: "accepted",
      lifecycle_state: "superseded",
      scope_kind: "project",
      scope_id: "proj-claim-stats",
    });
    insertClaimFixture(harness, context, {
      claim_type: "goal",
      subject_entity_id: goalEntity.id,
      predicate: "milestone",
      value_text: "active",
      lifecycle_state: "active",
      scope_kind: "project",
      scope_id: "proj-claim-stats",
    });

    const response = await fetch(`${harness.baseUrl}/api/claims/stats`);
    assert.equal(response.status, 200);
    const payload = await response.json();

    assert.deepEqual(payload.stats, {
      decision: { superseded: 1 },
      goal: { active: 1 },
      task: { active: 1, disputed: 1 },
    });
    assert.deepEqual(payload.totals, {
      superseded: 1,
      active: 2,
      disputed: 1,
    });
  } finally {
    await harness.close();
  }
});

test("POST /api/claims/backfill returns the Task 6.1 stub response", async () => {
  const harness = await createHarness();

  try {
    const response = await fetch(`${harness.baseUrl}/api/claims/backfill`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ dry_run: true }),
    });

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.deepEqual(payload, {
      status: "not_implemented",
      message: "Backfill available in Task 6.1",
      graph_version: 0,
    });
  } finally {
    await harness.close();
  }
});

test("POST /api/memory/consolidate returns the v2.3 stub response", async () => {
  const harness = await createHarness();

  try {
    const response = await fetch(`${harness.baseUrl}/api/memory/consolidate`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ mode: "full" }),
    });

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.deepEqual(payload, {
      status: "not_implemented",
      message: "Consolidation available in v2.3",
      graph_version: 0,
    });
  } finally {
    await harness.close();
  }
});
