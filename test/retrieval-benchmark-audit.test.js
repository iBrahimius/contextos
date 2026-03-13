import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";
import { handleRequest } from "../src/http/router.js";
import { skipUnlessEmbeddings } from "./skip-guards.js";

const FIXTURE_PATH = new URL("./fixtures/retrieval-audit-golden.json", import.meta.url);
const scenarios = JSON.parse(await fs.readFile(FIXTURE_PATH, "utf8"));

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-retrieval-audit-"));
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

  return {
    rootDir,
    contextOS,
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
    conversationTitle: overrides.conversationTitle ?? "Retrieval Benchmark Audit",
    role: overrides.role ?? "user",
    direction: overrides.direction ?? "inbound",
    actorId: overrides.actorId ?? "user:test",
    originKind: overrides.originKind ?? null,
    ingestId: overrides.ingestId ?? null,
    content: overrides.content ?? "retrieval audit test message",
    scopeKind: overrides.scopeKind ?? "project",
    scopeId: overrides.scopeId ?? "proj-retrieval-audit",
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
  const detail = overrides.detail ?? "retrieval benchmark audit observation";
  const confidence = overrides.confidence ?? 0.9;
  const scopeKind = overrides.scopeKind ?? context.scopeKind ?? "project";
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

function insertClaimFixture(harness, context, overrides = {}) {
  const claimType = overrides.claimType ?? "fact";
  const predicate = overrides.predicate ?? "status";
  const timestamp = overrides.createdAt ?? "2026-03-09T18:00:00.000Z";
  const observation = insertObservationRecord(harness, context, {
    actorId: overrides.actorId ?? context.actorId ?? "user:test",
    category: claimType,
    predicate,
    subjectEntityId: overrides.subjectEntityId ?? null,
    objectEntityId: overrides.objectEntityId ?? null,
    detail: overrides.detail ?? overrides.valueText ?? `${claimType} claim`,
    confidence: overrides.confidence ?? 0.9,
    scopeKind: overrides.scopeKind ?? context.scopeKind ?? "project",
    scopeId: overrides.scopeId ?? context.scopeId ?? null,
    createdAt: timestamp,
    metadata: overrides.observationMetadata ?? null,
  });

  return harness.contextOS.database.insertClaim({
    observation_id: observation.id,
    conversation_id: context.conversationId,
    message_id: context.messageId,
    actor_id: overrides.actorId ?? context.actorId ?? "user:test",
    claim_type: claimType,
    subject_entity_id: overrides.subjectEntityId ?? null,
    predicate,
    object_entity_id: overrides.objectEntityId ?? null,
    value_text: overrides.valueText ?? `${claimType} claim`,
    confidence: overrides.confidence ?? 0.9,
    source_type: overrides.sourceType ?? "explicit",
    lifecycle_state: overrides.lifecycleState ?? "active",
    valid_from: overrides.validFrom ?? timestamp,
    valid_to: overrides.validTo ?? null,
    resolution_key: overrides.resolutionKey ?? `${claimType}:${overrides.subjectEntityId ?? "missing"}:${predicate}`,
    facet_key: overrides.facetKey ?? predicate,
    supersedes_claim_id: overrides.supersedesClaimId ?? null,
    superseded_by_claim_id: overrides.supersededByClaimId ?? null,
    scope_kind: overrides.scopeKind ?? context.scopeKind ?? "project",
    scope_id: overrides.scopeId ?? context.scopeId ?? null,
    metadata_json: overrides.metadataJson ?? null,
    created_at: timestamp,
    updated_at: overrides.updatedAt ?? timestamp,
  });
}

async function seedAuditDataset(harness) {
  const scopeKind = "project";
  const scopeId = "proj-retrieval-audit";

  const contextOS = harness.contextOS;
  const contextOsEntity = contextOS.database.insertEntity({ label: "ContextOS", kind: "project" });
  const dnsEntity = contextOS.database.insertEntity({ label: "DNS", kind: "component" });

  const ownerMessage = await createMessage(harness, {
    ingestId: "retrieval_audit_owner_001",
    content: "Who owns ContextOS? ContextOS is owned by Ibrahim.",
    scopeKind,
    scopeId,
  });
  insertClaimFixture(harness, buildMessageContext(ownerMessage), {
    claimType: "fact",
    predicate: "owner",
    subjectEntityId: contextOsEntity.id,
    valueText: "Ibrahim",
    detail: "ContextOS is owned by Ibrahim.",
    confidence: 0.96,
    scopeKind,
    scopeId,
    createdAt: "2026-03-09T18:00:00.000Z",
    metadataJson: JSON.stringify({ title: "ContextOS ownership" }),
  });

  const dnsOldMessage = await createMessage(harness, {
    ingestId: "retrieval_audit_dns_001",
    content: "What's our DNS provider? DNS provider is Cloudflare.",
    scopeKind,
    scopeId,
  });
  const oldClaim = insertClaimFixture(harness, buildMessageContext(dnsOldMessage), {
    claimType: "decision",
    predicate: "dns_provider",
    subjectEntityId: dnsEntity.id,
    valueText: "Cloudflare",
    detail: "DNS provider is Cloudflare.",
    confidence: 0.92,
    lifecycleState: "superseded",
    scopeKind,
    scopeId,
    createdAt: "2026-03-09T18:05:00.000Z",
    metadataJson: JSON.stringify({ title: "Use Cloudflare for DNS" }),
  });

  const dnsNewMessage = await createMessage(harness, {
    ingestId: "retrieval_audit_dns_002",
    content: "What's our DNS provider now? Switch DNS provider to Route53.",
    scopeKind,
    scopeId,
  });
  const newClaim = insertClaimFixture(harness, buildMessageContext(dnsNewMessage), {
    claimType: "decision",
    predicate: "dns_provider",
    subjectEntityId: dnsEntity.id,
    valueText: "Route53",
    detail: "Switch DNS provider to Route53.",
    confidence: 0.94,
    lifecycleState: "active",
    resolutionKey: `decision:${dnsEntity.id}:dns_provider`,
    facetKey: "dns_provider",
    supersedesClaimId: oldClaim.id,
    scopeKind,
    scopeId,
    createdAt: "2026-03-09T18:10:00.000Z",
    metadataJson: JSON.stringify({ title: "Use Route53 for DNS" }),
  });
  harness.contextOS.database.updateClaim(oldClaim.id, {
    lifecycleState: "superseded",
    supersededByClaimId: newClaim.id,
  });

  const taskMessage = await createMessage(harness, {
    ingestId: "retrieval_audit_task_001",
    content: "What should I work on next? Ship retrieval coverage audit.",
    scopeKind,
    scopeId,
  });
  insertClaimFixture(harness, buildMessageContext(taskMessage), {
    claimType: "task",
    predicate: "workflow",
    subjectEntityId: contextOsEntity.id,
    valueText: "Ship retrieval coverage audit",
    detail: "Ship retrieval coverage audit.",
    confidence: 0.91,
    lifecycleState: "active",
    scopeKind,
    scopeId,
    createdAt: "2026-03-09T18:12:00.000Z",
    metadataJson: JSON.stringify({ title: "Ship retrieval coverage audit", priority: "high" }),
  });

  const constraintMessage = await createMessage(harness, {
    ingestId: "retrieval_audit_constraint_001",
    content: "What constraints should I keep in mind? Keep API compatibility.",
    scopeKind,
    scopeId,
  });
  insertClaimFixture(harness, buildMessageContext(constraintMessage), {
    claimType: "constraint",
    predicate: "guardrail",
    subjectEntityId: contextOsEntity.id,
    valueText: "Keep API compatibility",
    detail: "Keep API compatibility.",
    confidence: 0.93,
    lifecycleState: "active",
    scopeKind,
    scopeId,
    createdAt: "2026-03-09T18:14:00.000Z",
    metadataJson: JSON.stringify({ severity: "high" }),
  });
}

test("retrieval benchmark audit fixtures validate claims-aware and lifecycle-sensitive packet assembly", { skip: skipUnlessEmbeddings }, async () => {
  const harness = await createHarness();

  try {
    await seedAuditDataset(harness);

    for (const scenario of scenarios) {
      const packet = await harness.contextOS.contextPacket({
        query: scenario.query,
        scopeFilter: { scopeKind: "project", scopeId: "proj-retrieval-audit" },
        tokenBudget: 1800,
      });

      assert.equal(packet.intent, scenario.expectedIntent, `${scenario.id} intent mismatch`);
      assert.ok(packet.diagnostics.claims_scanned > 0, `${scenario.id} should scan claims`);

      if (scenario.expectedFocusEntity) {
        assert.ok(
          packet.working_set.focus_entities.some((entity) => entity.label === scenario.expectedFocusEntity),
          `${scenario.id} should focus entity ${scenario.expectedFocusEntity}`,
        );
      }

      if (scenario.expectedClaimValue) {
        assert.ok(
          packet.working_set.focus_claims.some((claim) => String(claim.value ?? "").includes(scenario.expectedClaimValue)),
          `${scenario.id} should surface claim value ${scenario.expectedClaimValue}`,
        );
      }

      if (scenario.unexpectedClaimValue) {
        assert.ok(
          !packet.working_set.focus_claims.some((claim) => String(claim.value ?? "").includes(scenario.unexpectedClaimValue)),
          `${scenario.id} should not surface superseded value ${scenario.unexpectedClaimValue}`,
        );
      }

      if (scenario.expectedTaskTitle) {
        assert.ok(
          packet.active_state.tasks.some((task) => String(task.title ?? "").includes(scenario.expectedTaskTitle)),
          `${scenario.id} should retain task ${scenario.expectedTaskTitle}`,
        );
      }

      if (scenario.expectedConstraintValue) {
        assert.ok(
          packet.stable_prefix.hard_constraints.some((constraint) => String(constraint.value ?? "").includes(scenario.expectedConstraintValue)),
          `${scenario.id} should retain constraint ${scenario.expectedConstraintValue}`,
        );
      }

      assert.ok(
        packet.evidence.structured.length + packet.evidence.messages.length > 0,
        `${scenario.id} should keep retrieval evidence populated`,
      );
    }
  } finally {
    await harness.close();
  }
});

test("retrieval benchmark audit prefers the active claim over its superseded predecessor", async () => {
  const harness = await createHarness();

  try {
    await seedAuditDataset(harness);

    const packet = await harness.contextOS.contextPacket({
      query: "What's our DNS provider now?",
      scopeFilter: { scopeKind: "project", scopeId: "proj-retrieval-audit" },
      tokenBudget: 1800,
    });

    const decisionTitles = packet.active_state.decisions.map((decision) => decision.title ?? decision.status ?? "");
    const claimValues = packet.working_set.focus_claims.map((claim) => String(claim.value ?? ""));

    assert.ok(decisionTitles.some((title) => title.includes("Route53")), "active state should keep the current DNS decision");
    assert.ok(!decisionTitles.some((title) => title.includes("Cloudflare")), "active state should exclude superseded DNS decisions");
    assert.ok(claimValues.some((value) => value.includes("Route53")), "working set should keep the active DNS claim");
    assert.ok(!claimValues.some((value) => value.includes("Cloudflare")), "working set should exclude the superseded DNS claim");
  } finally {
    await harness.close();
  }
});
