import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";
import {
  classifyRetrievalRoute,
  parseTemporalWindow,
  planRetrievalRoute,
} from "../src/core/retrieval.js";

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-routing-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  await fs.mkdir(path.join(root, "docs"), { recursive: true });
  return root;
}

async function createHarness() {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({
    rootDir,
    autoBackfillEmbeddings: false,
  });

  return {
    rootDir,
    contextOS,
    async close() {
      await contextOS.close();
      await fs.rm(rootDir, { recursive: true, force: true });
    },
  };
}

async function insertMessage(contextOS, conversation, {
  ingestId,
  role = "assistant",
  direction = "outbound",
  actorId = "assistant:test",
  content,
  scopeKind = "project",
  scopeId = "proj-routing",
}) {
  return contextOS.ingestMessage({
    conversationId: conversation.id,
    conversationTitle: conversation.title,
    role,
    direction,
    actorId,
    ingestId,
    content,
    scopeKind,
    scopeId,
  });
}

function updateMessageTimestamp(contextOS, messageId, capturedAt) {
  contextOS.database.prepare(`
    UPDATE messages
    SET captured_at = ?
    WHERE id = ?
  `).run(capturedAt, messageId);
}

function updateObservationTimestamp(contextOS, observationId, createdAt) {
  contextOS.database.prepare(`
    UPDATE observations
    SET created_at = ?
    WHERE id = ?
  `).run(createdAt, observationId);
}

test("route planner applies v2.6 precedence and emits stable diagnostics fields", () => {
  assert.equal(classifyRetrievalRoute("What DNS provider do we use?"), "current_state");
  assert.equal(classifyRetrievalRoute("Why did we retire modeld?"), "why_explanatory");
  assert.equal(classifyRetrievalRoute("What happened last week?"), "history_temporal");
  assert.equal(classifyRetrievalRoute("Summarize open questions"), "general");

  const mixedTemporalWhy = planRetrievalRoute("Why did we retire modeld last week?", {
    seedEntities: [{ id: "entity-modeld", label: "modeld" }],
  });
  assert.equal(mixedTemporalWhy.route, "why_explanatory");
  assert.equal(mixedTemporalWhy.routeReason, "temporal_explanatory_constraint");
  assert.equal(mixedTemporalWhy.temporalParseStatus, "parsed");
  assert.equal(mixedTemporalWhy.queryFeatures.has_temporal_phrase, true);
  assert.equal(mixedTemporalWhy.queryFeatures.has_explanatory_phrase, true);
  assert.equal(mixedTemporalWhy.queryFeatures.resolved_entities, 1);

  const fallbackPlan = planRetrievalRoute("Summarize open questions");
  assert.equal(fallbackPlan.route, "general");
  assert.equal(fallbackPlan.fallbackUsed, true);
  assert.equal(fallbackPlan.fallbackReason, "no_specialized_route");
});

test("temporal parser normalizes windows in Europe/Amsterdam and supports explicit ranges", () => {
  const yesterday = parseTemporalWindow("What changed yesterday?", {
    now: new Date("2026-03-10T00:30:00.000Z"),
    timezone: "Europe/Amsterdam",
  });
  assert.equal(yesterday?.parseStatus, "parsed");
  assert.equal(yesterday?.timezone, "Europe/Amsterdam");
  assert.equal(yesterday?.startAt, "2026-03-08T23:00:00.000Z");
  assert.equal(yesterday?.endAt, "2026-03-09T22:59:59.999Z");

  const lastWeek = parseTemporalWindow("What happened last week?", {
    now: new Date("2026-03-10T12:00:00.000Z"),
    timezone: "Europe/Amsterdam",
  });
  assert.equal(lastWeek?.startAt, "2026-03-01T23:00:00.000Z");
  assert.equal(lastWeek?.endAt, "2026-03-08T22:59:59.999Z");

  const explicitRange = parseTemporalWindow("What changed from 2026-03-01 to 2026-03-03?", {
    timezone: "Europe/Amsterdam",
  });
  assert.equal(explicitRange?.startAt, "2026-02-28T23:00:00.000Z");
  assert.equal(explicitRange?.endAt, "2026-03-03T22:59:59.999Z");
  assert.equal(explicitRange?.expression, "2026-03-01 to 2026-03-03");
});

test("current_state route prioritizes claim-backed answers and emits required source diagnostics", async () => {
  const harness = await createHarness();

  try {
    const conversation = harness.contextOS.database.createConversation("Current State Route");
    const dns = harness.contextOS.graph.ensureEntity({ label: "DNS", kind: "component" });
    const cloudflare = harness.contextOS.graph.ensureEntity({ label: "Cloudflare", kind: "vendor" });

    const capture = await insertMessage(harness.contextOS, conversation, {
      ingestId: "dns_memory_001",
      content: "We use Cloudflare as the DNS provider.",
    });
    const observation = harness.contextOS.database.insertObservation({
      conversationId: conversation.id,
      messageId: capture.message.id,
      actorId: "assistant:test",
      category: "fact",
      subjectEntityId: dns.id,
      objectEntityId: cloudflare.id,
      detail: "Cloudflare is the DNS provider.",
      confidence: 0.96,
      scopeKind: "project",
      scopeId: "proj-routing",
    });
    harness.contextOS.database.insertClaim({
      observationId: observation.id,
      conversationId: conversation.id,
      claimType: "fact",
      subjectEntityId: dns.id,
      objectEntityId: cloudflare.id,
      valueText: "Cloudflare",
      confidence: 0.96,
      lifecycleState: "active",
      importanceScore: 0.9,
      scopeKind: "project",
      scopeId: "proj-routing",
    });

    const documentId = harness.contextOS.database.upsertDocument({
      filePath: "benchmarks/dns-provider-diagnostic.json",
      checksum: "checksum-routing-artifact",
      metadata: { origin: "benchmark" },
    });
    const chunkId = harness.contextOS.database.insertDocumentChunk({
      documentId,
      ordinal: 0,
      heading: "DNS Benchmark",
      content: "benchmark diagnostic fixture: DNS provider expected Cloudflare in retrieval audit",
      tokenCount: 16,
      scopeKind: "project",
      scopeId: "proj-routing",
    });
    harness.contextOS.database.linkChunkEntity({
      chunkId,
      entityId: dns.id,
      score: 0.95,
    });

    const result = await harness.contextOS.retrieve({
      conversationId: conversation.id,
      queryText: "What DNS provider do we use?",
      scopeFilter: { scopeKind: "project", scopeId: "proj-routing" },
    });

    assert.equal(result.diagnostics.route, "current_state");
    assert.equal(result.diagnostics.route_reason, "current_truth_intent");
    assert.ok(result.diagnostics.route_confidence >= 0.8);
    assert.ok(result.diagnostics.candidate_source_counts.claims >= 1);
    assert.ok(result.items.slice(0, 3).some((item) => /Cloudflare/i.test(item.summary ?? "")));
    assert.ok(
      result.items.slice(0, 3).every((item) => !/benchmark diagnostic/i.test(item.payload?.content ?? "")),
      "meta artifact should not displace the answer-bearing claim in current_state",
    );
  } finally {
    await harness.close();
  }
});

test("why_explanatory route surfaces rationale-bearing evidence and preserves temporal modifiers", async () => {
  const harness = await createHarness();

  try {
    const conversation = harness.contextOS.database.createConversation("Why Route");
    const dns = harness.contextOS.graph.ensureEntity({ label: "DNS", kind: "component" });

    const capture = await insertMessage(harness.contextOS, conversation, {
      ingestId: "dns_decision_001",
      content: "We chose Cloudflare because it simplified DNS and registrar operations.",
    });
    const observation = harness.contextOS.database.insertObservation({
      conversationId: conversation.id,
      messageId: capture.message.id,
      actorId: "assistant:test",
      category: "decision",
      subjectEntityId: dns.id,
      detail: "Cloudflare simplified DNS and registrar operations.",
      confidence: 0.92,
      scopeKind: "project",
      scopeId: "proj-routing",
    });
    harness.contextOS.database.insertDecision({
      observationId: observation.id,
      entityId: dns.id,
      title: "Choose Cloudflare for DNS",
      rationale: "Because it simplified DNS and registrar operations.",
      scopeKind: "project",
      scopeId: "proj-routing",
    });

    const result = await harness.contextOS.retrieve({
      conversationId: conversation.id,
      queryText: "Why did we choose Cloudflare for DNS?",
      scopeFilter: { scopeKind: "project", scopeId: "proj-routing" },
    });

    assert.equal(result.diagnostics.route, "why_explanatory");
    assert.equal(result.diagnostics.temporal_parse_status, "not_applicable");
    assert.ok(result.diagnostics.candidate_source_counts.events >= 1);
    assert.match(result.items[0]?.summary ?? "", /because|simplified/i);
  } finally {
    await harness.close();
  }
});

test("history_temporal filters to in-range evidence, excludes untimestamped primaries, and orders chronologically", async () => {
  const harness = await createHarness();

  try {
    const conversation = harness.contextOS.database.createConversation("Temporal Route");
    const scribe = harness.contextOS.graph.ensureEntity({ label: "Scribe", kind: "system" });

    const firstCapture = await insertMessage(harness.contextOS, conversation, {
      ingestId: "scribe_change_001",
      content: "Scribe added route-aware retrieval fixtures.",
    });
    updateMessageTimestamp(harness.contextOS, firstCapture.message.id, "2026-03-09T09:00:00.000Z");
    const firstObservation = harness.contextOS.database.insertObservation({
      conversationId: conversation.id,
      messageId: firstCapture.message.id,
      actorId: "assistant:test",
      category: "fact",
      subjectEntityId: scribe.id,
      detail: "Scribe added route-aware retrieval fixtures.",
      confidence: 0.93,
      scopeKind: "project",
      scopeId: "proj-routing",
    });
    updateObservationTimestamp(harness.contextOS, firstObservation.id, "2026-03-09T09:00:00.000Z");

    const secondCapture = await insertMessage(harness.contextOS, conversation, {
      ingestId: "scribe_change_002",
      content: "Scribe normalized temporal diagnostics.",
    });
    updateMessageTimestamp(harness.contextOS, secondCapture.message.id, "2026-03-09T17:00:00.000Z");
    const secondObservation = harness.contextOS.database.insertObservation({
      conversationId: conversation.id,
      messageId: secondCapture.message.id,
      actorId: "assistant:test",
      category: "fact",
      subjectEntityId: scribe.id,
      detail: "Scribe normalized temporal diagnostics.",
      confidence: 0.94,
      scopeKind: "project",
      scopeId: "proj-routing",
    });
    updateObservationTimestamp(harness.contextOS, secondObservation.id, "2026-03-09T17:00:00.000Z");

    const olderCapture = await insertMessage(harness.contextOS, conversation, {
      ingestId: "scribe_change_older",
      content: "Scribe improved duplicate handling.",
    });
    updateMessageTimestamp(harness.contextOS, olderCapture.message.id, "2026-03-01T10:00:00.000Z");
    const olderObservation = harness.contextOS.database.insertObservation({
      conversationId: conversation.id,
      messageId: olderCapture.message.id,
      actorId: "assistant:test",
      category: "fact",
      subjectEntityId: scribe.id,
      detail: "Scribe improved duplicate handling.",
      confidence: 0.9,
      scopeKind: "project",
      scopeId: "proj-routing",
    });
    updateObservationTimestamp(harness.contextOS, olderObservation.id, "2026-03-01T10:00:00.000Z");

    const untimestampedDocumentId = harness.contextOS.database.upsertDocument({
      filePath: "docs/scribe-retrospective.md",
      checksum: "checksum-scribe-retrospective",
      metadata: { origin: "notes" },
    });
    const untimestampedChunkId = harness.contextOS.database.insertDocumentChunk({
      documentId: untimestampedDocumentId,
      ordinal: 0,
      heading: "Retrospective",
      content: "Scribe has an untimestamped retrospective note.",
      tokenCount: 8,
      scopeKind: "project",
      scopeId: "proj-routing",
    });
    harness.contextOS.database.linkChunkEntity({
      chunkId: untimestampedChunkId,
      entityId: scribe.id,
      score: 0.8,
    });
    const result = await harness.contextOS.retrieve({
      conversationId: conversation.id,
      queryText: "What changed with Scribe on 2026-03-09?",
      scopeFilter: { scopeKind: "project", scopeId: "proj-routing" },
    });

    assert.equal(result.diagnostics.route, "history_temporal");
    assert.equal(result.diagnostics.temporal_parse_status, "parsed");
    assert.equal(result.diagnostics.out_of_range_support_count, 0);
    assert.ok(result.diagnostics.excluded_untimestamped_count >= 1);
    assert.equal(result.diagnostics.noInRangeEvidence, false);
    const topSummaries = result.items.slice(0, 5).map((item) => item.summary ?? "");
    assert.ok(topSummaries.some((summary) => /route-aware retrieval fixtures/i.test(summary)));
    assert.ok(topSummaries.some((summary) => /normalized temporal diagnostics/i.test(summary)));
    const topTimestamps = result.items
      .slice(0, 5)
      .map((item) => item.payload?.created_at ?? item.payload?.captured_at ?? null)
      .filter(Boolean);
    assert.deepEqual(topTimestamps.slice().sort(), topTimestamps, "temporal evidence should be ordered chronologically");
    assert.ok(
      result.items.slice(0, 5).every((item) => !/duplicate handling|untimestamped retrospective/i.test(item.summary ?? "")),
      "only in-range timestamped evidence should participate in primary temporal ranking",
    );
  } finally {
    await harness.close();
  }
});

test("history_temporal point lookups prefer decision-bearing evidence over earlier discussion", async () => {
  const harness = await createHarness();

  try {
    const conversation = harness.contextOS.database.createConversation("Temporal Point Lookup");
    const phase = harness.contextOS.graph.ensureEntity({ label: "Phase 3", kind: "milestone" });

    const earlyCapture = await insertMessage(harness.contextOS, conversation, {
      ingestId: "phase3_discussion_001",
      content: "Let\'s treat Phase 3 as a later milestone for now.",
    });
    updateMessageTimestamp(harness.contextOS, earlyCapture.message.id, "2026-03-01T09:00:00.000Z");
    const earlyObservation = harness.contextOS.database.insertObservation({
      conversationId: conversation.id,
      messageId: earlyCapture.message.id,
      actorId: "assistant:test",
      category: "fact",
      subjectEntityId: phase.id,
      detail: "Phase 3 was treated as a later milestone.",
      confidence: 0.82,
      scopeKind: "project",
      scopeId: "proj-routing",
    });
    updateObservationTimestamp(harness.contextOS, earlyObservation.id, "2026-03-01T09:00:00.000Z");

    const decisionCapture = await insertMessage(harness.contextOS, conversation, {
      ingestId: "phase3_decision_001",
      content: "Decision made on 2026-03-09: Start Phase 3 now.",
    });
    updateMessageTimestamp(harness.contextOS, decisionCapture.message.id, "2026-03-09T14:00:00.000Z");
    const decisionObservation = harness.contextOS.database.insertObservation({
      conversationId: conversation.id,
      messageId: decisionCapture.message.id,
      actorId: "assistant:test",
      category: "decision",
      subjectEntityId: phase.id,
      detail: "Start Phase 3.",
      confidence: 0.96,
      scopeKind: "project",
      scopeId: "proj-routing",
    });
    updateObservationTimestamp(harness.contextOS, decisionObservation.id, "2026-03-09T14:00:00.000Z");
    harness.contextOS.database.insertClaim({
      observationId: decisionObservation.id,
      conversationId: conversation.id,
      claimType: "decision",
      subjectEntityId: phase.id,
      valueText: "Start Phase 3",
      confidence: 0.96,
      lifecycleState: "active",
      importanceScore: 0.95,
      scopeKind: "project",
      scopeId: "proj-routing",
    });
    harness.contextOS.database.insertDecision({
      observationId: decisionObservation.id,
      entityId: phase.id,
      title: "Start Phase 3",
      rationale: "Retrieval routing stabilized.",
      scopeKind: "project",
      scopeId: "proj-routing",
    });

    const result = await harness.contextOS.retrieve({
      conversationId: conversation.id,
      queryText: "When did we decide to start Phase 3?",
      scopeFilter: { scopeKind: "project", scopeId: "proj-routing" },
    });

    assert.equal(result.diagnostics.route, "history_temporal");
    assert.equal(result.diagnostics.temporal_parse_status, "parsed");
    const topSummaries = result.items.slice(0, 5).map((item) => item.summary ?? "");
    const decisionIndex = topSummaries.findIndex((summary) => /start phase 3|2026-03-09/i.test(summary));
    const noiseIndex = topSummaries.findIndex((summary) => /later milestone/i.test(summary));
    assert.ok(decisionIndex >= 0, "decision-bearing evidence should surface in the top 5");
    assert.ok(noiseIndex >= 0, "earlier discussion should still remain available as supporting history");
    assert.ok(decisionIndex < noiseIndex, "point lookup should rank the dated decision ahead of earlier discussion");
    assert.ok(
      topSummaries.slice(0, 3).some((summary) => /start phase 3|2026-03-09/i.test(summary)),
      "top results should include the decision-bearing answer",
    );
    assert.ok(
      topSummaries.slice(0, 3).every((summary) => !/later milestone/i.test(summary)),
      "earlier milestone chatter should not outrank the decision in the top 3",
    );
  } finally {
    await harness.close();
  }
});

test("history_temporal marks explicit fallback when the parsed window is empty", async () => {
  const harness = await createHarness();

  try {
    const conversation = harness.contextOS.database.createConversation("Temporal Empty Window");
    const scribe = harness.contextOS.graph.ensureEntity({ label: "Scribe", kind: "system" });

    const olderCapture = await insertMessage(harness.contextOS, conversation, {
      ingestId: "scribe_change_support",
      content: "Scribe improved duplicate handling.",
    });
    updateMessageTimestamp(harness.contextOS, olderCapture.message.id, "2026-03-01T10:00:00.000Z");
    const olderObservation = harness.contextOS.database.insertObservation({
      conversationId: conversation.id,
      messageId: olderCapture.message.id,
      actorId: "assistant:test",
      category: "fact",
      subjectEntityId: scribe.id,
      detail: "Scribe improved duplicate handling.",
      confidence: 0.9,
      scopeKind: "project",
      scopeId: "proj-routing",
    });
    updateObservationTimestamp(harness.contextOS, olderObservation.id, "2026-03-01T10:00:00.000Z");

    const untimestampedDocumentId = harness.contextOS.database.upsertDocument({
      filePath: "docs/scribe-retrospective.md",
      checksum: "checksum-scribe-retrospective-support",
      metadata: { origin: "notes" },
    });
    const untimestampedChunkId = harness.contextOS.database.insertDocumentChunk({
      documentId: untimestampedDocumentId,
      ordinal: 0,
      heading: "Retrospective",
      content: "Scribe has an untimestamped retrospective note.",
      tokenCount: 8,
      scopeKind: "project",
      scopeId: "proj-routing",
    });
    harness.contextOS.database.linkChunkEntity({
      chunkId: untimestampedChunkId,
      entityId: scribe.id,
      score: 0.8,
    });
    const result = await harness.contextOS.retrieve({
      conversationId: conversation.id,
      queryText: "What changed with Scribe on 2026-03-09?",
      scopeFilter: { scopeKind: "project", scopeId: "proj-routing" },
    });

    assert.equal(result.diagnostics.route, "history_temporal");
    assert.equal(result.diagnostics.temporal_parse_status, "fallback_support");
    assert.equal(result.diagnostics.fallback_used, true);
    assert.match(result.diagnostics.fallback_reason ?? "", /temporal_window_empty_using_out_of_range_support/);
    assert.ok(result.diagnostics.out_of_range_support_count >= 1);
    assert.ok(result.diagnostics.excluded_untimestamped_count >= 1);
    assert.equal(result.diagnostics.noInRangeEvidence, true);
    assert.ok(result.items.every((item) => item.temporalSupportLabel === "out_of_range_support"));
  } finally {
    await harness.close();
  }
});
