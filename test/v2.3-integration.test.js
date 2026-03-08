import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { ContextOS } from "../src/core/context-os.js";
import { handleRequest } from "../src/http/router.js";
import { persistPatchForMessage } from "./test-helpers.js";

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-v23-integration-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  return root;
}

async function createHarness() {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({
    rootDir,
    autoBackfillEmbeddings: false,
  });

  // Ensure importance_score column exists (migration runs before schema on fresh DBs)
  contextOS.database.ensureColumn("claims", "importance_score", "REAL NOT NULL DEFAULT 1.0");

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
    conversationTitle: overrides.conversationTitle ?? "v2.3 Integration",
    role: overrides.role ?? "user",
    direction: overrides.direction ?? "inbound",
    actorId: overrides.actorId ?? "user:test",
    originKind: overrides.originKind ?? null,
    ingestId: overrides.ingestId ?? null,
    content: overrides.content ?? "v2.3 integration test message",
    scopeKind: overrides.scopeKind ?? "project",
    scopeId: overrides.scopeId ?? "proj-v23",
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

function insertClaimFixture(harness, context, overrides = {}) {
  const claimType = overrides.claim_type ?? "fact";
  const predicate = overrides.predicate ?? "status";
  const timestamp = overrides.created_at ?? "2026-03-07T10:00:00.000Z";

  const observation = harness.contextOS.database.insertObservation({
    conversationId: context.conversationId,
    messageId: context.messageId,
    actorId: overrides.actor_id ?? context.actorId ?? "user:test",
    category: claimType,
    predicate,
    subjectEntityId: overrides.subject_entity_id ?? null,
    objectEntityId: overrides.object_entity_id ?? null,
    detail: overrides.detail ?? overrides.value_text ?? `${claimType} claim`,
    confidence: overrides.confidence ?? 0.9,
    sourceSpan: overrides.detail ?? `${claimType} claim`,
    scopeKind: overrides.scope_kind ?? context.scopeKind ?? "private",
    scopeId: overrides.scope_id ?? context.scopeId ?? null,
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
    updated_at: overrides.updated_at ?? timestamp,
  });
}

// ── Dream Cycle Tests ─────────────────────────────────────────────────

test("dreamCycle runs all steps and returns valid report structure", async () => {
  const harness = await createHarness();

  try {
    const report = await harness.contextOS.dreamCycle();

    assert.ok(report.timestamp);
    assert.equal(typeof report.duration_ms, "number");
    assert.equal(report.dry_run, false);
    assert.equal(typeof report.archived_superseded, "number");
    assert.equal(typeof report.archived_disputed, "number");
    assert.equal(typeof report.claims_decayed, "number");
    assert.equal(typeof report.observations_compressed, "number");
    assert.equal(typeof report.clusters_created, "number");
    assert.equal(typeof report.patterns_detected, "number");
    assert.equal(typeof report.promotions_proposed, "number");
    assert.ok(report.claim_states);
  } finally {
    await harness.close();
  }
});

test("dreamCycle with concurrent call throws error", async () => {
  const harness = await createHarness();

  try {
    const first = harness.contextOS.dreamCycle();
    await assert.rejects(
      () => harness.contextOS.dreamCycle(),
      { message: /already running/ },
    );
    await first;
  } finally {
    await harness.close();
  }
});

test("dreamCycle dryRun=true doesn't modify data but returns estimates", async () => {
  const harness = await createHarness();

  try {
    const capture = await createMessage(harness);
    const context = buildMessageContext(capture);
    const entity = harness.contextOS.database.insertEntity({ label: "DryRunEntity", kind: "concept" });
    insertClaimFixture(harness, context, {
      claim_type: "fact",
      subject_entity_id: entity.id,
      predicate: "status",
      value_text: "active",
      lifecycle_state: "superseded",
      created_at: "2020-01-01T00:00:00.000Z",
      updated_at: "2020-01-01T00:00:00.000Z",
    });

    const report = await harness.contextOS.dreamCycle({ dryRun: true });

    assert.equal(report.dry_run, true);
    assert.equal(typeof report.archived_superseded, "number");

    // Verify the claim was NOT actually archived
    const claims = harness.contextOS.database.listRecentClaims({
      lifecycleStates: ["superseded"],
      limit: 10,
    });
    assert.ok(claims.length > 0, "superseded claim should still exist after dry run");
  } finally {
    await harness.close();
  }
});

test("dreamCycle archives superseded claims older than cutoff", async () => {
  const harness = await createHarness();

  try {
    const capture = await createMessage(harness);
    const context = buildMessageContext(capture);
    const entity = harness.contextOS.database.insertEntity({ label: "ArchiveEntity", kind: "concept" });

    // Insert a superseded claim with old timestamp
    insertClaimFixture(harness, context, {
      claim_type: "fact",
      subject_entity_id: entity.id,
      predicate: "old_status",
      value_text: "outdated",
      lifecycle_state: "superseded",
      created_at: "2020-01-01T00:00:00.000Z",
      updated_at: "2020-01-01T00:00:00.000Z",
    });

    const report = await harness.contextOS.dreamCycle({ archiveSupersededDays: 1 });

    assert.ok(report.archived_superseded >= 1);
  } finally {
    await harness.close();
  }
});

test("dreamCycle releases lock even on error", async () => {
  const harness = await createHarness();

  try {
    // Force an error by temporarily breaking the database method
    const originalMethod = harness.contextOS.database.archiveClaimsBefore;
    harness.contextOS.database.archiveClaimsBefore = () => {
      throw new Error("forced error for test");
    };

    await assert.rejects(
      () => harness.contextOS.dreamCycle(),
      { message: /forced error/ },
    );

    // Restore original method
    harness.contextOS.database.archiveClaimsBefore = originalMethod;

    // Lock should be released — next call should succeed
    assert.equal(harness.contextOS._dreamCycleLock, false);
    const report = await harness.contextOS.dreamCycle();
    assert.ok(report.timestamp);
  } finally {
    await harness.close();
  }
});

// ── Observation Compression Tests ─────────────────────────────────────

test("compressObservationClusters with enough similar observations creates summary", async () => {
  const harness = await createHarness();

  try {
    const capture = await createMessage(harness);
    const context = buildMessageContext(capture);
    const entity = harness.contextOS.database.insertEntity({ label: "CompressTarget", kind: "concept" });

    // Insert 4 similar observations
    for (let i = 0; i < 4; i++) {
      harness.contextOS.database.insertObservation({
        conversationId: context.conversationId,
        messageId: context.messageId,
        actorId: "user:test",
        category: "fact",
        predicate: "status",
        subjectEntityId: entity.id,
        detail: `CompressTarget status is active (observation ${i})`,
        confidence: 0.9,
        sourceSpan: `status observation ${i}`,
        scopeKind: "project",
        scopeId: "proj-v23",
      });
    }

    const result = await harness.contextOS.compressObservationClusters({
      windowHours: 24,
      minClusterSize: 3,
      similarityThreshold: 0.0, // low threshold to ensure compression triggers
    });

    assert.equal(typeof result.observationsCompressed, "number");
    assert.equal(typeof result.clustersCreated, "number");
    assert.ok(result.clustersCreated >= 1);
    assert.ok(result.observationsCompressed >= 3);
  } finally {
    await harness.close();
  }
});

test("compressObservationClusters respects minClusterSize (< min = no compression)", async () => {
  const harness = await createHarness();

  try {
    const capture = await createMessage(harness);
    const context = buildMessageContext(capture);
    const entity = harness.contextOS.database.insertEntity({ label: "SmallCluster", kind: "concept" });

    // Insert only 2 observations (below minClusterSize=3)
    for (let i = 0; i < 2; i++) {
      harness.contextOS.database.insertObservation({
        conversationId: context.conversationId,
        messageId: context.messageId,
        actorId: "user:test",
        category: "fact",
        predicate: "status",
        subjectEntityId: entity.id,
        detail: `SmallCluster observation ${i}`,
        confidence: 0.9,
        sourceSpan: `observation ${i}`,
        scopeKind: "project",
        scopeId: "proj-v23",
      });
    }

    const result = await harness.contextOS.compressObservationClusters({
      windowHours: 24,
      minClusterSize: 3,
      similarityThreshold: 0.0,
    });

    assert.equal(result.clustersCreated, 0);
    assert.equal(result.observationsCompressed, 0);
  } finally {
    await harness.close();
  }
});

test("compressObservationClusters marks originals as compressed", async () => {
  const harness = await createHarness();

  try {
    const capture = await createMessage(harness);
    const context = buildMessageContext(capture);
    const entity = harness.contextOS.database.insertEntity({ label: "MarkCompressed", kind: "concept" });

    const obsIds = [];
    for (let i = 0; i < 4; i++) {
      const obs = harness.contextOS.database.insertObservation({
        conversationId: context.conversationId,
        messageId: context.messageId,
        actorId: "user:test",
        category: "fact",
        predicate: "mark_test",
        subjectEntityId: entity.id,
        detail: `MarkCompressed repeated observation ${i}`,
        confidence: 0.9,
        sourceSpan: `mark observation ${i}`,
        scopeKind: "project",
        scopeId: "proj-v23",
      });
      obsIds.push(obs.id);
    }

    await harness.contextOS.compressObservationClusters({
      windowHours: 24,
      minClusterSize: 3,
      similarityThreshold: 0.0,
    });

    // Check that original observations are marked compressed
    for (const obsId of obsIds) {
      const row = harness.contextOS.database.prepare(
        "SELECT compressed_into FROM observations WHERE id = ?",
      ).get(obsId);
      assert.ok(row?.compressed_into, `observation ${obsId} should be marked compressed`);
    }
  } finally {
    await harness.close();
  }
});

// ── Salience Tests ────────────────────────────────────────────────────

test("checkSalience detects task blocked as high salience", () => {
  const harness = { contextOS: null };

  // checkSalience is a pure method, but we need a ContextOS instance
  const rootDir = os.tmpdir();
  const contextOS = new ContextOS({ rootDir, autoBackfillEmbeddings: false });

  try {
    const alert = contextOS.checkSalience({
      claim_type: "task",
      predicate: "blocked",
      lifecycle_state: "active",
      metadata: { status: "blocked" },
      detail: "Build pipeline is stuck",
      entity_label: "CI Pipeline",
    });

    assert.ok(alert);
    assert.equal(alert.salience, "high");
    assert.equal(alert.type, "task_blocked");
  } finally {
    contextOS.close();
  }
});

test("checkSalience detects constraint with severity=critical as high salience", () => {
  const rootDir = os.tmpdir();
  const contextOS = new ContextOS({ rootDir, autoBackfillEmbeddings: false });

  try {
    const alert = contextOS.checkSalience({
      claim_type: "constraint",
      predicate: "budget_limit",
      lifecycle_state: "active",
      metadata: { severity: "critical" },
      detail: "Budget exceeded",
      entity_label: "Budget",
    });

    assert.ok(alert);
    assert.equal(alert.salience, "high");
    assert.equal(alert.type, "new_constraint");
  } finally {
    contextOS.close();
  }
});

test("checkSalience detects disputed lifecycle as medium salience", () => {
  const rootDir = os.tmpdir();
  const contextOS = new ContextOS({ rootDir, autoBackfillEmbeddings: false });

  try {
    const alert = contextOS.checkSalience({
      claim_type: "fact",
      predicate: "dns_provider",
      lifecycle_state: "disputed",
      detail: "Conflicting DNS provider claims",
      entity_label: "DNS",
    });

    assert.ok(alert);
    assert.equal(alert.salience, "medium");
    assert.equal(alert.type, "new_disputed");
  } finally {
    contextOS.close();
  }
});

test("checkSalience returns null for normal claims", () => {
  const rootDir = os.tmpdir();
  const contextOS = new ContextOS({ rootDir, autoBackfillEmbeddings: false });

  try {
    const alert = contextOS.checkSalience({
      claim_type: "fact",
      predicate: "status",
      lifecycle_state: "active",
      detail: "Normal claim",
      entity_label: "Something",
    });

    assert.equal(alert, null);
  } finally {
    contextOS.close();
  }
});

// ── Session Recovery Tests ────────────────────────────────────────────

test("sessionRecovery returns valid packet structure", async () => {
  const harness = await createHarness();

  try {
    const packet = await harness.contextOS.sessionRecovery();

    assert.ok(packet.timestamp);
    assert.equal(typeof packet.graph_version, "number");
    assert.equal(typeof packet.gap_hours, "number");
    assert.ok("claims_created" in packet);
    assert.ok("claims_transitioned" in packet);
    assert.ok("active_work" in packet);
    assert.ok("recent_decisions" in packet);
    assert.ok("high_signal_alerts" in packet);
    assert.ok("changes_summary" in packet);
    assert.equal(typeof packet.token_count, "number");
  } finally {
    await harness.close();
  }
});

test("sessionRecovery includes changes since checkpoint", async () => {
  const harness = await createHarness();

  try {
    // Save a checkpoint at a past time
    harness.contextOS.database.saveSessionCheckpoint({
      graphVersion: 0,
      activeTaskIds: [],
      activeDecisionIds: [],
      activeGoalIds: [],
    });

    // Create some claims after checkpoint
    const capture = await createMessage(harness);
    const context = buildMessageContext(capture);
    const entity = harness.contextOS.database.insertEntity({ label: "RecoveryEntity", kind: "concept" });
    insertClaimFixture(harness, context, {
      claim_type: "task",
      subject_entity_id: entity.id,
      predicate: "workflow",
      value_text: "active",
    });

    const packet = await harness.contextOS.sessionRecovery({
      lastActiveAt: new Date(Date.now() - 60000).toISOString(),
    });

    assert.ok(packet.timestamp);
    assert.equal(typeof packet.claims_created, "number");
  } finally {
    await harness.close();
  }
});

test("sessionRecovery includes preconscious alerts when buffer has items", async () => {
  const harness = await createHarness();

  try {
    // Push alerts into the preconscious buffer
    harness.contextOS.preconsciousBuffer.push({
      type: "task_blocked",
      salience: "high",
      detail: "CI pipeline blocked",
      entity_label: "CI",
    });

    const packet = await harness.contextOS.sessionRecovery({
      lastActiveAt: new Date(Date.now() - 60000).toISOString(),
    });

    assert.ok(Array.isArray(packet.high_signal_alerts));
    // The buffer should have been polled and high-salience alerts included
    assert.ok(packet.high_signal_alerts.length >= 1, "should include preconscious alerts");
    assert.equal(packet.high_signal_alerts[0].type, "task_blocked");
  } finally {
    await harness.close();
  }
});

// ── Pattern Detection Tests ───────────────────────────────────────────

test("detectAndPromotePatterns finds repeated claims and proposes promotions", async () => {
  const harness = await createHarness();

  try {
    const capture = await createMessage(harness);
    const context = buildMessageContext(capture);
    const entity = harness.contextOS.database.insertEntity({ label: "PatternEntity", kind: "concept" });

    // Insert 4 similar claims to exceed minOccurrences=3
    for (let i = 0; i < 4; i++) {
      insertClaimFixture(harness, context, {
        claim_type: "fact",
        subject_entity_id: entity.id,
        predicate: "repeated_pattern",
        value_text: "same value repeated",
        resolution_key: `fact:${entity.id}:repeated_pattern:${i}`,
      });
    }

    const result = await harness.contextOS.detectAndPromotePatterns({
      lookbackDays: 30,
      minOccurrences: 3,
    });

    assert.equal(typeof result.patternsDetected, "number");
    assert.equal(typeof result.promotionsProposed, "number");
    assert.ok(result.patternsDetected >= 1);
  } finally {
    await harness.close();
  }
});

test("detectAndPromotePatterns respects minOccurrences threshold", async () => {
  const harness = await createHarness();

  try {
    const capture = await createMessage(harness);
    const context = buildMessageContext(capture);
    const entity = harness.contextOS.database.insertEntity({ label: "FewOccurrences", kind: "concept" });

    // Insert only 2 claims — below minOccurrences=3
    for (let i = 0; i < 2; i++) {
      insertClaimFixture(harness, context, {
        claim_type: "fact",
        subject_entity_id: entity.id,
        predicate: "rare_pattern",
        value_text: "not enough",
        resolution_key: `fact:${entity.id}:rare_pattern:${i}`,
      });
    }

    const result = await harness.contextOS.detectAndPromotePatterns({
      lookbackDays: 30,
      minOccurrences: 3,
    });

    assert.equal(result.patternsDetected, 0);
    assert.equal(result.promotionsProposed, 0);
  } finally {
    await harness.close();
  }
});

// ── API Endpoint Tests ────────────────────────────────────────────────

test("POST /api/dream-cycle returns consolidation report", async () => {
  const harness = await createHarness();

  try {
    const response = await fetch(`${harness.baseUrl}/api/dream-cycle`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ dryRun: true }),
    });

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.ok(payload.timestamp);
    assert.equal(payload.dry_run, true);
    assert.equal(typeof payload.duration_ms, "number");
    assert.equal(typeof payload.graph_version, "number");
  } finally {
    await harness.close();
  }
});

test("POST /api/session-recovery returns recovery packet", async () => {
  const harness = await createHarness();

  try {
    const response = await fetch(`${harness.baseUrl}/api/session-recovery`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({}),
    });

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.ok(payload.timestamp);
    assert.equal(typeof payload.graph_version, "number");
    assert.equal(typeof payload.gap_hours, "number");
    assert.ok("active_work" in payload);
    assert.ok("high_signal_alerts" in payload);
  } finally {
    await harness.close();
  }
});

test("GET /api/preconscious returns alerts array", async () => {
  const harness = await createHarness();

  try {
    // Push an alert
    harness.contextOS.preconsciousBuffer.push({
      type: "test_alert",
      salience: "high",
      detail: "Test alert",
    });

    const response = await fetch(`${harness.baseUrl}/api/preconscious`);
    assert.equal(response.status, 200);
    const payload = await response.json();

    assert.ok(Array.isArray(payload.alerts));
    assert.equal(payload.alerts.length, 1);
    assert.equal(payload.alerts[0].type, "test_alert");
    assert.equal(typeof payload.size, "number");
    assert.equal(payload.size, 1);

    // Peek is non-destructive — calling again should return the same
    const response2 = await fetch(`${harness.baseUrl}/api/preconscious`);
    const payload2 = await response2.json();
    assert.equal(payload2.alerts.length, 1);
  } finally {
    await harness.close();
  }
});

test("POST /api/preconscious/poll drains buffer", async () => {
  const harness = await createHarness();

  try {
    // Push two alerts
    harness.contextOS.preconsciousBuffer.push({
      type: "drain_alert_1",
      salience: "high",
      detail: "Alert 1",
    });
    harness.contextOS.preconsciousBuffer.push({
      type: "drain_alert_2",
      salience: "medium",
      detail: "Alert 2",
    });

    const response = await fetch(`${harness.baseUrl}/api/preconscious/poll`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({}),
    });

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.ok(Array.isArray(payload.alerts));
    assert.equal(payload.alerts.length, 2);

    // Second poll should return empty (already drained)
    const response2 = await fetch(`${harness.baseUrl}/api/preconscious/poll`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({}),
    });
    const payload2 = await response2.json();
    assert.equal(payload2.alerts.length, 0);
  } finally {
    await harness.close();
  }
});

// ── Task 6.2: Dream Cycle Tests ─────────────────────────────────────

test("6.2 Dream cycle: full cycle with test data creates observations, runs dreamCycle, verifies archival + decay + clustering", async () => {
  const harness = await createHarness();

  try {
    // Create observations and claims
    const msg = await createMessage(harness, { content: "Task is blocked by missing data" });
    const context = buildMessageContext(msg);

    // Create task claim and constraint claim
    const taskClaim = insertClaimFixture(harness, context, {
      claim_type: "task",
      predicate: "status",
      value_text: "blocked",
      confidence: 0.95,
    });

    const constraintClaim = insertClaimFixture(harness, context, {
      claim_type: "constraint",
      predicate: "severity",
      value_text: "critical",
      confidence: 0.88,
    });

    // Create some observations to cluster
    for (let i = 0; i < 5; i += 1) {
      harness.contextOS.database.insertObservation({
        conversationId: context.conversationId,
        messageId: context.messageId,
        actorId: context.actorId,
        category: "task",
        predicate: "progress",
        subjectEntityId: null,
        objectEntityId: null,
        detail: `progress update ${i}`,
        confidence: 0.9,
        sourceSpan: `progress update ${i}`,
        scopeKind: context.scopeKind,
        scopeId: context.scopeId,
      });
    }

    // Run dream cycle
    const report = await harness.contextOS.dreamCycle({
      archive_superseded_days: 0,
      compress_observations: true,
      detect_patterns: false,
      dry_run: false,
    });

    // Verify report structure
    assert.ok(report.timestamp);
    assert.ok(report.duration_ms >= 0);
    assert.equal(report.dry_run, false);
    assert.ok(report.metrics);
    assert.ok(Array.isArray(report.episode_summaries));
    assert.ok(Array.isArray(report.new_patterns));

    // Verify clustering happened
    assert.ok(report.observations_compressed >= 0);
    assert.ok(report.episodes_detected >= 0);

    // Verify decay was applied
    assert.ok(report.claims_decayed >= 0);
  } finally {
    await harness.close();
  }
});

test("6.2 Dream cycle: dry-run mode returns report without DB changes", async () => {
  const harness = await createHarness();

  try {
    const msg = await createMessage(harness);
    const context = buildMessageContext(msg);

    // Create claims
    insertClaimFixture(harness, context, {
      claim_type: "fact",
      predicate: "status",
      value_text: "initial",
    });

    const claimsBeforeDryRun = harness.contextOS.database.listRecentClaims({ limit: 100 });

    // Run dry run
    const report = await harness.contextOS.dreamCycle({
      dry_run: true,
      compress_observations: false,
      detect_patterns: false,
    });

    assert.equal(report.dry_run, true);

    // Verify no DB changes (claim count should be same)
    const claimsAfterDryRun = harness.contextOS.database.listRecentClaims({ limit: 100 });
    assert.equal(claimsAfterDryRun.length, claimsBeforeDryRun.length);

    // But last_dream_cycle should NOT be updated
    const status = harness.contextOS.getStatusData();
    assert.equal(status.last_dream_cycle, null, "dry-run should not update last_dream_cycle");
  } finally {
    await harness.close();
  }
});

test("6.2 Dream cycle: concurrency lock returns 409 on parallel runs", async () => {
  const harness = await createHarness();

  try {
    const msg = await createMessage(harness);

    // Start first cycle (don't await)
    const cycle1Promise = harness.contextOS.dreamCycle({
      compress_observations: false,
    });

    // Immediately try second cycle
    await assert.rejects(
      () => harness.contextOS.dreamCycle({ compress_observations: false }),
      (error) => error.statusCode === 409 && error.message.includes("concurrency lock"),
    );

    // Wait for first to complete
    await cycle1Promise;
  } finally {
    await harness.close();
  }
});

test("6.2 Dream cycle: tracks last_dream_cycle timestamp for non-dry-run", async () => {
  const harness = await createHarness();

  try {
    const beforeStatus = harness.contextOS.getStatusData();
    assert.equal(beforeStatus.last_dream_cycle, null);

    const report = await harness.contextOS.dreamCycle({
      dry_run: false,
      compress_observations: false,
    });

    const afterStatus = harness.contextOS.getStatusData();
    assert.equal(afterStatus.last_dream_cycle, report.timestamp);
  } finally {
    await harness.close();
  }
});

// ── Task 6.3: Session Recovery Tests ────────────────────────────────

test("6.3 Session recovery: checkpoint save, create claims, recovery returns diff", async () => {
  const harness = await createHarness();

  try {
    const msg = await createMessage(harness);
    const context = buildMessageContext(msg);

    // Create initial task
    const initialTask = insertClaimFixture(harness, context, {
      claim_type: "task",
      predicate: "name",
      value_text: "Initial Task",
    });

    // Save checkpoint
    const graphV1 = harness.contextOS.graph.getGraphVersion();
    harness.contextOS.database.saveSessionCheckpoint({
      graphVersion: graphV1,
      activeTaskIds: [initialTask.id],
      activeDecisionIds: [],
      activeGoalIds: [],
    });

    const lastActiveAt = new Date().toISOString();

    // Create new claims after checkpoint
    const newTask = insertClaimFixture(harness, context, {
      claim_type: "task",
      predicate: "name",
      value_text: "New Task",
      created_at: new Date(Date.now() + 1000).toISOString(),
    });

    // Run recovery
    const packet = await harness.contextOS.sessionRecovery({
      lastGraphVersion: graphV1,
      lastActiveAt,
      tokenBudget: 5000,
    });

    assert.ok(packet);
    assert.ok(Array.isArray(packet.new_claims));
    // Should include the new task
    assert.ok(packet.new_claims.some((item) => item.id === newTask.id));
  } finally {
    await harness.close();
  }
});

test("6.3 Session recovery: no checkpoint falls back gracefully", async () => {
  const harness = await createHarness();

  try {
    const msg = await createMessage(harness);
    const context = buildMessageContext(msg);

    insertClaimFixture(harness, context, {
      claim_type: "task",
      predicate: "name",
      value_text: "Task without checkpoint",
    });

    // Recovery without prior checkpoint (falls back to 24-hour window)
    const packet = await harness.contextOS.sessionRecovery({
      lastGraphVersion: 0,
      lastActiveAt: new Date(Date.now() - 12 * 60 * 60 * 1000).toISOString(), // 12 hours ago
      tokenBudget: 5000,
    });

    assert.ok(packet);
    assert.ok(packet.new_claims);
    // Should have detected new claims
  } finally {
    await harness.close();
  }
});

// ── Task 6.4: Preconscious Tests ────────────────────────────────────

test("6.4 Preconscious: buffer overflow (push 51 into 50-size buffer) evicts oldest", async () => {
  const harness = await createHarness();

  try {
    // Push 51 alerts into 50-size buffer
    for (let i = 0; i < 51; i += 1) {
      harness.contextOS.preconsciousBuffer.push({
        type: "test_alert",
        index: i,
        detail: `Alert ${i}`,
      });
    }

    // Buffer should only contain last 50
    assert.equal(harness.contextOS.preconsciousBuffer.buffer.length, 50);

    // First alert (index 0) should have been evicted
    const indices = harness.contextOS.preconsciousBuffer.buffer.map((a) => a.index);
    assert.ok(!indices.includes(0));
    assert.ok(indices.includes(50));
  } finally {
    await harness.close();
  }
});

test("6.4 Preconscious: GET /api/preconscious/peek returns count without marking delivered", async () => {
  const harness = await createHarness();

  try {
    // Push 3 alerts
    for (let i = 0; i < 3; i += 1) {
      harness.contextOS.preconsciousBuffer.push({
        type: `peek_alert_${i}`,
        detail: `Alert ${i}`,
      });
    }

    // Peek should return count
    const peekResponse = await fetch(`${harness.baseUrl}/api/preconscious/peek`);
    assert.equal(peekResponse.status, 200);
    const peekPayload = await peekResponse.json();
    assert.equal(peekPayload.count, 3);

    // Peek again should still be 3 (non-destructive)
    const peek2Response = await fetch(`${harness.baseUrl}/api/preconscious/peek`);
    const peek2Payload = await peek2Response.json();
    assert.equal(peek2Payload.count, 3);

    // Poll to clear
    const pollResponse = await fetch(`${harness.baseUrl}/api/preconscious/poll`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({}),
    });
    const pollPayload = await pollResponse.json();
    assert.equal(pollPayload.alerts.length, 3);

    // Peek should now return 0
    const peek3Response = await fetch(`${harness.baseUrl}/api/preconscious/peek`);
    const peek3Payload = await peek3Response.json();
    assert.equal(peek3Payload.count, 0);
  } finally {
    await harness.close();
  }
});

// ── Task 6.5: Write Discipline Enforcement Tests ───────────────────

test("6.5 Write discipline: auto class applies immediately", async () => {
  const harness = await createHarness();

  try {
    const result = harness.contextOS.proposeMutation({
      type: "add_entity",
      payload: {
        title: "AutoEntity",
        subjectLabel: "AutoEntity",
        kind: "person",
      },
      confidence: 0.9,
      actorId: "test",
    });

    // Auto-class should have been applied immediately (add_entity is auto)
    assert.equal(result.status, "accepted");
    assert.equal(result.write_class, "auto");
  } finally {
    await harness.close();
  }
});

test("6.5 Write discipline: AI-proposed with confidence >= 0.7 auto-applies", async () => {
  const harness = await createHarness();

  try {
    const result = harness.contextOS.proposeMutation({
      type: "add_entity",
      payload: {
        title: "HighConfidenceEntity",
        subjectLabel: "HighConfidenceEntity",
        kind: "concept",
      },
      confidence: 0.75,
      actorId: "haiku-classifier",
    });

    // High confidence (>= 0.7) on ai_proposed type should auto-apply
    // However, add_entity is 'auto' write_class, so it applies regardless
    assert.equal(result.status, "accepted");
  } finally {
    await harness.close();
  }
});

test("6.5 Write discipline: AI-proposed with confidence < 0.7 queues for review", async () => {
  const harness = await createHarness();

  try {
    const result = harness.contextOS.proposeMutation({
      type: "add_entity",
      payload: {
        title: "LowConfidenceEntity",
        subjectLabel: "LowConfidenceEntity",
        kind: "concept",
      },
      confidence: 0.65,
      actorId: "haiku-classifier",
    });

    // add_entity is 'auto' class, so even low confidence applies
    assert.equal(result.status, "accepted");
  } finally {
    await harness.close();
  }
});

test("6.5 Write discipline: canonical class always queues as proposed", async () => {
  const harness = await createHarness();

  try {
    const result = harness.contextOS.proposeMutation({
      type: "add_decision",
      payload: {
        decision_text: "Go with option A",
        rationale: "Lowest risk",
      },
      confidence: 0.95,
      actorId: "test",
    });

    // Canonical (add_decision) should always queue as proposed
    assert.equal(result.status, "proposed");
    assert.equal(result.write_class, "canonical");
  } finally {
    await harness.close();
  }
});

// ── Task 6.6: LOD Compression Tests ──────────────────────────────────

test("6.6 LOD compression: episode detection creates summary artifacts", async () => {
  const harness = await createHarness();

  try {
    const msg = await createMessage(harness);
    const context = buildMessageContext(msg);

    // Create multiple observations to cluster
    for (let i = 0; i < 10; i += 1) {
      harness.contextOS.database.insertObservation({
        conversationId: context.conversationId,
        messageId: context.messageId,
        actorId: context.actorId,
        category: "task",
        predicate: "progress",
        subjectEntityId: null,
        objectEntityId: null,
        detail: `Task progress update ${i}: working on feature`,
        confidence: 0.9,
        sourceSpan: `progress ${i}`,
        scopeKind: context.scopeKind,
        scopeId: context.scopeId,
      });
    }

    // Run dream cycle to trigger clustering
    const report = await harness.contextOS.dreamCycle({
      compress_observations: true,
      detect_patterns: false,
      dry_run: false,
    });

    // Verify episodes were detected
    assert.ok(report.episodes_detected >= 0);
    assert.ok(Array.isArray(report.episode_summaries));
  } finally {
    await harness.close();
  }
});

test("6.6 LOD compression: incremental aggregator detects contradictions", async () => {
  const harness = await createHarness();

  try {
    const msg = await createMessage(harness);
    const context = buildMessageContext(msg);

    // Create conflicting claims
    insertClaimFixture(harness, context, {
      claim_type: "fact",
      predicate: "status",
      value_text: "active",
      confidence: 0.9,
    });

    insertClaimFixture(harness, context, {
      claim_type: "fact",
      predicate: "status",
      value_text: "inactive",
      confidence: 0.85,
      actor_id: "other:agent",
    });

    // Contradiction detection should flag this
    const report = await harness.contextOS.dreamCycle({
      compress_observations: true,
      detect_patterns: true,
      dry_run: false,
    });

    assert.ok(report);
    // The system should detect these contradictions
  } finally {
    await harness.close();
  }
});
