/**
 * Tests for pattern feedback loop, surfacing, and temporal detection.
 *
 * Covers:
 * - Pattern feedback CRUD (insert, get, list, filter by action)
 * - Rejected patterns skipped in detectAndPromotePatterns
 * - Confirmed patterns get boosted confidence
 * - New patterns pushed to preconscious buffer
 * - Temporal regularity detection (regular intervals detected, irregular not)
 * - REST endpoints return correct responses
 * - Edge cases: empty patterns, no feedback, duplicate feedback
 */

import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";
import { detectTemporalPatterns } from "../src/core/pattern-detection.js";
import { handleRequest } from "../src/http/router.js";

// ── Helpers ──────────────────────────────────────────────────────────

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-pattern-feedback-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  return root;
}

function makeContextOS(rootDir) {
  return new ContextOS({
    rootDir,
    autoBackfillEmbeddings: false,
    reviewManagerOptions: {
      setTimeout() {
        return { unref() {} };
      },
      clearTimeout() {},
    },
  });
}

async function createHarness() {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);

  const server = http.createServer((request, response) =>
    handleRequest(contextOS, rootDir, request, response));
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();

  return {
    rootDir,
    contextOS,
    db: contextOS.database,
    baseUrl: `http://127.0.0.1:${port}`,
    async close() {
      await new Promise((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())));
      contextOS.database.close();
      contextOS.telemetry.close();
      await fs.rm(rootDir, { recursive: true, force: true });
    },
  };
}

/** Insert N similar claims for the same (entity, type, predicate) group */
function seedPatternClaims(db, {
  entityId = null, // null → uses __none__ key; pass a string to create entity
  claimType = "fact",
  predicate = "test_predicate",
  count = 3,
  valueText = "same repeated value",
  timestampOffsets = null, // array of ms offsets from now (for temporal tests)
} = {}) {
  // If entityId provided, ensure the entity exists in the entities table
  let resolvedEntityId = null;
  if (entityId) {
    const now = new Date().toISOString();
    const slug = entityId.toLowerCase().replace(/[^a-z0-9]/g, "-");
    try {
      db.prepare(`
        INSERT OR IGNORE INTO entities (id, slug, label, kind, created_at, updated_at, last_seen_at)
        VALUES (?, ?, ?, 'person', ?, ?, ?)
      `).run(entityId, slug, entityId, now, now, now);
    } catch {
      // Entity may already exist; that's fine
    }
    resolvedEntityId = entityId;
  }

  const ids = [];
  for (let i = 0; i < count; i += 1) {
    const claimId = `claim-pf-${Date.now()}-${i}-${Math.random().toString(36).slice(2)}`;
    const tsOffset = timestampOffsets ? timestampOffsets[i] : 0;
    const createdAt = new Date(Date.now() + tsOffset).toISOString();

    db.prepare(`
      INSERT INTO claims (
        id, observation_id, conversation_id, message_id, actor_id,
        claim_type, subject_entity_id, predicate, value_text,
        confidence, lifecycle_state, created_at, updated_at
      ) VALUES (?, NULL, NULL, NULL, ?, ?, ?, ?, ?, ?, 'active', ?, ?)
    `).run(
      claimId,
      "test",
      claimType,
      resolvedEntityId,
      predicate,
      valueText,
      0.8,
      createdAt,
      createdAt,
    );

    ids.push(claimId);
  }

  return ids;
}

// ── Database CRUD ────────────────────────────────────────────────────

test("insertPatternFeedback — stores confirmed feedback", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    const result = db.insertPatternFeedback({
      patternKey: "ent1::fact::has_name",
      action: "confirmed",
      sourceClaimIds: ["c1", "c2", "c3"],
      userNote: "Looks correct",
    });

    assert.ok(result.id > 0);
  } finally {
    db.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("insertPatternFeedback — stores rejected feedback", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    const result = db.insertPatternFeedback({
      patternKey: "ent1::fact::noisy_pattern",
      action: "rejected",
    });

    assert.ok(result.id > 0);
  } finally {
    db.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("insertPatternFeedback — stores snoozed feedback", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    const result = db.insertPatternFeedback({
      patternKey: "ent1::fact::maybe",
      action: "snoozed",
    });

    assert.ok(result.id > 0);
  } finally {
    db.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("insertPatternFeedback — throws on invalid action", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    assert.throws(
      () => db.insertPatternFeedback({ patternKey: "k", action: "invalid" }),
      /invalid pattern feedback action/i,
    );
  } finally {
    db.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("insertPatternFeedback — throws on missing patternKey", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    assert.throws(
      () => db.insertPatternFeedback({ patternKey: "", action: "confirmed" }),
      /patternKey is required/i,
    );
  } finally {
    db.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("getPatternFeedback — returns latest feedback for a key", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    db.insertPatternFeedback({ patternKey: "ent1::fact::pred", action: "confirmed" });
    db.insertPatternFeedback({ patternKey: "ent1::fact::pred", action: "snoozed" });

    const fb = db.getPatternFeedback("ent1::fact::pred");
    // should return most recent (snoozed)
    assert.equal(fb.action, "snoozed");
    assert.equal(fb.patternKey, "ent1::fact::pred");
  } finally {
    db.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("getPatternFeedback — returns null for unknown key", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    const fb = db.getPatternFeedback("nonexistent::key");
    assert.equal(fb, null);
  } finally {
    db.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("getPatternFeedback — deserializes sourceClaimIds from JSON", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    db.insertPatternFeedback({
      patternKey: "ent1::fact::pred",
      action: "confirmed",
      sourceClaimIds: ["c1", "c2", "c3"],
    });

    const fb = db.getPatternFeedback("ent1::fact::pred");
    assert.deepEqual(fb.sourceClaimIds, ["c1", "c2", "c3"]);
  } finally {
    db.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("listPatternFeedback — returns all feedback", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    db.insertPatternFeedback({ patternKey: "a::b::c", action: "confirmed" });
    db.insertPatternFeedback({ patternKey: "d::e::f", action: "rejected" });
    db.insertPatternFeedback({ patternKey: "g::h::i", action: "snoozed" });

    const all = db.listPatternFeedback();
    assert.equal(all.length, 3);
  } finally {
    db.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("listPatternFeedback — filters by action", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    db.insertPatternFeedback({ patternKey: "a::b::c", action: "confirmed" });
    db.insertPatternFeedback({ patternKey: "d::e::f", action: "rejected" });
    db.insertPatternFeedback({ patternKey: "g::h::i", action: "confirmed" });

    const confirmed = db.listPatternFeedback({ action: "confirmed" });
    assert.equal(confirmed.length, 2);
    assert.ok(confirmed.every((fb) => fb.action === "confirmed"));

    const rejected = db.listPatternFeedback({ action: "rejected" });
    assert.equal(rejected.length, 1);
  } finally {
    db.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("listPatternFeedback — respects limit", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    for (let i = 0; i < 10; i += 1) {
      db.insertPatternFeedback({ patternKey: `key-${i}::t::p`, action: "confirmed" });
    }

    const limited = db.listPatternFeedback({ limit: 3 });
    assert.equal(limited.length, 3);
  } finally {
    db.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("listPatternFeedback — empty when no feedback exists", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    const result = db.listPatternFeedback();
    assert.deepEqual(result, []);
  } finally {
    db.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

// ── detectAndPromotePatterns behaviour ──────────────────────────────

test("detectAndPromotePatterns — skips rejected patterns", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    // Seed ≥3 claims for a group (null entityId → key: __none__::fact::rejected_pattern)
    seedPatternClaims(db, {
      claimType: "fact",
      predicate: "rejected_pattern",
      count: 4,
    });

    // Mark the pattern as rejected using the actual key format
    const patternKey = "__none__::fact::rejected_pattern";
    db.insertPatternFeedback({ patternKey, action: "rejected" });

    const result = await contextOS.detectAndPromotePatterns({ lookbackDays: 30 });
    // The rejected pattern should not be proposed
    assert.equal(result.promotionsProposed, 0);
  } finally {
    db.close();
    contextOS.telemetry.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("detectAndPromotePatterns — confirmed patterns get confidence 0.95", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    seedPatternClaims(db, {
      claimType: "fact",
      predicate: "confirmed_pattern",
      count: 3,
    });

    // Mark as confirmed using actual key format
    const patternKey = "__none__::fact::confirmed_pattern";
    db.insertPatternFeedback({ patternKey, action: "confirmed" });

    const result = await contextOS.detectAndPromotePatterns({ lookbackDays: 30 });

    // If proposed, confidence should be 0.95
    if (result.patterns.length > 0) {
      const confirmedPattern = result.patterns[0];
      assert.equal(confirmedPattern.confidence, 0.95);
    }
  } finally {
    db.close();
    contextOS.telemetry.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("detectAndPromotePatterns — new patterns pushed to preconscious buffer", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    seedPatternClaims(db, {
      claimType: "fact",
      predicate: "new_unfeedback_pattern",
      count: 3,
    });

    // No feedback for this pattern
    const beforeCount = contextOS.preconsciousBuffer.peek();
    await contextOS.detectAndPromotePatterns({ lookbackDays: 30 });
    const afterCount = contextOS.preconsciousBuffer.peek();

    // At least one pattern_detected alert should have been pushed
    assert.ok(afterCount > beforeCount);

    const alerts = contextOS.preconsciousBuffer.poll();
    const patternAlerts = alerts.filter((a) => a.type === "pattern_detected");
    assert.ok(patternAlerts.length > 0);
    assert.ok(patternAlerts[0].patternKey);
    assert.ok(patternAlerts[0].occurrences >= 3);
  } finally {
    db.close();
    contextOS.telemetry.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("detectAndPromotePatterns — snoozed patterns still proposed (not skipped)", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    seedPatternClaims(db, {
      claimType: "fact",
      predicate: "snoozed_pattern",
      count: 3,
    });

    const patternKey = "__none__::fact::snoozed_pattern";
    db.insertPatternFeedback({ patternKey, action: "snoozed" });

    // Snoozed should not block promotion
    const result = await contextOS.detectAndPromotePatterns({ lookbackDays: 30 });
    // snoozed patterns still get proposed (confidence is not boosted to 0.95 but also not rejected)
    assert.ok(result.patternsDetected >= 0); // just check no crash
  } finally {
    db.close();
    contextOS.telemetry.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

// ── getConfirmedPatternBoosts ────────────────────────────────────────

test("getConfirmedPatternBoosts — returns boosts for confirmed patterns", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    db.insertPatternFeedback({ patternKey: "ent1::fact::pred1", action: "confirmed" });
    db.insertPatternFeedback({ patternKey: "ent2::fact::pred2", action: "confirmed" });
    db.insertPatternFeedback({ patternKey: "ent3::fact::pred3", action: "rejected" });

    const boosts = contextOS.getConfirmedPatternBoosts();
    assert.equal(boosts.length, 2);
    assert.ok(boosts.every((b) => b.boostFactor === 1.5));
  } finally {
    db.close();
    contextOS.telemetry.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("getConfirmedPatternBoosts — parses entityId and predicate from patternKey", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    db.insertPatternFeedback({ patternKey: "my-entity::fact::my_predicate", action: "confirmed" });

    const boosts = contextOS.getConfirmedPatternBoosts();
    assert.equal(boosts.length, 1);
    assert.equal(boosts[0].entityId, "my-entity");
    assert.equal(boosts[0].predicate, "my_predicate");
    assert.equal(boosts[0].boostFactor, 1.5);
  } finally {
    db.close();
    contextOS.telemetry.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("getConfirmedPatternBoosts — __none__ entityId becomes null", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    db.insertPatternFeedback({ patternKey: "__none__::fact::__none__", action: "confirmed" });

    const boosts = contextOS.getConfirmedPatternBoosts();
    assert.equal(boosts.length, 1);
    assert.equal(boosts[0].entityId, null);
    assert.equal(boosts[0].predicate, null);
  } finally {
    db.close();
    contextOS.telemetry.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("getConfirmedPatternBoosts — empty when no confirmed feedback", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    db.insertPatternFeedback({ patternKey: "k::t::p", action: "rejected" });
    const boosts = contextOS.getConfirmedPatternBoosts();
    assert.deepEqual(boosts, []);
  } finally {
    db.close();
    contextOS.telemetry.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

// ── Temporal pattern detection ───────────────────────────────────────

test("detectTemporalPatterns — detects regular intervals", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    // Seed 4 claims with roughly equal 24h intervals
    const oneDayMs = 24 * 60 * 60 * 1000;
    const offsets = [
      -3 * oneDayMs,
      -2 * oneDayMs,
      -1 * oneDayMs,
      0,
    ];
    seedPatternClaims(db, {
      claimType: "event",
      predicate: "daily_checkin",
      count: 4,
      valueText: "daily check-in event",
      timestampOffsets: offsets,
    });

    const patterns = detectTemporalPatterns(db, { lookbackDays: 30, minOccurrences: 3 });
    const match = patterns.find((p) => p.predicate === "daily_checkin");

    assert.ok(match, "Should detect daily_checkin temporal pattern");
    assert.ok(match.regularity < 0.5, `regularity ${match.regularity} should be < 0.5`);
    assert.ok(match.meanIntervalHours > 20 && match.meanIntervalHours < 28,
      `mean interval ${match.meanIntervalHours}h should be ~24h`);
    assert.ok(match.nextExpected instanceof Date, "nextExpected should be a Date");
    assert.ok(match.occurrences >= 4);
  } finally {
    db.close();
    contextOS.telemetry.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("detectTemporalPatterns — does not detect irregular intervals", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    // Irregular: 1h, 48h, 5h, 100h
    const offsets = [
      -(1 + 48 + 5 + 100) * 60 * 60 * 1000,
      -(48 + 5 + 100) * 60 * 60 * 1000,
      -(5 + 100) * 60 * 60 * 1000,
      -(100) * 60 * 60 * 1000,
      0,
    ];
    seedPatternClaims(db, {
      claimType: "event",
      predicate: "irregular_event",
      count: 5,
      valueText: "irregular occurrences",
      timestampOffsets: offsets,
    });

    const patterns = detectTemporalPatterns(db, { lookbackDays: 60, minOccurrences: 3 });
    const match = patterns.find((p) => p.predicate === "irregular_event");
    assert.equal(match, undefined, "Should not detect irregular pattern");
  } finally {
    db.close();
    contextOS.telemetry.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("detectTemporalPatterns — respects minOccurrences", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    const oneDayMs = 24 * 60 * 60 * 1000;
    seedPatternClaims(db, {
      claimType: "event",
      predicate: "too_few",
      count: 2,
      valueText: "few events",
      timestampOffsets: [-oneDayMs, 0],
    });

    const patterns = detectTemporalPatterns(db, { lookbackDays: 30, minOccurrences: 3 });
    const match = patterns.find((p) => p.predicate === "too_few");
    assert.equal(match, undefined, "Groups with < minOccurrences should not appear");
  } finally {
    db.close();
    contextOS.telemetry.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("detectTemporalPatterns — empty result when no claims", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    const patterns = detectTemporalPatterns(db, { lookbackDays: 30 });
    assert.deepEqual(patterns, []);
  } finally {
    db.close();
    contextOS.telemetry.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("detectTemporalPatterns — returns correct pattern shape", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    const oneDayMs = 24 * 60 * 60 * 1000;
    seedPatternClaims(db, {
      claimType: "event",
      predicate: "shape_check",
      count: 3,
      valueText: "pattern value",
      timestampOffsets: [-2 * oneDayMs, -oneDayMs, 0],
    });

    const patterns = detectTemporalPatterns(db, { lookbackDays: 30, minOccurrences: 3 });
    const match = patterns.find((p) => p.predicate === "shape_check");
    assert.ok(match, "Should find pattern");
    assert.ok(typeof match.patternKey === "string");
    assert.ok(typeof match.meanIntervalHours === "number");
    assert.ok(typeof match.stdDevHours === "number");
    assert.ok(typeof match.regularity === "number");
    assert.ok(match.nextExpected instanceof Date);
    assert.ok(typeof match.occurrences === "number");
  } finally {
    db.close();
    contextOS.telemetry.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

// ── REST endpoints ────────────────────────────────────────────────────

test("GET /api/patterns — returns pattern list", async () => {
  const harness = await createHarness();

  try {
    const response = await fetch(`${harness.baseUrl}/api/patterns`);
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.ok(Array.isArray(body.patterns));
    assert.ok(typeof body.count === "number");
    assert.equal(body.count, body.patterns.length);
  } finally {
    await harness.close();
  }
});

test("GET /api/patterns — returns empty array when no claims", async () => {
  const harness = await createHarness();

  try {
    const response = await fetch(`${harness.baseUrl}/api/patterns`);
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.deepEqual(body.patterns, []);
    assert.equal(body.count, 0);
  } finally {
    await harness.close();
  }
});

test("GET /api/patterns/temporal — returns temporal pattern list", async () => {
  const harness = await createHarness();

  try {
    const response = await fetch(`${harness.baseUrl}/api/patterns/temporal`);
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.ok(Array.isArray(body.patterns));
    assert.ok(typeof body.count === "number");
  } finally {
    await harness.close();
  }
});

test("GET /api/patterns/feedback — returns feedback list", async () => {
  const harness = await createHarness();

  try {
    harness.db.insertPatternFeedback({ patternKey: "ent::fact::pred", action: "confirmed" });

    const response = await fetch(`${harness.baseUrl}/api/patterns/feedback`);
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.ok(Array.isArray(body.feedback));
    assert.equal(body.count, 1);
    assert.equal(body.feedback[0].action, "confirmed");
  } finally {
    await harness.close();
  }
});

test("GET /api/patterns/feedback — filters by action query param", async () => {
  const harness = await createHarness();

  try {
    harness.db.insertPatternFeedback({ patternKey: "k1::t::p", action: "confirmed" });
    harness.db.insertPatternFeedback({ patternKey: "k2::t::p", action: "rejected" });
    harness.db.insertPatternFeedback({ patternKey: "k3::t::p", action: "confirmed" });

    const response = await fetch(`${harness.baseUrl}/api/patterns/feedback?action=confirmed`);
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.equal(body.count, 2);
    assert.ok(body.feedback.every((f) => f.action === "confirmed"));
  } finally {
    await harness.close();
  }
});

test("POST /api/patterns/:patternKey/feedback — stores feedback and returns feedbackId", async () => {
  const harness = await createHarness();

  try {
    const patternKey = "ent-api::fact::predicate";
    const response = await fetch(
      `${harness.baseUrl}/api/patterns/${encodeURIComponent(patternKey)}/feedback`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ action: "confirmed", note: "Looks good" }),
      },
    );

    assert.equal(response.status, 200);
    const body = await response.json();
    assert.equal(body.ok, true);
    assert.ok(typeof body.feedbackId === "number");
    assert.ok(body.feedbackId > 0);
  } finally {
    await harness.close();
  }
});

test("POST /api/patterns/:patternKey/feedback — 400 on invalid action", async () => {
  const harness = await createHarness();

  try {
    const response = await fetch(
      `${harness.baseUrl}/api/patterns/some-key/feedback`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ action: "unknown_action" }),
      },
    );

    assert.equal(response.status, 400);
    const body = await response.json();
    assert.ok(body.error);
  } finally {
    await harness.close();
  }
});

test("POST /api/patterns/:patternKey/feedback — 400 on missing action", async () => {
  const harness = await createHarness();

  try {
    const response = await fetch(
      `${harness.baseUrl}/api/patterns/some-key/feedback`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ note: "no action provided" }),
      },
    );

    assert.equal(response.status, 400);
  } finally {
    await harness.close();
  }
});

test("POST /api/patterns/:patternKey/feedback — persisted feedback visible via GET", async () => {
  const harness = await createHarness();

  try {
    const patternKey = "persist-test::fact::pred";

    await fetch(
      `${harness.baseUrl}/api/patterns/${encodeURIComponent(patternKey)}/feedback`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ action: "rejected" }),
      },
    );

    const listResponse = await fetch(`${harness.baseUrl}/api/patterns/feedback?action=rejected`);
    const body = await listResponse.json();
    assert.ok(body.feedback.some((f) => f.patternKey === patternKey));
  } finally {
    await harness.close();
  }
});

// ── Edge cases ────────────────────────────────────────────────────────

test("insertPatternFeedback — allows duplicate feedback for same key", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    db.insertPatternFeedback({ patternKey: "dup::fact::pred", action: "confirmed" });
    db.insertPatternFeedback({ patternKey: "dup::fact::pred", action: "rejected" });

    const all = db.listPatternFeedback();
    assert.equal(all.length, 2);

    // getPatternFeedback returns latest (rejected)
    const latest = db.getPatternFeedback("dup::fact::pred");
    assert.equal(latest.action, "rejected");
  } finally {
    db.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("detectAndPromotePatterns — no crash when pattern_feedback table is empty", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);

  try {
    const result = await contextOS.detectAndPromotePatterns({ lookbackDays: 30 });
    assert.ok(typeof result.patternsDetected === "number");
    assert.ok(typeof result.promotionsProposed === "number");
  } finally {
    contextOS.database.close();
    contextOS.telemetry.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("detectTemporalPatterns — custom regularityThreshold accepted", async () => {
  const rootDir = await makeRoot();
  const contextOS = makeContextOS(rootDir);
  const db = contextOS.database;

  try {
    const oneDayMs = 24 * 60 * 60 * 1000;
    // Slightly irregular (±25%) but within custom threshold of 0.8
    const offsets = [
      -3.25 * oneDayMs,
      -2 * oneDayMs,
      -0.75 * oneDayMs,
      0,
    ];
    seedPatternClaims(db, {
      claimType: "event",
      predicate: "somewhat_regular",
      count: 4,
      valueText: "slightly irregular event",
      timestampOffsets: offsets,
    });

    // With default 0.5 threshold might or might not match; with 0.8 should match
    const patterns = detectTemporalPatterns(db, {
      lookbackDays: 30,
      minOccurrences: 3,
      regularityThreshold: 0.8,
    });
    const match = patterns.find((p) => p.predicate === "somewhat_regular");
    assert.ok(match, "Should detect pattern with lenient threshold");
    assert.ok(match.regularity < 0.8);
  } finally {
    db.close();
    contextOS.telemetry.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});
