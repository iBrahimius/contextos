import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";
import { classifyWriteClass } from "../src/core/write-discipline.js";
import { ContextDatabase } from "../src/db/database.js";

function createSilentLogger() {
  return {
    info() {},
    error() {},
  };
}

function createDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });

  return { promise, resolve, reject };
}

function createManualScheduler(initialDate) {
  let now = initialDate.getTime();
  let nextId = 1;
  const timers = new Map();

  async function runDueTimers() {
    while (true) {
      const due = [...timers.values()]
        .filter((timer) => timer.at <= now)
        .sort((left, right) => left.at - right.at || left.id - right.id)[0];
      if (!due) {
        break;
      }

      timers.delete(due.id);
      due.callback();
      await Promise.resolve();
    }
  }

  return {
    now: () => now,
    setTimeout(callback, delay) {
      const handle = {
        id: nextId++,
        unref() {},
      };
      timers.set(handle.id, {
        id: handle.id,
        at: now + Math.max(0, Number(delay) || 0),
        callback,
      });
      return handle;
    },
    clearTimeout(handle) {
      if (handle?.id) {
        timers.delete(handle.id);
      }
    },
    async advanceBy(milliseconds) {
      now += milliseconds;
      await runDueTimers();
    },
    async advanceTo(target) {
      now = target instanceof Date ? target.getTime() : Number(target);
      await runDueTimers();
    },
  };
}

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-review-manager-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  return root;
}

function buildProposalPayload({ proposalType, detail, subjectLabel = "ContextOS", predicate = null, objectLabel = null }) {
  const normalizedType = String(proposalType ?? "").replace(/^add_/, "");

  if (normalizedType === "relationship") {
    return {
      type: "relationship",
      subjectLabel,
      predicate: predicate ?? "related_to",
      objectLabel: objectLabel ?? "SQLite",
      detail,
    };
  }

  if (normalizedType === "fact") {
    return {
      type: "fact",
      entity: subjectLabel,
      detail,
    };
  }

  if (normalizedType === "decision") {
    return {
      type: proposalType,
      entity: subjectLabel,
      choice: detail,
    };
  }

  if (normalizedType === "constraint") {
    return {
      type: proposalType,
      entity: subjectLabel,
      content: detail,
    };
  }

  return {
    type: proposalType,
    entity: subjectLabel,
    title: detail,
  };
}

function seedReviewProposal(contextOS, {
  proposalType = "add_task",
  detail = "Queued mutation for review",
  confidence = 0.8,
  status = "proposed",
  subjectLabel = "ContextOS",
  predicate = null,
  objectLabel = null,
  payload = null,
  writeClass = classifyWriteClass(proposalType),
  createdAt = null,
} = {}) {
  const stored = contextOS.database.insertGraphProposal({
    conversationId: null,
    messageId: null,
    actorId: "seed",
    proposalType,
    subjectLabel,
    predicate,
    objectLabel,
    detail,
    confidence,
    status,
    payload: payload ?? buildProposalPayload({
      proposalType,
      detail,
      subjectLabel,
      predicate,
      objectLabel,
    }),
    writeClass,
  });

  if (createdAt) {
    const timestamp = createdAt instanceof Date ? createdAt.toISOString() : String(createdAt);
    contextOS.database.prepare(`
      UPDATE graph_proposals
      SET created_at = ?
      WHERE id = ?
    `).run(timestamp, stored.id);
  }

  return stored;
}

function seedQueuedProposal(contextOS, detail = "Queued mutation for review") {
  return seedReviewProposal(contextOS, { detail });
}

async function createHarness({
  initialDate,
  deferInit = false,
  processReview = null,
  reviewManagerOptions = {},
} = {}) {
  const rootDir = await makeRoot();
  const scheduler = createManualScheduler(initialDate ?? new Date(2026, 0, 5, 12, 0, 0, 0));
  const contextOS = new ContextOS({
    rootDir,
    autoBackfillEmbeddings: false,
    deferInit,
    reviewManagerOptions: {
      now: scheduler.now,
      setTimeout: scheduler.setTimeout,
      clearTimeout: scheduler.clearTimeout,
      processReview,
      logger: createSilentLogger(),
      ...reviewManagerOptions,
    },
  });
  if (!deferInit) {
    contextOS.reviewManager.start();
  }

  let closed = false;
  return {
    rootDir,
    scheduler,
    contextOS,
    async close() {
      if (closed) {
        return;
      }

      closed = true;
      await contextOS.close();
      await fs.rm(rootDir, { recursive: true, force: true });
    },
  };
}

test("ReviewManager triggers automatically when 50 queued mutations accumulate", async () => {
  const reviews = [];
  const harness = await createHarness({
    initialDate: new Date(2026, 0, 5, 12, 0, 0, 0),
    processReview: ({ trigger, queueStats }) => {
      reviews.push({ trigger, queueStats });
      return { reviewed_total: queueStats.total };
    },
  });

  try {
    seedQueuedProposal(harness.contextOS, "Count threshold task");
    harness.contextOS.reviewManager.noteQueuedMutations({
      count: 50,
      source: "test_count_threshold",
    });

    await harness.contextOS.reviewManager.waitForIdle();

    assert.equal(reviews.length, 1);
    assert.equal(reviews[0].trigger.reason, "mutation_count_threshold");
    assert.equal(reviews[0].queueStats.total, 1);

    const state = harness.contextOS.database.getReviewState();
    assert.equal(state.mutationsSinceLastReview, 0);
    assert.equal(state.reviewInProgress, false);
    assert.ok(state.lastReviewAt);
  } finally {
    await harness.close();
  }
});

test("ReviewManager defers quiet-hour triggers and runs them at morning catch-up", async () => {
  const reviews = [];
  const initialDate = new Date(2026, 0, 5, 23, 15, 0, 0);
  const harness = await createHarness({
    initialDate,
    processReview: ({ trigger }) => {
      reviews.push(trigger.reason);
      return { ok: true };
    },
  });

  try {
    seedQueuedProposal(harness.contextOS, "Quiet-hours queued task");
    harness.contextOS.reviewManager.noteQueuedMutations({
      count: 50,
      source: "test_quiet_hours",
    });

    await Promise.resolve();
    await harness.contextOS.reviewManager.waitForIdle();
    assert.deepEqual(reviews, []);

    const queuedStatus = harness.contextOS.reviewManager.getStatus();
    assert.equal(queuedStatus.quiet_hours.active, true);
    assert.equal(queuedStatus.quiet_hours.queued_for_morning, true);

    await harness.scheduler.advanceTo(new Date(2026, 0, 6, 8, 0, 0, 0));
    await harness.contextOS.reviewManager.waitForIdle();

    assert.deepEqual(reviews, ["morning_catch_up"]);
  } finally {
    await harness.close();
  }
});

test("ReviewManager skips triggers while a review is already in progress", async () => {
  const deferred = createDeferred();
  const harness = await createHarness({
    initialDate: new Date(2026, 0, 5, 13, 0, 0, 0),
    processReview: () => deferred.promise,
  });

  try {
    const firstTrigger = harness.contextOS.reviewManager.trigger({
      source: "manual",
      reason: "manual_trigger",
    });
    await Promise.resolve();

    const skipped = await harness.contextOS.reviewManager.trigger({
      source: "manual",
      reason: "manual_trigger",
    });

    assert.equal(skipped.status, "skipped");
    assert.equal(skipped.reason, "review_in_progress");
    assert.equal(harness.contextOS.database.getReviewState().reviewInProgress, true);

    deferred.resolve({ ok: true });
    const completed = await firstTrigger;
    assert.equal(completed.status, "completed");
    assert.equal(harness.contextOS.database.getReviewState().reviewInProgress, false);
  } finally {
    await harness.close();
  }
});

test("ReviewManager resets persisted in-progress state during init for crash recovery", async () => {
  const rootDir = await makeRoot();
  const dbPath = path.join(rootDir, "data", "contextos.db");
  const seedDatabase = new ContextDatabase(dbPath);
  seedDatabase.updateReviewState({
    lastReviewAt: new Date(2026, 0, 5, 6, 0, 0, 0).toISOString(),
    mutationsSinceLastReview: 12,
    reviewInProgress: true,
  });
  seedDatabase.close();

  const scheduler = createManualScheduler(new Date(2026, 0, 5, 9, 0, 0, 0));
  const contextOS = new ContextOS({
    rootDir,
    autoBackfillEmbeddings: false,
    deferInit: true,
    reviewManagerOptions: {
      now: scheduler.now,
      setTimeout: scheduler.setTimeout,
      clearTimeout: scheduler.clearTimeout,
      logger: createSilentLogger(),
    },
  });

  try {
    assert.equal(contextOS.database.getReviewState().reviewInProgress, true);
    await contextOS.init();

    const recovered = contextOS.database.getReviewState();
    assert.equal(recovered.reviewInProgress, false);
    assert.equal(recovered.mutationsSinceLastReview, 12);
    assert.equal(recovered.lastReviewAt, new Date(2026, 0, 5, 6, 0, 0, 0).toISOString());
  } finally {
    await contextOS.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("ContextDatabase persists review state updates across reopen", async () => {
  const rootDir = await makeRoot();
  const dbPath = path.join(rootDir, "data", "contextos.db");
  const initialDatabase = new ContextDatabase(dbPath);

  try {
    assert.deepEqual(initialDatabase.getReviewState(), {
      lastReviewAt: null,
      mutationsSinceLastReview: 0,
      reviewInProgress: false,
    });

    const lastReviewAt = new Date(2026, 0, 5, 7, 30, 0, 0).toISOString();
    const updated = initialDatabase.updateReviewState({
      lastReviewAt,
      mutationsSinceLastReview: 7,
      reviewInProgress: true,
    });

    assert.deepEqual(updated, {
      lastReviewAt,
      mutationsSinceLastReview: 7,
      reviewInProgress: true,
    });
  } finally {
    initialDatabase.close();
  }

  const reopenedDatabase = new ContextDatabase(dbPath);
  try {
    assert.deepEqual(reopenedDatabase.getReviewState(), {
      lastReviewAt: new Date(2026, 0, 5, 7, 30, 0, 0).toISOString(),
      mutationsSinceLastReview: 7,
      reviewInProgress: true,
    });
  } finally {
    reopenedDatabase.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("ReviewManager runs overdue time-based reviews and the scheduled end-of-day sweep", async () => {
  const reviews = [];
  const harness = await createHarness({
    initialDate: new Date(2026, 0, 5, 10, 0, 0, 0),
    processReview: ({ trigger }) => {
      reviews.push(trigger.reason);
      return { ok: true };
    },
  });

  try {
    seedQueuedProposal(harness.contextOS, "Time threshold task");
    harness.contextOS.database.updateReviewState({
      lastReviewAt: new Date(2026, 0, 5, 8, 0, 0, 0).toISOString(),
      mutationsSinceLastReview: 0,
      reviewInProgress: false,
    });
    harness.contextOS.reviewManager.noteQueuedMutations({
      count: 1,
      source: "test_time_threshold",
    });

    await harness.contextOS.reviewManager.waitForIdle();
    assert.deepEqual(reviews, ["time_threshold"]);

    await harness.close();

    const eveningHarness = await createHarness({
      initialDate: new Date(2026, 0, 5, 22, 29, 0, 0),
      processReview: ({ trigger, queueStats }) => {
        reviews.push(`${trigger.reason}:${queueStats.total}`);
        return { ok: true };
      },
    });

    try {
      seedQueuedProposal(eveningHarness.contextOS, "End of day task");
      await eveningHarness.scheduler.advanceBy(60 * 1000);
      await eveningHarness.contextOS.reviewManager.waitForIdle();

      assert.deepEqual(reviews, ["time_threshold", "end_of_day_sweep:1"]);
    } finally {
      await eveningHarness.close();
    }
  } finally {
    await harness.close();
  }
});

test("ReviewManager auto-applies high-confidence fact and relationship mutations during sweeps", async () => {
  const harness = await createHarness({
    initialDate: new Date(2026, 0, 5, 14, 0, 0, 0),
  });

  try {
    const fact = seedReviewProposal(harness.contextOS, {
      proposalType: "fact",
      detail: "ContextOS keeps SQLite local.",
      confidence: 0.92,
      subjectLabel: "ContextOS",
    });
    const relationship = seedReviewProposal(harness.contextOS, {
      proposalType: "relationship",
      detail: "ContextOS depends on SQLite.",
      confidence: 0.9,
      subjectLabel: "ContextOS",
      predicate: "depends_on",
      objectLabel: "SQLite",
    });

    const result = await harness.contextOS.reviewManager.trigger({
      source: "manual",
      reason: "test_auto_apply",
    });
    const appliedById = new Map(result.review.auto_applied.results.map((entry) => [entry.mutation_id, entry]));

    assert.equal(result.status, "completed");
    assert.equal(result.review.action, "auto_review_policy");
    assert.equal(result.review.auto_applied.count, 2);
    assert.equal(result.review.auto_expired.count, 0);
    assert.equal(harness.contextOS.database.getGraphProposal(fact.id).status, "accepted");
    assert.equal(harness.contextOS.database.getGraphProposal(relationship.id).status, "accepted");
    assert.ok(appliedById.get(fact.id)?.applied?.fact_id);
    assert.ok(appliedById.get(relationship.id)?.applied?.relationship_id);
  } finally {
    await harness.close();
  }
});

test("ReviewManager auto-expires parked mutations older than 30 days", async () => {
  const initialDate = new Date(2026, 1, 5, 12, 0, 0, 0);
  const harness = await createHarness({ initialDate });

  try {
    const oldParked = seedReviewProposal(harness.contextOS, {
      proposalType: "fact",
      detail: "Old parked fact",
      confidence: 0.4,
      createdAt: new Date(initialDate.getTime() - (31 * 24 * 60 * 60 * 1000)),
    });
    const recentParked = seedReviewProposal(harness.contextOS, {
      proposalType: "fact",
      detail: "Recent parked fact",
      confidence: 0.45,
      createdAt: new Date(initialDate.getTime() - (10 * 24 * 60 * 60 * 1000)),
    });

    const result = await harness.contextOS.reviewManager.trigger({
      source: "manual",
      reason: "test_auto_expire",
    });

    assert.equal(result.review.auto_applied.count, 0);
    assert.equal(result.review.auto_expired.count, 1);
    assert.equal(harness.contextOS.database.getGraphProposal(oldParked.id).status, "rejected");
    assert.equal(harness.contextOS.database.getGraphProposal(oldParked.id).reason, "auto_expired: parked_over_30_days");
    assert.equal(harness.contextOS.database.getGraphProposal(recentParked.id).status, "proposed");
  } finally {
    await harness.close();
  }
});

test("ReviewManager leaves tier 2 and tier 3 mutations queued during sweeps", async () => {
  const harness = await createHarness({
    initialDate: new Date(2026, 0, 5, 15, 0, 0, 0),
  });

  try {
    const task = seedReviewProposal(harness.contextOS, {
      proposalType: "add_task",
      detail: "Ship review queue policy",
      confidence: 0.97,
    });
    const decision = seedReviewProposal(harness.contextOS, {
      proposalType: "add_decision",
      detail: "Keep human approval for canonical writes",
      confidence: 0.99,
    });
    const constraint = seedReviewProposal(harness.contextOS, {
      proposalType: "add_constraint",
      detail: "Do not auto-apply constraint mutations",
      confidence: 0.99,
    });

    const result = await harness.contextOS.reviewManager.trigger({
      source: "manual",
      reason: "test_leave_queued",
    });

    assert.equal(result.review.auto_applied.count, 0);
    assert.equal(result.review.auto_expired.count, 0);
    assert.equal(harness.contextOS.database.getGraphProposal(task.id).status, "proposed");
    assert.equal(harness.contextOS.database.getGraphProposal(decision.id).status, "proposed");
    assert.equal(harness.contextOS.database.getGraphProposal(constraint.id).status, "proposed");
  } finally {
    await harness.close();
  }
});

test("ReviewManager configurable thresholds tune auto-apply types and auto-expiry", async () => {
  const initialDate = new Date(2026, 1, 5, 16, 0, 0, 0);
  const harness = await createHarness({
    initialDate,
    reviewManagerOptions: {
      autoApplyMinConfidence: 0.9,
      autoApplyTypes: ["fact"],
      autoExpireDays: 10,
    },
  });

  try {
    const belowThresholdFact = seedReviewProposal(harness.contextOS, {
      proposalType: "fact",
      detail: "Below threshold fact",
      confidence: 0.89,
    });
    const allowedFact = seedReviewProposal(harness.contextOS, {
      proposalType: "fact",
      detail: "High confidence fact",
      confidence: 0.93,
    });
    const excludedRelationship = seedReviewProposal(harness.contextOS, {
      proposalType: "relationship",
      detail: "Relationship excluded by config",
      confidence: 0.97,
      subjectLabel: "ContextOS",
      predicate: "depends_on",
      objectLabel: "SQLite",
    });
    const oldParked = seedReviewProposal(harness.contextOS, {
      proposalType: "fact",
      detail: "Old parked fact under custom threshold",
      confidence: 0.4,
      createdAt: new Date(initialDate.getTime() - (11 * 24 * 60 * 60 * 1000)),
    });
    const recentParked = seedReviewProposal(harness.contextOS, {
      proposalType: "fact",
      detail: "Recent parked fact under custom threshold",
      confidence: 0.4,
      createdAt: new Date(initialDate.getTime() - (9 * 24 * 60 * 60 * 1000)),
    });

    const result = await harness.contextOS.reviewManager.trigger({
      source: "manual",
      reason: "test_configurable_thresholds",
    });

    assert.equal(result.review.auto_apply_min_confidence, 0.9);
    assert.deepEqual(result.review.auto_apply_types, ["fact"]);
    assert.equal(result.review.auto_expire_days, 10);
    assert.equal(result.review.auto_applied.count, 1);
    assert.equal(result.review.auto_expired.count, 1);
    assert.equal(harness.contextOS.database.getGraphProposal(belowThresholdFact.id).status, "proposed");
    assert.equal(harness.contextOS.database.getGraphProposal(allowedFact.id).status, "accepted");
    assert.equal(harness.contextOS.database.getGraphProposal(excludedRelationship.id).status, "proposed");
    assert.equal(harness.contextOS.database.getGraphProposal(oldParked.id).status, "rejected");
    assert.equal(harness.contextOS.database.getGraphProposal(oldParked.id).reason, "auto_expired: parked_over_10_days");
    assert.equal(harness.contextOS.database.getGraphProposal(recentParked.id).status, "proposed");
  } finally {
    await harness.close();
  }
});
