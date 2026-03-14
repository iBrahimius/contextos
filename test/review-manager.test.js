import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";
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

function seedQueuedProposal(contextOS, detail = "Queued mutation for review") {
  return contextOS.database.insertGraphProposal({
    conversationId: null,
    messageId: null,
    actorId: "seed",
    proposalType: "add_task",
    detail,
    confidence: 0.8,
    status: "proposed",
    payload: {
      title: detail,
      type: "add_task",
    },
    writeClass: "ai_proposed",
  });
}

async function createHarness({
  initialDate,
  deferInit = false,
  processReview = null,
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
