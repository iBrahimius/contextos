import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";
import { ALERT_LEVELS, ALERT_TYPES, PushAlerter } from "../src/core/push-alerter.js";
import { handleRequest } from "../src/http/router.js";

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-alerts-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  return root;
}

function createClock(start = Date.parse("2026-03-14T12:00:00.000Z")) {
  let current = start;
  return {
    now: () => current,
    advance(milliseconds) {
      current += milliseconds;
      return current;
    },
  };
}

function createReviewManagerOptions() {
  return {
    setTimeout(callback, delay) {
      return {
        callback,
        delay,
        unref() {},
      };
    },
    clearTimeout() {},
  };
}

async function createContextOS({ sendAlert, deferInit = true, alerting = { minIntervalMs: 0, dedupWindowMs: 0 } } = {}) {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({
    rootDir,
    deferInit,
    autoBackfillEmbeddings: false,
    sendAlert,
    alerting,
    reviewManagerOptions: createReviewManagerOptions(),
  });

  return {
    rootDir,
    contextOS,
    async cleanup() {
      await contextOS.close();
      await fs.rm(rootDir, { recursive: true, force: true });
    },
  };
}

function createServer(contextOS, rootDir) {
  return http.createServer((request, response) => handleRequest(contextOS, rootDir, request, response));
}

test("PushAlerter dispatches all alert levels", async () => {
  const clock = createClock();
  const dispatched = [];
  const alerter = new PushAlerter({
    now: clock.now,
    minIntervalMs: 0,
    dedupWindowMs: 0,
    sendAlert(payload) {
      dispatched.push(payload);
    },
  });

  await alerter.alert(ALERT_LEVELS.INFO, "info_test", "Info alert", { key: "info" });
  await alerter.alert(ALERT_LEVELS.WARNING, "warning_test", "Warning alert", { key: "warning" });
  await alerter.alert(ALERT_LEVELS.ERROR, "error_test", "Error alert", { key: "error" });
  await alerter.alert(ALERT_LEVELS.CRITICAL, "critical_test", "Critical alert", { key: "critical" });

  assert.deepEqual(
    dispatched.map((payload) => payload.level),
    [ALERT_LEVELS.INFO, ALERT_LEVELS.WARNING, ALERT_LEVELS.ERROR, ALERT_LEVELS.CRITICAL],
  );
  assert.equal(alerter.getStatus().sentCount, 4);
});

test("PushAlerter rate limits alerts across dispatches", async () => {
  const clock = createClock();
  const dispatched = [];
  const alerter = new PushAlerter({
    now: clock.now,
    minIntervalMs: 30_000,
    dedupWindowMs: 0,
    sendAlert(payload) {
      dispatched.push(payload);
    },
  });

  const first = await alerter.alert(ALERT_LEVELS.ERROR, "pipeline_error", "First failure", { key: "first" });
  const second = await alerter.alert(ALERT_LEVELS.ERROR, "pipeline_error", "Second failure", { key: "second" });
  clock.advance(30_000);
  const third = await alerter.alert(ALERT_LEVELS.ERROR, "pipeline_error", "Third failure", { key: "third" });

  assert.equal(first.dispatched, true);
  assert.equal(second.dispatched, false);
  assert.equal(second.reason, "rate_limited");
  assert.equal(third.dispatched, true);
  assert.deepEqual(dispatched.map((payload) => payload.key), ["first", "third"]);
  assert.equal(alerter.getStatus().suppressedByReason.rate_limited, 1);
});

test("PushAlerter deduplicates by alert key within the dedup window", async () => {
  const clock = createClock();
  const dispatched = [];
  const alerter = new PushAlerter({
    now: clock.now,
    minIntervalMs: 0,
    dedupWindowMs: 30_000,
    sendAlert(payload) {
      dispatched.push(payload);
    },
  });

  const first = await alerter.alert(ALERT_LEVELS.WARNING, "duplicate_test", "Repeated alert", { key: "dup-key" });
  clock.advance(1_000);
  const duplicate = await alerter.alert(ALERT_LEVELS.WARNING, "duplicate_test", "Repeated alert", { key: "dup-key" });
  const differentKey = await alerter.alert(ALERT_LEVELS.WARNING, "duplicate_test", "Repeated alert", { key: "other-key" });
  clock.advance(30_000);
  const afterWindow = await alerter.alert(ALERT_LEVELS.WARNING, "duplicate_test", "Repeated alert", { key: "dup-key" });

  assert.equal(first.dispatched, true);
  assert.equal(duplicate.dispatched, false);
  assert.equal(duplicate.reason, "deduplicated");
  assert.equal(differentKey.dispatched, true);
  assert.equal(afterWindow.dispatched, true);
  assert.deepEqual(dispatched.map((payload) => payload.key), ["dup-key", "other-key", "dup-key"]);
  assert.equal(alerter.getStatus().suppressedByReason.deduplicated, 1);
});

test("PushAlerter exposes all pre-built alert types", async () => {
  const clock = createClock();
  const dispatched = [];
  const alerter = new PushAlerter({
    now: clock.now,
    minIntervalMs: 0,
    dedupWindowMs: 0,
    sendAlert(payload) {
      dispatched.push(payload);
    },
  });

  await alerter.pipelineError("persist_knowledge_patch", new Error("db unavailable"));
  await alerter.mutationFailed("proposal-1", new Error("auto-apply failed"));
  await alerter.classificationFailed("build_heuristic_patch", new Error("classifier offline"));
  await alerter.startupOk(12, 3);
  await alerter.startupFailed(new Error("graph load failed"));
  await alerter.embeddingError("message:msg-1", new Error("embedding runtime missing"));

  assert.deepEqual(
    dispatched.map((payload) => payload.type),
    [
      ALERT_TYPES.PIPELINE_ERROR,
      ALERT_TYPES.MUTATION_FAILED,
      ALERT_TYPES.CLASSIFICATION_FAILED,
      ALERT_TYPES.STARTUP_OK,
      ALERT_TYPES.STARTUP_FAILED,
      ALERT_TYPES.EMBEDDING_ERROR,
    ],
  );
  assert.deepEqual(
    dispatched.map((payload) => payload.level),
    [
      ALERT_LEVELS.ERROR,
      ALERT_LEVELS.WARNING,
      ALERT_LEVELS.ERROR,
      ALERT_LEVELS.INFO,
      ALERT_LEVELS.CRITICAL,
      ALERT_LEVELS.ERROR,
    ],
  );
});

test("ContextOS emits startup_ok and exposes alert status over HTTP", async () => {
  const alerts = [];
  const { contextOS, rootDir, cleanup } = await createContextOS({
    sendAlert(payload) {
      alerts.push(payload);
    },
  });
  const server = createServer(contextOS, rootDir);

  try {
    await contextOS.init();
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    const response = await fetch(`http://127.0.0.1:${address.port}/api/alerts/status`);
    const payload = await response.json();

    assert.equal(response.status, 200);
    assert.equal(alerts[0]?.type, ALERT_TYPES.STARTUP_OK);
    assert.equal(payload.sentCount, 1);
    assert.equal(payload.lastAlert?.type, ALERT_TYPES.STARTUP_OK);
    assert.equal(payload.lastAlert?.metadata?.reviewManagerStarted, true);
  } finally {
    await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
    await cleanup();
  }
});

test("ContextOS emits classification_failed when heuristic classification throws", async () => {
  const alerts = [];
  const { contextOS, cleanup } = await createContextOS({
    sendAlert(payload) {
      alerts.push(payload);
    },
    deferInit: false,
  });

  try {
    contextOS.classifier.classifyText = () => {
      throw new Error("classifier offline");
    };

    assert.throws(() => contextOS.buildHeuristicPatch("Classify this"), /classifier offline/);
    const classificationAlert = alerts.find((payload) => payload.type === ALERT_TYPES.CLASSIFICATION_FAILED);
    assert.ok(classificationAlert);
    assert.equal(classificationAlert.metadata.stage, "build_heuristic_patch");
  } finally {
    await cleanup();
  }
});

test("ContextOS emits pipeline_error when knowledge patch persistence fails", async () => {
  const alerts = [];
  const { contextOS, cleanup } = await createContextOS({
    sendAlert(payload) {
      alerts.push(payload);
    },
    deferInit: false,
  });

  try {
    const conversation = contextOS.database.createConversation("Alerts");
    const message = contextOS.database.insertMessage({
      conversationId: conversation.id,
      role: "user",
      direction: "inbound",
      actorId: "user:test",
      originKind: "user",
      content: "Remember this relationship",
      tokenCount: 4,
      raw: { seeded: true },
      ingestId: "push-alert-message",
    });
    const originalInsertGraphProposal = contextOS.database.insertGraphProposal.bind(contextOS.database);
    contextOS.database.insertGraphProposal = () => {
      throw new Error("proposal insert failed");
    };

    assert.throws(() => contextOS.persistKnowledgePatch({
      conversationId: conversation.id,
      messageId: message.id,
      patch: {
        entities: [],
        observations: [],
        graphProposals: [
          {
            proposalType: "relationship",
            subjectLabel: "ContextOS",
            predicate: "relates_to",
            objectLabel: "Alerts",
            detail: "ContextOS relates to alerts",
            confidence: 0.9,
          },
        ],
        retrieveHints: [],
        complexityAdjustments: [],
      },
    }), /proposal insert failed/);

    const pipelineAlert = alerts.find((payload) => payload.type === ALERT_TYPES.PIPELINE_ERROR);
    assert.ok(pipelineAlert);
    assert.equal(pipelineAlert.metadata.stage, "persist_knowledge_patch");
    contextOS.database.insertGraphProposal = originalInsertGraphProposal;
  } finally {
    await cleanup();
  }
});

test("ContextOS emits mutation_failed when auto-apply fails", async () => {
  const alerts = [];
  const { contextOS, cleanup } = await createContextOS({
    sendAlert(payload) {
      alerts.push(payload);
    },
    deferInit: false,
  });

  try {
    const originalApplyGraphProposal = contextOS.applyGraphProposal.bind(contextOS);
    contextOS.applyGraphProposal = () => {
      throw new Error("auto apply exploded");
    };

    const result = contextOS.proposeMutation({
      type: "add_entity",
      confidence: 0.95,
      payload: {
        label: "Push Alerts",
        kind: "feature",
      },
      actorId: "test-suite",
    });

    assert.equal(result.status, "proposed");
    const mutationAlert = alerts.find((payload) => payload.type === ALERT_TYPES.MUTATION_FAILED);
    assert.ok(mutationAlert);
    assert.equal(mutationAlert.metadata.mutationType, "add_entity");
    contextOS.applyGraphProposal = originalApplyGraphProposal;
  } finally {
    await cleanup();
  }
});
