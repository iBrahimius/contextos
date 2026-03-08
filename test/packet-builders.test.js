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
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-packet-builders-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  return root;
}

async function createHarness() {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({
    rootDir,
    autoBackfillEmbeddings: false,
  });
  contextOS.enqueueMessageEmbedding = () => null;
  contextOS.enqueueObservationEmbedding = () => null;

  return {
    rootDir,
    contextOS,
    conversation: null,
    async ingestAndPatch(content, category, detail, overrides = {}) {
      const capture = await contextOS.ingestMessage({
        conversationId: this.conversation?.id ?? null,
        conversationTitle: "Packet Builders Test",
        role: overrides.role ?? "user",
        direction: overrides.direction ?? "inbound",
        actorId: overrides.actorId ?? "user:test",
        originKind: null,
        ingestId: null,
        content,
        scopeKind: overrides.scopeKind ?? "project",
        scopeId: overrides.scopeId ?? "proj-packets",
      });
      if (!this.conversation) {
        this.conversation = { id: capture.conversationId };
      }

      const patch = {
        observations: [{
          category,
          predicate: overrides.predicate ?? "status",
          detail,
          confidence: overrides.confidence ?? 0.9,
          ...(overrides.subjectEntityId ? { subjectEntityId: overrides.subjectEntityId } : {}),
          ...(overrides.metadata ? { metadata: overrides.metadata } : {}),
        }],
        entities: overrides.entities ?? [],
        relationships: [],
        graphProposals: [],
        retrieveHints: [],
        complexityAdjustments: [],
      };

      await persistPatchForMessage(contextOS, capture, patch, {
        scopeKind: overrides.scopeKind ?? "project",
        scopeId: overrides.scopeId ?? "proj-packets",
      });

      return capture;
    },
    async close() {
      await contextOS.close();
      await fs.rm(rootDir, { recursive: true, force: true });
    },
  };
}

async function createApiHarness() {
  const harness = await createHarness();
  const server = http.createServer((request, response) => handleRequest(
    harness.contextOS,
    harness.rootDir,
    request,
    response,
  ));

  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();
  const closeHarness = harness.close.bind(harness);

  return {
    ...harness,
    baseUrl: `http://127.0.0.1:${port}`,
    async close() {
      await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
      await closeHarness();
    },
  };
}

async function seedPacketData(harness) {
  await harness.ingestAndPatch(
    "Always respect DNS cutover rules",
    "rule",
    "Keep DNS changes documented before rollout",
    { predicate: "dns_rule" },
  );
  await harness.ingestAndPatch(
    "Cloudflare remains mandatory for DNS",
    "constraint",
    "Use Cloudflare for all DNS management",
    {
      predicate: "dns_constraint",
      metadata: { severity: "critical" },
    },
  );
  await harness.ingestAndPatch(
    "We decided to keep Cloudflare for DNS",
    "decision",
    "Use Cloudflare for DNS",
    { predicate: "dns_provider" },
  );
  await harness.ingestAndPatch(
    "The DNS cutover checklist is blocked on registrar access",
    "task",
    "Finish the DNS cutover checklist",
    {
      predicate: "dns_task",
      metadata: { status: "blocked", priority: "high", blocker: "Registrar access pending" },
    },
  );
  const taskClaim = harness.contextOS.database.prepare(`
    SELECT id
    FROM claims
    WHERE claim_type = 'task'
    ORDER BY rowid DESC
    LIMIT 1
  `).get();
  harness.contextOS.database.updateClaim(taskClaim.id, {
    metadata_json: { status: "blocked", priority: "high", blocker: "Registrar access pending" },
  });
  await harness.ingestAndPatch(
    "Ship the DNS migration safely",
    "goal",
    "Complete the DNS migration without downtime",
    { predicate: "dns_goal", metadata: { status: "active" } },
  );
}

// --- buildStablePrefix ---

test("buildStablePrefix returns empty structure when no claims exist", async () => {
  const harness = await createHarness();
  try {
    const prefix = harness.contextOS.buildStablePrefix({ tokenBudget: 400 });
    assert.ok(prefix);
    assert.ok(Array.isArray(prefix.profile.preferences));
    assert.ok(Array.isArray(prefix.profile.rules));
    assert.ok(Array.isArray(prefix.profile.facts));
    assert.ok(Array.isArray(prefix.hard_constraints));
    assert.ok(typeof prefix.tokenCount === "number");
  } finally {
    await harness.close();
  }
});

test("buildStablePrefix includes active rule and constraint claims", async () => {
  const harness = await createHarness();
  try {
    await harness.ingestAndPatch(
      "Always use Cloudflare for DNS",
      "constraint",
      "Use Cloudflare for all DNS management",
      { metadata: { severity: "high" } },
    );

    const prefix = harness.contextOS.buildStablePrefix({
      scopeFilter: { scopeKind: "project", scopeId: "proj-packets" },
      tokenBudget: 800,
    });

    const totalItems = prefix.profile.rules.length
      + prefix.profile.preferences.length
      + prefix.profile.facts.length
      + prefix.hard_constraints.length;
    assert.ok(totalItems > 0, "stable prefix should contain at least one item from claims");
  } finally {
    await harness.close();
  }
});

// --- buildActiveState ---

test("buildActiveState returns empty structure when no claims exist", async () => {
  const harness = await createHarness();
  try {
    const state = harness.contextOS.buildActiveState({ tokenBudget: 600 });
    assert.ok(state);
    assert.ok(Array.isArray(state.tasks));
    assert.ok(Array.isArray(state.decisions));
    assert.ok(Array.isArray(state.goals));
    assert.ok(typeof state.tokenCount === "number");
  } finally {
    await harness.close();
  }
});

test("buildActiveState returns task claims", async () => {
  const harness = await createHarness();
  try {
    await harness.ingestAndPatch(
      "Build the context packet assembler",
      "task",
      "Build context packet assembler",
      { metadata: { status: "active", priority: "high" } },
    );

    const state = harness.contextOS.buildActiveState({
      scopeFilter: { scopeKind: "project", scopeId: "proj-packets" },
      limit: 6,
      tokenBudget: 600,
    });

    // Should have at least one task (from claim or fallback registry)
    assert.ok(state.tasks.length > 0 || state.decisions.length > 0,
      "active state should contain at least one task or decision");
  } finally {
    await harness.close();
  }
});

test("buildActiveState respects token budget", async () => {
  const harness = await createHarness();
  try {
    // Create several tasks
    for (let i = 0; i < 5; i++) {
      await harness.ingestAndPatch(
        `Task ${i}: do something important ${i}`,
        "task",
        `Important task number ${i} with a long description to consume tokens`,
        { metadata: { status: "active", priority: "medium" } },
      );
    }

    const state = harness.contextOS.buildActiveState({
      scopeFilter: { scopeKind: "project", scopeId: "proj-packets" },
      tokenBudget: 100, // Very tight budget
    });

    assert.ok(state.tokenCount <= 150, // Allow some overhead
      `token count ${state.tokenCount} exceeds tight budget`);
  } finally {
    await harness.close();
  }
});

// --- buildWorkingSet ---

test("buildWorkingSet returns structure with focus_claims and conflicts", async () => {
  const harness = await createHarness();
  try {
    const ws = harness.contextOS.buildWorkingSet({ tokenBudget: 600 });
    assert.ok(ws);
    assert.ok(Array.isArray(ws.focus_claims));
    assert.ok(Array.isArray(ws.unresolved_conflicts));
    assert.ok(Array.isArray(ws.focus_entities));
    assert.ok(typeof ws.tokenCount === "number");
    assert.ok(typeof ws.claims_scanned === "number");
  } finally {
    await harness.close();
  }
});

test("buildWorkingSet filters claims by strategy claim types", async () => {
  const harness = await createHarness();
  try {
    await harness.ingestAndPatch("DNS is on Cloudflare", "decision", "Use Cloudflare for DNS");
    await harness.ingestAndPatch("Ibrahim prefers dark mode", "fact", "Prefers dark mode");

    // Strategy that only wants decisions
    const ws = harness.contextOS.buildWorkingSet({
      strategy: {
        claimTypes: ["decision"],
        claimStates: ["active", "candidate"],
        evidenceRatio: 0.4,
        messageRatio: 0.3,
      },
      scopeFilter: { scopeKind: "project", scopeId: "proj-packets" },
      tokenBudget: 600,
    });

    // Focus claims should only contain decision-type claims (if any matched)
    for (const claim of ws.focus_claims) {
      if (claim.type) {
        assert.equal(claim.type, "decision",
          `expected only decision claims but got ${claim.type}`);
      }
    }
  } finally {
    await harness.close();
  }
});

// --- buildEvidence ---

test("buildEvidence returns empty when no retrieval provided", async () => {
  const harness = await createHarness();
  try {
    const ev = harness.contextOS.buildEvidence({ retrieval: null, tokenBudget: 800 });
    assert.ok(ev);
    assert.deepEqual(ev.structured, []);
    assert.deepEqual(ev.messages, []);
    assert.equal(ev.tokenCount, 0);
  } finally {
    await harness.close();
  }
});

test("buildEvidence respects token budget", async () => {
  const harness = await createHarness();
  try {
    // Create mock retrieval results
    const items = [];
    for (let i = 0; i < 10; i++) {
      items.push({
        type: "decision",
        id: `obs_${i}`,
        summary: `Decision ${i}: ${"x".repeat(200)}`,
        score: 0.9 - i * 0.05,
        payload: {},
        entityId: null,
      });
    }

    const ev = harness.contextOS.buildEvidence({
      retrieval: { items },
      tokenBudget: 200,
      strategy: { evidenceRatio: 0.6, messageRatio: 0.4 },
    });

    assert.ok(ev.tokenCount <= 250, // Allow some overhead
      `evidence token count ${ev.tokenCount} significantly exceeds budget`);
  } finally {
    await harness.close();
  }
});

// --- annotateResultsWithClaims (recall enrichment) ---

test("annotateResultsWithClaims adds claim field to results", async () => {
  const harness = await createHarness();
  try {
    await harness.ingestAndPatch("Use Cloudflare for DNS", "decision", "DNS on Cloudflare");

    // Get the observation ID
    const observations = harness.contextOS.database.prepare(
      "SELECT id FROM observations ORDER BY rowid DESC LIMIT 1"
    ).all();
    assert.ok(observations.length > 0);

    const items = [{
      type: "decision",
      id: observations[0].id,
      summary: "DNS on Cloudflare",
      score: 0.9,
      payload: { category: "decision" },
    }];

    const annotated = harness.contextOS.annotateResultsWithClaims(items);
    assert.equal(annotated.length, 1);
    assert.ok(annotated[0].claim !== undefined, "result should have claim field");
    if (annotated[0].claim) {
      assert.ok(annotated[0].claim.id, "claim should have id");
      assert.ok(annotated[0].claim.lifecycle_state, "claim should have lifecycle_state");
    }
  } finally {
    await harness.close();
  }
});

test("annotateResultsWithClaims returns claim:null for items without claims", async () => {
  const harness = await createHarness();
  try {
    const items = [{
      type: "message",
      id: "msg_nonexistent",
      summary: "some message",
      score: 0.5,
      payload: {},
    }];

    const annotated = harness.contextOS.annotateResultsWithClaims(items);
    assert.equal(annotated.length, 1);
    assert.equal(annotated[0].claim, null);
  } finally {
    await harness.close();
  }
});

test("annotateResultsWithClaims handles empty items array", async () => {
  const harness = await createHarness();
  try {
    const annotated = harness.contextOS.annotateResultsWithClaims([]);
    assert.deepEqual(annotated, []);
  } finally {
    await harness.close();
  }
});

// --- recall with claim enrichment ---

test("recall returns claim field on results", async () => {
  const harness = await createHarness();
  try {
    await harness.ingestAndPatch(
      "We decided to use Vercel for hosting",
      "decision",
      "Use Vercel for hosting",
    );

    const result = await harness.contextOS.recall({
      query: "orphan recall",
      scope: "recent",
      tokenBudget: 80,
      scopeFilter: { scopeKind: "project", scopeId: "proj-packets" },
    });

    // Every evidence item should have a claim field (could be null)
    for (const item of result.evidence) {
      assert.ok("claim" in item, `evidence item missing claim field: ${JSON.stringify(item)}`);
    }
  } finally {
    await harness.close();
  }
});

// --- contextPacket / memoryBrief ---

test("contextPacket returns valid packet structure with all tiers", async () => {
  const harness = await createHarness();
  try {
    await seedPacketData(harness);

    const packet = await harness.contextOS.contextPacket({
      query: "Cloudflare DNS",
      scopeFilter: { scopeKind: "project", scopeId: "proj-packets" },
      tokenBudget: 1600,
    });

    assert.equal(packet.query, "Cloudflare DNS");
    assert.ok(packet.timestamp);
    assert.ok(typeof packet.graph_version === "number");
    assert.ok(packet.stable_prefix);
    assert.ok(packet.active_state);
    assert.ok(packet.working_set);
    assert.ok(packet.evidence);
    assert.ok(packet.stable_prefix.hard_constraints.length > 0);
    assert.ok(packet.active_state.tasks.length > 0);
    assert.ok(packet.working_set.focus_claims.length > 0);
    assert.ok(packet.evidence.structured.length + packet.evidence.messages.length > 0);
    assert.ok(Array.isArray(packet.high_signal_alerts));
    assert.ok(packet.diagnostics.token_count > 0);
    assert.ok(packet.diagnostics.tier_tokens.stable > 0);
    assert.ok(packet.diagnostics.tier_tokens.active > 0);
  } finally {
    await harness.close();
  }
});

test("contextPacket auto-classifies intent from query", async () => {
  const harness = await createHarness();
  try {
    await seedPacketData(harness);

    const packet = await harness.contextOS.contextPacket({
      query: "What's our DNS provider?",
      scopeFilter: { scopeKind: "project", scopeId: "proj-packets" },
    });

    assert.equal(packet.intent, "current-state");
    assert.equal(packet.diagnostics.intent_source, "rules");
  } finally {
    await harness.close();
  }
});

test("contextPacket accepts explicit intent override", async () => {
  const harness = await createHarness();
  try {
    await seedPacketData(harness);

    const packet = await harness.contextOS.contextPacket({
      query: "What's our DNS provider?",
      intent: "history",
      scopeFilter: { scopeKind: "project", scopeId: "proj-packets" },
    });

    assert.equal(packet.intent, "history");
    assert.equal(packet.diagnostics.intent_source, "explicit");
  } finally {
    await harness.close();
  }
});

test("contextPacket with empty query returns general packet", async () => {
  const harness = await createHarness();
  try {
    await seedPacketData(harness);

    const packet = await harness.contextOS.contextPacket({
      query: "",
      scopeFilter: { scopeKind: "project", scopeId: "proj-packets" },
    });

    assert.equal(packet.intent, "general");
    assert.equal(packet.query, "");
    assert.equal(packet.diagnostics.intent_source, "default");
    assert.deepEqual(packet.evidence, {
      structured: [],
      messages: [],
    });
  } finally {
    await harness.close();
  }
});

test("memoryBrief returns valid brief structure", async () => {
  const harness = await createHarness();
  try {
    await seedPacketData(harness);

    const brief = await harness.contextOS.memoryBrief({
      query: "What should I work on next?",
      scopeFilter: { scopeKind: "project", scopeId: "proj-packets" },
    });

    assert.ok(brief.timestamp);
    assert.ok(typeof brief.graph_version === "number");
    assert.ok(brief.profile);
    assert.ok(Array.isArray(brief.active_work.tasks));
    assert.ok(Array.isArray(brief.active_work.goals));
    assert.ok(Array.isArray(brief.recent_decisions));
    assert.ok(Array.isArray(brief.active_constraints));
    assert.ok(Array.isArray(brief.unresolved_conflicts));
    assert.ok(Array.isArray(brief.high_signal_alerts));
    assert.ok(typeof brief.token_count === "number");
    assert.ok(typeof brief.claims_total === "number");
    assert.ok(brief.high_signal_alerts.some((alert) => alert.type === "blocked_task"));
  } finally {
    await harness.close();
  }
});

test("POST /api/context-packet endpoint works", async () => {
  const harness = await createApiHarness();
  try {
    await seedPacketData(harness);

    const response = await fetch(`${harness.baseUrl}/api/context-packet`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        query: "What's our DNS provider?",
        token_budget: 1400,
        scope_filter: {
          scope_kind: "project",
          scope_id: "proj-packets",
        },
      }),
    });

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.intent, "current-state");
    assert.ok(payload.stable_prefix);
    assert.ok(payload.active_state);
    assert.ok(payload.working_set);
    assert.ok(payload.evidence);
    assert.ok(Array.isArray(payload.high_signal_alerts));
  } finally {
    await harness.close();
  }
});

test("POST /api/memory-brief endpoint works", async () => {
  const harness = await createApiHarness();
  try {
    await seedPacketData(harness);

    const response = await fetch(`${harness.baseUrl}/api/memory-brief`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        query: "What should I work on next?",
        token_budget: 1800,
        scope_filter: {
          scope_kind: "project",
          scope_id: "proj-packets",
        },
      }),
    });

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.ok(payload.profile);
    assert.ok(payload.active_work);
    assert.ok(Array.isArray(payload.recent_decisions));
    assert.ok(Array.isArray(payload.active_constraints));
    assert.ok(Array.isArray(payload.high_signal_alerts));
    assert.ok(typeof payload.claims_total === "number");
  } finally {
    await harness.close();
  }
});
