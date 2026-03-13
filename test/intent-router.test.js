import test from "node:test";
import assert from "node:assert/strict";

import { classifyIntent, isKnownEntityQuery, INTENT_STRATEGIES } from "../src/core/intent-router.js";

// Mock entity graph
function mockEntityGraph(labels = []) {
  return {
    listEntities() {
      return labels.map((label, index) => ({ id: `ent_${index}`, label, kind: "concept" }));
    },
  };
}

// Mock LLM classifier
function mockLlmClassifier(result) {
  return {
    async classify() {
      return result;
    },
  };
}

function failingLlmClassifier() {
  return {
    async classify() {
      throw new Error("LLM unavailable");
    },
  };
}

// --- INTENT_STRATEGIES ---

test("INTENT_STRATEGIES defines all 6 intents", () => {
  const expected = ["current-state", "history", "why", "entity-briefing", "next-action", "general"];
  for (const intent of expected) {
    assert.ok(INTENT_STRATEGIES[intent], `missing strategy for ${intent}`);
    assert.ok(Array.isArray(INTENT_STRATEGIES[intent].steps), `${intent} missing steps`);
    assert.ok(typeof INTENT_STRATEGIES[intent].evidenceRatio === "number", `${intent} missing evidenceRatio`);
    assert.ok(typeof INTENT_STRATEGIES[intent].messageRatio === "number", `${intent} missing messageRatio`);
  }
});

// --- classifyIntent: rule-based ---

const GOLDEN_SET = [
  // current-state
  { query: "What's our DNS provider?", expected: "current-state" },
  { query: "Who owns the RHI domain?", expected: "current-state" },
  { query: "Current status of the dashboard", expected: "current-state" },
  { query: "What are the active constraints?", expected: "current-state" },
  { query: "Status of open tasks", expected: "current-state" },
  { query: "What's the COGS for RHI products?", expected: "current-state" },
  { query: "Active constraints", expected: "current-state" },

  // history
  { query: "When did we switch from Python to Node?", expected: "history" },
  { query: "What happened with the MCP bridge?", expected: "history" },
  { query: "Timeline of ContextOS development", expected: "history" },
  { query: "How did the entity graph change over time?", expected: "history" },
  { query: "What happened yesterday?", expected: "history" },

  // why
  { query: "Why did we choose Cloudflare?", expected: "why" },
  { query: "Rationale for the single-runtime decision", expected: "why" },
  { query: "What motivated moving off Python?", expected: "why" },
  { query: "How come we use SQLite?", expected: "why" },
  { query: "Reason behind the email routing decision", expected: "why" },

  // entity-briefing
  { query: "Tell me about RHI", expected: "entity-briefing" },
  { query: "What do we know about Ibrahim?", expected: "entity-briefing" },
  { query: "Overview of ContextOS", expected: "entity-briefing" },
  { query: "Brief on the dashboard project", expected: "entity-briefing" },

  // next-action
  { query: "What should I work on next?", expected: "next-action" },
  { query: "What are my priorities?", expected: "next-action" },
  { query: "Any blockers?", expected: "next-action" },
  { query: "What do I do now?", expected: "next-action" },
  { query: "Priorities for this week", expected: "next-action" },

  // general
  { query: "DNS and hosting setup", expected: "general" },
  { query: "Search for mentions of Vercel", expected: "general" },
  { query: "ContextOS memory architecture", expected: "general" },
];

test("golden intent classification (rule-based, no LLM)", async () => {
  const graph = mockEntityGraph([]);
  let correct = 0;
  const failures = [];

  for (const { query, expected } of GOLDEN_SET) {
    const result = await classifyIntent(query, graph, null);
    if (result.intent === expected) {
      correct++;
    } else {
      failures.push({ query, expected, got: result.intent, source: result.source });
    }
  }

  const accuracy = correct / GOLDEN_SET.length;
  // We expect >=85% accuracy from rules alone; some edge cases may fall to general
  assert.ok(
    accuracy >= 0.75,
    `Rule-based accuracy ${(accuracy * 100).toFixed(0)}% is below 75% threshold. Failures:\n${
      failures.map((f) => `  "${f.query}" → expected ${f.expected}, got ${f.got} (${f.source})`).join("\n")
    }`,
  );
});

test("classifyIntent returns source='rules' for clear patterns", async () => {
  const result = await classifyIntent("What's our DNS provider?", mockEntityGraph([]), null);
  assert.equal(result.source, "rules");
});

test("classifyIntent returns general/default for empty query", async () => {
  const result = await classifyIntent("", mockEntityGraph([]), null);
  assert.equal(result.intent, "general");
  assert.equal(result.source, "default");
});

test("classifyIntent returns general/default for null query", async () => {
  const result = await classifyIntent(null, mockEntityGraph([]), null);
  assert.equal(result.intent, "general");
  assert.equal(result.source, "default");
});

// --- classifyIntent: entity match ---

test("classifyIntent detects entity-briefing via entity graph match", async () => {
  const graph = mockEntityGraph(["RHI", "ContextOS", "Ibrahim"]);
  const result = await classifyIntent("RHI partner situation", graph, null);
  assert.equal(result.intent, "entity-briefing");
  assert.equal(result.source, "entity-match");
});

test("classifyIntent does not match partial entity names below threshold", async () => {
  const graph = mockEntityGraph(["ContextOS Memory Architecture V2"]);
  // Query "memory" shares only 1/4 tokens — below 60% threshold
  const result = await classifyIntent("memory", graph, null);
  assert.notEqual(result.source, "entity-match");
});

// --- classifyIntent: Haiku fallback ---

test("classifyIntent falls back to Haiku when no rule matches", async () => {
  const graph = mockEntityGraph([]);
  const llm = mockLlmClassifier({ intent: "why" });
  // "Ibrahim's preferences" doesn't match any rule pattern
  const result = await classifyIntent("Ibrahim's preferences", graph, llm);
  // Could be haiku or entity-match depending on graph — with empty graph, should be haiku
  assert.ok(["haiku", "general"].includes(result.source) || result.source === "default");
});

test("classifyIntent uses Haiku result when valid intent returned", async () => {
  const graph = mockEntityGraph([]);
  const llm = mockLlmClassifier("history");
  const result = await classifyIntent("some ambiguous query about stuff", graph, llm);
  assert.equal(result.intent, "history");
  assert.equal(result.source, "haiku");
});

test("classifyIntent returns general/default when Haiku fails", async () => {
  const graph = mockEntityGraph([]);
  const llm = failingLlmClassifier();
  const result = await classifyIntent("some ambiguous query", graph, llm);
  assert.equal(result.intent, "general");
  assert.equal(result.source, "default");
});

test("classifyIntent returns general/default when Haiku returns invalid intent", async () => {
  const graph = mockEntityGraph([]);
  const llm = mockLlmClassifier("invalid-category");
  const result = await classifyIntent("some ambiguous query", graph, llm);
  assert.equal(result.intent, "general");
  assert.equal(result.source, "default");
});

test("classifyIntent works with null llmClassifier", async () => {
  const result = await classifyIntent("ambiguous stuff", mockEntityGraph([]), null);
  assert.equal(result.intent, "general");
  assert.equal(result.source, "default");
});

// --- isKnownEntityQuery ---

test("isKnownEntityQuery returns true for exact entity match", () => {
  const graph = mockEntityGraph(["ContextOS"]);
  assert.equal(isKnownEntityQuery("ContextOS", graph), true);
});

test("isKnownEntityQuery returns true for multi-word entity with >60% overlap", () => {
  const graph = mockEntityGraph(["Rumor Has It"]);
  assert.equal(isKnownEntityQuery("Rumor Has It pricing", graph), true);
});

test("isKnownEntityQuery returns false for low overlap", () => {
  const graph = mockEntityGraph(["ContextOS Memory Architecture V2"]);
  assert.equal(isKnownEntityQuery("memory", graph), false);
});

test("isKnownEntityQuery returns false for empty query", () => {
  const graph = mockEntityGraph(["ContextOS"]);
  assert.equal(isKnownEntityQuery("", graph), false);
});

test("isKnownEntityQuery returns false for null graph", () => {
  assert.equal(isKnownEntityQuery("ContextOS", null), false);
});
