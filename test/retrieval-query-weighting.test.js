import test from "node:test";
import assert from "node:assert/strict";

import { applyQueryIntentWeighting, buildQueryProfile } from "../src/core/retrieval.js";

test("query profile expands key factual aliases", () => {
  const profile = buildQueryProfile("What is the RHI COGS per unit?");

  assert.match(profile.expandedQuery, /rumor has it/);
  assert.match(profile.expandedQuery, /manufacturing cost/);
  assert.equal(profile.wantsFact, true);
  assert.equal(profile.wantsDecision, false);
});

test("temporal decision queries recognize decide phrasing as decision intent", () => {
  const profile = buildQueryProfile("When did we decide to start Phase 3?");

  assert.equal(profile.wantsFact, true);
  assert.equal(profile.wantsDecision, true);
});

test("explicit factual queries downrank benchmark/meta summaries", () => {
  const profile = buildQueryProfile("What DNS provider do we use?");
  const [factResult, metaResult] = applyQueryIntentWeighting([
    {
      type: "fact",
      id: "fact-1",
      score: 1,
      summary: "All domains are managed through Cloudflare for DNS, SSL, and everything else.",
      payload: {},
    },
    {
      type: "fact",
      id: "fact-2",
      score: 1,
      summary: "Retrieval benchmark misses cluster around DNS provider questions.",
      payload: {},
    },
  ], profile);

  assert.ok(factResult.score > metaResult.score, "answer-bearing fact should outrank meta benchmark fact");
});

test("policy queries prefer decisions over generic entities", () => {
  const profile = buildQueryProfile("What was decided about local LLM usage in ContextOS?");
  const [decisionResult, entityResult] = applyQueryIntentWeighting([
    {
      type: "decision",
      id: "decision-1",
      score: 1,
      summary: "Cortex will remove the Qwen and Ollama real-time classifier path — no more local LLM classification",
      payload: {},
    },
    {
      type: "entity",
      id: "entity-1",
      score: 1,
      summary: "ContextOS (system)",
      payload: {},
    },
  ], profile);

  assert.ok(decisionResult.score > entityResult.score, "decision should outrank generic entity for policy query");
});

test("explicit factual queries strongly downrank retrieval artifacts from benchmark files", () => {
  const profile = buildQueryProfile("What DNS provider do we use?");
  const [factResult, artifactChunk] = applyQueryIntentWeighting([
    {
      type: "fact",
      id: "fact-1",
      score: 1,
      summary: "DNS provider is Cloudflare.",
      payload: {},
    },
    {
      type: "chunk",
      id: "chunk-1",
      score: 1,
      summary: "app/test/retrieval-quality-history.json#12",
      payload: {
        path: "app/test/retrieval-quality-history.json",
        content: "{\"queries\":[{\"query\":\"What DNS provider do we use?\",\"expected_top1_contains\":\"Cloudflare\",\"avgMRR\":0.71}]}",
      },
    },
  ], profile);

  assert.ok(factResult.score > artifactChunk.score, "answer-bearing fact should outrank retrieval-quality artifact chunk");
  assert.ok(artifactChunk.score < 0.3, "artifact chunk should receive a strong penalty");
});

test("policy queries strongly downrank task JSON dumps versus real decisions", () => {
  const profile = buildQueryProfile("What was decided about DNS?");
  const [decisionResult, taskDump] = applyQueryIntentWeighting([
    {
      type: "decision",
      id: "decision-1",
      score: 1,
      summary: "Use Cloudflare for DNS cutover.",
      payload: {},
    },
    {
      type: "fact",
      id: "fact-2",
      score: 1,
      summary: "dns tasks json",
      payload: {
        content: "{\"tasks\":[{\"title\":\"Use Cloudflare for DNS\",\"status\":\"open\",\"priority\":\"high\"}]}",
      },
    },
  ], profile);

  assert.ok(decisionResult.score > taskDump.score, "real decision should outrank task JSON dump");
  assert.ok(taskDump.score < 0.35, "task dump should receive a strong penalty");
});
