import test from "node:test";
import assert from "node:assert/strict";

import { VectorIndex } from "../src/core/vector-index.js";
import { cosineSimilarity } from "../src/core/vector-math.js";

const DIMS = 4; // small for fast tests

function randomVector(dims = DIMS) {
  const v = new Float32Array(dims);
  for (let i = 0; i < dims; i++) {
    v[i] = Math.random() * 2 - 1;
  }
  return v;
}

function makeItem(id, type = "message", embedding = null, metadata = {}) {
  return {
    id,
    type,
    embedding: embedding ?? randomVector(),
    ...metadata,
  };
}

// ── Basic construction ─────────────────────────────

test("empty index — size is 0, query returns []", () => {
  const index = new VectorIndex(DIMS);
  assert.equal(index.size, 0);
  assert.deepEqual(index.query(randomVector(), 10, 0), []);
});

test("rebuild from N items — size is N", () => {
  const index = new VectorIndex(DIMS);
  const items = Array.from({ length: 20 }, (_, i) => makeItem(`msg_${i}`));
  index.rebuild(items);
  assert.equal(index.size, 20);
});

test("rebuild deduplicates by id", () => {
  const index = new VectorIndex(DIMS);
  const emb = randomVector();
  index.rebuild([
    makeItem("dup", "message", emb),
    makeItem("dup", "message", emb),
    makeItem("dup", "observation", emb),
  ]);
  assert.equal(index.size, 1);
});

// ── Query correctness ──────────────────────────────

test("query returns items sorted by cosine similarity descending", () => {
  const index = new VectorIndex(DIMS);
  const queryVec = new Float32Array([1, 0, 0, 0]);
  const close = new Float32Array([0.9, 0.1, 0, 0]);
  const far = new Float32Array([0, 0, 0, 1]);
  const mid = new Float32Array([0.5, 0.5, 0, 0]);

  index.rebuild([
    makeItem("far", "message", far),
    makeItem("close", "message", close),
    makeItem("mid", "message", mid),
  ]);

  const results = index.query(queryVec, 10, 0);
  assert.ok(results.length >= 2, `expected at least 2 results, got ${results.length}`);
  assert.equal(results[0].id, "close");
  for (let i = 1; i < results.length; i++) {
    assert.ok(
      results[i - 1].score >= results[i].score,
      `results not sorted: ${results[i - 1].score} < ${results[i].score}`
    );
  }
});

test("query respects k limit", () => {
  const index = new VectorIndex(DIMS);
  const items = Array.from({ length: 50 }, (_, i) => makeItem(`msg_${i}`));
  index.rebuild(items);
  const results = index.query(randomVector(), 5, 0);
  assert.ok(results.length <= 5, `expected at most 5, got ${results.length}`);
});

test("query respects threshold — items below threshold excluded", () => {
  const index = new VectorIndex(DIMS);
  const queryVec = new Float32Array([1, 0, 0, 0]);
  const opposite = new Float32Array([-1, 0, 0, 0]);
  const close = new Float32Array([0.95, 0.05, 0, 0]);

  index.rebuild([
    makeItem("opposite", "message", opposite),
    makeItem("close", "message", close),
  ]);

  const results = index.query(queryVec, 10, 0.5);
  assert.ok(results.every((r) => r.score >= 0.5), "all results should be above threshold");
  assert.ok(results.some((r) => r.id === "close"), "close item should be found");
  assert.ok(!results.some((r) => r.id === "opposite"), "opposite item should be excluded");
});

test("results match brute-force cosine for same inputs", () => {
  const index = new VectorIndex(DIMS);
  const items = Array.from({ length: 30 }, (_, i) => makeItem(`item_${i}`));
  index.rebuild(items);

  const queryVec = randomVector();

  // Brute force
  const bruteForce = items
    .map((item) => ({
      id: item.id,
      score: cosineSimilarity(queryVec, item.embedding),
    }))
    .filter((r) => r.score > 0.3)
    .sort((a, b) => b.score - a.score)
    .slice(0, 10);

  // VP-tree
  const vpResults = index.query(queryVec, 10, 0.3);

  // Same ids in top results (VP-tree is exact, not approximate)
  const bruteIds = new Set(bruteForce.map((r) => r.id));
  const vpIds = new Set(vpResults.map((r) => r.id));

  for (const id of vpIds) {
    assert.ok(bruteIds.has(id), `VP-tree returned ${id} not in brute-force results`);
  }
  for (const id of bruteIds) {
    assert.ok(vpIds.has(id), `Brute-force returned ${id} not in VP-tree results`);
  }
});

// ── Insert / remove ────────────────────────────────

test("insert adds to index incrementally", () => {
  const index = new VectorIndex(DIMS);
  index.rebuild([makeItem("a"), makeItem("b")]);
  assert.equal(index.size, 2);

  index.insert("c", "message", randomVector());
  assert.equal(index.size, 3);

  // Use threshold -1 to include all items regardless of cosine similarity direction.
  // Random vectors in low dimensions have ~50% chance of negative similarity.
  const results = index.query(randomVector(), 10, -1);
  const ids = new Set(results.map((r) => r.id));
  assert.ok(ids.has("c"), "inserted item should be queryable");
});

test("insert with existing id updates, not duplicates", () => {
  const index = new VectorIndex(DIMS);
  const emb1 = new Float32Array([1, 0, 0, 0]);
  const emb2 = new Float32Array([0, 1, 0, 0]);

  index.rebuild([makeItem("x", "message", emb1)]);
  assert.equal(index.size, 1);

  index.insert("x", "message", emb2);
  assert.equal(index.size, 1);

  // Query for the new embedding
  const results = index.query(new Float32Array([0, 1, 0, 0]), 1, 0);
  assert.equal(results[0].id, "x");
  assert.ok(results[0].score > 0.9, "should match updated embedding");
});

test("remove removes from index", () => {
  const index = new VectorIndex(DIMS);
  index.rebuild([makeItem("a"), makeItem("b"), makeItem("c")]);
  assert.equal(index.size, 3);

  const removed = index.remove("b");
  assert.equal(removed, true);
  assert.equal(index.size, 2);

  // Use threshold -1 to include all items regardless of cosine similarity direction.
  // Random vectors in low dimensions have ~50% chance of negative similarity.
  const results = index.query(randomVector(), 10, -1);
  assert.ok(!results.some((r) => r.id === "b"), "removed item should not appear");
});

test("remove non-existent id returns false", () => {
  const index = new VectorIndex(DIMS);
  index.rebuild([makeItem("a")]);
  assert.equal(index.remove("nonexistent"), false);
  assert.equal(index.size, 1);
});

// ── Edge cases ─────────────────────────────────────

test("handles zero vectors gracefully", () => {
  const index = new VectorIndex(DIMS);
  const zeroVec = new Float32Array(DIMS); // all zeros
  index.rebuild([makeItem("zero", "message", zeroVec), makeItem("normal", "message", randomVector())]);
  assert.equal(index.size, 2);

  // Query with zero vector
  const results = index.query(zeroVec, 10, 0);
  // Zero vectors have 0 cosine similarity with everything, so no results above threshold
  assert.ok(Array.isArray(results));
});

test("handles single item", () => {
  const index = new VectorIndex(DIMS);
  const emb = new Float32Array([1, 0, 0, 0]);
  index.rebuild([makeItem("solo", "message", emb)]);

  const results = index.query(emb, 1, 0);
  assert.equal(results.length, 1);
  assert.equal(results[0].id, "solo");
  assert.ok(results[0].score > 0.99);
});

test("filter function filters results", () => {
  const index = new VectorIndex(DIMS);
  const emb = new Float32Array([1, 0, 0, 0]);
  index.rebuild([
    makeItem("msg1", "message", emb, { scopeKind: "private" }),
    makeItem("obs1", "observation", emb, { scopeKind: "project", scopeId: "proj1" }),
  ]);

  const all = index.query(emb, 10, 0);
  assert.equal(all.length, 2);

  const filtered = index.query(emb, 10, 0, (item) => item.type === "observation");
  assert.equal(filtered.length, 1);
  assert.equal(filtered[0].id, "obs1");
});

// ── Scale (moderate) ───────────────────────────────

test("1000 items — build + query in reasonable time", () => {
  const index = new VectorIndex(DIMS);
  const items = Array.from({ length: 1000 }, (_, i) => makeItem(`item_${i}`));

  const buildStart = performance.now();
  index.rebuild(items);
  const buildMs = performance.now() - buildStart;

  const queryStart = performance.now();
  const results = index.query(randomVector(), 50, 0.3);
  const queryMs = performance.now() - queryStart;

  assert.equal(index.size, 1000);
  assert.ok(buildMs < 1000, `build took ${buildMs.toFixed(0)}ms, expected <1000ms`);
  assert.ok(queryMs < 50, `query took ${queryMs.toFixed(0)}ms, expected <50ms`);
  assert.ok(results.length <= 50);
});
