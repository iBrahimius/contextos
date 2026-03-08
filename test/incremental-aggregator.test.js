import test from "node:test";
import assert from "node:assert/strict";
import { IncrementalAggregator } from "../src/core/incremental-aggregator.js";

test("IncrementalAggregator - constructor initializes empty state", () => {
  const agg = new IncrementalAggregator();

  assert.equal(agg.clusters.size, 0);
  assert.equal(agg.nextClusterId, 0);
  assert.equal(agg.observationToCluster.size, 0);
  assert.equal(agg.predicateIndex.size, 0);
});

test("IncrementalAggregator.ingestObservation - adds observation to cluster", () => {
  const agg = new IncrementalAggregator();

  agg.ingestObservation({
    id: 1,
    text: "First observation",
    timestamp: "2026-01-01T10:00:00Z",
    confidence: 0.9,
  });

  assert.equal(agg.clusters.size, 1);
  assert(agg.observationToCluster.has(1));
  assert.equal(agg.observationToCluster.get(1), 0);
});

test("IncrementalAggregator.ingestObservation - tracks entities and topics", () => {
  const agg = new IncrementalAggregator();

  agg.ingestObservation({
    id: 1,
    text: "Observation about database",
    timestamp: "2026-01-01T10:00:00Z",
    confidence: 0.9,
    entities: ["SQLite", "persistence"],
    topics: ["database", "architecture"],
  });

  const meta = agg.getClusterMeta(0);

  assert(meta.entities.has("SQLite"));
  assert(meta.entities.has("persistence"));
  assert(meta.topics.has("database"));
  assert(meta.topics.has("architecture"));
});

test("IncrementalAggregator.ingestObservation - updates time span", () => {
  const agg = new IncrementalAggregator();

  const t1 = new Date("2026-01-01T10:00:00Z");
  const t2 = new Date("2026-01-01T11:00:00Z");

  agg.ingestObservation({
    id: 1,
    text: "First",
    timestamp: t1,
    confidence: 0.9,
  });

  agg.ingestObservation({
    id: 2,
    text: "Second",
    timestamp: t2,
    confidence: 0.8,
  });

  const meta = agg.getClusterMeta(0);

  assert.equal(meta.startTime.getTime(), t1.getTime());
  assert.equal(meta.endTime.getTime(), t2.getTime());
});

test("IncrementalAggregator.ingestObservation - computes average confidence", () => {
  const agg = new IncrementalAggregator();

  agg.ingestObservation({
    id: 1,
    text: "High confidence",
    timestamp: "2026-01-01T10:00:00Z",
    confidence: 1.0,
  });

  agg.ingestObservation({
    id: 2,
    text: "Low confidence",
    timestamp: "2026-01-01T11:00:00Z",
    confidence: 0.5,
  });

  const meta = agg.getClusterMeta(0);

  assert.equal(meta.avgConfidence, 0.75);
});

test("IncrementalAggregator.ingestObservation - uses default confidence", () => {
  const agg = new IncrementalAggregator();

  agg.ingestObservation({
    id: 1,
    text: "No confidence specified",
    timestamp: "2026-01-01T10:00:00Z",
  });

  const meta = agg.getClusterMeta(0);

  assert.equal(meta.avgConfidence, 0.8);
});

test("IncrementalAggregator.detectContradictions - returns empty for single observation", () => {
  const agg = new IncrementalAggregator();

  agg.ingestObservation({
    id: 1,
    text: "Single observation",
    timestamp: "2026-01-01T10:00:00Z",
  });

  const contradictions = agg.detectContradictions(0);

  assert(Array.isArray(contradictions));
});

test("IncrementalAggregator.detectContradictions - nonexistent cluster returns empty", () => {
  const agg = new IncrementalAggregator();

  const contradictions = agg.detectContradictions(999);

  assert.deepEqual(contradictions, []);
});

test("IncrementalAggregator.detectRedundancy - returns kept IDs when no embeddings", async () => {
  const agg = new IncrementalAggregator();

  agg.ingestObservation({
    id: 1,
    text: "Obs 1",
    timestamp: "2026-01-01T10:00:00Z",
  });

  agg.ingestObservation({
    id: 2,
    text: "Obs 2",
    timestamp: "2026-01-01T11:00:00Z",
  });

  const kept = await agg.detectRedundancy(0);

  assert(kept.has(1));
  assert(kept.has(2));
});

test("IncrementalAggregator.detectRedundancy - single observation", async () => {
  const agg = new IncrementalAggregator();

  agg.ingestObservation({
    id: 1,
    text: "Only observation",
    timestamp: "2026-01-01T10:00:00Z",
  });

  const kept = await agg.detectRedundancy(0);

  assert.equal(kept.size, 1);
  assert(kept.has(1));
});

test("IncrementalAggregator.detectRedundancy - handles embedding error gracefully", async () => {
  const agg = new IncrementalAggregator();

  agg.ingestObservation({
    id: 1,
    text: "Obs 1",
    timestamp: "2026-01-01T10:00:00Z",
  });

  const errorEmbeddingFn = async () => {
    throw new Error("Embedding service unavailable");
  };

  const kept = await agg.detectRedundancy(0, errorEmbeddingFn);

  assert(kept.has(1));
});

test("IncrementalAggregator.getClusterMeta - returns complete metadata", () => {
  const agg = new IncrementalAggregator();

  agg.ingestObservation({
    id: 1,
    text: "Test observation",
    timestamp: "2026-01-01T10:00:00Z",
    confidence: 0.9,
    entities: ["Entity1"],
    topics: ["Topic1"],
  });

  const meta = agg.getClusterMeta(0);

  assert(meta);
  assert.equal(meta.clusterId, 0);
  assert.deepEqual(meta.observationIds, [1]);
  assert(meta.entities.has("Entity1"));
  assert(meta.topics.has("Topic1"));
  assert(meta.startTime);
  assert(meta.endTime);
  assert.equal(meta.avgConfidence, 0.9);
  assert.equal(meta.observationCount, 1);
  assert(Array.isArray(meta.contradictions));
});

test("IncrementalAggregator.getClusterMeta - nonexistent cluster returns null", () => {
  const agg = new IncrementalAggregator();

  const meta = agg.getClusterMeta(999);

  assert.equal(meta, null);
});

test("IncrementalAggregator.getDeduplicated - returns deduplicated IDs", async () => {
  const agg = new IncrementalAggregator();

  agg.ingestObservation({
    id: 1,
    text: "Obs 1",
    timestamp: "2026-01-01T10:00:00Z",
  });

  agg.ingestObservation({
    id: 2,
    text: "Obs 2",
    timestamp: "2026-01-01T11:00:00Z",
  });

  await agg.detectRedundancy(0);
  const kept = agg.getDeduplicated(0);

  assert(kept.has(1));
  assert(kept.has(2));
});

test("IncrementalAggregator.getDeduplicated - empty if detectRedundancy not called", () => {
  const agg = new IncrementalAggregator();

  agg.ingestObservation({
    id: 1,
    text: "Obs",
    timestamp: "2026-01-01T10:00:00Z",
  });

  const kept = agg.getDeduplicated(0);

  assert.equal(kept.size, 0);
});

test("IncrementalAggregator.reset - clears all state", () => {
  const agg = new IncrementalAggregator();

  agg.ingestObservation({
    id: 1,
    text: "Observation",
    timestamp: "2026-01-01T10:00:00Z",
  });

  assert.equal(agg.clusters.size, 1);
  assert.equal(agg.observationToCluster.size, 1);

  agg.reset();

  assert.equal(agg.clusters.size, 0);
  assert.equal(agg.nextClusterId, 0);
  assert.equal(agg.observationToCluster.size, 0);
  assert.equal(agg.predicateIndex.size, 0);
});

test("IncrementalAggregator - multi-observation workflow", async () => {
  const agg = new IncrementalAggregator();

  agg.ingestObservation({
    id: 1,
    text: "SQLite is chosen",
    timestamp: "2026-01-01T10:00:00Z",
    confidence: 0.95,
    entities: ["SQLite"],
    topics: ["database"],
  });

  agg.ingestObservation({
    id: 2,
    text: "Zero dependencies requirement",
    timestamp: "2026-01-01T11:00:00Z",
    confidence: 0.9,
    entities: ["dependency", "architecture"],
    topics: ["constraints"],
  });

  const meta = agg.getClusterMeta(0);
  assert.equal(meta.observationCount, 2);
  assert(meta.entities.has("SQLite"));
  assert(meta.topics.has("database"));

  const kept = await agg.detectRedundancy(0);
  assert(kept.has(1));
  assert(kept.has(2));

  const dedup = agg.getDeduplicated(0);
  assert(dedup.has(1));
  assert(dedup.has(2));
});
