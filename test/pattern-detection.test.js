import test from "node:test";
import assert from "node:assert/strict";
import { detectPatterns, computeSimilarity, buildPromotion } from "../src/core/pattern-detection.js";

test("computeSimilarity — identical text returns 1.0", () => {
  const sim = computeSimilarity("same value repeated", "same value repeated");
  assert.equal(sim, 1.0);
});

test("computeSimilarity — completely different text returns 0.0", () => {
  const sim = computeSimilarity("abc", "xyz");
  assert.ok(sim === 0);
});

test("computeSimilarity — partial overlap", () => {
  const sim = computeSimilarity("apple banana cherry", "apple banana orange");
  assert.ok(sim >= 0.5 && sim < 1.0);
});

test("detectPatterns — groups claims by entity+type+predicate", () => {
  const mockDb = {
    listRecentClaims: () => [
      {
        id: "claim1",
        claim_type: "fact",
        predicate: "repeated_pattern",
        subject_entity_id: "ent1",
        value_text: "same value repeated",
        created_at: new Date().toISOString(),
      },
      {
        id: "claim2",
        claim_type: "fact",
        predicate: "repeated_pattern",
        subject_entity_id: "ent1",
        value_text: "same value repeated",
        created_at: new Date().toISOString(),
      },
      {
        id: "claim3",
        claim_type: "fact",
        predicate: "repeated_pattern",
        subject_entity_id: "ent1",
        value_text: "same value repeated",
        created_at: new Date().toISOString(),
      },
      {
        id: "claim4",
        claim_type: "fact",
        predicate: "repeated_pattern",
        subject_entity_id: "ent1",
        value_text: "same value repeated",
        created_at: new Date().toISOString(),
      },
    ],
  };

  const result = detectPatterns(mockDb, {
    lookbackDays: 30,
    minOccurrences: 3,
    similarityThreshold: 0.75,
  });

  assert.equal(typeof result.patterns, "object");
  assert.ok(Array.isArray(result.patterns));
  assert.equal(result.patterns.length, 1);
  assert.equal(result.patterns[0].occurrences, 4);
});

test("detectPatterns — respects minOccurrences threshold", () => {
  const mockDb = {
    listRecentClaims: () => [
      {
        id: "claim1",
        claim_type: "fact",
        predicate: "rare_pattern",
        subject_entity_id: "ent1",
        value_text: "rare",
        created_at: new Date().toISOString(),
      },
      {
        id: "claim2",
        claim_type: "fact",
        predicate: "rare_pattern",
        subject_entity_id: "ent1",
        value_text: "rare",
        created_at: new Date().toISOString(),
      },
    ],
  };

  const result = detectPatterns(mockDb, {
    lookbackDays: 30,
    minOccurrences: 3, // requires 3 claims
    similarityThreshold: 0.75,
  });

  assert.equal(result.patterns.length, 0);
});

test("detectPatterns — respects similarity threshold", () => {
  const mockDb = {
    listRecentClaims: () => [
      {
        id: "claim1",
        claim_type: "fact",
        predicate: "diverse",
        subject_entity_id: "ent1",
        value_text: "apple",
        created_at: new Date().toISOString(),
      },
      {
        id: "claim2",
        claim_type: "fact",
        predicate: "diverse",
        subject_entity_id: "ent1",
        value_text: "banana",
        created_at: new Date().toISOString(),
      },
      {
        id: "claim3",
        claim_type: "fact",
        predicate: "diverse",
        subject_entity_id: "ent1",
        value_text: "cherry",
        created_at: new Date().toISOString(),
      },
    ],
  };

  const result = detectPatterns(mockDb, {
    lookbackDays: 30,
    minOccurrences: 3,
    similarityThreshold: 0.75, // high threshold
  });

  // These are completely different words, so similarity will be 0
  assert.equal(result.patterns.length, 0);
});

test("buildPromotion — creates mutation payload with canonical write_class", () => {
  const pattern = {
    sourceClaimIds: ["c1", "c2", "c3"],
    sourceType: "fact",
    targetType: "fact",
    entityId: "ent1",
    predicate: "pattern",
    occurrences: 3,
    avgSimilarity: 0.95,
    confidence: 0.8,
    bestValue: "test value",
  };

  const promotion = buildPromotion(pattern);

  assert.equal(promotion.type, "add_fact");
  assert.equal(promotion.confidence, 0.8);
  assert.equal(promotion.writeClass, "canonical");
  assert.ok(promotion.payload.metadata.pattern_source);
});

test("detectPatterns — confidence formula min(0.9, 0.5 + occurrences * 0.1)", () => {
  const claims = [];
  for (let i = 0; i < 6; i++) {
    claims.push({
      id: `claim${i}`,
      claim_type: "fact",
      predicate: "pattern",
      subject_entity_id: "ent1",
      value_text: "similar",
      created_at: new Date().toISOString(),
    });
  }

  const mockDb = {
    listRecentClaims: () => claims,
  };

  const result = detectPatterns(mockDb, {
    lookbackDays: 30,
    minOccurrences: 3,
  });

  // 6 occurrences: min(0.9, 0.5 + 6 * 0.1) = min(0.9, 1.1) = 0.9
  assert.equal(result.patterns[0].confidence, 0.9);
});
