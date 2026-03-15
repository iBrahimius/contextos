/**
 * Pattern Detection & Promotion
 *
 * Identifies recurring patterns in claims and proposes promotions to higher types.
 * Patterns are detected when ≥3 similar claims exist for the same (entity, type, predicate) tuple.
 */

const PATTERN_PROMOTION_TARGET = {
  preference: "preference",
  constraint: "rule",
  state_change: "habit",
  event: "habit",
  decision: "rule",
  task: "task",
  fact: "fact",
};

const PATTERN_PROMOTION_CAP = 5;

/**
 * Tokenize text for similarity computation.
 * Converts to lowercase, splits on non-alphanumeric, filters short tokens.
 */
function tokenize(text) {
  return new Set(
    String(text ?? "")
      .toLowerCase()
      .split(/[^a-z0-9]+/)
      .filter((token) => token.length > 2),
  );
}

/**
 * Compute Jaccard similarity between two token sets.
 * Returns 1.0 if both empty, otherwise intersection / union.
 */
function jaccardSimilarity(tokensA, tokensB) {
  if (!tokensA.size && !tokensB.size) {
    return 1.0;
  }

  const intersection = [...tokensA].filter((token) => tokensB.has(token)).length;
  const union = new Set([...tokensA, ...tokensB]).size;
  return union === 0 ? 0 : intersection / union;
}

/**
 * Compute average Jaccard similarity for a group of items.
 * Uses pairwise comparison of all token sets.
 */
function computeAverageJaccard(items) {
  const tokenSets = items.map((item) => tokenize(item.detail ?? item.value_text ?? ""));
  let totalSimilarity = 0;
  let pairCount = 0;

  for (let i = 0; i < tokenSets.length; i += 1) {
    for (let j = i + 1; j < tokenSets.length; j += 1) {
      totalSimilarity += jaccardSimilarity(tokenSets[i], tokenSets[j]);
      pairCount += 1;
    }
  }

  return pairCount === 0 ? 0 : totalSimilarity / pairCount;
}

/**
 * Compute similarity for a group of items.
 * If embeddings exist, uses cosine similarity; otherwise falls back to Jaccard.
 */
function computeGroupSimilarity(items) {
  if (items.length < 2) {
    return 1.0; // Single item = trivially similar to itself
  }

  // Fallback: Jaccard similarity on tokens (no embeddings available)
  return computeAverageJaccard(items);
}

/**
 * Detect patterns in claim data.
 *
 * @param {Object} db - Database instance with listRecentClaims() method
 * @param {Object} options - Detection options
 *   - lookbackDays: number (default 14)
 *   - minOccurrences: number (default 3)
 *   - similarityThreshold: number (default 0.75)
 * @returns {Object} { patterns: Array, promotions: Array }
 */
export function detectPatterns(db, options = {}) {
  const lookbackDays = options.lookbackDays ?? 14;
  const minOccurrences = options.minOccurrences ?? 3;
  const similarityThreshold = options.similarityThreshold ?? 0.75;

  const lookbackDate = new Date(Date.now() - lookbackDays * 24 * 60 * 60 * 1000).toISOString();

  // Query recent active claims
  const recentClaims = db.listRecentClaims({
    lifecycleStates: ["active", "candidate"],
    limit: 200,
  }).filter((claim) => {
    const claimDate = claim.created_at ?? claim.updated_at ?? "";
    return claimDate >= lookbackDate;
  });

  // Group by (subject_entity_id, claim_type, predicate)
  const groups = new Map();
  for (const claim of recentClaims) {
    const key = `${claim.subject_entity_id ?? "__none__"}::${claim.claim_type}::${claim.predicate ?? "__none__"}`;
    if (!groups.has(key)) {
      groups.set(key, []);
    }
    groups.get(key).push(claim);
  }

  // Identify candidate patterns
  const patterns = [];

  for (const [groupKey, groupClaims] of groups) {
    // Filter: must have ≥ minOccurrences
    if (groupClaims.length < minOccurrences) {
      continue;
    }

    // Compute similarity
    const avgSimilarity = computeGroupSimilarity(
      groupClaims.map((claim) => ({
        detail: claim.value_text ?? claim.predicate ?? "",
        value_text: claim.value_text,
      })),
    );

    // Filter: similarity must exceed threshold
    if (avgSimilarity < similarityThreshold) {
      continue;
    }

    // Pattern candidate identified
    const claimType = groupClaims[0].claim_type;
    const targetType = PATTERN_PROMOTION_TARGET[claimType] ?? claimType;
    const bestClaim = groupClaims.reduce((best, claim) =>
      Number(claim.confidence ?? 0) >= Number(best.confidence ?? 0) ? claim : best,
      groupClaims[0],
    );

    patterns.push({
      groupKey,
      sourceType: claimType,
      targetType,
      entityId: groupClaims[0].subject_entity_id,
      predicate: groupClaims[0].predicate,
      occurrences: groupClaims.length,
      avgSimilarity,
      confidence: Math.min(0.9, 0.5 + groupClaims.length * 0.1),
      sourceClaimIds: groupClaims.map((c) => c.id),
      bestValue: bestClaim.value_text,
    });
  }

  return {
    patterns,
    promotions: patterns.slice(0, PATTERN_PROMOTION_CAP),
  };
}

/**
 * Build a promotion mutation from a detected pattern.
 *
 * @param {Object} pattern - Detected pattern object
 * @returns {Object} Mutation payload
 */
export function buildPromotion(pattern) {
  return {
    type: `add_${pattern.targetType}`,
    payload: {
      entityId: pattern.entityId,
      predicate: pattern.predicate,
      detail: pattern.bestValue ?? `Pattern: ${pattern.predicate}`,
      metadata: {
        pattern_source: pattern.sourceClaimIds,
        pattern_type: pattern.sourceType,
        occurrences: pattern.occurrences,
        avg_similarity: pattern.avgSimilarity,
      },
    },
    confidence: pattern.confidence,
    writeClass: "canonical",
  };
}

/**
 * Compute similarity between two text strings using Jaccard word overlap.
 * Simple interface for comparing individual claim values.
 */
export function computeSimilarity(text1, text2) {
  const tokens1 = tokenize(text1);
  const tokens2 = tokenize(text2);
  return jaccardSimilarity(tokens1, tokens2);
}

/**
 * Detect temporal regularity patterns in claim data.
 *
 * For each (entity, type, predicate) group with ≥3 occurrences, checks whether
 * the timestamps show regularity: stdDev of intervals < 0.5 * mean interval.
 *
 * @param {Object} db - Database instance with listRecentClaims() method
 * @param {Object} options - Detection options
 *   - lookbackDays: number (default 30)
 *   - minOccurrences: number (default 3)
 *   - regularityThreshold: number (default 0.5) - max stdDev/mean to qualify
 * @returns {Array} Array of temporal pattern objects
 */
export function detectTemporalPatterns(db, options = {}) {
  const lookbackDays = options.lookbackDays ?? 30;
  const minOccurrences = options.minOccurrences ?? 3;
  const regularityThreshold = options.regularityThreshold ?? 0.5;

  const lookbackDate = new Date(Date.now() - lookbackDays * 24 * 60 * 60 * 1000).toISOString();

  const recentClaims = db.listRecentClaims({
    lifecycleStates: ["active", "candidate"],
    limit: 500,
  }).filter((claim) => {
    const claimDate = claim.created_at ?? claim.updated_at ?? "";
    return claimDate >= lookbackDate;
  });

  // Group by (subject_entity_id, claim_type, predicate)
  const groups = new Map();
  for (const claim of recentClaims) {
    const key = `${claim.subject_entity_id ?? "__none__"}::${claim.claim_type}::${claim.predicate ?? "__none__"}`;
    if (!groups.has(key)) {
      groups.set(key, []);
    }
    groups.get(key).push(claim);
  }

  const temporalPatterns = [];

  for (const [groupKey, groupClaims] of groups) {
    if (groupClaims.length < minOccurrences) {
      continue;
    }

    // Sort claims by timestamp ascending
    const sorted = groupClaims
      .map((claim) => ({
        claim,
        ts: new Date(claim.created_at ?? claim.updated_at ?? 0).getTime(),
      }))
      .filter((item) => !Number.isNaN(item.ts) && item.ts > 0)
      .sort((a, b) => a.ts - b.ts);

    if (sorted.length < minOccurrences) {
      continue;
    }

    // Compute intervals in hours between consecutive timestamps
    const intervals = [];
    for (let i = 1; i < sorted.length; i += 1) {
      const intervalMs = sorted[i].ts - sorted[i - 1].ts;
      intervals.push(intervalMs / (1000 * 60 * 60)); // convert to hours
    }

    if (intervals.length < 2) {
      continue;
    }

    // Compute mean and standard deviation of intervals
    const meanInterval = intervals.reduce((sum, v) => sum + v, 0) / intervals.length;

    if (meanInterval <= 0) {
      continue;
    }

    const variance = intervals.reduce((sum, v) => sum + Math.pow(v - meanInterval, 2), 0) / intervals.length;
    const stdDev = Math.sqrt(variance);
    const regularity = stdDev / meanInterval;

    if (regularity >= regularityThreshold) {
      continue;
    }

    // Estimate next expected occurrence
    const lastTs = sorted[sorted.length - 1].ts;
    const nextExpected = new Date(lastTs + meanInterval * 60 * 60 * 1000);

    const firstClaim = sorted[0].claim;

    temporalPatterns.push({
      patternKey: groupKey,
      entityId: firstClaim.subject_entity_id ?? null,
      claimType: firstClaim.claim_type,
      predicate: firstClaim.predicate ?? null,
      occurrences: sorted.length,
      meanIntervalHours: meanInterval,
      stdDevHours: stdDev,
      regularity,
      nextExpected,
    });
  }

  return temporalPatterns;
}

export default {
  detectPatterns,
  buildPromotion,
  computeSimilarity,
  detectTemporalPatterns,
};
