/**
 * Reciprocal Rank Fusion (RRF) - Combines multiple ranked lists using rank-based scoring
 *
 * RRF normalizes across signals by rank position, not raw score magnitude.
 * This prevents signals with naturally higher scores (e.g., graph: 0-3.0) from dominating
 * signals with lower ranges (e.g., vector: 0-1.0).
 */

/**
 * Compute RRF score for an item appearing in multiple ranked lists
 * @param {Array<{type: string, id: string|number, ...}>} rankedLists - Multiple ranked lists
 * @param {number} k - Constant to prevent top items from dominating (typical: 60)
 * @returns {Array} Merged and re-ranked results with RRF scores
 */
export function reciprocalRankFusion(rankedLists, k = 60) {
  const scores = new Map(); // "type:id" → { score, item }

  // For each ranked list, accumulate RRF scores by position
  for (const list of rankedLists) {
    for (let rank = 0; rank < list.length; rank++) {
      const item = list[rank];
      const key = `${item.type}:${item.id}`;
      const rrfScore = 1 / (k + rank + 1); // rank is 0-indexed, formula uses 1-indexed

      const existing = scores.get(key);
      if (existing) {
        // Accumulate RRF scores from different signals
        existing.score += rrfScore;
        // Keep the richer item (more metadata)
        const existingDetail = (existing.item.payload?.detail ?? '').length;
        const itemDetail = (item.payload?.detail ?? '').length;
        if (itemDetail > existingDetail) {
          existing.item = item;
        }
      } else {
        scores.set(key, { score: rrfScore, item });
      }
    }
  }

  // Sort by RRF score (descending) and return with metadata
  return [...scores.values()]
    .sort((a, b) => b.score - a.score)
    .map(({ score, item }, index) => ({
      ...item,
      rrfScore: score,
      score, // Override original score with RRF score
      rank: index + 1,
    }));
}

/**
 * Apply category boosts after RRF (post-RRF adjustments)
 * Categories encode domain knowledge: decisions are more important than facts, etc.
 * Applied as multiplicative factor to RRF score.
 *
 * @param {Array} results - Results after RRF fusion
 * @param {Object} boosts - Category → boost factor mapping
 * @returns {Array} Results with category boosts applied
 */
export function applyCategoryBoosts(results, boosts = {}) {
  const defaultBoosts = {
    decision: 1.25,
    constraint: 1.20,
    fact: 1.15,
    task: 1.10,
    message: 1.0,
    entity: 1.05,
  };

  const finalBoosts = { ...defaultBoosts, ...boosts };

  return results.map((result) => {
    const boost = finalBoosts[result.type] ?? 1.0;
    return {
      ...result,
      categoryBoost: boost,
      score: result.rrfScore * boost,
    };
  });
}

/**
 * Apply origin penalty to reduce agent-generated content in results
 * Used to filter out system messages or low-quality automated observations
 *
 * @param {number} score - Current RRF score
 * @param {Object} payload - Result payload (contains origin info)
 * @returns {number} Adjusted score
 */
export function applyOriginPenalty(score, payload = {}) {
  const origin = payload.origin_kind ?? payload.origin ?? payload.source ?? '';
  // Reduce agent-generated content — must be strong enough to differentiate
  // identical user/agent content after RRF normalizes by rank
  if (origin && (origin === 'agent' || origin === 'system' || origin.includes('agent'))) {
    return score * 0.85;
  }
  return score;
}

/**
 * Apply seed entity bonus to results linked to entities mentioned in the query
 * Scaled down for RRF (which produces smaller score magnitudes)
 *
 * @param {Array} results - RRF-fused results
 * @param {Set<string|number>} seedEntityIds - Entity IDs mentioned in query
 * @returns {Array} Results with seed bonuses applied
 */
export function applySeedBonus(results, seedEntityIds = new Set()) {
  if (seedEntityIds.size === 0) return results;

  return results.map((result) => {
    let bonus = 0;

    // Direct entity match
    if (result.type === 'entity' && seedEntityIds.has(result.id)) {
      bonus = 0.01;
    }

    // Result linked to seed entity
    if (result.linkedEntityIds && Array.isArray(result.linkedEntityIds)) {
      for (const linkedId of result.linkedEntityIds) {
        if (seedEntityIds.has(linkedId)) {
          bonus = Math.max(bonus, 0.01);
          break;
        }
      }
    }

    if (bonus > 0) {
      return {
        ...result,
        seedBonus: bonus,
        score: result.score + bonus,
      };
    }

    return result;
  });
}

/**
 * Break ties in RRF results using secondary sort criteria
 * When RRF scores are equal, prefer: newer → decisions → facts → tasks → messages
 *
 * @param {Array} results - RRF-ranked results
 * @returns {Array} Results with ties broken
 */
export function breakRRFTies(results) {
  const TYPE_PRIORITY = {
    decision: 0,
    constraint: 1,
    fact: 2,
    task: 3,
    message: 4,
    entity: 5,
  };

  const grouped = new Map();
  for (const result of results) {
    const score = result.rrfScore ?? result.score ?? 0;
    const scoreKey = score.toFixed(10); // Group by RRF score
    if (!grouped.has(scoreKey)) {
      grouped.set(scoreKey, []);
    }
    grouped.get(scoreKey).push(result);
  }

  const sorted = [];
  for (const [, group] of grouped) {
    group.sort((a, b) => {
      // Within same RRF score, prefer newer first
      const aTime = a.timestamp ? new Date(a.timestamp).getTime() : 0;
      const bTime = b.timestamp ? new Date(b.timestamp).getTime() : 0;
      if (aTime !== bTime) return bTime - aTime; // newer first

      // Then by type priority
      const aPriority = TYPE_PRIORITY[a.type] ?? 999;
      const bPriority = TYPE_PRIORITY[b.type] ?? 999;
      return aPriority - bPriority;
    });
    sorted.push(...group);
  }

  return sorted.map((result, index) => ({
    ...result,
    rank: index + 1,
  }));
}

/**
 * Validate and clean RRF input lists
 * Ensures all items have required fields: type, id
 *
 * @param {Array<Array>} rankedLists - Lists to validate
 * @returns {Array<Array>} Cleaned lists
 */
export function validateRRFInput(rankedLists) {
  return rankedLists
    .map((list) =>
      list.filter((item) => {
        if (!item.type || (item.id === undefined && item.id !== 0)) {
          console.warn('[RRF] Skipping invalid item (missing type or id):', item);
          return false;
        }
        return true;
      })
    )
    .filter((list) => list.length > 0);
}
