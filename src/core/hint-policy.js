function clamp(value, minimum, maximum) {
  return Math.min(maximum, Math.max(minimum, value));
}

function normalizeScoreMass(results) {
  const scores = results.slice(0, 8).map((r) => Number(r.graphScore ?? r.score ?? 0));
  if (scores.length === 0) return 0;
  // Adaptive normalization: detect RRF-scale scores (typically < 0.1) vs legacy scores (0-3.0)
  const maxScore = Math.max(...scores);
  const divisor = maxScore < 0.15 ? 0.05 : 2.5;
  const total = scores.reduce((sum, s) => sum + clamp(s / divisor, 0, 1), 0);
  return clamp(total / 4, 0, 1.5);
}

export function scoreHintOutcome({
  hint,
  applied,
  graphReachableWithoutHint,
  attributedResults,
  uniqueAttributedResults,
  uniqueEntityIds,
}) {
  const attributedItemCount = attributedResults.length;
  const uniqueEntityCount = uniqueEntityIds.size;
  const attributedResultMass = normalizeScoreMass(attributedResults);
  const uniqueResultMass = normalizeScoreMass(uniqueAttributedResults);
  const noveltyGain = graphReachableWithoutHint ? 0.2 : 1;

  const reward = applied
    ? clamp(
      (0.45 * uniqueResultMass) +
        (0.25 * clamp(attributedItemCount / 6, 0, 1)) +
        (0.2 * clamp(uniqueEntityCount / 3, 0, 1)) +
        (0.1 * noveltyGain),
      0,
      1.5,
    )
    : 0;

  const penalty = !applied
    ? 0.65
    : clamp(
      (graphReachableWithoutHint ? 0.18 : 0) +
        (attributedItemCount === 0 ? 0.25 : 0) +
        (uniqueEntityCount === 0 ? 0.22 : 0) +
        (uniqueResultMass < 0.12 ? 0.18 : 0),
      0,
      1.2,
    );

  const netReward = Number((reward - penalty).toFixed(3));
  const nextWeight = clamp(
    Number(hint.baseWeight ?? hint.weight ?? 0.75) + (reward * 0.4) - (penalty * (applied ? 0.22 : 0.42)),
    0.15,
    3,
  );

  let ttlDelta = 0;
  if (reward >= 0.65) {
    ttlDelta += 1;
  }
  if (!applied) {
    ttlDelta -= 2;
  } else if (penalty >= 0.35) {
    ttlDelta -= 1;
  }

  const nextTtlTurns = Math.round(clamp(Number(hint.baseTtlTurns ?? hint.ttlTurns ?? 4) + ttlDelta, 1, 12));

  return {
    reward: Number(reward.toFixed(3)),
    penalty: Number(penalty.toFixed(3)),
    netReward,
    nextWeight: Number(nextWeight.toFixed(3)),
    nextTtlTurns,
    rewarded: netReward > 0.02,
    decayed: penalty > 0.12,
    metrics: {
      attributedItemCount,
      attributedResultMass: Number(attributedResultMass.toFixed(3)),
      uniqueEntityCount,
      uniqueResultMass: Number(uniqueResultMass.toFixed(3)),
      noveltyGain,
      graphReachableWithoutHint,
    },
  };
}
