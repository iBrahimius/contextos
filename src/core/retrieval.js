import { performance } from "node:perf_hooks";

import { embedText } from "./embeddings.js";
import { scoreHintOutcome } from "./hint-policy.js";
import { getWeight } from "./relation-types.js";
import { estimateTokens } from "./utils.js";

const HINT_BASE_WEIGHT = 0.72;
const GRAPH_SCORE_WEIGHT = 0.4;
const VECTOR_SCORE_WEIGHT = 0.6;
const OBSERVATION_CATEGORY_BOOSTS = {
  decision: 0.25,
  constraint: 0.20,
  fact: 0.15,
  task: 0.10,
  relationship: 0.05,
};

function clamp(value, minimum, maximum) {
  return Math.min(maximum, Math.max(minimum, value));
}

function normalizeText(value) {
  return String(value ?? "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function scoreHintSeed(queryText, seedLabel) {
  const query = normalizeText(queryText);
  const seed = normalizeText(seedLabel);
  if (!query || !seed) {
    return 0;
  }

  if (query.includes(seed) || seed.includes(query)) {
    return 2.4;
  }

  const queryTokens = new Set(query.split(/[^a-z0-9]+/).filter((token) => token.length > 2));
  const seedTokens = seed.split(/[^a-z0-9]+/).filter((token) => token.length > 2);
  const overlap = seedTokens.filter((token) => queryTokens.has(token)).length;

  return overlap * 0.65;
}

function inferHintPredicate(reason) {
  const detail = normalizeText(reason);
  if (detail.includes("depends on")) {
    return "depends_on";
  }
  if (detail.includes("part of")) {
    return "part_of";
  }
  if (detail.includes("integrates with")) {
    return "integrates_with";
  }
  if (detail.includes("stores in")) {
    return "stores_in";
  }
  if (detail.includes("indexes")) {
    return "indexes";
  }
  if (detail.includes("retrieves")) {
    return "retrieves";
  }
  if (detail.includes("captures")) {
    return "captures";
  }

  return "related_to";
}

function dedupeResults(results) {
  const deduped = new Map();

  for (const result of results) {
    const key = `${result.type}:${result.id}`;
    const existing = deduped.get(key);

    if (!existing) {
      deduped.set(key, {
        ...result,
        hintIds: uniqueValues(result.hintIds ?? []),
      });
      continue;
    }

    const existingScore = Number(existing.score ?? 0);
    const nextScore = Number(result.score ?? 0);
    const preferred = nextScore > existingScore ? result : existing;

    deduped.set(key, {
      ...preferred,
      hintIds: mergeHintIds(existing.hintIds ?? [], result.hintIds ?? []),
    });
  }

  return [...deduped.values()];
}

function uniqueValues(values) {
  return [...new Set(values.filter(Boolean))];
}

function normalizeScopeFilter(scopeFilter) {
  if (!scopeFilter) {
    return null;
  }

  if (typeof scopeFilter === "string") {
    return {
      scopeKind: scopeFilter,
      scopeId: null,
    };
  }

  return {
    scopeKind: scopeFilter.scopeKind ?? null,
    scopeId: scopeFilter.scopeId ?? null,
  };
}

function provenanceHintIds(provenance, entityId) {
  return provenance.get(entityId)?.hintIds ?? [];
}

function mergeHintIds(...lists) {
  return uniqueValues(lists.flat());
}

function applyOriginPenalty(score, payload) {
  if (payload?.origin_kind === "agent") {
    return score * 0.85;
  }

  return score;
}

function cosineSimilarity(a, b) {
  if (!a?.length || !b?.length || a.length !== b.length) {
    return 0;
  }

  let dot = 0;
  let normA = 0;
  let normB = 0;
  for (let index = 0; index < a.length; index += 1) {
    dot += a[index] * b[index];
    normA += a[index] * a[index];
    normB += b[index] * b[index];
  }

  if (!normA || !normB) {
    return 0;
  }

  return dot / (Math.sqrt(normA) * Math.sqrt(normB));
}

function getResultMessageId(result) {
  if (result.type === "message") {
    return result.id;
  }

  return result.payload?.message_id ?? result.payload?.messageId ?? null;
}

function createVectorResult(message, vectorScore) {
  return {
    type: "message",
    id: message.id,
    entityId: null,
    score: VECTOR_SCORE_WEIGHT * vectorScore,
    summary: message.content,
    payload: message,
    tokenCount: Number(message.tokenCount ?? estimateTokens(message.content)),
    hintIds: [],
    vectorScore,
    graphScore: 0,
  };
}

function isObservationResult(result) {
  return Boolean(result?.id && result?.payload?.category && result?.payload?.detail);
}

function getObservationCategoryBoost(category) {
  return OBSERVATION_CATEGORY_BOOSTS[String(category ?? "").trim().toLowerCase()] ?? 0;
}

function createObservationVectorResult(obs, seedEntityIdSet = new Set()) {
  const categoryBoost = getObservationCategoryBoost(obs.category);
  const seedBonus = (
    (obs.subject_entity_id && seedEntityIdSet.has(obs.subject_entity_id))
    || (obs.object_entity_id && seedEntityIdSet.has(obs.object_entity_id))
  )
    ? 0.15
    : 0;

  return {
    type: obs.category,
    id: obs.id,
    entityId: obs.subject_entity_id ?? obs.object_entity_id ?? null,
    score: applyOriginPenalty(VECTOR_SCORE_WEIGHT * obs.vectorScore + categoryBoost + seedBonus, obs),
    summary: obs.detail,
    payload: obs,
    tokenCount: estimateTokens(obs.detail),
    hintIds: [],
    vectorScore: obs.vectorScore,
    graphScore: 0,
    categoryBoost,
    seedBonus,
  };
}

function mergeHybridResults(graphResults, vectorResults, observationVectorResults = []) {
  const vectorScoresByMessageId = new Map(
    vectorResults.map((result) => [result.id, Number(result.vectorScore ?? 0)]),
  );
  const observationVectorsById = new Map(
    observationVectorResults.map((result) => [result.id, result]),
  );
  const coveredMessageIds = new Set();
  const coveredObservationIds = new Set();
  const merged = [];

  for (const result of graphResults) {
    const graphScore = Number(result.score ?? 0);
    const messageId = getResultMessageId(result);
    const messageVectorScore = messageId ? vectorScoresByMessageId.get(messageId) : undefined;
    const observationVector = isObservationResult(result)
      ? observationVectorsById.get(result.id)
      : null;

    if (typeof messageVectorScore === "number" && messageId) {
      coveredMessageIds.add(messageId);
    }

    if (observationVector) {
      coveredObservationIds.add(result.id);
      const observationVectorScore = Number(observationVector.vectorScore ?? 0);
      const hybridVectorScore = Math.max(Number(messageVectorScore ?? 0), observationVectorScore);
      const categoryBoost = Number(observationVector.categoryBoost ?? 0);
      const seedBonus = Number(observationVector.seedBonus ?? 0);
      merged.push({
        ...result,
        graphScore,
        vectorScore: hybridVectorScore,
        messageVectorScore: Number(messageVectorScore ?? 0),
        observationVectorScore,
        categoryBoost,
        seedBonus,
        score: GRAPH_SCORE_WEIGHT * graphScore
          + applyOriginPenalty(VECTOR_SCORE_WEIGHT * hybridVectorScore + categoryBoost + seedBonus, result.payload),
      });
      continue;
    }

    if (typeof messageVectorScore === "number") {
      merged.push({
        ...result,
        graphScore,
        vectorScore: messageVectorScore,
        score: GRAPH_SCORE_WEIGHT * graphScore + VECTOR_SCORE_WEIGHT * messageVectorScore,
      });
      continue;
    }

    merged.push({
      ...result,
      graphScore,
      vectorScore: 0,
      score: GRAPH_SCORE_WEIGHT * graphScore,
    });
  }

  for (const result of vectorResults) {
    if (coveredMessageIds.has(result.id)) {
      continue;
    }

    merged.push(result);
  }

  for (const result of observationVectorResults) {
    if (coveredObservationIds.has(result.id)) {
      continue;
    }

    merged.push(result);
  }

  return merged;
}

export class RetrievalEngine {
  constructor({ graph, database, telemetry, classifier }) {
    this.graph = graph;
    this.database = database;
    this.telemetry = telemetry;
    this.classifier = classifier;
    this.lastQueryByConversation = new Map();
  }

  findSeedEntities(queryText, activeHints = []) {
    const directMatches = this.graph.matchQuery(queryText).slice(0, 12);
    const extracted = this.classifier.classifyText(queryText).entities;
    const extractedMatches = extracted.flatMap((entity) => this.graph.matchQuery(entity.label));
    const hintedMatches = activeHints
      .map((hint) => {
        const entity = hint.seed_entity_id
          ? this.graph.getEntity(hint.seed_entity_id)
          : this.graph.findEntityByLabel(hint.seed_label);
        const score = scoreHintSeed(queryText, hint.seed_label);

        if (!entity || score <= 0) {
          return null;
        }

        return {
          entity,
          score: 1.6 + score,
        };
      })
      .filter(Boolean);

    const merged = new Map();
    for (const match of [...directMatches, ...extractedMatches, ...hintedMatches]) {
      const current = merged.get(match.entity.id);
      if (!current || match.score > current.score) {
        merged.set(match.entity.id, match);
      }
    }

    return [...merged.values()]
      .sort((left, right) => right.score - left.score)
      .map((entry) => entry.entity);
  }

  findRelevantHints({ conversationId = null, queryText, seedEntities, activeHints }) {
    const seedIds = new Set(seedEntities.map((entity) => entity.id));
    const seedLabels = new Set(seedEntities.map((entity) => normalizeText(entity.label)).filter(Boolean));
    const relevant = new Map();

    for (const row of activeHints) {
      const sourceEntity = row.seed_entity_id
        ? this.graph.getEntity(row.seed_entity_id)
        : this.graph.findEntityByLabel(row.seed_label);
      const targetEntity = row.expand_entity_id
        ? this.graph.getEntity(row.expand_entity_id)
        : this.graph.findEntityByLabel(row.expand_label);
      const queryScore = scoreHintSeed(queryText, row.seed_label);
      const seedLabel = normalizeText(row.seed_label);

      if (!sourceEntity || !targetEntity || sourceEntity.id === targetEntity.id) {
        continue;
      }

      if (!seedIds.has(sourceEntity.id) && !seedLabels.has(seedLabel) && queryScore <= 0) {
        continue;
      }

      const ttlTurns = Math.max(1, Number(row.ttl_turns ?? 1));
      const turnsRemaining = Math.max(0, Number(row.turns_remaining ?? ttlTurns));
      const freshness = clamp(turnsRemaining / ttlTurns, 0.25, 1);
      const scopeBoost = conversationId && row.conversation_id === conversationId ? 1.15 : 1;
      const matchBoost = clamp(1 + queryScore * 0.12, 1, 1.35);
      const weight = clamp(Number(row.weight ?? 0.75) * freshness * scopeBoost * matchBoost, 0.15, 3);
      const hint = {
        id: row.id,
        seedEntityId: sourceEntity.id,
        seedLabel: sourceEntity.label,
        expandEntityId: targetEntity.id,
        expandLabel: targetEntity.label,
        predicate: inferHintPredicate(row.reason),
        reason: row.reason,
        conversationId: row.conversation_id ?? null,
        baseWeight: Number(row.weight ?? 0.75),
        baseTtlTurns: ttlTurns,
        weight,
        ttlTurns,
        turnsRemaining,
      };
      const key = `${hint.seedEntityId}:${hint.expandEntityId}:${hint.predicate}`;
      const current = relevant.get(key);
      if (!current || hint.weight > current.weight) {
        relevant.set(key, hint);
      }
    }

    return [...relevant.values()].sort((left, right) => right.weight - left.weight);
  }

  expandSeeds(seedEntities, retrievalHints = []) {
    const visited = new Map();
    const provenance = new Map();
    const expansionPath = [];
    const queue = seedEntities.map((entity) => ({ entityId: entity.id, score: 1, depth: 0 }));
    const hintsBySeed = new Map();

    for (const entity of seedEntities) {
      visited.set(entity.id, 1);
      provenance.set(entity.id, {
        entityId: entity.id,
        parentEntityId: null,
        source: "seed",
        hintIds: [],
        depth: 0,
      });
    }

    for (const hint of retrievalHints) {
      if (!hintsBySeed.has(hint.seedEntityId)) {
        hintsBySeed.set(hint.seedEntityId, []);
      }

      hintsBySeed.get(hint.seedEntityId).push(hint);
    }

    while (queue.length) {
      queue.sort((left, right) => right.score - left.score);
      const current = queue.shift();
      const entity = this.graph.getEntity(current.entityId);
      if (!entity) {
        continue;
      }

      const maxDepth = 1 + Math.min(4, Math.floor(Math.log2(entity.complexityScore + 1)) + 1);
      if (current.depth >= maxDepth) {
        continue;
      }

      for (const neighbor of this.graph.neighbors(entity.id)) {
        const predicateWeight = getWeight(neighbor.relationship.predicate);
        const complexityMultiplier = 1 + Math.min(1.5, neighbor.entity.complexityScore * 0.15);
        const nextScore = current.score * predicateWeight * complexityMultiplier;
        const threshold = 0.18 / complexityMultiplier;

        if (nextScore < threshold) {
          continue;
        }

        const bestScore = visited.get(neighbor.entity.id) ?? 0;
        if (nextScore <= bestScore) {
          continue;
        }

        visited.set(neighbor.entity.id, nextScore);
        const inheritedHintIds = provenanceHintIds(provenance, entity.id);
        provenance.set(neighbor.entity.id, {
          entityId: neighbor.entity.id,
          parentEntityId: entity.id,
          source: inheritedHintIds.length ? "hint_chain" : "graph",
          hintIds: inheritedHintIds,
          depth: current.depth + 1,
        });
        queue.push({
          entityId: neighbor.entity.id,
          score: nextScore,
          depth: current.depth + 1,
        });
        expansionPath.push({
          from: entity.id,
          to: neighbor.entity.id,
          predicate: neighbor.relationship.predicate,
          depth: current.depth + 1,
          score: Number(nextScore.toFixed(3)),
          source: "graph",
        });
      }

      for (const hint of hintsBySeed.get(entity.id) ?? []) {
        const target = this.graph.getEntity(hint.expandEntityId);
        if (!target) {
          continue;
        }

        const complexityMultiplier = 1 + Math.min(1.75, target.complexityScore * 0.18);
        const nextScore = current.score * HINT_BASE_WEIGHT * hint.weight * complexityMultiplier;
        const threshold = 0.1 / complexityMultiplier;

        if (nextScore < threshold) {
          continue;
        }

        const bestScore = visited.get(target.id) ?? 0;
        if (nextScore <= bestScore) {
          continue;
        }

        visited.set(target.id, nextScore);
        const inheritedHintIds = provenanceHintIds(provenance, entity.id);
        const nextHintIds = mergeHintIds(inheritedHintIds, [hint.id]);
        provenance.set(target.id, {
          entityId: target.id,
          parentEntityId: entity.id,
          source: "hint",
          hintIds: nextHintIds,
          depth: current.depth + 1,
        });
        queue.push({
          entityId: target.id,
          score: nextScore,
          depth: current.depth + 1,
        });
        expansionPath.push({
          from: entity.id,
          to: target.id,
          predicate: hint.predicate,
          depth: current.depth + 1,
          score: Number(nextScore.toFixed(3)),
          source: "hint",
          hintId: hint.id,
          reason: hint.reason,
          turnsRemaining: hint.turnsRemaining,
        });
      }
    }

    return { visited, provenance, expansionPath };
  }

  collectHintIds(provenance, entityIds) {
    return uniqueValues(entityIds.flatMap((entityId) => provenanceHintIds(provenance, entityId)));
  }

  learnHintPolicy({ conversationId = null, queryId, hints, graphBaselineVisited, provenance, results }) {
    const resultsByHint = new Map();
    const uniqueEntityIdsByHint = new Map();
    const appliedHintIds = new Set();

    for (const result of results) {
      for (const hintId of result.hintIds ?? []) {
        if (!resultsByHint.has(hintId)) {
          resultsByHint.set(hintId, []);
        }
        resultsByHint.get(hintId).push(result);
      }
    }

    for (const [entityId, entry] of provenance.entries()) {
      for (const hintId of entry.hintIds ?? []) {
        if (!uniqueEntityIdsByHint.has(hintId)) {
          uniqueEntityIdsByHint.set(hintId, new Set());
        }

        if (!graphBaselineVisited.has(entityId)) {
          uniqueEntityIdsByHint.get(hintId).add(entityId);
        }
        appliedHintIds.add(hintId);
      }
    }

    const outcomes = [];
    for (const hint of hints) {
      const applied = appliedHintIds.has(hint.id);
      const attributedResults = (resultsByHint.get(hint.id) ?? []).sort((left, right) => left.rank - right.rank);
      const uniqueEntityIds = uniqueEntityIdsByHint.get(hint.id) ?? new Set();
      const uniqueAttributedResults = attributedResults.filter((result) => {
        if (!result.entityId) {
          return uniqueEntityIds.size > 0;
        }

        return uniqueEntityIds.has(result.entityId);
      });
      const graphReachableWithoutHint = graphBaselineVisited.has(hint.expandEntityId);
      const scored = scoreHintOutcome({
        hint,
        applied,
        graphReachableWithoutHint,
        attributedResults,
        uniqueAttributedResults,
        uniqueEntityIds,
      });

      this.telemetry.recordRetrievalHintOutcome({
        hintId: hint.id,
        queryId,
        conversationId: conversationId ?? hint.conversationId ?? null,
        applied,
        rewarded: scored.rewarded,
        decayed: scored.decayed,
        reward: scored.reward,
        penalty: scored.penalty,
        nextWeight: scored.nextWeight,
        nextTtlTurns: scored.nextTtlTurns,
        detail: {
          seedLabel: hint.seedLabel,
          expandLabel: hint.expandLabel,
          queryHintWeight: hint.weight,
          applied,
          metrics: scored.metrics,
          topSummaries: attributedResults.slice(0, 3).map((result) => result.summary),
        },
      });

      outcomes.push({
        hintId: hint.id,
        seedLabel: hint.seedLabel,
        expandLabel: hint.expandLabel,
        applied,
        reward: scored.reward,
        penalty: scored.penalty,
        netReward: scored.netReward,
        nextWeight: scored.nextWeight,
        nextTtlTurns: scored.nextTtlTurns,
        metrics: scored.metrics,
      });
    }

    return outcomes.sort((left, right) => right.netReward - left.netReward);
  }

  applyMissHeuristic(conversationId, seedEntities) {
    const previous = this.lastQueryByConversation.get(conversationId);
    if (!previous || !seedEntities.length) {
      return [];
    }

    const misses = [];
    for (const seed of seedEntities) {
      if (previous.expandedEntityIds.includes(seed.id)) {
        continue;
      }

      const linked = previous.seedEntityIds.some((priorId) => this.graph.findPath(priorId, seed.id, 2));
      if (linked) {
        this.graph.registerMiss(seed.id);
        misses.push(seed.id);
      }
    }

    return misses;
  }

  async retrieve({ conversationId = null, queryText, scopeFilter = null }) {
    const start = performance.now();
    const resolvedScopeFilter = normalizeScopeFilter(scopeFilter);
    const activeHints = this.telemetry.listActiveRetrievalHints(128);
    const seedEntities = this.findSeedEntities(queryText, activeHints);
    const seedEntityIdSet = new Set(seedEntities.map((entity) => entity.id));
    const relevantHints = this.findRelevantHints({
      conversationId,
      queryText,
      seedEntities,
      activeHints,
    });
    const graphBaseline = relevantHints.length ? this.expandSeeds(seedEntities, []) : { visited: new Map() };
    const { visited, provenance, expansionPath } = this.expandSeeds(seedEntities, relevantHints);
    const expandedEntityIds = [...visited.keys()];

    const observations = this.database.listObservationsForEntities(expandedEntityIds, resolvedScopeFilter).map((item) => ({
      type: item.category,
      id: item.id,
      entityId: item.subject_entity_id ?? item.object_entity_id ?? null,
      score: applyOriginPenalty(visited.get(item.subject_entity_id) ?? visited.get(item.object_entity_id) ?? 0.4, item),
      summary: item.detail,
      payload: item,
      tokenCount: estimateTokens(item.detail),
      hintIds: this.collectHintIds(provenance, [item.subject_entity_id, item.object_entity_id]),
    }));

    const tasks = this.database.listTasksForEntities(expandedEntityIds, resolvedScopeFilter).map((item) => ({
      type: "task",
      id: item.id,
      entityId: item.entity_id ?? null,
      score: applyOriginPenalty((visited.get(item.entity_id) ?? 0.35) + 0.15, item),
      summary: item.title,
      payload: item,
      tokenCount: estimateTokens(item.title),
      hintIds: this.collectHintIds(provenance, [item.entity_id]),
    }));

    const decisions = this.database.listDecisionsForEntities(expandedEntityIds, resolvedScopeFilter).map((item) => ({
      type: "decision",
      id: item.id,
      entityId: item.entity_id ?? null,
      score: applyOriginPenalty((visited.get(item.entity_id) ?? 0.3) + 0.1, item),
      summary: item.title,
      payload: item,
      tokenCount: estimateTokens(item.title),
      hintIds: this.collectHintIds(provenance, [item.entity_id]),
    }));

    const constraints = this.database.listConstraintsForEntities(expandedEntityIds, resolvedScopeFilter).map((item) => ({
      type: "constraint",
      id: item.id,
      entityId: item.entity_id ?? null,
      score: applyOriginPenalty((visited.get(item.entity_id) ?? 0.3) + 0.18, item),
      summary: item.detail,
      payload: item,
      tokenCount: estimateTokens(item.detail),
      hintIds: this.collectHintIds(provenance, [item.entity_id]),
    }));

    const facts = this.database.listFactsForEntities(expandedEntityIds, resolvedScopeFilter).map((item) => ({
      type: "fact",
      id: item.id,
      entityId: item.entity_id ?? null,
      score: applyOriginPenalty((visited.get(item.entity_id) ?? 0.3) + 0.08, item),
      summary: item.detail,
      payload: item,
      tokenCount: estimateTokens(item.detail),
      hintIds: this.collectHintIds(provenance, [item.entity_id]),
    }));

    const linkedChunks = this.database.listChunksForEntities(expandedEntityIds, resolvedScopeFilter).map((item) => ({
      type: "chunk",
      id: item.id,
      entityId: item.entityId ?? null,
      score: (visited.get(item.entityId) ?? 0.25) + Number(item.score ?? 0),
      summary: `${item.path}#${item.ordinal}`,
      payload: item,
      tokenCount: Number(item.tokenCount ?? estimateTokens(item.content)),
      hintIds: this.collectHintIds(provenance, [item.entityId]),
    }));

    const ftsQuery = queryText
      .split(/\s+/)
      .map((token) => token.replace(/[^A-Za-z0-9]/g, "").trim())
      .filter((token) => token.length > 2)
      .join(" OR ");

    const ftsChunks = (ftsQuery ? this.database.searchChunks(ftsQuery, resolvedScopeFilter) : []).map((item) => ({
      type: "chunk",
      id: item.id,
      entityId: null,
      score: Math.max(0.2, 1 - Number(item.rank ?? 1)),
      summary: `${item.path}#${item.ordinal}`,
      payload: item,
      tokenCount: Number(item.tokenCount ?? estimateTokens(item.content)),
      hintIds: [],
    }));

    const ftsObservations = (ftsQuery
      ? this.database.searchObservations(ftsQuery, resolvedScopeFilter)
      : []
    ).map((obs) => ({
      type: obs.category,
      id: obs.id,
      entityId: obs.subject_entity_id ?? obs.object_entity_id ?? null,
      score: applyOriginPenalty(
        Math.max(0.3, 1 - Number(obs.rank ?? 1)) + getObservationCategoryBoost(obs.category),
        obs,
      ),
      summary: obs.detail,
      payload: obs,
      tokenCount: estimateTokens(obs.detail),
      hintIds: [],
    }));

    const entityResults = expandedEntityIds
      .map((entityId) => this.graph.getEntity(entityId))
      .filter(Boolean)
      .map((entity) => ({
        type: "entity",
        id: entity.id,
        entityId: entity.id,
        score: visited.get(entity.id) ?? 0.3,
        summary: `${entity.label} (${entity.kind})`,
        payload: entity,
        tokenCount: estimateTokens(entity.summary ?? entity.label),
        hintIds: provenanceHintIds(provenance, entity.id),
      }));

    const graphResults = dedupeResults([
      ...entityResults,
      ...constraints,
      ...tasks,
      ...decisions,
      ...facts,
      ...observations,
      ...linkedChunks,
      ...ftsChunks,
      ...ftsObservations,
    ]);

    const queryEmbedding = String(queryText ?? "").trim()
      ? await embedText(queryText)
      : null;
    const vectorResults = queryEmbedding
      ? this.database.listEmbeddedMessages(resolvedScopeFilter)
          .map((message) => ({
            ...message,
            vectorScore: cosineSimilarity(queryEmbedding, message.embedding),
          }))
          .filter((message) => Number.isFinite(message.vectorScore) && message.vectorScore > 0)
          .sort((left, right) => right.vectorScore - left.vectorScore)
          .slice(0, 50)
          .map((message) => createVectorResult(message, message.vectorScore))
      : [];
    const observationVectorResults = queryEmbedding
      ? this.database.listEmbeddedObservations(resolvedScopeFilter)
          .map((obs) => ({
            ...obs,
            vectorScore: cosineSimilarity(queryEmbedding, obs.embedding),
          }))
          .filter((obs) => Number.isFinite(obs.vectorScore) && obs.vectorScore > 0)
          .sort((left, right) => right.vectorScore - left.vectorScore)
          .slice(0, 50)
          .map((obs) => createObservationVectorResult(obs, seedEntityIdSet))
      : [];

    const allResults = dedupeResults(mergeHybridResults(graphResults, vectorResults, observationVectorResults))
      .sort((left, right) => right.score - left.score)
      .map((result, index) => ({
        ...result,
        rank: index + 1,
      }));

    const tokensConsumed = allResults.reduce((sum, result) => sum + result.tokenCount, 0);
    const latencyMs = Math.round(performance.now() - start);
    const missEntityIds = conversationId ? this.applyMissHeuristic(conversationId, seedEntities) : [];

    const queryId = this.telemetry.logRetrieval({
      conversationId,
      queryText,
      latencyMs,
      seedEntityIds: seedEntities.map((entity) => entity.id),
      expandedEntityIds,
      expansionPath,
      itemsReturned: allResults.length,
      tokensConsumed,
      missEntityIds,
      results: allResults,
    });
    const hintOutcomes = relevantHints.length
      ? this.learnHintPolicy({
        conversationId,
        queryId,
        hints: relevantHints,
        graphBaselineVisited: graphBaseline.visited,
        provenance,
        results: allResults,
      })
      : [];

    if (conversationId) {
      this.lastQueryByConversation.set(conversationId, {
        seedEntityIds: seedEntities.map((entity) => entity.id),
        expandedEntityIds,
      });
    }

    const graphVersion = this.database.getGraphVersion();

    return {
      queryId,
      latencyMs,
      graphVersion,
      seedEntities,
      expandedEntities: expandedEntityIds.map((entityId) => this.graph.getEntity(entityId)).filter(Boolean),
      expansionPath,
      retrievalHints: relevantHints,
      hintOutcomes,
      items: allResults,
      tokensConsumed,
      missEntityIds,
    };
  }
}
