import path from "node:path";
import { performance } from "node:perf_hooks";

import { ObservationClassifier } from "./classifier.js";
import { DocumentIndexer } from "./document-indexer.js";
import { embedBatch, embedText } from "./embeddings.js";
import { EntityGraph } from "./entity-graph.js";
import { clusterObservations, detectEpisodes, detectTopicClusters } from "./episode-clustering.js";
import { applyDecayToAllClaims } from "./importance-decay.js";
import { InjectionGuard } from "./injection-guard.js";
import { classifyIntent, INTENT_STRATEGIES } from "./intent-router.js";
import { CLAIM_TYPE_VALUES, LIFECYCLE_STATES } from "./claim-types.js";
import { normalizeLabel, validateKnowledgePatch } from "./knowledge-patch.js";
import { PreconsciousBuffer } from "./preconscious.js";
import { RetrievalEngine } from "./retrieval.js";
import { VectorIndex } from "./vector-index.js";
import { clamp, estimateTokens, parseJson } from "./utils.js";
import { AI_AUTO_APPLY_CONFIDENCE_THRESHOLD, classifyWriteClass, getQueuePressureDisposition, getWriteClassDisposition } from "./write-discipline.js";
import { createAssemblyCache } from "./assembly-cache.js";
import { analyzeClaimsTruthSet, ensureClaimForObservation, selectPreferredClaimsByResolution } from "../core/claim-resolution.js";
import { ContextDatabase } from "../db/database.js";
import { TelemetryDatabase } from "../db/telemetry-database.js";

function summarizeResults(items) {
  return items
    .slice(0, 8)
    .map((item) => `- ${item.type}: ${item.summary}`)
    .join("\n");
}

function lowerKey(value) {
  return normalizeLabel(value)?.toLowerCase() ?? null;
}

function defaultOriginKindForRole(role) {
  if (role === "user") {
    return "user";
  }

  if (role === "assistant") {
    return "agent";
  }

  return "system";
}

const RECALL_SCOPE_TYPES = {
  all: new Set(["message", "task", "decision", "constraint", "fact", "relationship"]),
  decisions: new Set(["decision"]),
  tasks: new Set(["task"]),
  constraints: new Set(["constraint"]),
  recent: new Set(["message", "task", "decision", "constraint", "fact", "relationship"]),
};
const EMBEDDING_BACKFILL_BATCH_SIZE = 10;
const EMBEDDING_BACKFILL_RATE_PER_SECOND = 10;

function mapTaskStatus(status) {
  return String(status ?? "").trim().toLowerCase() === "open" ? "active" : status ?? "active";
}

function delay(milliseconds) {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, Math.max(0, milliseconds));
    timer.unref?.();
  });
}

function logEmbeddingWarning(message, error = null) {
  const suffix = error?.message ? `: ${error.message}` : "";
  console.warn(`[embeddings] ${message}${suffix}`);
}

function embeddingTaskKey(kind, id) {
  return `${kind}:${id}`;
}

function truncateTextToTokens(text, tokenBudget) {
  const content = String(text ?? "");
  const remaining = Math.max(0, Math.trunc(tokenBudget));
  if (!content.trim() || remaining <= 0) {
    return { content: "", tokenCount: 0 };
  }

  if (estimateTokens(content) <= remaining) {
    return { content, tokenCount: estimateTokens(content) };
  }

  const maxCharacters = Math.max(0, remaining * 4 - 3);
  const truncated = content.slice(0, maxCharacters).trimEnd();
  const clipped = truncated ? `${truncated}...` : "";
  return {
    content: clipped,
    tokenCount: clipped ? estimateTokens(clipped) : 0,
  };
}

function toEventRecord(message, extra = {}) {
  return {
    event_id: message.ingestId ?? message.eventId ?? null,
    role: message.role,
    content: message.content,
    timestamp: message.capturedAt ?? message.timestamp ?? null,
    source: message.originKind ?? message.source ?? "memory",
    ...extra,
  };
}

function summarizeProposalPayload(payload = null) {
  if (!payload || typeof payload !== "object") {
    return [];
  }

  return Object.entries(payload)
    .filter(([, value]) => value !== null && value !== undefined && value !== "")
    .slice(0, 4)
    .map(([field, value]) => ({
      field,
      value: Array.isArray(value) ? value.join(", ") : String(value),
    }));
}

function getProposalQueueMetadata(row) {
  const writeClass = row.write_class ?? classifyWriteClass(row.proposal_type);
  return getQueuePressureDisposition({
    writeClass,
    status: row.status,
    confidence: row.confidence,
  });
}

function toProposalResponse(row, { sourceEventId = null } = {}) {
  const payload = parseJson(row.payload_json, null);
  const queueMetadata = getProposalQueueMetadata(row);

  return {
    mutation_id: row.id,
    proposal_id: row.id,
    type: row.proposal_type,
    status: row.status,
    confidence: Number(row.confidence ?? 0),
    detail: row.detail,
    reason: row.reason,
    subject_label: row.subject_label,
    predicate: row.predicate,
    object_label: row.object_label,
    created_at: row.created_at,
    reviewed_at: row.reviewed_at ?? null,
    reviewed_by_actor: row.reviewed_by_actor ?? null,
    source_event_id: sourceEventId,
    payload,
    payload_summary: summarizeProposalPayload(payload),
    ...queueMetadata,
  };
}

const STRUCTURED_EVIDENCE_TYPES = new Set(["task", "decision", "constraint", "fact", "relationship"]);
const TASK_STATUS_VALUES = new Set(["open", "pending", "active", "blocked", "done", "cancelled"]);
const DECISION_STATUS_VALUES = new Set(["proposed", "accepted", "rejected", "deferred", "superseded"]);
const GOAL_STATUS_VALUES = new Set(["active", "on_hold", "completed", "abandoned"]);
const CLAIM_STATE_RANK = {
  active: 5,
  candidate: 4,
  disputed: 3,
  superseded: 2,
  archived: 1,
};
const PRIORITY_RANK = {
  critical: 4,
  high: 3,
  medium: 2,
  low: 1,
};
const SCOPE_ORDER = ["private", "project", "shared", "public"];

function normalizeEntityIds(entityIds) {
  if (!Array.isArray(entityIds)) {
    return [];
  }

  return [...new Set(entityIds.map((value) => String(value ?? "").trim()).filter(Boolean))];
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
    scopeKind: scopeFilter.scopeKind ?? scopeFilter.scope_kind ?? null,
    scopeId: scopeFilter.scopeId ?? scopeFilter.scope_id ?? null,
  };
}

function scopeRank(scopeKind, defaultScopeKind = "private") {
  const index = SCOPE_ORDER.indexOf(scopeKind ?? defaultScopeKind);
  return index >= 0 ? index : SCOPE_ORDER.indexOf(defaultScopeKind);
}

function _matchesScopeFilter(row, scopeFilter, defaultScopeKind = "private") {
  const filter = normalizeScopeFilter(scopeFilter);
  if (!filter?.scopeKind) {
    return true;
  }

  const rowScopeKind = row?.scope_kind ?? row?.scopeKind ?? defaultScopeKind;
  if (scopeRank(rowScopeKind, defaultScopeKind) < scopeRank(filter.scopeKind, defaultScopeKind)) {
    return false;
  }

  if (rowScopeKind === "project" && filter.scopeKind === "project" && filter.scopeId) {
    return (row?.scope_id ?? row?.scopeId ?? null) === filter.scopeId;
  }

  return true;
}

function parseClaimMetadata(metadataJson) {
  return parseJson(metadataJson, {}) ?? {};
}

function claimEntityId(claim) {
  return claim?.subject_entity_id ?? claim?.subjectEntityId ?? claim?.object_entity_id ?? claim?.objectEntityId ?? null;
}

function claimTimestamp(claim) {
  const raw = claim?.updated_at
    ?? claim?.updatedAt
    ?? claim?.valid_from
    ?? claim?.validFrom
    ?? claim?.created_at
    ?? claim?.createdAt
    ?? "";
  const timestamp = Date.parse(raw);
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function compareClaimsForPriority(left, right) {
  const stateDelta = (CLAIM_STATE_RANK[left?.lifecycle_state] ?? 0) - (CLAIM_STATE_RANK[right?.lifecycle_state] ?? 0);
  if (stateDelta !== 0) {
    return stateDelta;
  }

  const importanceDelta = Number(left?.importance_score ?? 1.0) - Number(right?.importance_score ?? 1.0);
  if (importanceDelta !== 0) {
    return importanceDelta;
  }

  const leftConfidence = Number(left?.truth?.effective_confidence ?? left?.effective_confidence ?? left?.confidence ?? 0);
  const rightConfidence = Number(right?.truth?.effective_confidence ?? right?.effective_confidence ?? right?.confidence ?? 0);
  const confidenceDelta = leftConfidence - rightConfidence;
  if (confidenceDelta !== 0) {
    return confidenceDelta;
  }

  return claimTimestamp(left) - claimTimestamp(right);
}

function matchesEntityFilter(row, entityIds) {
  if (!entityIds.length) {
    return true;
  }

  const rowEntityId = row?.entity_id
    ?? row?.entityId
    ?? row?.subject_entity_id
    ?? row?.subjectEntityId
    ?? row?.object_entity_id
    ?? row?.objectEntityId
    ?? null;

  return rowEntityId ? entityIds.includes(rowEntityId) : false;
}

function getObservationIdFromResult(result) {
  if (!result) {
    return null;
  }

  if (result.type !== "message" && result.payload?.category && result.id) {
    return result.id;
  }

  return result.payload?.observation_id
    ?? result.payload?.observationId
    ?? result.payload?.id
    ?? null;
}

function sourceEventIdForResult(result) {
  return result?.payload?.message_ingest_id
    ?? result?.payload?.messageIngestId
    ?? result?.payload?.ingestId
    ?? result?.payload?.eventId
    ?? null;
}

function timestampForResult(result) {
  return result?.payload?.message_captured_at
    ?? result?.payload?.captured_at
    ?? result?.payload?.capturedAt
    ?? result?.payload?.created_at
    ?? result?.payload?.createdAt
    ?? result?.payload?.valid_from
    ?? result?.payload?.validFrom
    ?? null;
}

function normalizeDedupText(value) {
  return String(value ?? "")
    .toLowerCase()
    .replace(/[’']/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

function canonicalDedupTextForResult(result) {
  return normalizeDedupText(result?.summary ?? "");
}

function dedupEntityIdForResult(result) {
  return result?.entityId
    ?? result?.payload?.subject_entity_id
    ?? result?.payload?.subjectEntityId
    ?? result?.payload?.entity_id
    ?? result?.payload?.entityId
    ?? null;
}

function dedupFingerprintForResult(result) {
  if (!STRUCTURED_EVIDENCE_TYPES.has(result?.type)) {
    return null;
  }

  const normalizedSummary = canonicalDedupTextForResult(result);
  if (!normalizedSummary || normalizedSummary.length < 12) {
    return null;
  }

  return JSON.stringify({
    type: result?.type ?? null,
    entity_id: dedupEntityIdForResult(result),
    summary: normalizedSummary,
    lifecycle_state: result?.claim?.lifecycle_state ?? null,
    resolution_key: result?.claim?.resolution_key ?? null,
    temporal_support_label: result?.temporalSupportLabel ?? null,
  });
}

function compareResultsForDedupPreference(left, right) {
  const leftScore = Number(left?.score ?? 0);
  const rightScore = Number(right?.score ?? 0);
  if (leftScore !== rightScore) {
    return leftScore - rightScore;
  }

  const familyRank = (item) => {
    if (item?.targetFamily === "canonical") return 3;
    if (item?.targetFamily === "conversational") return 1;
    return 2;
  };
  const leftFamily = familyRank(left);
  const rightFamily = familyRank(right);
  if (leftFamily !== rightFamily) {
    return leftFamily - rightFamily;
  }

  const provenanceRank = (item) => {
    let rank = 0;
    if (getObservationIdFromResult(item)) rank += 2;
    if (sourceEventIdForResult(item)) rank += 1;
    if (item?.claim?.id) rank += 1;
    return rank;
  };
  const leftProvenance = provenanceRank(left);
  const rightProvenance = provenanceRank(right);
  if (leftProvenance !== rightProvenance) {
    return leftProvenance - rightProvenance;
  }

  return String(timestampForResult(left) ?? "").localeCompare(String(timestampForResult(right) ?? ""));
}

function fitObjectToBudget(item, primaryField, tokenBudget) {
  const remaining = Math.max(0, Math.trunc(tokenBudget));
  if (remaining <= 0) {
    return null;
  }

  const fullTokenCount = estimateTokens(JSON.stringify(item));
  if (fullTokenCount <= remaining) {
    return { item, tokenCount: fullTokenCount };
  }

  if (!primaryField || !item?.[primaryField]) {
    return null;
  }

  const staticTokenCount = estimateTokens(JSON.stringify({
    ...item,
    [primaryField]: "",
  }));
  if (staticTokenCount >= remaining) {
    return null;
  }

  const fitted = truncateTextToTokens(item[primaryField], remaining - staticTokenCount);
  if (!fitted.content) {
    return null;
  }

  return {
    item: {
      ...item,
      [primaryField]: fitted.content,
    },
    tokenCount: staticTokenCount + fitted.tokenCount,
  };
}

function normalizePacketStrategy(strategy = null) {
  return {
    claimTypes: Array.isArray(strategy?.claimTypes) ? strategy.claimTypes : [],
    claimStates: Array.isArray(strategy?.claimStates) ? strategy.claimStates : ["active", "candidate", "disputed"],
    evidenceRatio: Number(strategy?.evidenceRatio ?? 0.45),
    messageRatio: Number(strategy?.messageRatio ?? 0.35),
    steps: Array.isArray(strategy?.steps) ? strategy.steps : [],
  };
}

function normalizePacketRequest(input = {}) {
  const request = input && typeof input === "object" ? input : {};
  const scopeFilter = request.scopeFilter
    ?? request.scope_filter
    ?? (request.scopeKind || request.scopeId
      ? { scopeKind: request.scopeKind ?? null, scopeId: request.scopeId ?? null }
      : null)
    ?? (request.scope_kind || request.scope_id
      ? { scopeKind: request.scope_kind ?? null, scopeId: request.scope_id ?? null }
      : null);

  return {
    conversationId: request.conversationId ?? request.conversation_id ?? null,
    query: request.query ?? request.queryText ?? request.query_text ?? "",
    intent: request.intent ?? null,
    scopeFilter: normalizeScopeFilter(scopeFilter),
    tokenBudget: request.tokenBudget ?? request.token_budget ?? null,
  };
}

function normalizePacketIntent(intent) {
  const normalized = String(intent ?? "").trim();
  return Object.prototype.hasOwnProperty.call(INTENT_STRATEGIES, normalized) ? normalized : null;
}

function normalizeOpenItemsKind(kind) {
  const normalized = String(kind ?? "all").trim().toLowerCase();
  return normalized || "all";
}

function normalizeRegistryQueryRequest(input = {}) {
  const request = input && typeof input === "object" ? input : {};
  const filtersInput = request.filters && typeof request.filters === "object" && !Array.isArray(request.filters)
    ? request.filters
    : {};

  return {
    name: String(request.name ?? "").trim().toLowerCase(),
    query: String(request.query ?? "").trim(),
    filters: {
      ...filtersInput,
      status: Object.prototype.hasOwnProperty.call(filtersInput, "status")
        ? String(filtersInput.status ?? "").trim().toLowerCase()
        : filtersInput.status,
      date_from: Object.prototype.hasOwnProperty.call(filtersInput, "date_from")
        ? String(filtersInput.date_from ?? "").trim()
        : filtersInput.date_from,
      date_to: Object.prototype.hasOwnProperty.call(filtersInput, "date_to")
        ? String(filtersInput.date_to ?? "").trim()
        : filtersInput.date_to,
      tags: Array.isArray(filtersInput.tags)
        ? [...new Set(filtersInput.tags.map((tag) => String(tag ?? "").trim().toLowerCase()).filter(Boolean))].sort()
        : filtersInput.tags,
    },
  };
}

function resolveWorkingSetShare(strategy = null) {
  return clamp(0.3 - Number(strategy?.evidenceRatio ?? 0.45) * 0.1, 0.25, 0.3);
}

function allocatePacketTierBudgets(totalBudget, strategy = null) {
  let stable = Math.max(Math.floor(totalBudget * 0.18), Math.min(200, totalBudget));
  let active = Math.max(
    Math.floor(totalBudget * 0.28),
    Math.min(300, Math.max(0, totalBudget - stable)),
  );
  let working = totalBudget >= 700 ? Math.floor(totalBudget * resolveWorkingSetShare(strategy)) : 0;

  let overflow = stable + active + working - totalBudget;
  if (overflow > 0) {
    const workingReduction = Math.min(working, overflow);
    working -= workingReduction;
    overflow -= workingReduction;
  }

  if (overflow > 0) {
    const activeFloor = Math.min(300, Math.max(0, totalBudget - stable));
    const activeReduction = Math.min(Math.max(0, active - activeFloor), overflow);
    active -= activeReduction;
    overflow -= activeReduction;
  }

  if (overflow > 0) {
    const stableFloor = Math.min(200, Math.max(0, totalBudget - active));
    const stableReduction = Math.min(Math.max(0, stable - stableFloor), overflow);
    stable -= stableReduction;
    overflow -= stableReduction;
  }

  if (overflow > 0) {
    active = Math.max(0, active - overflow);
  }

  return {
    stable,
    active,
    working,
  };
}

function countUniqueIds(collections = []) {
  const ids = new Set();
  for (const collection of collections) {
    for (const item of collection ?? []) {
      if (item?.id) {
        ids.add(item.id);
      }
    }
  }
  return ids.size;
}

export class ContextOS {
  constructor({ rootDir, autoBackfillEmbeddings = true, deferInit = false }) {
    this.rootDir = rootDir;
    this.database = new ContextDatabase(path.join(rootDir, "data", "contextos.db"));
    this.telemetry = new TelemetryDatabase(path.join(rootDir, "data", "contextos_telemetry.db"));
    this.graph = new EntityGraph(this.database);
    this.classifier = new ObservationClassifier();
    this.guard = new InjectionGuard();
    this.vectorIndex = new VectorIndex(768);
    this.retrieval = new RetrievalEngine({
      graph: this.graph,
      database: this.database,
      telemetry: this.telemetry,
      classifier: this.classifier,
      vectorIndex: this.vectorIndex,
    });
    this.indexer = new DocumentIndexer({
      database: this.database,
      graph: this.graph,
      classifier: this.classifier,
    });
    this.backgroundTasks = new Set();
    this.embeddingTasks = new Map();
    this.startupEmbeddingBackfill = null;
    // Monitoring counters (in-memory)
    this._packetsByIntent = {};
    this._briefsAssembled = 0;
    // REQ-38: graph-version-aware packet assembly cache
    this._assemblyCache = createAssemblyCache();
    // v2.3: preconscious buffer and dream cycle lock
    this.preconsciousBuffer = new PreconsciousBuffer(50);
    this._dreamCycleLock = false;
    this._lastDreamCycleTimestamp = null;
    this._autoBackfillEmbeddings = autoBackfillEmbeddings;
    this.ready = false;
    if (!deferInit) {
      this.graph.load();
      this._rebuildVectorIndex();
      if (autoBackfillEmbeddings) {
        this.startEmbeddingBackfill();
      }
      this.ready = true;
    }
  }

  async init() {
    if (this.ready) {
      return;
    }
    this.graph.load();
    this._rebuildVectorIndex();
    if (this._autoBackfillEmbeddings) {
      this.startEmbeddingBackfill();
    }
    this.ready = true;
  }

  ensureConversation({ conversationId = null, title = "ContextOS Session" } = {}) {
    if (conversationId) {
      return this.database.getConversation(conversationId) ?? this.database.createConversation(title);
    }

    return this.database.createConversation(title);
  }

  buildGraphContext(limit = 120) {
    return {
      entities: this.graph
        .listEntities()
        .sort((left, right) => right.mentionCount - left.mentionCount || right.complexityScore - left.complexityScore)
        .slice(0, limit)
        .map((entity) => ({
          id: entity.id,
          label: entity.label,
          kind: entity.kind,
          complexityScore: entity.complexityScore,
          mentionCount: entity.mentionCount,
          missCount: entity.missCount,
        })),
    };
  }

  buildHeuristicPatch(content) {
    return validateKnowledgePatch(this.classifier.classifyText(content));
  }



  resolveEntityReference(entitiesByLabel, label) {
    const key = lowerKey(label);
    if (!key) {
      return null;
    }

    const existing = entitiesByLabel.get(key);
    if (existing) {
      return existing;
    }

    const resolved = this.graph.ensureEntity({ label });
    entitiesByLabel.set(key, resolved);
    return resolved;
  }

  persistKnowledgePatch({
    conversationId,
    messageId,
    patch,
    modelRuns = {},
    actorId = "system",
    scopeKind = "private",
    scopeId = null,
  }) {
    const entitiesByLabel = new Map();

    for (const entity of patch.entities) {
      const resolved = this.graph.ensureEntity(entity);
      entitiesByLabel.set(resolved.label.toLowerCase(), resolved);
      entitiesByLabel.set(entity.label.toLowerCase(), resolved);
      for (const alias of entity.aliases ?? []) {
        entitiesByLabel.set(alias.toLowerCase(), resolved);
      }
    }

    const observationRecords = [];
    const graphProposalIds = [];
    const retrievalHintIds = [];
    const claimStats = {
      created: 0,
      errors: [],
    };

    for (const proposal of patch.graphProposals) {
      const storedProposal = this.database.insertGraphProposal({
        conversationId,
        messageId,
        sourceRunId: modelRuns.predictExpansion ?? modelRuns.extractTurn ?? null,
        actorId,
        scopeKind,
        scopeId,
        proposalType: proposal.proposalType ?? proposal.category ?? "relationship",
        subjectLabel: proposal.subjectLabel,
        predicate: proposal.predicate,
        objectLabel: proposal.objectLabel,
        detail: proposal.detail,
        confidence: proposal.confidence,
        reason: proposal.reason,
        payload: proposal.payload ?? proposal,
      });
      this.graph.updateGraphVersion(storedProposal.graphVersion);
      graphProposalIds.push(storedProposal.id);
    }

    for (const observation of patch.observations) {
      if (observation.confidence < AI_AUTO_APPLY_CONFIDENCE_THRESHOLD) {
        const storedProposal = this.database.insertGraphProposal({
          conversationId,
          messageId,
          sourceRunId: modelRuns.extractTurn ?? null,
          actorId,
          scopeKind,
          scopeId,
          proposalType: observation.category,
          subjectLabel: observation.subjectLabel,
          predicate: observation.predicate,
          objectLabel: observation.objectLabel,
          detail: observation.detail,
          confidence: observation.confidence,
          reason: "Confidence below auto-apply threshold",
          payload: observation,
        });
        this.graph.updateGraphVersion(storedProposal.graphVersion);
        graphProposalIds.push(storedProposal.id);
        continue;
      }

      const subject = this.resolveEntityReference(entitiesByLabel, observation.subjectLabel);
      const object = this.resolveEntityReference(entitiesByLabel, observation.objectLabel);

      if (observation.category === "relationship" && subject && object) {
        this.graph.connect({
          subjectEntityId: subject.id,
          predicate: observation.predicate,
          objectEntityId: object.id,
          weight: observation.confidence,
          provenanceMessageId: messageId,
          metadata: {
            source: "model_patch",
            span: observation.sourceSpan,
          },
        });
      }

      const stored = this.database.insertObservation({
        conversationId,
        messageId,
        actorId,
        category: observation.category,
        predicate: observation.predicate ?? null,
        subjectEntityId: subject?.id ?? null,
        objectEntityId: object?.id ?? null,
        detail: observation.detail,
        confidence: observation.confidence,
        sourceSpan: observation.sourceSpan,
        metadata: observation.metadata ?? null,
        scopeKind,
        scopeId,
      });
      this.graph.updateGraphVersion(stored.graphVersion);

      try {
        const claim = ensureClaimForObservation(this.database, {
          id: stored.id,
          conversation_id: conversationId,
          message_id: messageId,
          actor_id: actorId,
          category: observation.category,
          predicate: observation.predicate ?? null,
          subject_entity_id: subject?.id ?? null,
          object_entity_id: object?.id ?? null,
          detail: observation.detail,
          confidence: observation.confidence,
          scope_kind: scopeKind,
          scope_id: scopeId,
          created_at: stored.createdAt,
        });

        if (claim?.id) {
          claimStats.created += 1;
          // v2.3: check salience and push to preconscious buffer if triggered
          const alert = this.checkSalience({
            type: "create",
            claim_type: observation.category,
            predicate: observation.predicate ?? null,
            metadata: observation.metadata ?? {},
            lifecycle_state: claim.lifecycle_state ?? "candidate",
            entity_label: subject?.label ?? null,
            detail: observation.detail,
            claim_id: claim.id,
          });
          if (alert) {
            this.preconsciousBuffer.push(alert);
          }
        }
      } catch (error) {
        console.warn(
          `[claims] Failed to create claim for observation ${stored.id}: ${error.message}`,
          error,
        );
        claimStats.errors.push({
          observationId: stored.id,
          message: error.message,
        });
      }

      if (observation.category === "task") {
        this.database.insertTask({
          observationId: stored.id,
          entityId: subject?.id ?? object?.id ?? null,
          title: observation.detail,
          priority: observation.metadata?.priority ?? "medium",
          status: "open",
        });
      }

      if (observation.category === "decision") {
        this.database.insertDecision({
          observationId: stored.id,
          entityId: subject?.id ?? object?.id ?? null,
          title: observation.detail,
          rationale: observation.metadata?.rationale ?? null,
        });
      }

      if (observation.category === "constraint") {
        this.database.insertConstraint({
          observationId: stored.id,
          entityId: subject?.id ?? object?.id ?? null,
          detail: observation.detail,
          severity: observation.metadata?.severity ?? "high",
        });
      }

      if (observation.category === "fact") {
        this.database.insertFact({
          observationId: stored.id,
          entityId: subject?.id ?? object?.id ?? null,
          detail: observation.detail,
        });
      }

      observationRecords.push({
        ...observation,
        id: stored.id,
        subjectEntityId: subject?.id ?? null,
        objectEntityId: object?.id ?? null,
      });
      this.enqueueObservationEmbedding({
        id: stored.id,
        detail: observation.detail,
      });
    }

    for (const hint of patch.retrieveHints) {
      const seed = this.resolveEntityReference(entitiesByLabel, hint.seed);
      const expandTo = this.resolveEntityReference(entitiesByLabel, hint.expandTo);
      const storedHint = this.telemetry.insertRetrievalHint({
        conversationId,
        messageId,
        sourceRunId: modelRuns.predictExpansion ?? null,
        actorId,
        seedEntityId: seed?.id ?? null,
        seedLabel: seed?.label ?? hint.seed,
        expandEntityId: expandTo?.id ?? null,
        expandLabel: expandTo?.label ?? hint.expandTo,
        reason: hint.reason,
        weight: hint.weight,
        ttlTurns: hint.ttlTurns,
      });
      retrievalHintIds.push(storedHint.id);
    }

    for (const adjustment of patch.complexityAdjustments) {
      const entity = this.resolveEntityReference(entitiesByLabel, adjustment.entity);
      if (entity) {
        this.graph.adjustComplexity(entity.id, adjustment.delta, adjustment.missIncrement ?? 0);
      }
    }

    return {
      entities: [...new Set([...entitiesByLabel.values()])],
      observations: observationRecords,
      claimStats,
      retrievalHintIds,
      graphProposalIds,
    };
  }

  trackBackgroundTask(task) {
    this.backgroundTasks.add(task);
    task.finally(() => {
      this.backgroundTasks.delete(task);
    });
    return task;
  }

  async waitForBackgroundTasks() {
    while (this.backgroundTasks.size) {
      await Promise.allSettled([...this.backgroundTasks]);
    }
  }

  async close() {
    await this.waitForBackgroundTasks();
    this.database.close();
    this.telemetry.close();
  }

  enqueueMessageEmbedding(message) {
    if (!message?.id || !String(message.content ?? "").trim()) {
      return null;
    }

    const taskKey = embeddingTaskKey("message", message.id);
    const existingTask = this.embeddingTasks.get(taskKey);
    if (existingTask) {
      return existingTask;
    }

    const task = (async () => {
      try {
        if (this.database.getMessageEmbedding(message.id)) {
          return null;
        }

        const embedding = await embedText(message.content);
        if (!embedding?.length) {
          return null;
        }

        const result = this.database.upsertMessageEmbedding({
          messageId: message.id,
          embedding,
        });
        this.vectorIndex?.insert(message.id, "message", embedding, {
          scopeKind: message.scopeKind ?? message.scope_kind,
          scopeId: message.scopeId ?? message.scope_id,
        });
        return result;
      } catch (error) {
        logEmbeddingWarning(`Failed to embed message ${message.id}`, error);
        return null;
      } finally {
        this.embeddingTasks.delete(taskKey);
      }
    })();

    this.embeddingTasks.set(taskKey, task);
    return this.trackBackgroundTask(task);
  }

  enqueueObservationEmbedding(observation) {
    if (!observation?.id || !String(observation.detail ?? "").trim()) {
      return null;
    }

    const taskKey = embeddingTaskKey("observation", observation.id);
    const existingTask = this.embeddingTasks.get(taskKey);
    if (existingTask) {
      return existingTask;
    }

    const task = (async () => {
      try {
        if (this.database.getObservationEmbedding(observation.id)) {
          return null;
        }

        const embedding = await embedText(observation.detail);
        if (!embedding?.length) {
          return null;
        }

        const result = this.database.upsertObservationEmbedding({
          observationId: observation.id,
          embedding,
        });
        this.vectorIndex?.insert(observation.id, "observation", embedding, {
          scopeKind: observation.scopeKind ?? observation.scope_kind,
          scopeId: observation.scopeId ?? observation.scope_id,
        });
        return result;
      } catch (error) {
        logEmbeddingWarning(`Failed to embed observation ${observation.id}`, error);
        return null;
      } finally {
        this.embeddingTasks.delete(taskKey);
      }
    })();

    this.embeddingTasks.set(taskKey, task);
    return this.trackBackgroundTask(task);
  }


  _rebuildVectorIndex() {
    if (!this.vectorIndex) {
      return;
    }

    const messages = this.database.listEmbeddedMessages();
    const observations = this.database.listEmbeddedObservations();
    
    // Include cluster level embeddings (L0 and L1 for scatter phase)
    let clusterLevelEmbeddings = [];
    try {
      const clusterLevels = this.database.prepare(`
        SELECT cluster_id, level, embedding
        FROM cluster_level_embeddings
        WHERE level IN (0, 1)
      `).all();
      
      clusterLevelEmbeddings = clusterLevels.map((cl) => {
        let embedding = cl.embedding;
        // Deserialize if it's a blob
        if (embedding instanceof Uint8Array) {
          const bytes = embedding;
          if (bytes.byteLength > 0 && bytes.byteLength % Float32Array.BYTES_PER_ELEMENT === 0) {
            embedding = new Float32Array(bytes.buffer, bytes.byteOffset, bytes.byteLength / Float32Array.BYTES_PER_ELEMENT).slice();
          }
        }
        return {
          id: `cl:${cl.cluster_id}:${cl.level}`,
          type: `cluster_l${cl.level}`,
          embedding,
        };
      });
    } catch (_err) {
      // Cluster levels might not exist yet (graceful degradation)
    }
    
    const items = [
      ...messages.map((m) => ({
        id: m.id,
        type: "message",
        embedding: m.embedding,
        scopeKind: m.scopeKind ?? m.scope_kind,
        scopeId: m.scopeId ?? m.scope_id,
      })),
      ...observations.map((o) => ({
        id: o.id,
        type: "observation",
        embedding: o.embedding,
        scopeKind: o.scope_kind,
        scopeId: o.scope_id,
      })),
      ...clusterLevelEmbeddings,
    ];
    this.vectorIndex.rebuild(items);
  }

  startEmbeddingBackfill() {
    if (this.startupEmbeddingBackfill) {
      return this.startupEmbeddingBackfill;
    }

    const task = (async () => {
      try {
        await this.backfillEmbeddings();
      } catch (error) {
        logEmbeddingWarning("Startup embedding backfill failed", error);
      }
    })();

    this.startupEmbeddingBackfill = this.trackBackgroundTask(task);
    return this.startupEmbeddingBackfill;
  }

  async backfillEmbeddings({
    batchSize = EMBEDDING_BACKFILL_BATCH_SIZE,
    rateLimitPerSecond = EMBEDDING_BACKFILL_RATE_PER_SECOND,
    logProgress = false,
  } = {}) {
    const resolvedBatchSize = Math.max(1, Math.trunc(batchSize) || EMBEDDING_BACKFILL_BATCH_SIZE);
    const resolvedRate = Math.max(1, Math.trunc(rateLimitPerSecond) || EMBEDDING_BACKFILL_RATE_PER_SECOND);
    const intervalMs = Math.ceil((resolvedBatchSize / resolvedRate) * 1000);
    let processedMessages = 0;
    let processedObservations = 0;

    const backfillBatch = async ({
      kind,
      listMissing,
      selectText,
      persist,
    }) => {
      let processed = 0;

      while (true) {
        const missingRows = listMissing(resolvedBatchSize);
        if (!missingRows.length) {
          break;
        }

        const startedAt = performance.now();
        const embeddings = await embedBatch(missingRows.map((row) => selectText(row)));
        if (!embeddings) {
          break;
        }

        for (let index = 0; index < missingRows.length; index += 1) {
          const embedding = embeddings[index];
          if (!embedding?.length) {
            continue;
          }

          persist(missingRows[index], embedding);
          processed += 1;

          if (logProgress && processed % 100 === 0) {
            console.log(`[embeddings] Backfilled ${processed} ${kind}`);
          }
        }

        if (missingRows.length < resolvedBatchSize) {
          break;
        }

        const elapsedMs = performance.now() - startedAt;
        if (elapsedMs < intervalMs) {
          await delay(intervalMs - elapsedMs);
        }
      }

      return processed;
    };

    processedMessages = await backfillBatch({
      kind: "messages",
      listMissing: (limit) => this.database.listMessagesMissingEmbeddings(limit),
      selectText: (message) => message.content,
      persist: (message, embedding) => {
        this.database.upsertMessageEmbedding({
          messageId: message.id,
          embedding,
        });
      },
    });

    processedObservations = await backfillBatch({
      kind: "observations",
      listMissing: (limit) => this.database.listObservationsMissingEmbeddings(limit),
      selectText: (observation) => observation.detail,
      persist: (observation, embedding) => {
        this.database.upsertObservationEmbedding({
          observationId: observation.id,
          embedding,
        });
      },
    });

    return {
      processed: processedMessages + processedObservations,
      processedMessages,
      processedObservations,
      ...this.database.getEmbeddingCoverage(),
    };
  }

  async ingestMessage({
    conversationId = null,
    conversationTitle = "ContextOS Session",
    role,
    direction,
    content,
    raw = null,
    ingestId = null,
    actorId = "system",
    originKind = null,
    sourceMessageId = null,
    scopeKind = "private",
    scopeId = null,
  }) {
    const conversation = this.ensureConversation({ conversationId, title: conversationTitle });
    const message = this.database.insertMessage({
      conversationId: conversation.id,
      role,
      direction,
      actorId,
      originKind: originKind ?? defaultOriginKindForRole(role),
      sourceMessageId,
      scopeKind,
      scopeId,
      content,
      tokenCount: estimateTokens(content),
      raw,
      ingestId,
    });

    if (message.deduped) {
      return {
        conversationId: conversation.id,
        message,
        entities: [],
        observations: [],
        retrievalHintIds: [],
        graphProposalIds: [],
        aliases: [],
        modelRuns: {},
      };
    }

    this.enqueueMessageEmbedding(message);

    return {
      conversationId: conversation.id,
      message,
      entities: [],
      observations: [],
      retrievalHintIds: [],
      graphProposalIds: [],
      aliases: [],
      modelRuns: {},
    };
  }

  async retrieve({ conversationId = null, queryText, scopeFilter = null }) {
    return this.retrieval.retrieve({ conversationId, queryText, scopeFilter });
  }

  dedupeRankedEvidence(items = []) {
    const rankedItems = Array.isArray(items) ? items : [];
    const keptByFingerprint = new Map();
    const deduped = [];
    let droppedCount = 0;

    for (const item of rankedItems) {
      const fingerprint = dedupFingerprintForResult(item);
      if (!fingerprint) {
        deduped.push(item);
        continue;
      }

      const existing = keptByFingerprint.get(fingerprint);
      if (!existing) {
        keptByFingerprint.set(fingerprint, { item, index: deduped.length });
        deduped.push(item);
        continue;
      }

      if (compareResultsForDedupPreference(item, existing.item) > 0) {
        deduped[existing.index] = item;
        keptByFingerprint.set(fingerprint, { item, index: existing.index });
      }
      droppedCount += 1;
    }

    return {
      items: deduped,
      diagnostics: {
        applied: true,
        before_count: rankedItems.length,
        after_count: deduped.length,
        dropped_count: droppedCount,
        duplicate_group_count: rankedItems.length - deduped.length,
        mode: "exact_normalized_structured_summary",
      },
    };
  }

  buildRecallEvidence(items, scope = "all", tokenBudget = 2000) {
    const allowedTypes = RECALL_SCOPE_TYPES[scope] ?? RECALL_SCOPE_TYPES.all;
    const byEventId = new Map();

    for (const item of items) {
      if (!allowedTypes.has(item.type)) {
        continue;
      }

      const payload = item.payload ?? {};
      const eventId = payload.message_ingest_id ?? payload.messageIngestId ?? payload.ingestId ?? payload.eventId ?? null;
      const content = payload.message_content ?? payload.content ?? null;
      const role = payload.message_role ?? payload.role ?? "system";
      const timestamp = payload.message_captured_at
        ?? payload.captured_at
        ?? payload.capturedAt
        ?? payload.createdAt
        ?? payload.created_at
        ?? null;
      const source = payload.origin_kind ?? payload.originKind ?? "memory";

      if (!eventId || !content) {
        continue;
      }

      const current = byEventId.get(eventId);
      const candidate = {
        event_id: eventId,
        role,
        content,
        timestamp,
        score: Number(Number(item.score ?? 0).toFixed(3)),
        source,
        claim: item.claim ?? null,
        target_family: item.targetFamily ?? null,
        artifact_kind: item.artifactKind ?? null,
        retrieval_type: item.type ?? null,
      };

      if (!current || candidate.score > current.score) {
        byEventId.set(eventId, candidate);
      }
    }

    const ranked = [...byEventId.values()].sort((left, right) =>
      right.score - left.score || String(right.timestamp ?? "").localeCompare(String(left.timestamp ?? "")));

    const evidence = [];
    let tokenCount = 0;

    for (const candidate of ranked) {
      const remaining = Math.max(0, tokenBudget - tokenCount);
      if (remaining <= 0) {
        break;
      }

      const fitted = truncateTextToTokens(candidate.content, remaining);
      if (!fitted.content) {
        continue;
      }

      evidence.push({
        ...candidate,
        content: fitted.content,
      });
      tokenCount += fitted.tokenCount;
    }

    return {
      evidence,
      totalResults: ranked.length,
      tokenCount,
    };
  }

  resolveSourceEventId(messageId, messageCache = null) {
    if (!messageId) {
      return null;
    }

    if (messageCache?.has(messageId)) {
      return messageCache.get(messageId);
    }

    const eventId = this.database.getMessage(messageId)?.ingestId ?? null;
    messageCache?.set(messageId, eventId);
    return eventId;
  }

  claimEntityLabel(claim) {
    const entity = this.graph.getEntity(claimEntityId(claim));
    return entity?.label ?? null;
  }

  fitCompactClaim(claim, tokenBudget = Number.POSITIVE_INFINITY, messageCache = null) {
    const truth = claim?.truth ?? null;
    const fitted = fitObjectToBudget({
      id: claim.id,
      type: claim.claim_type ?? null,
      value: claim.value_text ?? claim.predicate ?? "",
      state: claim.lifecycle_state ?? null,
      confidence: Number(claim.confidence ?? 0),
      effective_confidence: Number(truth?.effective_confidence ?? claim.effective_confidence ?? claim.confidence ?? 0),
      support_count: Number(truth?.support_count ?? claim.support_count ?? 1),
      has_conflict: Boolean(truth?.has_conflict ?? claim.has_conflict ?? false),
      conflict_set_id: truth?.conflict_set_id ?? claim.conflict_set_id ?? null,
      entity_label: this.claimEntityLabel(claim),
      valid_from: claim.valid_from ?? null,
      source_event_id: this.resolveSourceEventId(claim.message_id, messageCache),
    }, "value", tokenBudget);

    return fitted;
  }

  fitTaskSummary(claim, tokenBudget = Number.POSITIVE_INFINITY) {
    const metadata = parseClaimMetadata(claim.metadata_json);
    const rawValue = String(claim.value_text ?? "").trim();
    const normalizedValue = rawValue.toLowerCase();
    const rawLooksLikeStatus = TASK_STATUS_VALUES.has(normalizedValue);
    const status = String(
      metadata.status
      ?? metadata.taskStatus
      ?? (rawLooksLikeStatus ? mapTaskStatus(normalizedValue) : (claim.lifecycle_state === "candidate" ? "candidate" : "active")),
    ).trim().toLowerCase();
    const title = String(
      metadata.title
      ?? metadata.taskTitle
      ?? metadata.detail
      ?? (rawLooksLikeStatus ? (claim.predicate ?? rawValue) : rawValue)
      ?? claim.predicate
      ?? "task",
    ).trim();

    return fitObjectToBudget({
      id: claim.id,
      title,
      status,
      priority: String(metadata.priority ?? "medium").trim().toLowerCase() || "medium",
      entity_label: this.claimEntityLabel(claim),
      blocker: status === "blocked"
        ? String(metadata.blocker ?? (rawLooksLikeStatus ? "" : rawValue)).trim() || null
        : null,
    }, "title", tokenBudget);
  }

  fitDecisionSummary(claim, tokenBudget = Number.POSITIVE_INFINITY) {
    const metadata = parseClaimMetadata(claim.metadata_json);
    const rawValue = String(claim.value_text ?? "").trim();
    const normalizedValue = rawValue.toLowerCase();
    const rawLooksLikeStatus = DECISION_STATUS_VALUES.has(normalizedValue);
    const status = String(
      metadata.status
      ?? (rawLooksLikeStatus ? normalizedValue : (claim.lifecycle_state === "candidate" ? "proposed" : "accepted")),
    ).trim().toLowerCase();
    const title = String(
      metadata.title
      ?? metadata.detail
      ?? (rawLooksLikeStatus ? (claim.predicate ?? rawValue) : rawValue)
      ?? claim.predicate
      ?? "decision",
    ).trim();

    return fitObjectToBudget({
      id: claim.id,
      title,
      status,
      rationale: metadata.rationale ?? null,
      entity_label: this.claimEntityLabel(claim),
    }, "title", tokenBudget);
  }

  fitGoalSummary(claim, tokenBudget = Number.POSITIVE_INFINITY) {
    const metadata = parseClaimMetadata(claim.metadata_json);
    const rawValue = String(claim.value_text ?? "").trim();
    const normalizedValue = rawValue.toLowerCase();
    const rawLooksLikeStatus = GOAL_STATUS_VALUES.has(normalizedValue);
    const status = String(
      metadata.status
      ?? metadata.goalStatus
      ?? (rawLooksLikeStatus ? normalizedValue : (claim.lifecycle_state === "candidate" ? "candidate" : "active")),
    ).trim().toLowerCase();
    const detail = String(
      metadata.detail
      ?? metadata.title
      ?? (rawLooksLikeStatus ? (claim.predicate ?? rawValue) : rawValue)
      ?? claim.predicate
      ?? "goal",
    ).trim();

    return fitObjectToBudget({
      id: claim.id,
      detail,
      status,
      entity_label: this.claimEntityLabel(claim),
    }, "detail", tokenBudget);
  }

  annotateResultsWithClaims(items, scopeFilter = null) {
    const observationIds = [...new Set((items ?? []).map((item) => getObservationIdFromResult(item)).filter(Boolean))];
    if (!observationIds.length) {
      return (items ?? []).map((item) => ({
        ...item,
        claim: null,
      }));
    }

    const claimsByObservationId = new Map();
    for (const claim of this.database.listClaimsByObservationIds(observationIds, scopeFilter)) {
      const observationId = claim.observation_id ?? null;
      if (!observationId) {
        continue;
      }

      const current = claimsByObservationId.get(observationId);
      if (!current || compareClaimsForPriority(claim, current) > 0) {
        claimsByObservationId.set(observationId, claim);
      }
    }

    return (items ?? []).map((item) => {
      const claim = claimsByObservationId.get(getObservationIdFromResult(item)) ?? null;
      // v2.3: multiply score by claim importance_score
      const importanceScore = Number(claim?.importance_score ?? 1.0);
      return {
        ...item,
        score: claim ? Number((Number(item.score ?? 0) * importanceScore).toFixed(3)) : item.score,
        claim: claim
          ? {
            id: claim.id,
            lifecycle_state: claim.lifecycle_state,
            superseded_by: claim.superseded_by_claim_id ?? null,
            confidence: Number(claim.confidence ?? 0),
            valid_from: claim.valid_from ?? null,
            resolution_key: claim.resolution_key ?? null,
          }
          : null,
      };
    });
  }

  buildStructuredEvidence(items, tokenBudget = 1200) {
    const evidence = [];
    let tokenCount = 0;

    for (const item of items ?? []) {
      if (!STRUCTURED_EVIDENCE_TYPES.has(item.type)) {
        continue;
      }

      const remaining = Math.max(0, tokenBudget - tokenCount);
      if (remaining <= 0) {
        break;
      }

      const sourceText = item.summary
        ?? item.payload?.detail
        ?? item.payload?.content
        ?? "";
      const fitted = truncateTextToTokens(sourceText, remaining);
      if (!fitted.content) {
        continue;
      }

      evidence.push({
        type: item.type,
        id: item.id,
        content: fitted.content,
        score: Number(Number(item.score ?? 0).toFixed(3)),
        entity_id: item.entityId ?? item.payload?.subject_entity_id ?? item.payload?.subjectEntityId ?? null,
        source_event_id: sourceEventIdForResult(item),
        observation_id: getObservationIdFromResult(item),
      });
      tokenCount += fitted.tokenCount;
    }

    return {
      evidence,
      tokenCount,
    };
  }

  buildStablePrefix({ scopeFilter = null, tokenBudget = 400 } = {}) {
    const budget = Math.max(0, Math.trunc(tokenBudget));
    const messageCache = new Map();
    const result = {
      profile: {
        preferences: [],
        rules: [],
        facts: [],
      },
      hard_constraints: [],
      tokenCount: 0,
    };
    let remaining = budget;
    const currentClaims = this.database.listCurrentClaims({
      types: ["preference", "rule", "fact", "constraint"],
      scopeFilter,
      limit: 48,
    });
    const addClaims = (target, claims) => {
      for (const claim of claims) {
        const fitted = this.fitCompactClaim(claim, remaining, messageCache);
        if (!fitted) {
          continue;
        }

        target.push(fitted.item);
        remaining -= fitted.tokenCount;
        result.tokenCount += fitted.tokenCount;
      }
    };

    addClaims(result.profile.rules, currentClaims.filter((claim) => claim.claim_type === "rule"));
    addClaims(result.profile.preferences, currentClaims.filter((claim) => claim.claim_type === "preference"));
    addClaims(result.profile.facts, currentClaims.filter((claim) => claim.claim_type === "fact"));
    addClaims(result.hard_constraints, currentClaims.filter((claim) => {
      if (claim.claim_type !== "constraint") {
        return false;
      }

      const severity = String(parseClaimMetadata(claim.metadata_json).severity ?? "").trim().toLowerCase();
      return !severity || severity === "high" || severity === "critical";
    }));

    if (result.tokenCount > 0) {
      return result;
    }

    const fallbackFacts = this.database.queryRegistry("profile").slice(0, 8);
    const fallbackConstraints = this.database.queryRegistry("constraints")
      .filter((row) => {
        const severity = String(row.severity ?? "").trim().toLowerCase();
        return !severity || severity === "high" || severity === "critical";
      })
      .slice(0, 8);
    const addFallback = (target, rows, type) => {
      for (const row of rows) {
        const fitted = fitObjectToBudget({
          id: row.id,
          type,
          value: row.detail ?? row.title ?? "",
          state: row.status ?? "active",
          confidence: 1,
          entity_label: row.entityLabel ?? null,
          valid_from: row.createdAt ?? null,
          source_event_id: row.eventId ?? null,
        }, "value", remaining);
        if (!fitted) {
          continue;
        }

        target.push(fitted.item);
        remaining -= fitted.tokenCount;
        result.tokenCount += fitted.tokenCount;
      }
    };

    addFallback(result.profile.facts, fallbackFacts, "fact");
    addFallback(result.hard_constraints, fallbackConstraints, "constraint");

    return result;
  }

  buildActiveState({ entityIds = [], scopeFilter = null, limit = 6, tokenBudget = 600 } = {}) {
    const normalizedEntityIds = normalizeEntityIds(entityIds);
    const normalizedLimit = Math.max(1, Math.trunc(limit || 6));
    const budget = Math.max(0, Math.trunc(tokenBudget));
    const result = {
      tasks: [],
      decisions: [],
      goals: [],
      tokenCount: 0,
    };
    let remaining = budget;
    const currentClaims = this.database.listCurrentClaims({
      types: ["task", "decision", "goal"],
      entityIds: normalizedEntityIds.length ? normalizedEntityIds : null,
      scopeFilter,
      limit: Math.max(normalizedLimit * 8, 24),
    });
    const taskClaims = currentClaims
      .filter((claim) => claim.claim_type === "task")
      .sort((left, right) => {
        const leftPriority = String(parseClaimMetadata(left.metadata_json).priority ?? "medium").toLowerCase();
        const rightPriority = String(parseClaimMetadata(right.metadata_json).priority ?? "medium").toLowerCase();
        return (PRIORITY_RANK[rightPriority] ?? 0) - (PRIORITY_RANK[leftPriority] ?? 0)
          // v2.3: importance_score as secondary sort after priority
          || (Number(right.importance_score ?? 1.0) - Number(left.importance_score ?? 1.0))
          || claimTimestamp(right) - claimTimestamp(left);
      });
    const decisionClaims = currentClaims
      .filter((claim) => claim.claim_type === "decision")
      .sort((left, right) => claimTimestamp(right) - claimTimestamp(left));
    const goalClaims = currentClaims
      .filter((claim) => claim.claim_type === "goal")
      .sort((left, right) => claimTimestamp(right) - claimTimestamp(left));
    const addSummaries = (target, claims, mapper, predicate = null) => {
      for (const claim of claims) {
        if (target.length >= normalizedLimit) {
          break;
        }

        const fitted = mapper.call(this, claim, remaining);
        if (!fitted) {
          continue;
        }

        if (predicate && !predicate(fitted.item)) {
          continue;
        }

        target.push(fitted.item);
        remaining -= fitted.tokenCount;
        result.tokenCount += fitted.tokenCount;
      }
    };

    addSummaries(result.tasks, taskClaims, this.fitTaskSummary, (item) => !["done", "cancelled"].includes(item.status));
    addSummaries(result.decisions, decisionClaims, this.fitDecisionSummary, (item) => item.status === "accepted");
    addSummaries(result.goals, goalClaims, this.fitGoalSummary, (item) => !["completed", "abandoned"].includes(item.status));

    if (!result.tasks.length) {
      for (const row of this.database.listOpenTasks().filter((entry) => matchesEntityFilter(entry, normalizedEntityIds))) {
        if (result.tasks.length >= normalizedLimit) {
          break;
        }

        const fitted = fitObjectToBudget({
          id: row.id,
          title: row.title,
          status: mapTaskStatus(row.status),
          priority: row.priority ?? "medium",
          entity_label: row.entityLabel ?? null,
          blocker: null,
        }, "title", remaining);
        if (!fitted) {
          continue;
        }

        result.tasks.push(fitted.item);
        remaining -= fitted.tokenCount;
        result.tokenCount += fitted.tokenCount;
      }
    }

    if (!result.decisions.length) {
      for (const row of this.database.listOpenDecisions().filter((entry) => matchesEntityFilter(entry, normalizedEntityIds))) {
        if (result.decisions.length >= normalizedLimit) {
          break;
        }

        const fitted = fitObjectToBudget({
          id: row.id,
          title: row.title,
          status: "accepted",
          rationale: row.rationale ?? null,
          entity_label: row.entityLabel ?? null,
        }, "title", remaining);
        if (!fitted) {
          continue;
        }

        result.decisions.push(fitted.item);
        remaining -= fitted.tokenCount;
        result.tokenCount += fitted.tokenCount;
      }
    }

    return result;
  }

  buildUnresolvedConflicts({ entityIds = [], scopeFilter = null, limit = 6, tokenBudget = Number.POSITIVE_INFINITY } = {}) {
    const normalizedEntityIds = normalizeEntityIds(entityIds);
    const normalizedLimit = Math.max(1, Math.trunc(limit || 6));
    const budget = Math.max(0, Math.trunc(tokenBudget));
    const messageCache = new Map();
    const candidateClaims = this.database.listRecentClaims({
      scopeFilter,
      entityIds: normalizedEntityIds.length ? normalizedEntityIds : null,
      lifecycleStates: ["active", "candidate", "disputed"],
      limit: Math.max(normalizedLimit * 8, 24),
    });
    const truthAnalysis = analyzeClaimsTruthSet(candidateClaims);

    const groups = truthAnalysis.conflicts
      .map((conflict) => ({
        ...conflict,
        claims: conflict.claim_ids
          .map((claimId) => {
            const claim = candidateClaims.find((entry) => entry.id === claimId);
            if (!claim) {
              return null;
            }

            return {
              ...claim,
              truth: truthAnalysis.byClaimId.get(claim.id) ?? null,
            };
          })
          .filter(Boolean),
      }))
      .filter((group) => group.claims.length > 0)
      .sort((left, right) => right.claim_count - left.claim_count)
      .slice(0, normalizedLimit);

    const result = [];
    let tokenCount = 0;
    for (const group of groups) {
      const compactClaims = group.claims
        .sort((left, right) => compareClaimsForPriority(right, left))
        .slice(0, 3)
        .map((claim) => this.fitCompactClaim(claim, Number.POSITIVE_INFINITY, messageCache)?.item)
        .filter(Boolean);
      const fitted = fitObjectToBudget({
        conflict_set_id: group.conflict_set_id,
        resolution_key: group.resolution_key,
        claim_type: group.claims[0]?.claim_type ?? null,
        entity_label: this.claimEntityLabel(group.claims[0] ?? null),
        claims: compactClaims,
        claim_count: group.claim_count,
      }, null, Number.isFinite(budget) ? (budget - tokenCount) : Number.POSITIVE_INFINITY);
      if (!fitted) {
        continue;
      }

      result.push(fitted.item);
      tokenCount += fitted.tokenCount;
    }

    return {
      conflicts: result,
      tokenCount,
    };
  }

  buildWorkingSet({ strategy = null, retrieval = null, entityIds = [], scopeFilter = null, tokenBudget = 600 } = {}) {
    const normalizedStrategy = normalizePacketStrategy(strategy);
    const focusEntities = retrieval
      ? (retrieval.seedEntities?.length ? retrieval.seedEntities : (retrieval.expandedEntities ?? []).slice(0, 6))
      : normalizeEntityIds(entityIds).map((entityId) => this.graph.getEntity(entityId)).filter(Boolean);
    const focusEntityIds = normalizeEntityIds(
      focusEntities.length
        ? focusEntities.map((entity) => entity?.id)
        : entityIds,
    );
    const observationIds = [...new Set((retrieval?.items ?? []).map((item) => getObservationIdFromResult(item)).filter(Boolean))];
    const claimTypes = normalizedStrategy.claimTypes.length ? normalizedStrategy.claimTypes : null;
    const recentClaims = this.database.listRecentClaims({
      scopeFilter,
      entityIds: focusEntityIds.length ? focusEntityIds : null,
      claimTypes,
      lifecycleStates: normalizedStrategy.claimStates,
      limit: 32,
    });
    const observationClaims = observationIds.length
      ? this.database.listClaimsByObservationIds(observationIds, scopeFilter)
      : [];
    const candidateClaims = [...observationClaims, ...recentClaims]
      .filter((claim) => !claimTypes || claimTypes.includes(claim.claim_type))
      .filter((claim) => normalizedStrategy.claimStates.includes(claim.lifecycle_state))
      .filter((claim) => !focusEntityIds.length || matchesEntityFilter(claim, focusEntityIds));
    const truthAnalysis = analyzeClaimsTruthSet(candidateClaims);
    const annotatedClaims = candidateClaims.map((claim) => ({
      ...claim,
      truth: truthAnalysis.byClaimId.get(claim.id) ?? null,
    }));
    const preferredClaims = selectPreferredClaimsByResolution(annotatedClaims)
      .sort((left, right) => compareClaimsForPriority(right, left));

    const result = {
      focus_claims: [],
      unresolved_conflicts: [],
      focus_entities: focusEntities.slice(0, 6).map((entity) => ({
        id: entity.id,
        label: entity.label,
        kind: entity.kind,
      })),
      claims_scanned: candidateClaims.length,
      tokenCount: 0,
    };
    let remaining = Math.max(0, Math.trunc(tokenBudget));
    const messageCache = new Map();

    for (const claim of preferredClaims) {
      const fitted = this.fitCompactClaim(claim, remaining, messageCache);
      if (!fitted) {
        continue;
      }

      result.focus_claims.push(fitted.item);
      remaining -= fitted.tokenCount;
      result.tokenCount += fitted.tokenCount;
    }

    const conflicts = this.buildUnresolvedConflicts({
      entityIds: focusEntityIds,
      scopeFilter,
      limit: 4,
      tokenBudget: remaining,
    });
    result.unresolved_conflicts = conflicts.conflicts;
    result.tokenCount += conflicts.tokenCount;

    return result;
  }

  buildEvidence({ retrieval = null, tokenBudget = 800, strategy = null } = {}) {
    const budget = Math.max(0, Math.trunc(tokenBudget));
    if (!retrieval?.items?.length || budget <= 0) {
      return {
        structured: [],
        messages: [],
        tokenCount: 0,
      };
    }

    const normalizedStrategy = normalizePacketStrategy(strategy);
    let structuredBudget = Math.max(0, Math.floor(budget * normalizedStrategy.evidenceRatio));
    let messageBudget = Math.max(0, Math.floor(budget * normalizedStrategy.messageRatio));
    const overrun = Math.max(0, structuredBudget + messageBudget - budget);
    if (overrun > 0) {
      messageBudget = Math.max(0, messageBudget - overrun);
    }

    structuredBudget += Math.max(0, budget - structuredBudget - messageBudget);
    const structured = this.buildStructuredEvidence(retrieval.items, structuredBudget);
    const messages = this.buildRecallEvidence(retrieval.items, "recent", messageBudget);

    return {
      structured: structured.evidence,
      messages: messages.evidence,
      tokenCount: structured.tokenCount + messages.tokenCount,
    };
  }

  buildHighSignalAlerts({ activeState = null, hardConstraints = [], unresolvedConflicts = [] } = {}) {
    const alerts = [];
    const seen = new Set();
    const pushAlert = (alert) => {
      if (!alert?.detail) {
        return;
      }

      const key = `${alert.type}:${alert.entity_label ?? ""}:${alert.detail}`;
      if (seen.has(key)) {
        return;
      }

      seen.add(key);
      alerts.push(alert);
    };

    for (const task of activeState?.tasks ?? []) {
      if (task.status !== "blocked") {
        continue;
      }

      pushAlert({
        type: "blocked_task",
        detail: task.blocker ? `${task.title}: ${task.blocker}` : task.title,
        entity_label: task.entity_label ?? null,
      });
    }

    for (const constraint of hardConstraints ?? []) {
      pushAlert({
        type: "hard_constraint",
        detail: constraint.value ?? "",
        entity_label: constraint.entity_label ?? null,
      });
    }

    for (const conflict of unresolvedConflicts ?? []) {
      pushAlert({
        type: "memory_conflict",
        detail: conflict.claims?.[0]?.value ?? conflict.resolution_key ?? "",
        entity_label: conflict.entity_label ?? null,
      });
    }

    return alerts.slice(0, 8);
  }

  buildActiveConstraints({ scopeFilter = null, limit = 8, tokenBudget = 400 } = {}) {
    const normalizedLimit = Math.max(1, Math.trunc(limit || 8));
    const budget = Math.max(0, Math.trunc(tokenBudget));
    const messageCache = new Map();
    const constraints = [];
    let tokenCount = 0;

    const currentClaims = this.database.listCurrentClaims({
      types: ["constraint"],
      scopeFilter,
      limit: Math.max(normalizedLimit * 4, 12),
    }).sort((left, right) => {
      const leftSeverity = String(parseClaimMetadata(left.metadata_json).severity ?? "medium").toLowerCase();
      const rightSeverity = String(parseClaimMetadata(right.metadata_json).severity ?? "medium").toLowerCase();
      return (PRIORITY_RANK[rightSeverity] ?? 0) - (PRIORITY_RANK[leftSeverity] ?? 0)
        || claimTimestamp(right) - claimTimestamp(left);
    });

    for (const claim of currentClaims) {
      if (constraints.length >= normalizedLimit) {
        break;
      }

      const fitted = this.fitCompactClaim(claim, budget - tokenCount, messageCache);
      if (!fitted) {
        continue;
      }

      constraints.push(fitted.item);
      tokenCount += fitted.tokenCount;
    }

    if (constraints.length || budget <= 0) {
      return { constraints, tokenCount };
    }

    for (const row of this.database.queryRegistry("constraints")) {
      if (constraints.length >= normalizedLimit) {
        break;
      }

      const fitted = fitObjectToBudget({
        id: row.id,
        type: "constraint",
        value: row.detail ?? row.title ?? "",
        state: row.status ?? "active",
        confidence: 1,
        entity_label: row.entityLabel ?? null,
        valid_from: row.createdAt ?? null,
        source_event_id: row.eventId ?? null,
      }, "value", budget - tokenCount);
      if (!fitted) {
        continue;
      }

      constraints.push(fitted.item);
      tokenCount += fitted.tokenCount;
    }

    return { constraints, tokenCount };
  }

  buildRecentDecisionSummaries({ scopeFilter = null, limit = 8, tokenBudget = 500, sinceDays = 7 } = {}) {
    const normalizedLimit = Math.max(1, Math.trunc(limit || 8));
    const budget = Math.max(0, Math.trunc(tokenBudget));
    const cutoff = Date.now() - sinceDays * 24 * 60 * 60 * 1000;
    const decisions = [];
    let tokenCount = 0;

    const recentClaims = this.database.listRecentClaims({
      scopeFilter,
      claimTypes: ["decision"],
      lifecycleStates: ["active", "candidate", "superseded", "disputed", "archived"],
      limit: Math.max(normalizedLimit * 4, 16),
    }).filter((claim) => claimTimestamp(claim) >= cutoff)
      .sort((left, right) => claimTimestamp(right) - claimTimestamp(left));

    for (const claim of recentClaims) {
      if (decisions.length >= normalizedLimit) {
        break;
      }

      const fitted = this.fitDecisionSummary(claim, budget - tokenCount);
      if (!fitted) {
        continue;
      }

      decisions.push(fitted.item);
      tokenCount += fitted.tokenCount;
    }

    if (decisions.length || budget <= 0) {
      return { decisions, tokenCount };
    }

    for (const row of this.database.listOpenDecisions()) {
      if (decisions.length >= normalizedLimit) {
        break;
      }

      const fitted = fitObjectToBudget({
        id: row.id,
        title: row.title,
        status: "accepted",
        rationale: row.rationale ?? null,
        entity_label: row.entityLabel ?? null,
      }, "title", budget - tokenCount);
      if (!fitted) {
        continue;
      }

      decisions.push(fitted.item);
      tokenCount += fitted.tokenCount;
    }

    return { decisions, tokenCount };
  }

  async contextPacket(input = {}) {
    const {
      conversationId,
      query,
      intent,
      scopeFilter,
      tokenBudget,
    } = normalizePacketRequest(input);
    const budget = clamp(Number(tokenBudget) || 2000, 256, 8000);
    const currentGraphVersion = this.graph.getGraphVersion();
    const cacheRequest = { conversationId, query, intent, scopeFilter, tokenBudget: budget };
    const cached = this._assemblyCache.get("context-packet", cacheRequest, currentGraphVersion);
    if (cached.status === "hit") {
      this._packetsByIntent[cached.payload.intent] = (this._packetsByIntent[cached.payload.intent] ?? 0) + 1;
      return {
        ...cached.payload,
        diagnostics: {
          ...cached.payload.diagnostics,
          cache_status: "hit",
        },
      };
    }
    const trimmedQuery = String(query ?? "").trim();
    const explicitIntent = normalizePacketIntent(intent);
    const intentResolution = explicitIntent
      ? { intent: explicitIntent, source: "explicit" }
      : await classifyIntent(trimmedQuery, this.graph, null);
    const classifiedIntent = normalizePacketIntent(intentResolution.intent) ?? "general";
    const strategy = INTENT_STRATEGIES[classifiedIntent] ?? INTENT_STRATEGIES.general;
    const retrieval = trimmedQuery
      ? await this.retrieve({
        conversationId,
        queryText: trimmedQuery,
        scopeFilter,
      })
      : null;
    const assemblyStart = performance.now();
    const tierBudgets = allocatePacketTierBudgets(budget, strategy);
    const focusEntities = retrieval
      ? (retrieval.seedEntities?.length ? retrieval.seedEntities : (retrieval.expandedEntities ?? []).slice(0, 6))
      : [];
    const focusEntityIds = normalizeEntityIds(focusEntities.map((entity) => entity?.id));
    const activeEntityIds = classifiedIntent === "next-action" ? [] : focusEntityIds;
    const activeLimit = budget >= 1800 ? 10 : 6;
    const stablePrefix = this.buildStablePrefix({
      scopeFilter,
      tokenBudget: tierBudgets.stable,
    });
    const activeState = this.buildActiveState({
      entityIds: activeEntityIds,
      scopeFilter,
      limit: activeLimit,
      tokenBudget: tierBudgets.active,
    });
    const workingSet = this.buildWorkingSet({
      strategy,
      retrieval,
      entityIds: focusEntityIds,
      scopeFilter,
      tokenBudget: tierBudgets.working,
    });
    const evidenceBudget = Math.max(
      0,
      budget - stablePrefix.tokenCount - activeState.tokenCount - workingSet.tokenCount,
    );
    const evidence = this.buildEvidence({
      retrieval,
      tokenBudget: evidenceBudget,
      strategy,
    });
    const highSignalAlerts = this.buildHighSignalAlerts({
      activeState,
      hardConstraints: stablePrefix.hard_constraints,
      unresolvedConflicts: workingSet.unresolved_conflicts,
    });
    const assemblyLatencyMs = Math.round(performance.now() - assemblyStart);

    // Monitoring: increment packets_assembled counter by intent
    this._packetsByIntent[classifiedIntent] = (this._packetsByIntent[classifiedIntent] ?? 0) + 1;

    const packet = {
      query: trimmedQuery,
      intent: classifiedIntent,
      graph_version: this.graph.getGraphVersion(),
      timestamp: new Date().toISOString(),
      stable_prefix: {
        profile: stablePrefix.profile,
        hard_constraints: stablePrefix.hard_constraints,
      },
      active_state: {
        tasks: activeState.tasks,
        decisions: activeState.decisions,
        goals: activeState.goals,
      },
      working_set: {
        focus_claims: workingSet.focus_claims,
        unresolved_conflicts: workingSet.unresolved_conflicts,
        focus_entities: workingSet.focus_entities,
      },
      evidence: {
        structured: evidence.structured,
        messages: evidence.messages,
      },
      high_signal_alerts: highSignalAlerts,
      diagnostics: {
        retrieval_latency_ms: retrieval?.latencyMs ?? 0,
        retrieval_route: retrieval?.diagnostics?.route ?? null,
        retrieval_target_families: retrieval?.diagnostics?.targetFamilies ?? null,
        retrieval_temporal: retrieval?.diagnostics?.temporal ?? null,
        retrieval_artifact_boundary: retrieval?.diagnostics?.artifactBoundary ?? null,
        assembly_latency_ms: assemblyLatencyMs,
        token_count: stablePrefix.tokenCount + activeState.tokenCount + workingSet.tokenCount + evidence.tokenCount,
        tier_tokens: {
          stable: stablePrefix.tokenCount,
          active: activeState.tokenCount,
          working: workingSet.tokenCount,
          evidence: evidence.tokenCount,
        },
        intent_source: intentResolution.source ?? "default",
        seed_entities: (retrieval?.seedEntities ?? []).map((entity) => entity.label).filter(Boolean),
        claims_scanned: workingSet.claims_scanned,
        cache_status: `miss:${cached.reason}`,
      },
    };

    this._assemblyCache.set("context-packet", cacheRequest, currentGraphVersion, packet);
    return packet;
  }

  async memoryBrief(input = {}) {
    const {
      conversationId,
      query,
      scopeFilter,
      tokenBudget,
    } = normalizePacketRequest(input);
    const budget = clamp(Number(tokenBudget) || 1800, 256, 8000);
    const trimmedQuery = String(query ?? "").trim();
    const currentGraphVersion = this.graph.getGraphVersion();
    const cacheRequest = { conversationId, query: trimmedQuery, scopeFilter, tokenBudget: budget };
    const cached = this._assemblyCache.get("memory-brief", cacheRequest, currentGraphVersion);
    if (cached.status === "hit") {
      this._briefsAssembled = (this._briefsAssembled ?? 0) + 1;
      return cached.payload;
    }
    const packet = await this.contextPacket({
      conversationId,
      query: trimmedQuery,
      scopeFilter,
      tokenBudget: budget,
      intent: trimmedQuery ? "next-action" : "general",
    });
    const recentDecisionData = this.buildRecentDecisionSummaries({
      scopeFilter,
      limit: 8,
      tokenBudget: Math.max(240, Math.floor(budget * 0.18)),
    });
    const activeConstraintData = this.buildActiveConstraints({
      scopeFilter,
      limit: 8,
      tokenBudget: Math.max(180, Math.floor(budget * 0.12)),
    });
    const recentDecisions = recentDecisionData.decisions.length
      ? recentDecisionData.decisions
      : packet.active_state.decisions.slice(0, 8);
    const activeConstraints = activeConstraintData.constraints.length
      ? activeConstraintData.constraints
      : packet.stable_prefix.hard_constraints.slice(0, 8);
    const activeWork = {
      tasks: packet.active_state.tasks.slice(0, 10),
      goals: packet.active_state.goals.slice(0, 6),
    };
    const briefPayload = {
      timestamp: packet.timestamp,
      graph_version: packet.graph_version,
      profile: packet.stable_prefix.profile,
      active_work: activeWork,
      recent_decisions: recentDecisions,
      active_constraints: activeConstraints,
      unresolved_conflicts: packet.working_set.unresolved_conflicts,
      high_signal_alerts: packet.high_signal_alerts,
    };

    // Monitoring: increment briefs_assembled counter
    this._briefsAssembled = (this._briefsAssembled ?? 0) + 1;

    const brief = {
      ...briefPayload,
      token_count: estimateTokens(JSON.stringify(briefPayload)),
      claims_total: countUniqueIds([
        briefPayload.profile.preferences,
        briefPayload.profile.rules,
        briefPayload.profile.facts,
        briefPayload.active_work.tasks,
        briefPayload.active_work.goals,
        briefPayload.recent_decisions,
        briefPayload.active_constraints,
        briefPayload.unresolved_conflicts.flatMap((conflict) => conflict.claims ?? []),
      ]),
    };

    this._assemblyCache.set("memory-brief", cacheRequest, currentGraphVersion, brief);
    return brief;
  }

  async recall({ conversationId = null, query, mode = "hybrid", scope = "all", tokenBudget = 2000, scopeFilter = null }) {
    void mode;

    const budget = clamp(Number(tokenBudget) || 2000, 1, 8000);
    const retrieval = await this.retrieve({
      conversationId,
      queryText: query ?? "",
      scopeFilter,
    });
    const annotatedItems = this.annotateResultsWithClaims(retrieval.items, scopeFilter);
    const assembled = this.buildRecallEvidence(annotatedItems, scope, budget);

    return {
      evidence: assembled.evidence,
      total_results: assembled.totalResults,
      token_count: assembled.tokenCount,
      query: query ?? "",
      route: retrieval?.diagnostics?.route ?? null,
      latency_ms: retrieval?.latencyMs ?? null,
      diagnostics: retrieval?.diagnostics ?? null,
    };
  }

  getContextWindow({ eventId, before = 6, after = 6 }) {
    const window = this.database.getMessageContextWindowByIngestId(eventId, { before, after });
    if (!window) {
      return null;
    }

    return {
      center: toEventRecord(window.center),
      before: window.before.map((message) => toEventRecord(message)),
      after: window.after.map((message) => toEventRecord(message)),
    };
  }

  listOpenItems(kind = "all") {
    const normalizedKind = normalizeOpenItemsKind(kind);
    const currentGraphVersion = this.graph.getGraphVersion();
    const cached = this._assemblyCache.get("open-items", { kind: normalizedKind }, currentGraphVersion);

    if (cached.status === "hit") {
      return {
        ...cached.payload,
        diagnostics: {
          ...cached.payload.diagnostics,
          cache_status: "hit",
        },
      };
    }

    const includeTasks = normalizedKind === "all" || normalizedKind === "tasks";
    const includeDecisions = normalizedKind === "all" || normalizedKind === "decisions";
    const includeConstraints = normalizedKind === "all" || normalizedKind === "constraints";

    const payload = {
      tasks: includeTasks
        ? this.database.listOpenTasks().map((row) => ({
          id: row.id,
          title: row.title,
          status: mapTaskStatus(row.status),
          priority: row.priority,
          entity_label: row.entityLabel ?? null,
          source_event_id: row.eventId ?? null,
        }))
        : [],
      decisions: includeDecisions
        ? this.database.listOpenDecisions().map((row) => ({
          id: row.id,
          title: row.title,
          status: "active",
          rationale: row.rationale ?? null,
          entity_label: row.entityLabel ?? null,
          source_event_id: row.eventId ?? null,
        }))
        : [],
      constraints: includeConstraints
        ? this.database.listOpenConstraints().map((row) => ({
          id: row.id,
          title: row.detail,
          status: "active",
          severity: row.severity,
          entity_label: row.entityLabel ?? null,
          source_event_id: row.eventId ?? null,
        }))
        : [],
      diagnostics: {
        cache_status: `miss:${cached.reason}`,
      },
    };

    this._assemblyCache.set("open-items", { kind: normalizedKind }, currentGraphVersion, payload);
    return payload;
  }

  queryRegistry(input = {}) {
    const { name, query, filters } = normalizeRegistryQueryRequest(input);
    const currentGraphVersion = this.graph.getGraphVersion();
    const cached = this._assemblyCache.get("registry-query", { name, query, filters }, currentGraphVersion);

    if (cached.status === "hit") {
      return {
        ...cached.payload,
        diagnostics: {
          ...cached.payload.diagnostics,
          cache_status: "hit",
        },
      };
    }

    const results = this.database.queryRegistry(name, { query, filters }).map((row) => {
      if (row.type === "task") {
        return {
          id: row.id,
          type: row.type,
          title: row.title,
          detail: row.detail,
          status: mapTaskStatus(row.status),
          priority: row.priority,
          entity_label: row.entityLabel ?? null,
          source_event_id: row.eventId ?? null,
          created_at: row.createdAt ?? null,
        };
      }

      if (row.type === "decision") {
        return {
          id: row.id,
          type: row.type,
          title: row.title,
          detail: row.detail,
          status: "active",
          rationale: row.rationale ?? null,
          entity_label: row.entityLabel ?? null,
          source_event_id: row.eventId ?? null,
          created_at: row.createdAt ?? null,
        };
      }

      if (row.type === "constraint") {
        return {
          id: row.id,
          type: row.type,
          title: row.title,
          detail: row.detail,
          status: "active",
          severity: row.severity,
          entity_label: row.entityLabel ?? null,
          source_event_id: row.eventId ?? null,
          created_at: row.createdAt ?? null,
        };
      }

      if (row.type === "entity" || row.type === "project") {
        return {
          id: row.id,
          type: row.type,
          slug: row.slug,
          label: row.label,
          kind: row.kind,
          summary: row.summary ?? null,
          complexity_score: Number(row.complexityScore ?? 1),
          mention_count: Number(row.mentionCount ?? 0),
          created_at: row.createdAt ?? null,
          updated_at: row.updatedAt ?? null,
        };
      }

      return {
        id: row.id,
        type: row.type,
        title: row.title,
        detail: row.detail,
        entity_label: row.entityLabel ?? null,
        source_event_id: row.eventId ?? null,
        created_at: row.createdAt ?? null,
      };
    });

    const payload = {
      results,
      total: results.length,
      diagnostics: {
        cache_status: `miss:${cached.reason}`,
      },
    };

    this._assemblyCache.set("registry-query", { name, query, filters }, currentGraphVersion, payload);
    return payload;
  }

  getEntityDetail(name, { includeRecentEvents = false } = {}) {
    const row = this.database.findEntityByName(name);
    if (!row) {
      return null;
    }

    const entity = this.graph.getEntity(row.id) ?? this.graph.findEntityByLabel(row.label);
    if (!entity) {
      return null;
    }

    const relationships = this.database.listRelationshipsForEntity(entity.id).map((relationship) => {
      const outgoing = relationship.subjectEntityId === entity.id;
      return {
        target: outgoing ? relationship.objectLabel : relationship.subjectLabel,
        target_id: outgoing ? relationship.objectEntityId : relationship.subjectEntityId,
        target_slug: outgoing ? relationship.objectSlug : relationship.subjectSlug,
        predicate: relationship.predicate,
        weight: Number(relationship.weight ?? 1),
        direction: outgoing ? "outgoing" : "incoming",
      };
    });

    const detail = {
      entity: {
        id: entity.id,
        slug: entity.slug,
        label: entity.label,
        kind: entity.kind,
        summary: entity.summary,
        complexity_score: entity.complexityScore,
        mention_count: entity.mentionCount,
        relationships,
      },
    };

    if (includeRecentEvents) {
      detail.recent_events = this.database
        .listRecentMessagesForEntity(entity.id, 10)
        .map((message) => toEventRecord(message));
    }

    return detail;
  }

  proposeMutation({ type, payload = {}, confidence = 0.5, sourceEventId = null, actorId = "system" }) {
    const sourceMessage = sourceEventId ? this.database.getMessageByIngestId(sourceEventId) : null;
    if (sourceEventId && !sourceMessage) {
      throw new Error(`Unknown source_event_id: ${sourceEventId}`);
    }

    const detail = normalizeLabel(payload.title ?? payload.detail ?? payload.summary ?? null);
    const normalizedConfidence = clamp(Number(confidence) || 0.5, 0, 1);
    const writeClass = classifyWriteClass(type);
    const disposition = getWriteClassDisposition(writeClass);
    const shouldAutoApply = disposition.autoApply || writeClass === 'canonical';
    const _queueReason = disposition.queueReason;

    const stored = this.database.insertGraphProposal({
      conversationId: sourceMessage?.conversationId ?? null,
      messageId: sourceMessage?.id ?? null,
      actorId,
      scopeKind: sourceMessage?.scopeKind ?? "private",
      scopeId: sourceMessage?.scopeId ?? null,
      proposalType: type,
      subjectLabel: normalizeLabel(payload.subjectLabel ?? payload.subject_label ?? payload.entityLabel ?? payload.entity_label ?? payload.entity ?? payload.label),
      predicate: normalizeLabel(payload.predicate),
      objectLabel: normalizeLabel(payload.objectLabel ?? payload.object_label ?? payload.targetLabel ?? payload.target_label),
      detail,
      confidence: normalizedConfidence,
      status: "proposed",
      reason: normalizeLabel(payload.reason),
      payload,
      writeClass,
    });
    this.graph.updateGraphVersion(stored.graphVersion);

    // v2.3: auto and canonical apply immediately; ai_proposed queues for review
    if (shouldAutoApply) {
      try {
        const proposal = this.database.getGraphProposal(stored.id);
        if (proposal) {
          const applied = this.applyGraphProposal(proposal, { actorId });
          const review = this.database.updateGraphProposalStatus(stored.id, {
            status: "accepted",
            reason: `Auto-applied (write_class=${writeClass}, confidence=${normalizedConfidence})`,
            actorId,
          });
          this.graph.updateGraphVersion(review.graphVersion);

          return {
            ok: true,
            proposal_id: stored.id,
            mutation_id: stored.id,
            status: "accepted",
            write_class: writeClass,
            ...getQueuePressureDisposition({
              writeClass,
              status: "accepted",
              confidence: normalizedConfidence,
            }),
            applied,
          };
        }
      } catch (error) {
        // If auto-apply fails, fall through and return as proposed
        console.warn(`[proposeMutation] Auto-apply failed for ${stored.id}: ${error.message}`);
      }
    }

    return {
      ok: true,
      proposal_id: stored.id,
      mutation_id: stored.id,
      status: "proposed",
      write_class: writeClass,
      ...getQueuePressureDisposition({
        writeClass,
        status: "proposed",
        confidence: normalizedConfidence,
      }),
    };
  }

  resolveProposalSourceMessage(proposal, actorId = "system") {
    if (proposal.message_id) {
      return this.database.getMessage(proposal.message_id);
    }

    const conversation = proposal.conversation_id
      ? (this.database.getConversation(proposal.conversation_id) ?? this.database.createConversation("Mutation Review"))
      : this.database.createConversation("Mutation Review");
    const content = proposal.detail ?? `Mutation review for ${proposal.id}`;

    return this.database.insertMessage({
      conversationId: conversation.id,
      role: "system",
      direction: "inbound",
      actorId,
      originKind: "system",
      scopeKind: proposal.scope_kind ?? "private",
      scopeId: proposal.scope_id ?? null,
      content,
      tokenCount: estimateTokens(content),
      raw: {
        synthetic: true,
        source: "mutation_review",
        proposalId: proposal.id,
      },
    });
  }

  applyGraphProposal(proposal, { actorId = "system" } = {}) {
    const payload = parseJson(proposal.payload_json, {}) ?? {};
    const normalizedType = String(payload.type ?? proposal.proposal_type ?? "")
      .trim()
      .toLowerCase()
      .replace(/^add_/, "");

    if (normalizedType === "entity") {
      const entity = this.graph.ensureEntity({
        label: payload.label ?? proposal.subject_label ?? proposal.detail,
        kind: payload.kind ?? "concept",
        summary: payload.summary ?? proposal.detail ?? null,
        aliases: payload.aliases ?? [],
        metadata: payload.metadata ?? null,
      });

      return {
        entity_id: entity.id,
      };
    }

    const sourceMessage = this.resolveProposalSourceMessage(proposal, actorId);
    const scopeKind = sourceMessage.scopeKind ?? proposal.scope_kind ?? "private";
    const scopeId = sourceMessage.scopeId ?? proposal.scope_id ?? null;
    const metadata = {
      ...(payload.metadata ?? {}),
      tags: Array.isArray(payload.tags) ? payload.tags : undefined,
      proposalId: proposal.id,
      source: "mutation_review",
    };

    if (normalizedType === "relationship") {
      const subject = this.graph.ensureEntity({
        label: payload.subjectLabel ?? payload.subject_label ?? proposal.subject_label,
        kind: payload.subjectKind ?? payload.subject_kind ?? "concept",
      });
      const object = this.graph.ensureEntity({
        label: payload.objectLabel ?? payload.object_label ?? proposal.object_label,
        kind: payload.objectKind ?? payload.object_kind ?? "concept",
      });
      const predicate = payload.predicate ?? proposal.predicate ?? "related_to";
      const detail = normalizeLabel(payload.detail ?? proposal.detail ?? `${subject.label} ${predicate} ${object.label}`);
      const confidence = clamp(Number(payload.confidence ?? proposal.confidence ?? 0.8), 0, 1);

      const relationship = this.graph.connect({
        subjectEntityId: subject.id,
        predicate,
        objectEntityId: object.id,
        weight: confidence,
        provenanceMessageId: sourceMessage.id,
        metadata,
      });
      const observation = this.database.insertObservation({
        conversationId: sourceMessage.conversationId,
        messageId: sourceMessage.id,
        actorId,
        category: "relationship",
        predicate,
        subjectEntityId: subject.id,
        objectEntityId: object.id,
        detail,
        confidence,
        sourceSpan: detail,
        metadata,
        scopeKind,
        scopeId,
      });
      this.graph.updateGraphVersion(observation.graphVersion);
      this.enqueueObservationEmbedding({
        id: observation.id,
        detail,
      });

      return {
        relationship_id: relationship?.id ?? null,
        observation_id: observation.id,
      };
    }

    const entityLabel = normalizeLabel(
      payload.entityLabel
      ?? payload.entity_label
      ?? payload.entity
      ?? payload.subjectLabel
      ?? payload.subject_label
      ?? proposal.subject_label,
    );
    const entity = entityLabel ? this.graph.ensureEntity({ label: entityLabel, kind: payload.entityKind ?? payload.entity_kind ?? "concept" }) : null;
    const detail = normalizeLabel(payload.title ?? payload.detail ?? proposal.detail);
    const confidence = clamp(Number(payload.confidence ?? proposal.confidence ?? 0.8), 0, 1);

    if (!detail) {
      throw new Error(`Mutation ${proposal.id} is missing detail/title`);
    }

    const observationType = normalizedType === "breakthrough" || normalizedType === "profile" || normalizedType === "update_profile"
      ? "fact"
      : normalizedType;
    if (!["task", "decision", "constraint", "fact"].includes(observationType)) {
      throw new Error(`Unsupported mutation type: ${proposal.proposal_type}`);
    }

    const observation = this.database.insertObservation({
      conversationId: sourceMessage.conversationId,
      messageId: sourceMessage.id,
      actorId,
      category: observationType,
      predicate: null,
      subjectEntityId: entity?.id ?? null,
      objectEntityId: null,
      detail,
      confidence,
      sourceSpan: detail,
      metadata,
      scopeKind,
      scopeId,
    });
    this.graph.updateGraphVersion(observation.graphVersion);
    this.enqueueObservationEmbedding({
      id: observation.id,
      detail,
    });

    if (observationType === "task") {
      const taskStatus = String(payload.status ?? "open").trim().toLowerCase() === "active" ? "open" : (payload.status ?? "open");
      const taskId = this.database.insertTask({
        observationId: observation.id,
        entityId: entity?.id ?? null,
        title: detail,
        status: taskStatus,
        priority: payload.priority ?? "medium",
      });

      return {
        observation_id: observation.id,
        task_id: taskId,
      };
    }

    if (observationType === "decision") {
      const decisionId = this.database.insertDecision({
        observationId: observation.id,
        entityId: entity?.id ?? null,
        title: detail,
        rationale: payload.rationale ?? null,
      });

      return {
        observation_id: observation.id,
        decision_id: decisionId,
      };
    }

    if (observationType === "constraint") {
      const constraintId = this.database.insertConstraint({
        observationId: observation.id,
        entityId: entity?.id ?? null,
        detail,
        severity: payload.severity ?? "high",
      });

      return {
        observation_id: observation.id,
        constraint_id: constraintId,
      };
    }

    const factId = this.database.insertFact({
      observationId: observation.id,
      entityId: entity?.id ?? null,
      detail,
    });

    return {
      observation_id: observation.id,
      fact_id: factId,
    };
  }

  reviewMutations({ action, mutationId = null, mutationIds = null, reason = null, actorId = "system", filters = {} }) {
    const invalidReviewPayload = (message) => {
      const error = new Error(message);
      error.statusCode = 400;
      throw error;
    };
    const batchActions = {
      apply_batch: "accepted",
      reject_batch: "rejected",
    };
    const singleActions = {
      apply: "accepted",
      reject: "rejected",
    };

    if (action === "list") {
      const defaultStatuses = ["pending", "proposed"];
      const normalizeValues = (value) => Array.isArray(value)
        ? value.map((entry) => String(entry ?? "").trim()).filter(Boolean)
        : [];
      const countBy = (rows, selector) => rows.reduce((accumulator, row) => {
        const key = selector(row);
        if (key) {
          accumulator[key] = (accumulator[key] ?? 0) + 1;
        }
        return accumulator;
      }, {});

      const requestedStatuses = normalizeValues(filters.statuses);
      if (filters.status && !requestedStatuses.length) {
        requestedStatuses.push(String(filters.status).trim());
      }

      const requestedWriteClasses = normalizeValues(filters.writeClasses);
      if (filters.writeClass && !requestedWriteClasses.length) {
        requestedWriteClasses.push(String(filters.writeClass).trim());
      }

      const requestedProposalTypes = normalizeValues(filters.proposalTypes);
      if (filters.proposalType && !requestedProposalTypes.length) {
        requestedProposalTypes.push(String(filters.proposalType).trim());
      }

      const requestedTriage = String(filters.triage ?? "").trim() || null;
      const includeParked = filters.includeParked === true
        || String(filters.includeParked ?? "").trim().toLowerCase() === "true"
        || requestedTriage === "parked_backlog";
      const reviewQuery = {
        statuses: requestedStatuses.length ? requestedStatuses : defaultStatuses,
        writeClasses: requestedWriteClasses.length ? requestedWriteClasses : null,
        proposalTypes: requestedProposalTypes.length ? requestedProposalTypes : null,
        sourceEventId: filters.sourceEventId ?? filters.source_event_id ?? null,
        minConfidence: filters.minConfidence ?? filters.min_confidence ?? null,
        maxConfidence: filters.maxConfidence ?? filters.max_confidence ?? null,
        sort: filters.sort ?? "newest",
      };
      const hasExplicitLimit = filters.limit !== null && filters.limit !== undefined && String(filters.limit).trim() !== "";
      const limitValue = Number(filters.limit);
      const resolvedLimit = hasExplicitLimit && Number.isFinite(limitValue)
        ? Math.max(1, Math.trunc(limitValue))
        : 200;
      const applyQueueFilters = (rows) => rows.filter((row) => {
        const metadata = getProposalQueueMetadata(row);
        if (requestedTriage && metadata.triage !== requestedTriage) {
          return false;
        }

        if (!includeParked && metadata.queue_bucket === "parked") {
          return false;
        }

        return true;
      });

      const rawQueueRows = this.database.listGraphProposals({
        statuses: reviewQuery.statuses,
        limit: null,
      });
      const queueRows = applyQueueFilters(rawQueueRows);
      const filteredRows = applyQueueFilters(this.database.listGraphProposals({
        ...reviewQuery,
        limit: null,
      }));
      const mutations = filteredRows
        .slice(0, resolvedLimit)
        .map((row) => toProposalResponse(row, { sourceEventId: row.source_event_id ?? null }));

      return {
        mutations,
        total: filteredRows.length,
        returned: mutations.length,
        applied_filters: {
          statuses: reviewQuery.statuses,
          write_classes: requestedWriteClasses,
          proposal_types: requestedProposalTypes,
          triage: requestedTriage,
          include_parked: includeParked,
          min_confidence: reviewQuery.minConfidence === null ? null : Number(reviewQuery.minConfidence),
          max_confidence: reviewQuery.maxConfidence === null ? null : Number(reviewQuery.maxConfidence),
          source_event_id: reviewQuery.sourceEventId ?? null,
          sort: reviewQuery.sort,
          limit: resolvedLimit,
        },
        summary: {
          queue_total: queueRows.length,
          parked_total: rawQueueRows.filter((row) => getProposalQueueMetadata(row).queue_bucket === "parked").length,
          filtered_total: filteredRows.length,
          by_status: countBy(filteredRows, (row) => row.status),
          by_write_class: countBy(filteredRows, (row) => row.write_class ?? classifyWriteClass(row.proposal_type)),
          by_proposal_type: countBy(filteredRows, (row) => row.proposal_type),
          by_triage: countBy(filteredRows, (row) => getProposalQueueMetadata(row).triage),
          by_queue_bucket: countBy(filteredRows, (row) => getProposalQueueMetadata(row).queue_bucket),
          by_policy_decision: countBy(filteredRows, (row) => getProposalQueueMetadata(row).policy_decision),
        },
      };
    }

    const normalizeBatchIds = (value) => {
      if (!Array.isArray(value)) {
        return null;
      }

      return value
        .map((entry) => String(entry ?? "").trim())
        .filter(Boolean);
    };
    const reviewProposal = (proposal, nextStatus) => {
      let applied = null;
      if (nextStatus === "accepted") {
        applied = this.applyGraphProposal(proposal, { actorId });
      }

      const review = this.database.updateGraphProposalStatus(proposal.id, {
        status: nextStatus,
        reason,
        actorId,
      });
      this.graph.updateGraphVersion(review.graphVersion);

      return {
        mutation_id: proposal.id,
        status: nextStatus,
        reason: review.reason,
        reviewed_at: review.reviewedAt,
        reviewed_by_actor: review.actorId,
        applied,
      };
    };
    const isReviewedStatus = (status) => ["accepted", "rejected"].includes(String(status ?? "").trim().toLowerCase());

    if (action in batchActions) {
      if (mutationId) {
        invalidReviewPayload(`Ambiguous review payload: ${action} requires mutation_ids and does not accept mutation_id`);
      }

      const normalizedMutationIds = normalizeBatchIds(mutationIds);
      if (normalizedMutationIds === null) {
        invalidReviewPayload("mutation_ids must be an array for batch review");
      }

      if (!normalizedMutationIds.length) {
        invalidReviewPayload("mutation_ids must contain at least one id");
      }

      const duplicates = normalizedMutationIds.filter((id, index) => normalizedMutationIds.indexOf(id) !== index);
      if (duplicates.length) {
        invalidReviewPayload(`mutation_ids contains duplicates: ${Array.from(new Set(duplicates)).join(", ")}`);
      }

      const proposals = this.database.getGraphProposalsByIds(normalizedMutationIds);
      const foundIds = new Set(proposals.map((proposal) => proposal.id));
      const unknownIds = normalizedMutationIds.filter((id) => !foundIds.has(id));
      if (unknownIds.length) {
        invalidReviewPayload(`Unknown mutation_ids: ${unknownIds.join(", ")}`);
      }

      const alreadyReviewed = proposals.filter((proposal) => isReviewedStatus(proposal.status));
      if (alreadyReviewed.length) {
        invalidReviewPayload(`Already reviewed mutation_ids: ${alreadyReviewed.map((proposal) => proposal.id).join(", ")}`);
      }

      const results = proposals.map((proposal) => reviewProposal(proposal, batchActions[action]));
      const updatedMutations = this.database.getGraphProposalsByIds(normalizedMutationIds)
        .map((proposal) => toProposalResponse(proposal, { sourceEventId: proposal.source_event_id ?? null }));

      return {
        ok: true,
        action,
        status: batchActions[action],
        reason,
        mutation_ids: normalizedMutationIds,
        count: results.length,
        mutations: updatedMutations,
        results,
      };
    }

    if (action in singleActions) {
      if (mutationIds !== null && mutationIds !== undefined) {
        invalidReviewPayload(`Ambiguous review payload: ${action} requires mutation_id and does not accept mutation_ids`);
      }

      if (!mutationId) {
        invalidReviewPayload("mutation_id is required");
      }

      const proposal = this.database.getGraphProposal(mutationId);
      if (!proposal) {
        invalidReviewPayload(`Unknown mutation_id: ${mutationId}`);
      }

      if (action === "reject") {
        const result = reviewProposal(proposal, singleActions[action]);
        return {
          ok: true,
          mutation_id: proposal.id,
          status: result.status,
        };
      }

      if (action === "apply") {
        const result = reviewProposal(proposal, singleActions[action]);
        return {
          ok: true,
          mutation_id: proposal.id,
          status: result.status,
          applied: result.applied,
        };
      }
    }

    invalidReviewPayload(`Unsupported action: ${action}`);
  }

  getStatusData() {
    const counts = this.database.getDashboardStats().counts;
    const registries = this.database.getRegistryCounts();
    const embeddings = this.database.getEmbeddingCoverage();
    const claims = this.getClaimMetrics();

    return {
      cortex_up: true,
      event_count: Number(counts.messages ?? 0),
      entity_count: Number(counts.entities ?? 0),
      relationship_count: Number(counts.relationships ?? 0),
      pending_mutations: registries.pendingMutations,
      graph_version: this.graph.getGraphVersion(),
      registries: {
        tasks: registries.tasks,
        decisions: registries.decisions,
        constraints: registries.constraints,
      },
      observation_categories: this.database.countObservationCategories(),
      embeddings,
      claims,
      packet_metrics: this.getPacketMetrics(),
      preconscious_depth: this.preconsciousBuffer.peek(),
      last_dream_cycle: this._lastDreamCycleTimestamp,
    };
  }

  getPacketMetrics() {
    return {
      packets_assembled: { ...this._packetsByIntent },
      briefs_assembled: this._briefsAssembled ?? 0,
    };
  }

  getHealthData() {
    const dashboard = this.getDashboardData();
    const registries = this.database.getRegistryCounts();
    const embeddings = this.database.getEmbeddingCoverage();

    const walCheck = this.database.checkWalIntegrity();

    return {
      status: walCheck.ok ? "ok" : "degraded",
      counts: dashboard.counts,
      graph_version: this.graph.getGraphVersion(),
      observation_categories: this.database.countObservationCategories(),
      registries: {
        tasks: registries.tasks,
        decisions: registries.decisions,
        constraints: registries.constraints,
      },
      pending_mutations: registries.pendingMutations,
      embeddings,
      wal: walCheck,
    };
  }

  getClaimMetrics() {
    const claimStateStats = this.database.getClaimStateStats();
    const coverage = this.database.getClaimCoverageRatio();
    const backfill = this.database.getClaimBackfillCoverage();
    const byState = Object.fromEntries(LIFECYCLE_STATES.map((state) => [state, 0]));
    const byType = Object.fromEntries(CLAIM_TYPE_VALUES.map((type) => [type, 0]));
    let total = 0;

    for (const [claimType, states] of Object.entries(claimStateStats)) {
      for (const [state, rawCount] of Object.entries(states ?? {})) {
        const count = Number(rawCount ?? 0);

        byType[claimType] = (byType[claimType] ?? 0) + count;
        byState[state] = (byState[state] ?? 0) + count;
        total += count;
      }
    }

    return {
      total,
      by_state: byState,
      by_type: byType,
      coverage_ratio: coverage.ratio,
      disputed_count: byState.disputed ?? 0,
      backfill,
    };
  }

  getClaimBackfillStatus() {
    return this.database.getClaimBackfillCoverage();
  }

  backfillClaims({ limit = 100 } = {}) {
    const candidates = this.database.listObservationsForClaimBackfill(limit);
    const batch = {
      attempted: 0,
      claim_created: 0,
      no_claim: 0,
      failed: 0,
      errors: [],
    };

    for (const observation of candidates) {
      batch.attempted += 1;

      try {
        const existingClaim = this.database.getClaimByObservationId(observation.id);
        if (existingClaim?.id) {
          this.database.upsertClaimBackfillStatus({
            observationId: observation.id,
            status: "claim_created",
            claimId: existingClaim.id,
          });
          batch.claim_created += 1;
          continue;
        }

        const claim = ensureClaimForObservation(this.database, {
          ...observation,
          metadata: parseJson(observation.metadata_json),
        });

        if (claim?.id) {
          this.database.upsertClaimBackfillStatus({
            observationId: observation.id,
            status: "claim_created",
            claimId: claim.id,
          });
          batch.claim_created += 1;
          continue;
        }

        this.database.upsertClaimBackfillStatus({
          observationId: observation.id,
          status: "no_claim",
        });
        batch.no_claim += 1;
      } catch (error) {
        this.database.upsertClaimBackfillStatus({
          observationId: observation.id,
          status: "failed",
          errorMessage: error.message,
        });
        batch.failed += 1;
        batch.errors.push({
          observationId: observation.id,
          message: error.message,
        });
      }
    }

    return {
      batch,
      status: this.database.getClaimBackfillCoverage(),
    };
  }

  async indexMarkdownDirectory(relativeOrAbsolutePath, { scopeKind = "shared", scopeId = null } = {}) {
    const resolvedPath = path.isAbsolute(relativeOrAbsolutePath)
      ? relativeOrAbsolutePath
      : path.join(this.rootDir, relativeOrAbsolutePath);
    return this.indexer.indexDirectory(resolvedPath, { scopeKind, scopeId });
  }

  async proxyChat({
    conversationId = null,
    title = "Proxy Session",
    messages = [],
    mockResponse = null,
    actorId = "system",
    scopeKind = "private",
    scopeId = null,
  }) {
    const conversation = this.ensureConversation({ conversationId, title });
    const guardEvents = [];
    const captured = [];
    const proxyActorId = "proxy";

    for (const message of messages) {
      const direction = message.role === "assistant" ? "outbound" : "inbound";
      const scan = this.guard.scan({ direction, text: message.content });
      this.telemetry.logProxyEvent({
        conversationId: conversation.id,
        actorId: proxyActorId,
        direction,
        stage: "message_ingest",
        verdict: scan.verdict,
        reasons: scan.reasons,
        payload: message,
      });

      guardEvents.push({ role: message.role, ...scan });
      captured.push(
        await this.ingestMessage({
          conversationId: conversation.id,
          conversationTitle: title,
          role: message.role,
          direction,
          actorId,
          content: message.content,
          raw: message,
          scopeKind,
          scopeId,
        }),
      );
    }

    const blockedInbound = guardEvents.find((event) => event.role !== "assistant" && event.verdict === "block");
    const latestUser = [...messages].reverse().find((message) => message.role === "user");
    const latestUserCapture = [...captured].reverse().find((record) => record.message.role === "user");
    const _latestCapture = captured.at(-1) ?? null;
    const retrieval = latestUser && !blockedInbound
      ? await this.retrieve({
          conversationId: conversation.id,
          queryText: latestUser.content,
          scopeFilter: { scopeKind, scopeId },
        })
      : null;

    let synthesizedResponse = mockResponse;
    if (!synthesizedResponse) {
      if (blockedInbound) {
        synthesizedResponse = "Request blocked by ContextOS proxy because it matched a prompt-injection rule.";
      } else {
        synthesizedResponse = [
          "ContextOS proxy response",
          "",
          "Retrieved context:",
          retrieval ? summarizeResults(retrieval.items) : "- none",
        ].join("\n");
      }
    }

    const outboundScan = this.guard.scan({ direction: "outbound", text: synthesizedResponse });
    this.telemetry.logProxyEvent({
      conversationId: conversation.id,
      actorId: proxyActorId,
      direction: "outbound",
      stage: "response_emit",
      verdict: outboundScan.verdict,
      reasons: outboundScan.reasons,
      payload: { content: synthesizedResponse },
    });

    const assistantCapture = await this.ingestMessage({
      conversationId: conversation.id,
      conversationTitle: title,
      role: "assistant",
      direction: "outbound",
      actorId: proxyActorId,
      originKind: "agent",
      sourceMessageId: latestUserCapture?.message?.id ?? null,
      content: synthesizedResponse,
      raw: { content: synthesizedResponse, synthetic: true },
      scopeKind,
      scopeId,
    });

    return {
      conversationId: conversation.id,
      guardEvents: [...guardEvents, { role: "assistant", ...outboundScan }],
      retrieval,
      assistant: {
        ...assistantCapture.message,
        content: synthesizedResponse,
      },
    };
  }

  getDashboardData() {
    const contentStats = this.database.getDashboardStats();
    const telemetryStats = this.telemetry.getDashboardStats();
    const claims = this.getClaimMetrics();
    const recentRetrievals = this.telemetry.listRecentRetrievals(12).map((row) => {
      const expansionPath = JSON.parse(row.expansion_path_json);
      return {
        id: row.id,
        queryText: row.query_text,
        latencyMs: row.latency_ms,
        itemsReturned: row.items_returned,
        tokensConsumed: row.tokens_consumed,
        createdAt: row.created_at,
        seedEntities: JSON.parse(row.seed_entities_json),
        expandedEntities: JSON.parse(row.expanded_entities_json),
        missEntities: JSON.parse(row.miss_entities_json),
        expansionPath,
        hintHops: expansionPath.filter((edge) => edge.source === "hint").length,
        graphHops: expansionPath.filter((edge) => edge.source !== "hint").length,
      };
    });

    const warnings = this.telemetry.listRecentProxyEvents(12).map((row) => ({
      id: row.id,
      direction: row.direction,
      stage: row.stage,
      verdict: row.verdict,
      reasons: JSON.parse(row.reasons_json),
      createdAt: row.created_at,
    }));

    const recentModelRuns = this.telemetry.listRecentModelRuns(12).map((row) => ({
      id: row.id,
      stage: row.stage,
      provider: row.provider,
      modelName: row.model_name,
      transport: row.transport,
      status: row.status,
      latencyMs: row.latency_ms,
      createdAt: row.created_at,
    }));

    const activeHints = this.telemetry.listActiveRetrievalHints(12).map((row) => ({
      id: row.id,
      seedLabel: row.seed_label,
      expandLabel: row.expand_label,
      reason: row.reason,
      weight: row.weight,
      ttlTurns: row.ttl_turns,
      turnsElapsed: row.turns_elapsed,
      turnsRemaining: row.turns_remaining,
      timesConsidered: row.times_considered ?? 0,
      timesApplied: row.times_applied ?? 0,
      timesRewarded: row.times_rewarded ?? 0,
      timesUnused: row.times_unused ?? 0,
      avgReward: row.avg_reward ?? 0,
      lastReward: row.last_reward ?? 0,
      createdAt: row.created_at,
    }));

    const topHintStats = this.telemetry.listRetrievalHintStats(12).map((row) => ({
      id: row.id,
      seedLabel: row.seed_label,
      expandLabel: row.expand_label,
      weight: row.weight,
      ttlTurns: row.ttl_turns,
      status: row.status,
      timesConsidered: row.times_considered,
      timesApplied: row.times_applied,
      timesRewarded: row.times_rewarded,
      timesUnused: row.times_unused,
      avgReward: row.avg_reward,
      lastReward: row.last_reward,
      lastAppliedAt: row.last_applied_at,
      updatedAt: row.updated_at,
    }));

    const recentHintEvents = this.telemetry.listRecentRetrievalHintEvents(12).map((row) => ({
      id: row.id,
      hintId: row.hint_id,
      eventType: row.event_type,
      reward: row.reward,
      penalty: row.penalty,
      seedLabel: row.seedLabel,
      expandLabel: row.expandLabel,
      detail: JSON.parse(row.detail_json),
      createdAt: row.created_at,
    }));

    const graphProposals = this.database.listRecentGraphProposals(12).map((row) => ({
      id: row.id,
      proposalType: row.proposal_type,
      subjectLabel: row.subject_label,
      predicate: row.predicate,
      objectLabel: row.object_label,
      detail: row.detail,
      confidence: row.confidence,
      status: row.status,
      reason: row.reason,
      createdAt: row.created_at,
    }));

    return {
      ...contentStats,
      counts: {
        ...contentStats.counts,
        ...telemetryStats.counts,
      },
      recentRetrievals,
      warnings,
      recentModelRuns,
      activeHints,
      topHintStats,
      recentHintEvents,
      graphProposals,
      graph: this.graph.graphSnapshot(20),
      conversations: this.database.listConversations().slice(0, 8),
      claims,
    };
  }

  // ── v2.3: Salience Rules ─────────────────────────────────────────────

  /**
   * Check if a claim mutation triggers a salience rule. Returns an Alert or null.
   *
   * Rules per spec §4.5:
   *   - task → blocked: high
   *   - new constraint severity high/critical: high
   *   - new disputed claim: medium
   *   - decision superseded: medium
   *
   * @param {object} claimMutation - Object with claim_type, predicate, metadata, lifecycle_state, etc.
   * @returns {object|null} Alert object or null
   */
  checkSalience(claimMutation) {
    const claimType = String(claimMutation?.claim_type ?? "").trim().toLowerCase();
    const lifecycleState = String(claimMutation?.lifecycle_state ?? "").trim().toLowerCase();
    const predicate = String(claimMutation?.predicate ?? "").trim().toLowerCase();
    const metadata = claimMutation?.metadata ?? {};
    const entityLabel = claimMutation?.entity_label ?? null;
    const detail = claimMutation?.detail ?? "";
    const claimId = claimMutation?.claim_id ?? null;

    // Rule 1: task transitions to blocked → high
    if (claimType === "task") {
      const status = String(metadata?.status ?? predicate ?? "").trim().toLowerCase();
      if (status === "blocked" || lifecycleState === "blocked") {
        return {
          type: "task_blocked",
          salience: "high",
          detail: detail || "Task blocked",
          entity_label: entityLabel,
          claim_id: claimId,
        };
      }
    }

    // Rule 2: new constraint with severity high or critical → high
    if (claimType === "constraint") {
      const severity = String(metadata?.severity ?? "").trim().toLowerCase();
      if (severity === "high" || severity === "critical") {
        return {
          type: "new_constraint",
          salience: "high",
          detail: detail || "New high-severity constraint",
          entity_label: entityLabel,
          claim_id: claimId,
        };
      }
    }

    // Rule 3: new disputed claim → medium
    if (lifecycleState === "disputed") {
      return {
        type: "new_disputed",
        salience: "medium",
        detail: detail || "Disputed claim detected",
        entity_label: entityLabel,
        claim_id: claimId,
      };
    }

    // Rule 4: decision superseded → medium
    if (claimType === "decision" && lifecycleState === "superseded") {
      return {
        type: "decision_superseded",
        salience: "medium",
        detail: detail || "Decision superseded",
        entity_label: entityLabel,
        claim_id: claimId,
      };
    }

    return null;
  }

  // ── v2.3: Observation Compression ────────────────────────────────────

  /**
   * Compress near-duplicate observation clusters within a time window.
   *
   * Groups observations by (subject_entity_id, category) within windowHours.
   * If a group has >= minClusterSize members with average similarity > similarityThreshold,
   * creates a summary observation and marks originals as compressed.
   *
   * @param {object} options
   * @param {number} [options.windowHours=24] - Time window in hours
   * @param {number} [options.minClusterSize=3] - Minimum cluster size to compress
   * @param {number} [options.similarityThreshold=0.6] - Min average similarity to trigger compression
   * @returns {{ observationsCompressed: number, clustersCreated: number }}
   */
  async compressObservationClusters({ windowHours = 24, minClusterSize = 3, similarityThreshold = 0.6 } = {}) {
    const cutoffDate = new Date(Date.now() - windowHours * 60 * 60 * 1000).toISOString();
    const rows = this.database.listObservationsForCompression(cutoffDate);

    // Group by (subject_entity_id, category)
    const groups = new Map();
    for (const row of rows) {
      const key = `${row.subject_entity_id ?? "__none__"}::${row.category}`;
      if (!groups.has(key)) {
        groups.set(key, []);
      }
      groups.get(key).push(row);
    }

    let observationsCompressed = 0;
    let clustersCreated = 0;

    for (const [, groupRows] of groups) {
      // Cap at 200 per group
      const capped = groupRows.slice(0, 200);
      if (capped.length < minClusterSize) {
        continue;
      }

      const avgSimilarity = computeGroupSimilarity(capped);
      if (avgSimilarity < similarityThreshold) {
        continue;
      }

      // Create summary observation
      const bestRow = capped.reduce((best, row) =>
        Number(row.confidence ?? 0) >= Number(best.confidence ?? 0) ? row : best, capped[0]);

      // Collect unique facts from other rows
      const uniqueFacts = capped
        .filter((row) => row.id !== bestRow.id)
        .map((row) => String(row.detail ?? "").trim())
        .filter(Boolean)
        .filter((detail) => !bestRow.detail?.includes(detail.slice(0, 30)));
      const summaryDetail = [bestRow.detail, ...uniqueFacts.slice(0, 3)]
        .join("; ")
        .slice(0, 500);

      const compressedIds = capped.map((row) => row.id);
      const { summaryObs, markedCount } = this.database.withTransaction(() => {
        const summaryObs = this.database.insertObservation({
          conversationId: bestRow.conversationId,
          messageId: bestRow.messageId,
          actorId: bestRow.actorId ?? "system",
          category: bestRow.category,
          predicate: bestRow.predicate ?? null,
          subjectEntityId: bestRow.subject_entity_id ?? null,
          objectEntityId: bestRow.object_entity_id ?? null,
          detail: summaryDetail,
          confidence: Number(bestRow.confidence ?? 0.5),
          sourceSpan: null,
          metadata: {
            compressed_count: capped.length,
            compressed_from: capped.map((row) => row.id),
          },
          scopeKind: bestRow.scopeKind ?? "private",
          scopeId: bestRow.scopeId ?? null,
        });
        const markedCount = this.database.markObservationsCompressed(compressedIds, summaryObs.id);
        this.database.relinkClaimsToObservation(compressedIds, summaryObs.id);
        return { summaryObs, markedCount };
      });
      this.graph.updateGraphVersion(summaryObs.graphVersion);

      // Enqueue embedding for the summary
      this.enqueueObservationEmbedding({ id: summaryObs.id, detail: summaryDetail });

      observationsCompressed += markedCount;
      clustersCreated += 1;
    }

    return { observationsCompressed, clustersCreated };
  }

  loadSessionCheckpoint() {
    return this.database.loadLatestCheckpoint();
  }

  captureCheckpointSnapshot({ scopeFilter = null, limit = 200 } = {}) {
    const activeClaims = this.database.listCurrentClaims({
      types: ["task", "decision", "goal"],
      scopeFilter,
      limit,
    });

    return {
      activeClaims,
      activeTaskIds: activeClaims
        .filter((claim) => claim.claim_type === "task")
        .map((claim) => claim.id),
      activeDecisionIds: activeClaims
        .filter((claim) => claim.claim_type === "decision")
        .map((claim) => claim.id),
      activeGoalIds: activeClaims
        .filter((claim) => claim.claim_type === "goal")
        .map((claim) => claim.id),
      activeEntityIds: normalizeEntityIds(activeClaims.map((claim) => claimEntityId(claim))),
    };
  }

  buildDreamCycleClusterPlan({ since = null, until = null, sessionGapMinutes = 30 } = {}) {
    const params = [];
    const clauses = ["compressed_into IS NULL"];

    if (since) {
      clauses.push("created_at >= ?");
      params.push(since);
    }

    if (until) {
      clauses.push("created_at <= ?");
      params.push(until);
    }

    const observations = this.database.prepare(`
      SELECT
        id,
        conversation_id,
        message_id,
        category,
        predicate,
        subject_entity_id,
        object_entity_id,
        detail,
        confidence,
        source_span,
        metadata_json,
        created_at,
        compressed_into
      FROM observations
      WHERE ${clauses.join(" AND ")}
      ORDER BY created_at ASC
      LIMIT 10000
    `).all(...params);

    if (!observations.length) {
      return {
        episodes: [],
        clusters: [],
        episodeSummaries: [],
        observationsClustered: 0,
      };
    }

    const embeddings = new Map();
    for (const observation of observations) {
      const embeddingRecord = this.database.getObservationEmbedding(observation.id);
      if (embeddingRecord?.embedding) {
        embeddings.set(observation.id, embeddingRecord.embedding);
      }
    }

    const episodes = detectEpisodes(observations, sessionGapMinutes).map((episode, episodeIndex) => {
      const clusters = detectTopicClusters(episode.observations, { embeddings });
      return {
        ...episode,
        episode_id: `episode-${episodeIndex + 1}`,
        clusters: clusters.map((cluster, clusterIndex) => ({
          ...cluster,
          episode_id: `episode-${episodeIndex + 1}`,
          cluster_id: `episode-${episodeIndex + 1}:cluster-${clusterIndex + 1}`,
        })),
      };
    });

    return {
      episodes,
      clusters: episodes.flatMap((episode) => episode.clusters),
      episodeSummaries: episodes.map((episode) => ({
        episode_id: episode.episode_id,
        started_at: episode.started_at,
        ended_at: episode.ended_at,
        cluster_count: episode.clusters.length,
        total_observations: episode.observations.length,
      })),
      observationsClustered: observations.length,
    };
  }

  extractClusterAtoms(cluster) {
    const atoms = [];

    for (const observation of cluster?.observations ?? []) {
      const text = String(observation.detail ?? observation.predicate ?? "").trim();
      if (!text) {
        continue;
      }

      const atomType = observation.category === "decision"
        ? "decision"
        : observation.category === "task"
          ? "open_loop"
          : observation.category === "constraint"
            ? "tension"
            : "fact";

      atoms.push({
        atom_type: atomType,
        text,
        source_observation_ids: [observation.id],
      });
    }

    return atoms;
  }

  generateClusterLevels(cluster, atoms) {
    const sourceObservationIds = [...new Set(
      (cluster?.observations ?? []).map((observation) => observation.id).filter(Boolean),
    )];
    const combinedText = [
      ...atoms.map((atom) => atom.text),
      ...(cluster?.observations ?? []).map((observation) => String(observation.detail ?? observation.predicate ?? "").trim()),
    ]
      .filter(Boolean)
      .join(" | ");

    return {
      l2: {
        text: compactText(combinedText, 4000),
        source_observation_ids: sourceObservationIds,
      },
      l1: {
        text: compactText(combinedText, 2000),
        source_observation_ids: sourceObservationIds,
      },
      l0: {
        text: compactText(combinedText, 300),
        source_observation_ids: sourceObservationIds,
      },
    };
  }

  processDreamCycleClusters(clusters) {
    let atomsExtracted = 0;
    const levelsGenerated = { l0: 0, l1: 0, l2: 0 };

    for (const cluster of clusters ?? []) {
      if ((cluster?.observations?.length ?? 0) < 3) {
        continue;
      }

      const atoms = this.extractClusterAtoms(cluster);
      atomsExtracted += atoms.length;

      const levels = this.generateClusterLevels(cluster, atoms);
      for (const level of ["l0", "l1", "l2"]) {
        if (levels[level]?.text) {
          levelsGenerated[level] += 1;
        }
      }
    }

    return { atomsExtracted, levelsGenerated };
  }

  collectPatternCandidates({ lookbackDays = 14, minOccurrences = 3 } = {}) {
    const lookbackDate = new Date(Date.now() - lookbackDays * 24 * 60 * 60 * 1000).toISOString();
    const recentClaims = this.database.listRecentClaims({
      lifecycleStates: ["active", "candidate"],
      limit: 200,
    }).filter((claim) => {
      const claimDate = claim.created_at ?? claim.updated_at ?? "";
      return claimDate >= lookbackDate;
    });

    const groups = new Map();
    for (const claim of recentClaims) {
      const key = `${claim.subject_entity_id ?? "__none__"}::${claim.claim_type}::${claim.predicate ?? "__none__"}`;
      if (!groups.has(key)) {
        groups.set(key, []);
      }
      groups.get(key).push(claim);
    }

    const candidates = [];

    for (const [, groupClaims] of groups) {
      if (groupClaims.length < minOccurrences) {
        continue;
      }

      const avgSimilarity = computeGroupSimilarity(groupClaims.map((claim) => ({
        detail: claim.value_text ?? claim.predicate ?? "",
        embedding: null,
      })));
      if (avgSimilarity < 0.75) {
        continue;
      }

      const claimType = groupClaims[0].claim_type;
      const targetType = PATTERN_PROMOTION_TARGET[claimType] ?? claimType;
      
      // Only check for existing promotions if we're actually promoting to a different type.
      // If targetType == claimType, the pattern is on the same type (no promotion happening).
      if (targetType !== claimType) {
        const groupKey = groupClaims[0].subject_entity_id ?? "__none__";
        const groupPredicate = groupClaims[0].predicate ?? "__none__";
        const existingPromotion = this.database.listCurrentClaims({
          types: [targetType],
        }).find((claim) => {
          const existingEntity = claim.subject_entity_id ?? "__none__";
          const existingPredicate = claim.predicate ?? "__none__";
          return existingEntity === groupKey && existingPredicate === groupPredicate;
        });
        if (existingPromotion) {
          continue;
        }
      }

      const bestClaim = groupClaims.reduce((best, claim) =>
        Number(claim.confidence ?? 0) >= Number(best.confidence ?? 0) ? claim : best, groupClaims[0]);
      const entityLabel = this.claimEntityLabel(bestClaim) ?? claimEntityId(bestClaim);
      const occurrences = groupClaims.length;

      candidates.push({
        targetType,
        sourceType: claimType,
        entities: entityLabel ? [entityLabel] : [],
        entityLabel,
        confidence: Math.min(0.9, 0.5 + occurrences * 0.1),
        detail: bestClaim.value_text ?? bestClaim.predicate ?? "Detected pattern",
        sourceClaimIds: groupClaims.map((claim) => claim.id),
        occurrences,
      });
    }

    return candidates
      .sort((left, right) => right.confidence - left.confidence || right.occurrences - left.occurrences)
      .slice(0, PATTERN_PROMOTION_CAP);
  }

  // ── v2.3: Pattern Detection & Promotion ──────────────────────────────

  /**
   * Scan recent claims for recurring patterns and propose promotions.
   *
   * Groups claims by (subject_entity_id, claim_type, predicate), checks for
   * >= minOccurrences with high similarity, then proposes canonical mutations.
   *
   * @param {object} options
   * @param {number} [options.lookbackDays=14] - Days to look back for patterns
   * @param {number} [options.minOccurrences=3] - Minimum occurrences to trigger promotion
   * @returns {{ patternsDetected: number, promotionsProposed: number, patterns: object[] }}
   */
  async detectAndPromotePatterns({ lookbackDays = 14, minOccurrences = 3 } = {}) {
    const candidates = this.collectPatternCandidates({ lookbackDays, minOccurrences });
    let promotionsProposed = 0;
    const patterns = [];

    for (const candidate of candidates) {
      const proposalResult = this.proposeMutation({
        type: `add_${candidate.targetType}`,
        payload: {
          entityLabel: candidate.entityLabel,
          detail: candidate.detail,
          metadata: {
            pattern_source: candidate.sourceClaimIds,
            pattern_type: candidate.sourceType,
            occurrences: candidate.occurrences,
          },
        },
        confidence: candidate.confidence,
        actorId: "system",
      });

      if (proposalResult?.ok) {
        promotionsProposed += 1;
        patterns.push({
          type: candidate.targetType,
          entities: candidate.entities,
          confidence: candidate.confidence,
        });
      }
    }

    return {
      patternsDetected: candidates.length,
      promotionsProposed,
      patterns,
    };
  }

  // ── v2.3: Dream Cycle Orchestrator ───────────────────────────────────

  /**
   * Run the dream cycle — all consolidation steps in order.
   *
   * Steps: archive superseded → archive disputed → decay → cluster observations
   *        → extract atoms → generate LOD levels → detect patterns → save checkpoint
   *
   * Episode-end trigger (documented, not wired here):
   * Scribe session boundary detection should call
   * dreamCycle({ compress_observations: true, detect_patterns: false }).
   *
   * @param {object} options
   * @returns {object} ConsolidationReport
   */
  async dreamCycle(options = {}) {
    const archiveSupersededDays = Number(
      options.archive_superseded_days
      ?? options.archiveSupersededDays
      ?? 30,
    );
    const archiveDisputedDaysInput = options.archive_disputed_days
      ?? options.archiveDisputedDays
      ?? null;
    const archiveDisputedDays = archiveDisputedDaysInput === null || archiveDisputedDaysInput === undefined
      ? null
      : Number(archiveDisputedDaysInput);
    const compressObservations = options.compress_observations
      ?? options.compressObservations
      ?? true;
    const detectPatterns = options.detect_patterns
      ?? options.detectPatterns
      ?? true;
    const dryRun = options.dry_run
      ?? options.dryRun
      ?? false;

    if (this._dreamCycleLock) {
      const error = new Error("Dream cycle is already running (concurrency lock held)");
      error.statusCode = 409;
      throw error;
    }

    this._dreamCycleLock = true;
    const startTime = performance.now();
    const timestamp = new Date().toISOString();
    const clusterPlan = compressObservations
      ? this.buildDreamCycleClusterPlan({ until: timestamp, sessionGapMinutes: 30 })
      : { episodes: [], clusters: [], episodeSummaries: [], observationsClustered: 0 };

    let archivedSuperseded = 0;
    let archivedDisputed = 0;
    let claimsDecayed = 0;
    let observationsCompressed = 0;
    let episodesDetected = 0;
    let clustersCreated = 0;
    let atomsExtracted = 0;
    let levelBreakdown = { l0: 0, l1: 0, l2: 0 };
    let patternsDetected = 0;
    let promotionsProposed = 0;
    let newPatterns = [];

    try {
      if (!dryRun) {
        const supersededCutoff = new Date(Date.now() - archiveSupersededDays * 24 * 60 * 60 * 1000).toISOString();
        archivedSuperseded = this.database.archiveClaimsBefore({
          lifecycleStates: ["superseded"],
          beforeDate: supersededCutoff,
        });

        if (archiveDisputedDays !== null) {
          const disputedCutoff = new Date(Date.now() - archiveDisputedDays * 24 * 60 * 60 * 1000).toISOString();
          archivedDisputed = this.database.archiveClaimsBefore({
            lifecycleStates: ["disputed"],
            beforeDate: disputedCutoff,
          });
        }

        claimsDecayed = applyDecayToAllClaims(this.database).claimsDecayed;

        if (compressObservations) {
          const clusteringResult = clusterObservations(this.database, {
            until: timestamp,
            sessionGapMinutes: 30,
          });
          observationsCompressed = clusteringResult.observations_clustered;
          episodesDetected = clusteringResult.episodes_detected;
          clustersCreated = clusteringResult.clusters_detected;

          const artifactResult = this.processDreamCycleClusters(clusterPlan.clusters);
          atomsExtracted = artifactResult.atomsExtracted;
          levelBreakdown = artifactResult.levelsGenerated;
        }

        if (detectPatterns) {
          const patternResult = await this.detectAndPromotePatterns();
          patternsDetected = patternResult.patternsDetected;
          promotionsProposed = patternResult.promotionsProposed;
          newPatterns = patternResult.patterns;
        }

        const checkpointSnapshot = this.captureCheckpointSnapshot();
        this.database.saveSessionCheckpoint({
          graphVersion: this.graph.getGraphVersion(),
          activeTaskIds: checkpointSnapshot.activeTaskIds,
          activeDecisionIds: checkpointSnapshot.activeDecisionIds,
          activeGoalIds: checkpointSnapshot.activeGoalIds,
        });
      } else {
        const supersededCutoff = new Date(Date.now() - archiveSupersededDays * 24 * 60 * 60 * 1000).toISOString();
        archivedSuperseded = this.database.listRecentClaims({
          lifecycleStates: ["superseded"],
          limit: 200,
        }).filter((claim) => (claim.updated_at ?? "") < supersededCutoff).length;

        if (archiveDisputedDays !== null) {
          const disputedCutoff = new Date(Date.now() - archiveDisputedDays * 24 * 60 * 60 * 1000).toISOString();
          archivedDisputed = this.database.listRecentClaims({
            lifecycleStates: ["disputed"],
            limit: 200,
          }).filter((claim) => (claim.updated_at ?? "") < disputedCutoff).length;
        }

        claimsDecayed = this.database.listRecentClaims({
          lifecycleStates: ["active", "candidate"],
          limit: 200,
        }).length;

        if (compressObservations) {
          observationsCompressed = clusterPlan.observationsClustered;
          episodesDetected = clusterPlan.episodes.length;
          clustersCreated = clusterPlan.clusters.length;

          const artifactResult = this.processDreamCycleClusters(clusterPlan.clusters);
          atomsExtracted = artifactResult.atomsExtracted;
          levelBreakdown = artifactResult.levelsGenerated;
        }

        if (detectPatterns) {
          const candidates = this.collectPatternCandidates();
          patternsDetected = candidates.length;
          promotionsProposed = candidates.length;
          newPatterns = candidates.map((candidate) => ({
            type: candidate.targetType,
            entities: candidate.entities,
            confidence: candidate.confidence,
          }));
        }
      }

      const claimStates = Object.values(this.database.getClaimStateStats()).reduce((totals, states) => {
        for (const [state, count] of Object.entries(states ?? {})) {
          totals[state] = (totals[state] ?? 0) + Number(count ?? 0);
        }
        return totals;
      }, {});
      const durationMs = Math.round(performance.now() - startTime);
      const levelsGeneratedTotal = levelBreakdown.l0 + levelBreakdown.l1 + levelBreakdown.l2;
      const metrics = {
        claims_archived: archivedSuperseded + archivedDisputed,
        claims_decayed: claimsDecayed,
        episodes_detected: episodesDetected,
        clusters_created: clustersCreated,
        atoms_extracted: atomsExtracted,
        levels_generated: levelsGeneratedTotal,
        patterns_promoted: promotionsProposed,
      };

      // Track successful (non-dry-run) dream cycle execution
      if (!dryRun) {
        this._lastDreamCycleTimestamp = timestamp;
      }

      return {
        timestamp,
        duration_ms: durationMs,
        dry_run: dryRun,
        metrics,
        episode_summaries: clusterPlan.episodeSummaries,
        new_patterns: newPatterns,
        errors: [],
        archived_superseded: archivedSuperseded,
        archived_disputed: archivedDisputed,
        claims_decayed: claimsDecayed,
        observations_compressed: observationsCompressed,
        episodes_detected: episodesDetected,
        clusters_created: clustersCreated,
        atoms_extracted: atomsExtracted,
        levels_generated: levelBreakdown,
        patterns_detected: patternsDetected,
        promotions_proposed: promotionsProposed,
        claim_states: claimStates,
        graph_version: this.graph.getGraphVersion(),
      };
    } finally {
      this._dreamCycleLock = false;
    }
  }

  // ── v2.3: Session Recovery Packet ────────────────────────────────────

  /**
   * Generate a session recovery packet — what changed since last active session.
   *
   * @param {object} options
   * @returns {object} SessionRecoveryPacket
   */
  async sessionRecovery(options = {}) {
    const lastGraphVersion = options.lastGraphVersion
      ?? options.last_graph_version
      ?? null;
    const lastActiveAt = options.lastActiveAt
      ?? options.last_active_at
      ?? null;
    const tokenBudget = clamp(
      Number(options.tokenBudget ?? options.token_budget ?? 4000) || 4000,
      200,
      12000,
    );
    const timestamp = new Date().toISOString();
    const graphVersion = this.graph.getGraphVersion();
    const checkpoint = this.loadSessionCheckpoint();
    const resolvedLastVersion = lastGraphVersion ?? checkpoint?.graphVersion ?? graphVersion;
    const fallbackSince = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
    const usingFallback = !(lastActiveAt ?? checkpoint?.savedAt);
    const resolvedLastActiveAt = lastActiveAt ?? checkpoint?.savedAt ?? fallbackSince;
    const sinceTimestamp = Date.parse(resolvedLastActiveAt);

    const gapMs = resolvedLastActiveAt ? (Date.now() - new Date(resolvedLastActiveAt).getTime()) : 0;
    const gapHours = Number((gapMs / (1000 * 60 * 60)).toFixed(2));
    const checkpointSnapshot = {
      activeTaskIds: checkpoint?.activeTaskIds ?? [],
      activeDecisionIds: checkpoint?.activeDecisionIds ?? [],
      activeGoalIds: checkpoint?.activeGoalIds ?? [],
    };
    const currentSnapshot = this.captureCheckpointSnapshot();
    const createdClaims = this.database.listClaimsCreatedSince(resolvedLastActiveAt, 200);
    const transitionedClaims = this.database.listClaimsTransitionedSince(resolvedLastActiveAt, 200);
    const checkpointActiveIds = new Set([
      ...checkpointSnapshot.activeTaskIds,
      ...checkpointSnapshot.activeDecisionIds,
      ...checkpointSnapshot.activeGoalIds,
    ]);
    const currentActiveIds = new Set([
      ...currentSnapshot.activeTaskIds,
      ...currentSnapshot.activeDecisionIds,
      ...currentSnapshot.activeGoalIds,
    ]);

    const newClaims = createdClaims.map((claim) => ({
      id: claim.id,
      claim_type: claim.claim_type,
      subject: this.claimEntityLabel(claim) ?? claim.subject_entity_id ?? null,
      predicate: claim.predicate ?? null,
      value: claim.value_text ?? null,
      confidence: Number(claim.confidence ?? 0),
      lifecycle_state: claim.lifecycle_state,
    }));

    const updatedClaimsById = new Map();
    for (const claim of transitionedClaims) {
      updatedClaimsById.set(claim.id, {
        id: claim.id,
        previous_state: checkpointActiveIds.has(claim.id) ? "active" : "unknown",
        current_state: claim.lifecycle_state ?? "unknown",
        changed_at: claim.updated_at ?? claim.created_at ?? resolvedLastActiveAt,
      });
    }

    for (const claimId of new Set([...checkpointActiveIds, ...currentActiveIds])) {
      const wasActive = checkpointActiveIds.has(claimId);
      const isActive = currentActiveIds.has(claimId);
      if (wasActive === isActive) {
        continue;
      }

      const claim = currentSnapshot.activeClaims.find((row) => row.id === claimId) ?? this.database.getClaim(claimId);
      updatedClaimsById.set(claimId, {
        id: claimId,
        previous_state: wasActive ? "active" : "inactive",
        current_state: isActive ? "active" : String(claim?.lifecycle_state ?? "inactive"),
        changed_at: claim?.updated_at ?? claim?.created_at ?? resolvedLastActiveAt,
      });
    }

    const newConflicts = [];
    const seenConflictKeys = new Set();
    for (const claim of this.database.listDisputedClaims({ limit: 200 })) {
      if (Number.isFinite(sinceTimestamp) && claimTimestamp(claim) < sinceTimestamp) {
        continue;
      }

      const key = claim.resolution_key ?? `${claim.claim_type}:${claimEntityId(claim) ?? "unknown"}:${claim.predicate ?? "unknown"}`;
      if (seenConflictKeys.has(key)) {
        continue;
      }

      seenConflictKeys.add(key);
      const relatedClaims = claim.resolution_key
        ? this.database.listClaimsByResolutionKey(claim.resolution_key)
        : [claim];
      const conflictIds = [...new Set(relatedClaims.map((row) => row.id).filter(Boolean))];
      if (conflictIds.length < 2) {
        continue;
      }

      newConflicts.push({
        entity_id: claimEntityId(claim),
        conflicts_between: conflictIds,
      });
    }

    const changesSummary = [];
    if (usingFallback) {
      changesSummary.push("No checkpoint found; returning a last-24-hours recovery brief");
    }
    if (newClaims.length > 0) {
      changesSummary.push(`${newClaims.length} new claim${newClaims.length !== 1 ? "s" : ""} created`);
    }
    if (updatedClaimsById.size > 0) {
      changesSummary.push(`${updatedClaimsById.size} claim${updatedClaimsById.size !== 1 ? "s" : ""} updated`);
    }
    if (newConflicts.length > 0) {
      changesSummary.push(`${newConflicts.length} new conflict${newConflicts.length !== 1 ? "s" : ""} detected`);
    }
    if (graphVersion !== resolvedLastVersion) {
      changesSummary.push(`Graph version advanced from ${resolvedLastVersion} to ${graphVersion}`);
    }

    const activeState = this.buildActiveState({ limit: 8, tokenBudget: Math.floor(tokenBudget * 0.2) });
    const recentDecisions = this.buildRecentDecisionSummaries({
      limit: 6,
      tokenBudget: Math.floor(tokenBudget * 0.12),
    });
    const highSignalAlerts = this.preconsciousBuffer.poll().filter((alert) => alert.salience === "high");
    const currentAlerts = this.buildHighSignalAlerts({
      activeState,
      hardConstraints: [],
      unresolvedConflicts: [],
    });
    const updatedClaims = [...updatedClaimsById.values()]
      .sort((left, right) => String(left.changed_at).localeCompare(String(right.changed_at)));
    const packet = fitSessionRecoveryPacketToBudget({
      timestamp,
      graph_version: graphVersion,
      since_datetime: resolvedLastActiveAt,
      new_claims: newClaims,
      updated_claims: updatedClaims,
      new_conflicts: newConflicts,
      active_entities: currentSnapshot.activeEntityIds,
      summary: {
        claims_created: newClaims.length,
        claims_updated: updatedClaims.length,
        new_conflicts: newConflicts.length,
        active_entity_count: currentSnapshot.activeEntityIds.length,
      },
      last_known_version: resolvedLastVersion,
      gap_hours: gapHours,
      claims_created: newClaims.length,
      claims_transitioned: updatedClaims.length,
      active_work: {
        tasks: activeState.tasks,
        goals: activeState.goals,
      },
      recent_decisions: recentDecisions.decisions,
      high_signal_alerts: [...highSignalAlerts, ...currentAlerts].slice(0, 8),
      changes_summary: changesSummary,
    }, tokenBudget);

    return {
      ...packet,
      token_count: estimateTokens(JSON.stringify({
        ...packet,
        token_count: 0,
      })),
    };
  }

}

// ── Module-level helpers for v2.3 ───────────────────────────────────────

/**
 * Compute average pairwise similarity for a group of observations/claims.
 * Uses cosine similarity on embeddings if available, Jaccard on tokens as fallback.
 *
 * @param {object[]} items - Array of objects with { detail, embedding }
 * @returns {number} Average similarity [0, 1]
 */
function computeGroupSimilarity(items) {
  if (items.length < 2) {
    return 1.0; // Single item = trivially similar to itself
  }

  // Try cosine similarity if all items have embeddings
  const hasEmbeddings = items.every((item) => item.embedding?.length > 0);
  if (hasEmbeddings) {
    return computeAverageCosine(items);
  }

  // Fallback: Jaccard similarity on tokens
  return computeAverageJaccard(items);
}

function tokenize(text) {
  return new Set(
    String(text ?? "")
      .toLowerCase()
      .split(/[^a-z0-9]+/)
      .filter((token) => token.length > 2),
  );
}

function jaccardSimilarity(tokensA, tokensB) {
  if (!tokensA.size && !tokensB.size) {
    return 1.0;
  }

  const intersection = [...tokensA].filter((token) => tokensB.has(token)).length;
  const union = new Set([...tokensA, ...tokensB]).size;
  return union === 0 ? 0 : intersection / union;
}

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

function cosineSimilarityVec(a, b) {
  if (!a?.length || !b?.length || a.length !== b.length) {
    return 0;
  }

  let dot = 0;
  let normA = 0;
  let normB = 0;
  for (let i = 0; i < a.length; i += 1) {
    dot += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }

  if (!normA || !normB) {
    return 0;
  }

  return dot / (Math.sqrt(normA) * Math.sqrt(normB));
}

function computeAverageCosine(items) {
  let totalSimilarity = 0;
  let pairCount = 0;

  for (let i = 0; i < items.length; i += 1) {
    for (let j = i + 1; j < items.length; j += 1) {
      totalSimilarity += cosineSimilarityVec(items[i].embedding, items[j].embedding);
      pairCount += 1;
    }
  }

  return pairCount === 0 ? 0 : totalSimilarity / pairCount;
}

function compactText(text, maxCharacters) {
  const value = String(text ?? "").replace(/\s+/g, " ").trim();
  if (!value || value.length <= maxCharacters) {
    return value;
  }

  return `${value.slice(0, Math.max(0, maxCharacters - 3)).trimEnd()}...`;
}

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

function getNestedArray(root, path) {
  const parts = path.split(".");
  let current = root;

  for (const part of parts) {
    current = current?.[part];
  }

  return Array.isArray(current) ? current : null;
}

function fitSessionRecoveryPacketToBudget(packet, tokenBudget) {
  const fitted = cloneJson(packet);
  const trimOrder = [
    "new_claims",
    "updated_claims",
    "new_conflicts",
    "recent_decisions",
    "high_signal_alerts",
    "active_work.tasks",
    "active_work.goals",
    "active_entities",
    "changes_summary",
  ];

  for (const path of trimOrder) {
    const target = getNestedArray(fitted, path);
    if (!target) {
      continue;
    }

    while (target.length > 0 && estimateTokens(JSON.stringify({ ...fitted, token_count: 0 })) > tokenBudget) {
      target.pop();
    }
  }

  return fitted;
}

/** Pattern promotion target types per spec §4.2 Step 5 */
const PATTERN_PROMOTION_CAP = 5;
const PATTERN_PROMOTION_TARGET = {
  preference: "preference",
  constraint: "rule",
  state_change: "habit",
  event: "habit",
  decision: "rule",
  task: "task",
  fact: "fact",
};
