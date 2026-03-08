import { createId } from "./utils.js";
import {
  isValidClaimType,
  isValidLifecycleState,
  isValidSourceType,
  mapObservationCategory,
} from "./claim-types.js";

export const SOURCE_TYPE_RANK = {
  explicit: 4,
  implicit: 3,
  inference: 2,
  derived: 1,
};

export const CLAIM_SOURCE_TYPES = new Set(["explicit", "implicit", "inference", "derived", "unknown"]);

function pickField(record, ...keys) {
  for (const key of keys) {
    if (record?.[key] !== undefined) {
      return record[key];
    }
  }

  return null;
}

function normalizeString(value) {
  if (value === null || value === undefined) {
    return null;
  }

  const normalized = String(value).trim();
  return normalized ? normalized : null;
}

/**
 * Normalize claim text: lowercase + collapse whitespace
 * Used for resolution and facet key construction
 */
function normalizeClaimText(value) {
  return String(value ?? "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Infer claim source type with confidence thresholds
 * Explicit metadata override → confidence-based classification → origin_kind fallback
 */
function inferSourceType(observation) {
  // Check for explicit metadata override
  const metadata = observation?.metadata || observation?.meta || {};
  const explicitSourceType = normalizeString(metadata?.sourceType || metadata?.source_type);
  if (explicitSourceType && CLAIM_SOURCE_TYPES.has(explicitSourceType)) {
    return explicitSourceType;
  }

  const confidence = getConfidence(observation?.confidence);
  const originKind = normalizeString(pickField(observation, "origin_kind", "originKind"))?.toLowerCase();

  // User-originated claims: confidence-based thresholds
  if (originKind === "user") {
    if (confidence >= 0.85) return "explicit";
    if (confidence >= 0.65) return "implicit";
    return "inference";
  }

  // Agent-originated claims: high confidence = derived
  if (originKind === "agent") {
    return confidence >= 0.8 ? "derived" : "inference";
  }

  // Fallback: check actor/role for compatibility with existing behavior
  const actorId = normalizeString(pickField(observation, "actor_id", "actorId"))?.toLowerCase();
  const role = normalizeString(pickField(observation, "role"))?.toLowerCase();

  if (
    role === "user"
    || originKind === "user"
    || actorId === "user"
    || actorId === "human"
    || actorId?.startsWith("user:")
    || actorId?.startsWith("human:")
  ) {
    return "explicit";
  }

  if (
    role === "assistant"
    || originKind === "agent"
    || actorId === "assistant"
    || actorId === "agent"
    || actorId?.startsWith("assistant:")
    || actorId?.startsWith("agent:")
  ) {
    return "implicit";
  }

  if (
    role === "system"
    || originKind === "system"
    || originKind === "import"
    || actorId === "system"
    || actorId?.startsWith("system:")
  ) {
    return "derived";
  }

  return "implicit";
}

function getConfidence(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : 0.5;
}

function getRecencyTimestamp(claim) {
  const value = pickField(
    claim,
    "updated_at",
    "updatedAt",
    "created_at",
    "createdAt",
    "valid_from",
    "validFrom",
  );
  const timestamp = Date.parse(value ?? "");
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function compareClaimStrength(left, right) {
  const sourceDelta = (SOURCE_TYPE_RANK[left?.source_type] ?? 0) - (SOURCE_TYPE_RANK[right?.source_type] ?? 0);
  if (sourceDelta !== 0) {
    return sourceDelta;
  }

  const confidenceDelta = getConfidence(left?.confidence) - getConfidence(right?.confidence);
  if (confidenceDelta !== 0) {
    return confidenceDelta;
  }

  return getRecencyTimestamp(left) - getRecencyTimestamp(right);
}

/**
 * Check if metadata contains an explicit resolution key
 */
function hasExplicitResolutionKey(metadata = null) {
  const explicit = normalizeClaimText(
    metadata?.resolutionKey
    ?? metadata?.resolution_key
    ?? metadata?.stateKey
    ?? metadata?.state_key
    ?? metadata?.registryKey
    ?? metadata?.registry_key
    ?? metadata?.slot,
  );
  return Boolean(explicit);
}

/**
 * Build claim resolution key with 4-level fallback cascade:
 * 1. Explicit metadata (never orphans claims)
 * 2. Relationship-specific (subject + predicate + object)
 * 3. Predicate-based (predicate + subject/object)
 * 4. Value-text based (fallback to claim value)
 * Returns null only if all levels fail.
 */
export function buildClaimResolutionKey(claimType, subjectEntityId, predicate, objectEntityId, valueText, metadata = null) {
  // Level 1: Explicit metadata — highest priority
  const explicit = normalizeClaimText(
    metadata?.resolutionKey
    ?? metadata?.resolution_key
    ?? metadata?.stateKey
    ?? metadata?.state_key
    ?? metadata?.registryKey
    ?? metadata?.registry_key
    ?? metadata?.slot
    ?? metadata?.topic,
  );

  if (explicit) {
    return [claimType, explicit].join("|");
  }

  // Level 2: Relationship-specific (both subject and object required)
  if (claimType === "relationship" && subjectEntityId && objectEntityId) {
    return [
      claimType,
      subjectEntityId,
      normalizeClaimText(predicate ?? "related_to") || "related_to",
      objectEntityId,
    ].join("|");
  }

  // Level 3: Predicate-based (predicate + at least one of subject/object)
  if (predicate && (subjectEntityId || objectEntityId)) {
    return [
      claimType,
      subjectEntityId ?? "none",
      normalizeClaimText(predicate) || claimType,
      objectEntityId ?? "none",
    ].join("|");
  }

  // Level 4: Value-text based (fallback to claim value)
  const normalizedValue = normalizeClaimText(valueText);
  if (normalizedValue) {
    return [
      claimType,
      subjectEntityId ?? "none",
      normalizedValue.slice(0, 160),
    ].join("|");
  }

  return null;
}

/**
 * Legacy function: computes resolution key from 3 main fields
 * Uses `:` delimiter for backward compatibility with existing tests and data
 */
export function computeResolutionKey(claimType, subjectEntityId, predicate) {
  const normalizedType = normalizeString(claimType);
  const normalizedSubject = normalizeString(subjectEntityId);
  const normalizedPredicate = normalizeString(predicate);

  if (!normalizedType || !normalizedSubject || !normalizedPredicate) {
    return null;
  }

  if (!isValidClaimType(normalizedType)) {
    return null;
  }

  // Use `:` delimiter for backward compatibility
  return `${normalizedType}:${normalizedSubject}:${normalizedPredicate}`;
}

/**
 * Build facet key: 5-component key prevents collisions between claims
 * about different entities with same predicate
 * Components: type | subject | predicate | object | value
 */
export function buildClaimFacetKey(claimType, subjectEntityId, predicate, objectEntityId, valueText) {
  return [
    claimType,
    subjectEntityId ?? "none",
    normalizeClaimText(predicate ?? claimType) || "detail",
    objectEntityId ?? "none",
    normalizeClaimText(valueText).slice(0, 160) || "none",
  ].join("|");
}

/**
 * Deduplicate claims by facet key at retrieval time
 * Picks best claim per facet based on lifecycle state, confidence, and timestamp
 */
export function dedupeClaimsByFacet(claims) {
  const deduped = new Map();

  for (const claim of claims) {
    const key = claim.facet_key ?? claim.facetKey ?? claim.id;
    const existing = deduped.get(key);

    if (!existing) {
      deduped.set(key, claim);
      continue;
    }

    const existingState = String(existing.lifecycle_state ?? existing.lifecycleState ?? "active");
    const nextState = String(claim.lifecycle_state ?? claim.lifecycleState ?? "active");
    const existingConfidence = Number(existing.confidence ?? 0);
    const nextConfidence = Number(claim.confidence ?? 0);
    const existingTimestamp = String(existing.valid_from ?? existing.created_at ?? existing.createdAt ?? "");
    const nextTimestamp = String(claim.valid_from ?? claim.created_at ?? claim.createdAt ?? "");

    // Prefer active claims; if both same state, prefer higher confidence or newer
    const preferred = nextState === "active" && existingState !== "active"
      ? claim
      : nextState === existingState && (nextConfidence > existingConfidence || nextTimestamp > existingTimestamp)
        ? claim
        : existing;

    deduped.set(key, preferred);
  }

  return Array.from(deduped.values());
}

/**
 * Legacy function: computes facet key from observation
 * Returns simple predicate for certain claim types
 */
export function computeFacetKey(observation) {
  const claimType = mapObservationCategory(pickField(observation, "category"));
  const predicate = normalizeString(pickField(observation, "predicate"));

  if (["task", "decision", "goal", "constraint", "rule", "fact", "preference"].includes(claimType)) {
    return predicate;
  }

  return null;
}

export function buildClaimFromObservation(observation) {
  const claimType = mapObservationCategory(pickField(observation, "category"));
  if (!isValidClaimType(claimType)) {
    throw new Error(`Invalid claim type for observation category: ${pickField(observation, "category")}`);
  }

  const confidence = getConfidence(pickField(observation, "confidence"));
  const sourceType = inferSourceType(observation);
  if (!isValidSourceType(sourceType)) {
    throw new Error(`Invalid source type inferred for observation: ${sourceType}`);
  }

  const lifecycleState = confidence < 0.5 ? "candidate" : "active";
  if (!isValidLifecycleState(lifecycleState)) {
    throw new Error(`Invalid lifecycle state inferred for observation: ${lifecycleState}`);
  }

  const predicate = normalizeString(pickField(observation, "predicate"))
    ?? normalizeString(pickField(observation, "category"))?.toLowerCase()
    ?? null;
  const subjectEntityId = pickField(observation, "subject_entity_id", "subjectEntityId");
  const createdAt = pickField(observation, "created_at", "createdAt");

  const claim = {
    id: createId("claim"),
    observation_id: pickField(observation, "id", "observation_id", "observationId"),
    conversation_id: pickField(observation, "conversation_id", "conversationId"),
    message_id: pickField(observation, "message_id", "messageId"),
    actor_id: pickField(observation, "actor_id", "actorId"),
    claim_type: claimType,
    subject_entity_id: subjectEntityId,
    object_entity_id: pickField(observation, "object_entity_id", "objectEntityId"),
    predicate,
    value_text: pickField(observation, "detail"),
    confidence,
    source_type: sourceType,
    lifecycle_state: lifecycleState,
    resolution_key: computeResolutionKey(claimType, subjectEntityId, predicate),
    facet_key: computeFacetKey(observation),
    supersedes_claim_id: null,
    superseded_by_claim_id: null,
    scope_kind: pickField(observation, "scope_kind", "scopeKind"),
    scope_id: pickField(observation, "scope_id", "scopeId"),
  };

  if (createdAt) {
    claim.valid_from = createdAt;
    claim.created_at = createdAt;
    claim.updated_at = createdAt;
  }

  return claim;
}

export function resolveSupersession(newClaim, existingClaims) {
  const activeClaims = (existingClaims ?? []).filter((claim) => claim?.lifecycle_state === "active");
  if (activeClaims.length === 0) {
    return { action: "activate", supersedes: null };
  }

  const matchingFacetClaims = activeClaims.filter(
    (claim) => (claim?.facet_key ?? null) === (newClaim?.facet_key ?? null),
  );
  if (matchingFacetClaims.length === 0) {
    return { action: "activate", supersedes: null };
  }

  const strongestExisting = matchingFacetClaims.reduce((best, claim) => {
    if (!best) {
      return claim;
    }

    return compareClaimStrength(claim, best) > 0 ? claim : best;
  }, null);

  if (compareClaimStrength(newClaim, strongestExisting) > 0) {
    return { action: "supersede", supersedes: strongestExisting.id };
  }

  return { action: "dispute", conflictsWith: strongestExisting.id };
}

export function ensureClaimForObservation(db, observation) {
  const claim = buildClaimFromObservation(observation);
  const insertClaimWithState = (lifecycleState) => db.insertClaim({
    ...claim,
    lifecycle_state: lifecycleState,
  });

  if (!claim.resolution_key) {
    return insertClaimWithState("active");
  }

  let resolution = { action: "activate", supersedes: null };

  try {
    const existingClaims = db.listClaimsByResolutionKey(claim.resolution_key);
    resolution = resolveSupersession(claim, existingClaims);
  } catch (error) {
    console.warn(
      `[claim-resolution] Failed to resolve claim ${claim.id}; inserting as active.`,
      error,
    );
    return insertClaimWithState("active");
  }

  if (resolution.action === "supersede" && resolution.supersedes) {
    let insertedClaim = null;

    try {
      insertedClaim = insertClaimWithState("active");
      db.updateClaim(resolution.supersedes, {
        lifecycle_state: "superseded",
        superseded_by_claim_id: insertedClaim.id,
      });

      return db.updateClaim(insertedClaim.id, {
        supersedes_claim_id: resolution.supersedes,
      });
    } catch (error) {
      console.warn(
        `[claim-resolution] Failed to supersede claim ${resolution.supersedes}; keeping new claim active.`,
        error,
      );
      return insertedClaim ?? insertClaimWithState("active");
    }
  }

  if (resolution.action === "dispute" && resolution.conflictsWith) {
    let insertedClaim = null;

    try {
      insertedClaim = insertClaimWithState("disputed");
      db.updateClaim(resolution.conflictsWith, {
        lifecycle_state: "disputed",
      });
      return insertedClaim;
    } catch (error) {
      console.warn(
        `[claim-resolution] Failed to dispute claim ${resolution.conflictsWith}; inserting as active.`,
        error,
      );
      return insertedClaim ?? insertClaimWithState("active");
    }
  }

  return insertClaimWithState("active");
}
