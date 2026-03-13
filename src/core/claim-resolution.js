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
export const CLAIM_LIFECYCLE_RANK = {
  active: 5,
  candidate: 4,
  disputed: 3,
  superseded: 2,
  archived: 1,
};

export const CLAIM_LIFECYCLE_TRANSITIONS = {
  candidate: ["active", "disputed", "archived"],
  active: ["superseded", "disputed", "archived"],
  disputed: ["active", "superseded", "archived"],
  superseded: ["archived"],
  archived: [],
};

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

export function getValidLifecycleTransitions(currentState) {
  const normalizedState = normalizeString(currentState);
  if (!normalizedState || !isValidLifecycleState(normalizedState)) {
    return [];
  }

  return [...(CLAIM_LIFECYCLE_TRANSITIONS[normalizedState] ?? [])];
}

export function validateLifecycleTransition(fromState, toState) {
  const normalizedFrom = normalizeString(fromState);
  const normalizedTo = normalizeString(toState);

  if (!normalizedFrom || !normalizedTo) {
    return false;
  }

  if (normalizedFrom === normalizedTo) {
    return true;
  }

  return getValidLifecycleTransitions(normalizedFrom).includes(normalizedTo);
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

function clampConfidence(value) {
  if (!Number.isFinite(value)) {
    return 0;
  }

  return Math.max(0, Math.min(1, value));
}

function roundConfidence(value) {
  return Number(clampConfidence(value).toFixed(4));
}

export function claimHasSupersededSuccessor(claim) {
  return Boolean(claim?.superseded_by_claim_id ?? claim?.supersededByClaimId);
}

export function isClaimCurrent(claim) {
  const lifecycleState = claim?.lifecycle_state ?? claim?.lifecycleState ?? null;
  if (["superseded", "archived"].includes(lifecycleState)) {
    return false;
  }

  return !claimHasSupersededSuccessor(claim);
}

function isScalarClaimValue(value) {
  const normalized = normalizeClaimText(value);
  if (!normalized) {
    return false;
  }

  if (normalized.length > 80 || /[.!?;:]/.test(normalized)) {
    return false;
  }

  if (/^[€$£]?\d+(?:[.,]\d+)?$/.test(normalized)) {
    return true;
  }

  const tokens = normalized.split(/\s+/).filter(Boolean);
  return tokens.length <= 2;
}

export function getClaimValueSignature(claim) {
  const objectEntityId = normalizeString(claim?.object_entity_id ?? claim?.objectEntityId);
  if (objectEntityId) {
    return `object:${objectEntityId}`;
  }

  const valueText = normalizeString(claim?.value_text ?? claim?.valueText);
  if (isScalarClaimValue(valueText)) {
    return `value:${normalizeClaimText(valueText).slice(0, 160)}`;
  }

  return null;
}

export function getClaimSupportKey(claim) {
  const observationId = normalizeString(claim?.observation_id ?? claim?.observationId);
  if (observationId) {
    return `observation:${observationId}`;
  }

  const messageId = normalizeString(claim?.message_id ?? claim?.messageId);
  if (messageId) {
    return `message:${messageId}`;
  }

  const actorId = normalizeString(claim?.actor_id ?? claim?.actorId) ?? "unknown";
  const createdAt = normalizeString(claim?.created_at ?? claim?.createdAt ?? claim?.updated_at ?? claim?.updatedAt) ?? "unknown";
  return `claim:${actorId}:${createdAt}`;
}

export function claimsMateriallyConflict(left, right) {
  if (claimResolutionGroupKey(left) !== claimResolutionGroupKey(right)) {
    return false;
  }

  const leftSignature = getClaimValueSignature(left);
  const rightSignature = getClaimValueSignature(right);
  if (!leftSignature || !rightSignature) {
    return false;
  }

  return leftSignature !== rightSignature;
}

function aggregateConfidenceFromClaims(claims) {
  const uniqueSupport = new Map();
  for (const claim of claims ?? []) {
    const supportKey = getClaimSupportKey(claim);
    const confidence = getConfidence(claim?.confidence);
    const existing = uniqueSupport.get(supportKey) ?? 0;
    if (confidence > existing) {
      uniqueSupport.set(supportKey, confidence);
    }
  }

  let complement = 1;
  for (const confidence of uniqueSupport.values()) {
    complement *= (1 - clampConfidence(confidence));
  }

  return {
    support_keys: [...uniqueSupport.keys()],
    aggregated_confidence: roundConfidence(1 - complement),
  };
}

export function analyzeClaimsTruthSet(claims) {
  const groups = new Map();
  for (const claim of claims ?? []) {
    const resolutionKey = claimResolutionGroupKey(claim) ?? `claim:${claim?.id ?? "unknown"}`;
    const group = groups.get(resolutionKey) ?? [];
    group.push(claim);
    groups.set(resolutionKey, group);
  }

  const byClaimId = new Map();
  const conflicts = [];
  const variants = [];

  for (const [resolutionKey, resolutionClaims] of groups.entries()) {
    const variantMap = new Map();
    for (const claim of resolutionClaims) {
      const valueSignature = getClaimValueSignature(claim);
      const variantKey = valueSignature ?? `claim:${claim.id}`;
      const variant = variantMap.get(variantKey) ?? {
        resolution_key: resolutionKey,
        value_signature: valueSignature,
        claims: [],
        current_claims: [],
      };
      variant.claims.push(claim);
      if (isClaimCurrent(claim)) {
        variant.current_claims.push(claim);
      }
      variantMap.set(variantKey, variant);
    }

    const currentComparableVariants = [...variantMap.values()].filter((variant) => variant.current_claims.length > 0 && variant.value_signature);
    const hasConflict = currentComparableVariants.length > 1;
    const conflictSetId = hasConflict ? `conflict:${resolutionKey}` : null;
    const conflictClaimIds = hasConflict
      ? currentComparableVariants.flatMap((variant) => variant.current_claims.map((claim) => claim.id))
      : [];

    if (hasConflict) {
      conflicts.push({
        conflict_set_id: conflictSetId,
        resolution_key: resolutionKey,
        claim_ids: [...conflictClaimIds],
        claim_count: conflictClaimIds.length,
        value_signatures: currentComparableVariants.map((variant) => variant.value_signature),
      });
    }

    for (const variant of variantMap.values()) {
      const supportClaims = variant.current_claims.length
        ? variant.current_claims
        : variant.claims.filter((claim) => (claim?.lifecycle_state ?? null) !== "archived");
      const aggregated = aggregateConfidenceFromClaims(supportClaims);
      const representativePool = variant.current_claims.length ? variant.current_claims : variant.claims;
      const representative = representativePool.reduce((best, claim) => (!best || compareClaimLifecyclePriority(claim, best) > 0 ? claim : best), null);
      const variantConflictIds = hasConflict
        ? conflictClaimIds.filter((claimId) => !variant.current_claims.some((claim) => claim.id === claimId))
        : [];
      const conflictPenalty = hasConflict ? 0.65 : 1;
      const effectiveConfidence = roundConfidence(aggregated.aggregated_confidence * conflictPenalty);
      const variantSummary = {
        resolution_key: resolutionKey,
        representative_claim_id: representative?.id ?? null,
        value_signature: variant.value_signature,
        claim_ids: variant.claims.map((claim) => claim.id),
        current_claim_ids: variant.current_claims.map((claim) => claim.id),
        support_claim_ids: supportClaims.map((claim) => claim.id),
        support_keys: aggregated.support_keys,
        support_count: aggregated.support_keys.length,
        aggregated_confidence: aggregated.aggregated_confidence,
        effective_confidence: effectiveConfidence,
        extraction_confidence: roundConfidence(getConfidence(representative?.confidence)),
        has_conflict: hasConflict,
        conflict_set_id: conflictSetId,
        conflicting_claim_ids: variantConflictIds,
      };
      variants.push(variantSummary);

      for (const claim of variant.claims) {
        const isCurrent = isClaimCurrent(claim);
        byClaimId.set(claim.id, {
          ...variantSummary,
          has_conflict: hasConflict && isCurrent,
          lifecycle_state: claim?.lifecycle_state ?? null,
          is_current: isCurrent,
          is_superseded: !isCurrent,
        });
      }
    }
  }

  return {
    byClaimId,
    conflicts,
    variants,
  };
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

function effectiveClaimLifecycleState(claim) {
  if ((claim?.superseded_by_claim_id ?? claim?.supersededByClaimId) && claim?.lifecycle_state === "active") {
    return "superseded";
  }

  return claim?.lifecycle_state ?? null;
}

function compareClaimLifecyclePriority(left, right) {
  const lifecycleDelta = (CLAIM_LIFECYCLE_RANK[effectiveClaimLifecycleState(left)] ?? 0)
    - (CLAIM_LIFECYCLE_RANK[effectiveClaimLifecycleState(right)] ?? 0);
  if (lifecycleDelta !== 0) {
    return lifecycleDelta;
  }

  const strengthDelta = compareClaimStrength(left, right);
  if (strengthDelta !== 0) {
    return strengthDelta;
  }

  return getRecencyTimestamp(left) - getRecencyTimestamp(right);
}

function claimResolutionGroupKey(claim) {
  return normalizeString(claim?.resolution_key ?? claim?.resolutionKey)
    ?? normalizeString(claim?.facet_key ?? claim?.facetKey)
    ?? normalizeString(claim?.id)
    ?? null;
}

/**
 * Check if metadata contains an explicit resolution key
 */
function _hasExplicitResolutionKey(metadata = null) {
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
 * Picks best claim per facet based on lifecycle state, strength, and recency
 */
export function dedupeClaimsByFacet(claims) {
  const deduped = new Map();

  for (const claim of claims) {
    const key = claim.facet_key ?? claim.facetKey ?? claim.id;
    const existing = deduped.get(key);

    if (!existing || compareClaimLifecyclePriority(claim, existing) > 0) {
      deduped.set(key, claim);
    }
  }

  return Array.from(deduped.values());
}

/**
 * Collapse contradictory lifecycle/history rows into one preferred current-truth
 * path per resolution key. Historical rows stay queryable via the database.
 */
export function selectPreferredClaimsByResolution(claims) {
  const preferred = new Map();

  for (const claim of claims ?? []) {
    const key = claimResolutionGroupKey(claim);
    if (!key) {
      continue;
    }

    const existing = preferred.get(key);
    if (!existing || compareClaimLifecyclePriority(claim, existing) > 0) {
      preferred.set(key, claim);
    }
  }

  return Array.from(preferred.values());
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
  const currentClaims = (existingClaims ?? []).filter((claim) => isClaimCurrent(claim));
  const activeClaims = currentClaims.filter((claim) => claim?.lifecycle_state === "active");
  if (currentClaims.length === 0) {
    return { action: "activate", supersedes: null };
  }

  const newSignature = getClaimValueSignature(newClaim);
  if (newSignature) {
    const conflictingClaims = currentClaims.filter((claim) => claimsMateriallyConflict(newClaim, claim));
    if (conflictingClaims.length > 0) {
      const conflictsWithIds = conflictingClaims.map((claim) => claim.id);
      return {
        action: "dispute",
        conflictsWithIds,
        conflictsWith: conflictsWithIds[0] ?? null,
      };
    }

    const supportingClaims = currentClaims.filter((claim) => getClaimValueSignature(claim) === newSignature);
    if (supportingClaims.length > 0) {
      return { action: "support", supports: supportingClaims.map((claim) => claim.id) };
    }
  }

  if (activeClaims.length === 0) {
    return { action: "activate", supersedes: null };
  }

  const matchingFacetClaims = activeClaims.filter(
    (claim) => (claim?.facet_key ?? null) === (newClaim?.facet_key ?? null),
  );
  const comparisonPool = matchingFacetClaims.length ? matchingFacetClaims : activeClaims;
  const strongestExisting = comparisonPool.reduce((best, claim) => {
    if (!best) {
      return claim;
    }

    return compareClaimStrength(claim, best) > 0 ? claim : best;
  }, null);

  if (!strongestExisting) {
    return { action: "activate", supersedes: null };
  }

  if (compareClaimStrength(newClaim, strongestExisting) > 0) {
    return { action: "supersede", supersedes: strongestExisting.id };
  }

  return {
    action: "dispute",
    conflictsWithIds: [strongestExisting.id],
    conflictsWith: strongestExisting.id,
  };
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
      const supersededAt = insertedClaim.valid_from ?? insertedClaim.created_at ?? new Date().toISOString();
      db.updateClaim(resolution.supersedes, {
        lifecycle_state: "superseded",
        superseded_by_claim_id: insertedClaim.id,
        valid_to: supersededAt,
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

  if (resolution.action === "support") {
    return insertClaimWithState("active");
  }

  if (resolution.action === "dispute" && resolution.conflictsWithIds?.length) {
    let insertedClaim = null;

    try {
      insertedClaim = insertClaimWithState("disputed");
      for (const conflictId of resolution.conflictsWithIds) {
        db.updateClaim(conflictId, {
          lifecycle_state: "disputed",
        });
      }
      return insertedClaim;
    } catch (error) {
      console.warn(
        `[claim-resolution] Failed to dispute claims ${resolution.conflictsWithIds.join(", ")}; inserting as active.`,
        error,
      );
      return insertedClaim ?? insertClaimWithState("active");
    }
  }

  return insertClaimWithState("active");
}
