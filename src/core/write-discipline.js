/**
 * Write discipline classifier for ContextOS v2.12.
 *
 * Every mutation is classified into one of three write classes:
 *   - auto:        Safe to apply immediately, no review needed
 *   - ai_proposed: Queue for review as an AI-authored proposal
 *   - canonical:   Always queue for human approval, never auto-apply
 *
 * The runtime does not auto-accept `ai_proposed` mutations. Instead, v2.12 adds
 * an explicit queue-pressure policy that parks only low-confidence AI proposals in
 * a visible backlog bucket while keeping them auditable and manually reviewable.
 *
 * @module write-discipline
 */

// ── Write Class Rules ────────────────────────────────────────────────

/**
 * Classification rules by mutation type.
 *
 * auto:      Entity/relationship/profile enrichment — low risk, append-only.
 * canonical: Decisions, constraints, projects, milestones — change long-term memory.
 * ai_proposed (default): Everything else — queue for explicit review.
 */
export const WRITE_CLASS_RULES = {
  // Class 1: Auto — apply immediately, no review
  auto: new Set([
    "add_entity",
    "link_entities",
    "update_profile",
  ]),

  // Class 3: Canonical — always require human approval
  canonical: new Set([
    "add_decision",
    "supersede_decision",
    "add_constraint",
    "update_constraint",
    "expire_constraint",
    "add_project",
    "update_project",
    "mark_breakthrough",
  ]),
};

export const AI_PROPOSED_PARKING_THRESHOLD = 0.6;
export const QUEUE_PRESSURE_POLICY_KEY = "low_confidence_ai_proposed_parking";
export const QUEUE_PRESSURE_POLICY_VERSION = "v2.12";

/**
 * Classify a mutation type into its write class.
 *
 * @param {string} mutationType - The mutation type string (e.g., 'add_task', 'add_decision')
 * @returns {'auto' | 'ai_proposed' | 'canonical'} The write class
 */
export function classifyWriteClass(mutationType) {
  if (WRITE_CLASS_RULES.auto.has(mutationType)) {
    return "auto";
  }

  if (WRITE_CLASS_RULES.canonical.has(mutationType)) {
    return "canonical";
  }

  return "ai_proposed";
}

/**
 * Describe how the runtime should treat a write class at proposal time.
 *
 * @param {'auto' | 'ai_proposed' | 'canonical'} writeClass
 * @returns {{ autoApply: boolean, reviewRequired: boolean, queueReason: string | null }}
 */
export function getWriteClassDisposition(writeClass) {
  switch (writeClass) {
    case "auto":
      return {
        autoApply: true,
        reviewRequired: false,
        queueReason: null,
      };
    case "canonical":
      return {
        autoApply: false,
        reviewRequired: true,
        queueReason: "canonical_requires_review",
      };
    case "ai_proposed":
    default:
      return {
        autoApply: false,
        reviewRequired: true,
        queueReason: "ai_proposed_requires_review",
      };
  }
}

/**
 * Compute deterministic queue-pressure metadata for a proposal.
 *
 * This does not change write-class semantics or auto-apply behavior. It only
 * decides whether a queued item belongs in the actionable review surface or in a
 * parked backlog bucket.
 *
 * @param {{
 *   writeClass?: 'auto' | 'ai_proposed' | 'canonical',
 *   status?: string,
 *   confidence?: number,
 * }} input
 */
export function getQueuePressureDisposition({ writeClass = "ai_proposed", status = null, confidence = 0 } = {}) {
  const normalizedStatus = String(status ?? "").trim().toLowerCase();
  const normalizedConfidence = Number.isFinite(Number(confidence)) ? Number(confidence) : 0;
  const disposition = getWriteClassDisposition(writeClass);

  const basePolicy = {
    policy_key: QUEUE_PRESSURE_POLICY_KEY,
    policy_version: QUEUE_PRESSURE_POLICY_VERSION,
    confidence_threshold: AI_PROPOSED_PARKING_THRESHOLD,
    confidence: normalizedConfidence,
    write_class: writeClass,
  };

  if (normalizedStatus === "accepted" && disposition.autoApply) {
    return {
      ...basePolicy,
      queue_bucket: "not_queued",
      actionable: false,
      queue_reason: null,
      triage: "auto_applied",
      policy_decision: "auto_apply",
    };
  }

  if (!["pending", "proposed"].includes(normalizedStatus)) {
    return {
      ...basePolicy,
      queue_bucket: "not_queued",
      actionable: false,
      queue_reason: null,
      triage: null,
      policy_decision: "review_closed",
    };
  }

  if (writeClass === "auto") {
    return {
      ...basePolicy,
      queue_bucket: "actionable",
      actionable: true,
      queue_reason: "auto_apply_failed",
      triage: "needs_attention",
      policy_decision: "queue_auto_apply_failure",
    };
  }

  if (writeClass === "canonical") {
    return {
      ...basePolicy,
      queue_bucket: "actionable",
      actionable: true,
      queue_reason: disposition.queueReason,
      triage: "human_canonical",
      policy_decision: "queue_canonical_review",
    };
  }

  if (normalizedConfidence < AI_PROPOSED_PARKING_THRESHOLD) {
    return {
      ...basePolicy,
      queue_bucket: "parked",
      actionable: false,
      queue_reason: "low_confidence_ai_proposed_parked",
      triage: "parked_backlog",
      policy_decision: "park_low_confidence_ai_proposed",
    };
  }

  return {
    ...basePolicy,
    queue_bucket: "actionable",
    actionable: true,
    queue_reason: disposition.queueReason,
    triage: "ai_review",
    policy_decision: "queue_ai_review",
  };
}
