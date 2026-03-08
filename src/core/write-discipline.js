/**
 * Write discipline classifier for ContextOS v2.3.
 *
 * Every mutation is classified into one of three write classes:
 *   - auto:        Safe to apply immediately, no review needed
 *   - ai_proposed: Apply if confidence ≥ threshold, else queue for review
 *   - canonical:   Always queue for human approval, never auto-apply
 *
 * Per spec §4.6.
 *
 * @module write-discipline
 */

// ── Write Class Rules ────────────────────────────────────────────────

/**
 * Classification rules by mutation type.
 *
 * auto:      Entity/relationship/profile enrichment — low risk, append-only.
 * canonical: Decisions, constraints, projects, milestones — change long-term memory.
 * ai_proposed (default): Everything else — apply with confidence gate.
 */
export const WRITE_CLASS_RULES = {
  // Class 1: Auto — apply immediately, no review
  auto: new Set([
    'add_entity',       // Entity extraction is safe
    'link_entities',    // Relationship detection is safe
    'update_profile',   // Profile enrichment
  ]),

  // Class 3: Canonical — always require human approval
  canonical: new Set([
    'add_decision',
    'supersede_decision',
    'add_constraint',
    'update_constraint',
    'expire_constraint',
    'add_project',
    'update_project',
    'mark_breakthrough',
  ]),

  // Class 2: AI-Proposed (everything else)
  // add_task, update_task, close_task, reopen_task,
  // assert_fact, retract_fact, update_entity, update_profile (not in auto), etc.
};

/**
 * Classify a mutation type into its write class.
 *
 * @param {string} mutationType - The mutation type string (e.g., 'add_task', 'add_decision')
 * @returns {'auto' | 'ai_proposed' | 'canonical'} The write class
 */
export function classifyWriteClass(mutationType) {
  if (WRITE_CLASS_RULES.auto.has(mutationType)) {
    return 'auto';
  }

  if (WRITE_CLASS_RULES.canonical.has(mutationType)) {
    return 'canonical';
  }

  // Default: AI-proposed (includes unknown types per spec §8)
  return 'ai_proposed';
}
