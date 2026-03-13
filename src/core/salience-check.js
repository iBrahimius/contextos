/**
 * Salience Checking
 *
 * Evaluates mutations to determine if they should generate alerts in the preconscious buffer.
 * Salience levels: null (no alert), "low", "medium", "high"
 */

/**
 * Check a mutation for salience triggers.
 *
 * @param {Object} mutation - Mutation object with type, payload, and context
 * @param {Object} context - Context object with:
 *   - database: database instance
 *   - focusedEntityId: currently focused entity (if any)
 *   - config: configuration object
 * @returns {Object|null} - { salience: "high"|"medium"|"low", type: string, detail: string, entityLabel?: string, entityId?: string }
 *           or null if no salience trigger
 */
export function checkSalience(mutation, context = {}) {
  const { type, payload } = mutation;
  const { database, focusedEntityId, config } = context;

  // Trigger 1: Task → blocked
  if (type === "update_task" && payload?.lifecycle_state === "blocked") {
    return {
      salience: "high",
      type: "task_blocked",
      detail: `Task "${payload.title ?? "Untitled"}" is now blocked`,
      entityLabel: payload.title,
      entityId: payload.id,
    };
  }

  // Trigger 2: New constraint with high/critical severity
  if ((type === "add_constraint" || type === "update_constraint") &&
      (payload?.severity === "high" || payload?.severity === "critical")) {
    return {
      salience: "high",
      type: "constraint_created",
      detail: `New ${payload.severity} constraint: ${payload.label ?? "Constraint"}`,
      entityLabel: payload.label,
      entityId: payload.id,
    };
  }

  // Trigger 3: New disputed claim on active entity
  if (type === "add_claim" && payload?.lifecycle_state === "disputed") {
    return {
      salience: "medium",
      type: "disputed_claim",
      detail: `New disputed claim on entity ${payload.subject_entity_id}`,
      entityId: payload.subject_entity_id,
    };
  }

  // Trigger 4: Decision superseded
  if (type === "supersede_decision") {
    return {
      salience: "medium",
      type: "decision_superseded",
      detail: `Decision was superseded by a newer one`,
      entityId: payload.decision_id,
    };
  }

  // Trigger 5: Task approaching deadline
  if ((type === "add_task" || type === "update_task") && payload?.deadline) {
    const deadline = new Date(payload.deadline);
    const now = new Date();
    const daysUntil = (deadline - now) / (1000 * 60 * 60 * 24);

    if (daysUntil > 0 && daysUntil <= 3) {
      // Within 3 days
      return {
        salience: "high",
        type: "task_deadline_approaching",
        detail: `Task "${payload.title ?? "Untitled"}" approaching deadline in ${Math.ceil(daysUntil)} days`,
        entityLabel: payload.title,
        entityId: payload.id,
      };
    }
  }

  // Trigger 6: New claim on currently-focused entity (low salience, can be disabled)
  if ((type === "add_claim" || type === "assert_fact") &&
      focusedEntityId &&
      payload?.subject_entity_id === focusedEntityId &&
      config?.salience?.enableLowOnFocused !== false) {
    return {
      salience: "low",
      type: "claim_on_focused_entity",
      detail: `New claim on focused entity`,
      entityId: focusedEntityId,
    };
  }

  // No salience trigger
  return null;
}

/**
 * Check if a salience alert should be created for a mutation.
 * Wrapper that formats the check result as an alert object.
 *
 * @param {Object} mutation - The mutation
 * @param {Object} context - Context with database, focusedEntityId, config
 * @returns {Object|null} - Alert object or null
 */
export function getSalienceAlert(mutation, context = {}) {
  const result = checkSalience(mutation, context);
  if (!result) {
    return null;
  }

  return {
    type: result.type,
    salience: result.salience,
    detail: result.detail,
    entityLabel: result.entityLabel,
    entityId: result.entityId,
    timestamp: new Date().toISOString(),
    mutationId: mutation.id,
  };
}

export default { checkSalience, getSalienceAlert };
