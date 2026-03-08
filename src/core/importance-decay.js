/**
 * Importance decay module for ContextOS v2.3.
 *
 * Calculates time-based importance scores for claims, weighted by type.
 * Events fade fast (~5-day half-life), rules never decay.
 *
 * @module importance-decay
 */

// ── Decay Rates ─────────────────────────────────────────────────────
// Per spec §4.3: base importance and daily decay rate per claim type.

export const DECAY_RATES = {
  event:        { base: 0.5, rate: 0.87 },   // ~5 day half-life
  state_change: { base: 0.5, rate: 0.87 },   // ~5 day half-life
  fact:         { base: 0.7, rate: 0.97 },   // ~23 day half-life
  task:         { base: 0.8, rate: 0.98 },   // ~35 day half-life
  decision:     { base: 0.8, rate: 0.99 },   // ~69 day half-life
  constraint:   { base: 0.9, rate: 0.995 },  // ~139 day half-life
  preference:   { base: 0.7, rate: 0.998 },  // ~347 day half-life
  rule:         { base: 1.0, rate: 1.0 },    // never decay
  goal:         { base: 0.9, rate: 1.0 },    // never decay
  habit:        { base: 0.8, rate: 1.0 },    // never decay
  relationship: { base: 0.6, rate: 0.99 },   // ~69 day half-life
};

const MIN_IMPORTANCE = 0.01;

/**
 * Calculate the importance score for a claim type given its age in days.
 *
 * Formula: importance = base * rate^daysSinceUpdate, clamped to [0.01, base].
 *
 * @param {string} claimType - The claim type (e.g., 'event', 'rule', 'task')
 * @param {number} daysSinceUpdate - Age of the claim in days (can be fractional)
 * @returns {number} Importance score between 0.01 and the type's base importance
 */
export function calculateImportance(claimType, daysSinceUpdate) {
  const config = DECAY_RATES[claimType] ?? { base: 0.5, rate: 0.95 };
  const raw = config.base * Math.pow(config.rate, daysSinceUpdate);
  return Math.max(MIN_IMPORTANCE, raw);
}

/**
 * Apply importance decay to all active and candidate claims in the database.
 *
 * Reads each claim's updated_at, calculates days elapsed, and writes the
 * new importance_score. Runs synchronously.
 *
 * @param {import('../db/database.js').ContextDatabase} database
 * @returns {{ claimsDecayed: number }} Count of claims whose scores were updated
 */
export function applyDecayToAllClaims(database) {
  const now = new Date();

  const rows = database.prepare(`
    SELECT id, claim_type, updated_at
    FROM claims
    WHERE lifecycle_state IN ('active', 'candidate')
  `).all();

  const updateStmt = database.prepare(`
    UPDATE claims
    SET importance_score = ?
    WHERE id = ?
  `);

  let claimsDecayed = 0;

  for (const row of rows) {
    const updatedAt = new Date(row.updated_at);
    const msElapsed = now - updatedAt;
    const daysSinceUpdate = Math.max(0, msElapsed / (1000 * 60 * 60 * 24));
    const importance = calculateImportance(row.claim_type, daysSinceUpdate);

    updateStmt.run(importance, row.id);
    claimsDecayed++;
  }

  return { claimsDecayed };
}
