/**
 * Claim type taxonomy and state machine definitions for ContextOS v2.1.
 *
 * Each claim type has metadata (description, TTL hint, whether it has a state machine)
 * and state machine types define valid transitions.
 *
 * @module claim-types
 */

// ── Claim Type Registry ─────────────────────────────────────────────

export const CLAIM_TYPES = {
  fact:         { description: 'Factual assertion',                    hasStateMachine: false, ttlDays: 90,   immutable: false },
  decision:     { description: 'A choice that was made',               hasStateMachine: true,  ttlDays: null, immutable: false },
  task:         { description: 'Something to do',                      hasStateMachine: true,  ttlDays: 90,   immutable: false },
  constraint:   { description: 'A limitation or boundary',             hasStateMachine: true,  ttlDays: null, immutable: false },
  preference:   { description: 'User preference or chosen approach',   hasStateMachine: true,  ttlDays: 180,  immutable: false },
  goal:         { description: 'Something being worked toward',        hasStateMachine: true,  ttlDays: 365,  immutable: false },
  habit:        { description: 'Recurring behavior or pattern',        hasStateMachine: false, ttlDays: 365,  immutable: false },
  rule:         { description: 'Hard operational constraint',          hasStateMachine: true,  ttlDays: null, immutable: false },
  event:        { description: 'One-off occurrence',                   hasStateMachine: false, ttlDays: 14,   immutable: true  },
  state_change: { description: 'A transition that happened',           hasStateMachine: false, ttlDays: 30,   immutable: true  },
  relationship: { description: 'An edge between entities',             hasStateMachine: false, ttlDays: null, immutable: false },
};

export const CLAIM_TYPE_VALUES = Object.keys(CLAIM_TYPES);

// ── Lifecycle States ────────────────────────────────────────────────

export const LIFECYCLE_STATES = ['candidate', 'active', 'superseded', 'disputed', 'archived'];

// ── Source Types ─────────────────────────────────────────────────────

export const SOURCE_TYPES = ['explicit', 'implicit', 'inference', 'derived'];

// ── State Machine Definitions ───────────────────────────────────────
//
// Keys are claim types. Values map fromState → [valid toStates].
// Only claim types with hasStateMachine: true are listed here.
// Types without state machines use lifecycle_state only (candidate/active/superseded/disputed/archived).

export const STATE_MACHINES = {
  task: {
    pending:   ['active', 'cancelled'],
    active:    ['blocked', 'done', 'cancelled'],
    blocked:   ['active', 'cancelled'],
    done:      [],  // terminal
    cancelled: [],  // terminal
  },

  decision: {
    proposed:   ['accepted', 'rejected', 'deferred'],
    accepted:   ['superseded'],
    rejected:   ['superseded'],
    deferred:   ['proposed', 'superseded'],
    superseded: [],  // terminal
  },

  constraint: {
    active:     ['expired', 'overridden'],
    expired:    [],  // terminal
    overridden: [],  // terminal
  },

  rule: {
    active:     ['expired', 'overridden'],
    expired:    [],  // terminal
    overridden: [],  // terminal
  },

  preference: {
    active:     ['superseded'],
    superseded: [],  // terminal
  },

  goal: {
    active:    ['on_hold', 'completed', 'abandoned'],
    on_hold:   ['active', 'abandoned'],
    completed: [],  // terminal
    abandoned: [],  // terminal
  },
};

// ── Initial States ──────────────────────────────────────────────────
// Default value_text state for claim types with state machines.

export const INITIAL_STATES = {
  task:       'pending',
  decision:   'proposed',
  constraint: 'active',
  rule:       'active',
  preference: 'active',
  goal:       'active',
};

// ── Validation Functions ────────────────────────────────────────────

/**
 * Check if a claim type is valid.
 * @param {string} claimType
 * @returns {boolean}
 */
export function isValidClaimType(claimType) {
  return claimType in CLAIM_TYPES;
}

/**
 * Check if a lifecycle state is valid.
 * @param {string} state
 * @returns {boolean}
 */
export function isValidLifecycleState(state) {
  return LIFECYCLE_STATES.includes(state);
}

/**
 * Check if a source type is valid.
 * @param {string} sourceType
 * @returns {boolean}
 */
export function isValidSourceType(sourceType) {
  return SOURCE_TYPES.includes(sourceType);
}

/**
 * Validate a state machine transition for an operational claim type.
 *
 * Returns true if the transition is valid, false if not.
 * Returns true for claim types without state machines (they don't constrain value_text).
 *
 * @param {string} claimType - The claim type (e.g., 'task', 'decision')
 * @param {string} fromState - Current value_text state
 * @param {string} toState   - Desired new state
 * @returns {boolean}
 */
export function validateTransition(claimType, fromState, toState) {
  const machine = STATE_MACHINES[claimType];
  if (!machine) return true;  // no state machine → no constraint

  const validTargets = machine[fromState];
  if (!validTargets) return false;  // fromState not recognized → invalid

  return validTargets.includes(toState);
}

/**
 * Get valid transitions from a given state for a claim type.
 *
 * Returns empty array for terminal states or claim types without state machines.
 *
 * @param {string} claimType - The claim type
 * @param {string} currentState - Current value_text state
 * @returns {string[]} Array of valid target states
 */
export function getValidTransitions(claimType, currentState) {
  const machine = STATE_MACHINES[claimType];
  if (!machine) return [];

  return machine[currentState] ?? [];
}

/**
 * Check if a state is terminal (no valid transitions out).
 *
 * @param {string} claimType
 * @param {string} state
 * @returns {boolean}
 */
export function isTerminalState(claimType, state) {
  const machine = STATE_MACHINES[claimType];
  if (!machine) return false;

  const validTargets = machine[state];
  return Array.isArray(validTargets) && validTargets.length === 0;
}

/**
 * Get the initial state for a claim type with a state machine.
 *
 * @param {string} claimType
 * @returns {string|null} Initial state or null if no state machine
 */
export function getInitialState(claimType) {
  return INITIAL_STATES[claimType] ?? null;
}

/**
 * Check if a claim type is immutable (events, state_changes — record-only, never modified).
 *
 * @param {string} claimType
 * @returns {boolean}
 */
export function isImmutableType(claimType) {
  return CLAIM_TYPES[claimType]?.immutable === true;
}

/**
 * Get all valid value_text states for a claim type's state machine.
 *
 * @param {string} claimType
 * @returns {string[]} All states, or empty if no state machine
 */
export function getAllStates(claimType) {
  const machine = STATE_MACHINES[claimType];
  if (!machine) return [];
  return Object.keys(machine);
}

/**
 * Map an observation category to a claim type.
 * Falls back to 'fact' for unrecognized categories.
 *
 * @param {string} category - Observation category from Haiku extraction
 * @returns {string} Claim type
 */
export function mapObservationCategory(category) {
  const mapping = {
    fact:         'fact',
    decision:     'decision',
    task:         'task',
    constraint:   'constraint',
    relationship: 'relationship',
    // Extended mappings for categories that might appear
    preference:   'preference',
    goal:         'goal',
    habit:        'habit',
    rule:         'rule',
    event:        'event',
    state_change: 'state_change',
  };

  return mapping[category?.toLowerCase()] ?? 'fact';
}
