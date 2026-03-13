import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  CLAIM_TYPES,
  CLAIM_TYPE_VALUES,
  LIFECYCLE_STATES,
  SOURCE_TYPES,
  isValidClaimType,
  isValidLifecycleState,
  isValidSourceType,
  validateTransition,
  getValidTransitions,
  isTerminalState,
  getInitialState,
  isImmutableType,
  getAllStates,
  mapObservationCategory,
} from '../src/core/claim-types.js';

describe('Claim Types Registry', () => {
  it('defines exactly 11 claim types', () => {
    assert.equal(CLAIM_TYPE_VALUES.length, 11);
  });

  it('all expected types are present', () => {
    const expected = [
      'fact', 'decision', 'task', 'constraint', 'preference',
      'goal', 'habit', 'rule', 'event', 'state_change', 'relationship',
    ];
    for (const t of expected) {
      assert.ok(isValidClaimType(t), `missing type: ${t}`);
    }
  });

  it('rejects invalid claim types', () => {
    assert.equal(isValidClaimType('banana'), false);
    assert.equal(isValidClaimType(''), false);
    assert.equal(isValidClaimType(null), false);
  });

  it('each type has description and ttlDays', () => {
    for (const [key, meta] of Object.entries(CLAIM_TYPES)) {
      assert.ok(typeof meta.description === 'string', `${key} missing description`);
      assert.ok(meta.ttlDays === null || typeof meta.ttlDays === 'number', `${key} bad ttlDays`);
    }
  });
});

describe('Lifecycle States', () => {
  it('defines 5 lifecycle states', () => {
    assert.equal(LIFECYCLE_STATES.length, 5);
  });

  it('validates known states', () => {
    for (const s of ['candidate', 'active', 'superseded', 'disputed', 'archived']) {
      assert.ok(isValidLifecycleState(s));
    }
  });

  it('rejects unknown states', () => {
    assert.equal(isValidLifecycleState('deleted'), false);
    assert.equal(isValidLifecycleState(''), false);
  });
});

describe('Source Types', () => {
  it('defines 4 source types', () => {
    assert.equal(SOURCE_TYPES.length, 4);
  });

  it('validates known source types', () => {
    for (const s of ['explicit', 'implicit', 'inference', 'derived']) {
      assert.ok(isValidSourceType(s));
    }
  });

  it('rejects unknown source types', () => {
    assert.equal(isValidSourceType('guess'), false);
  });
});

describe('Task State Machine', () => {
  it('allows pending → active', () => {
    assert.ok(validateTransition('task', 'pending', 'active'));
  });

  it('allows active → blocked', () => {
    assert.ok(validateTransition('task', 'active', 'blocked'));
  });

  it('allows blocked → active (unblock)', () => {
    assert.ok(validateTransition('task', 'blocked', 'active'));
  });

  it('allows active → done', () => {
    assert.ok(validateTransition('task', 'active', 'done'));
  });

  it('allows pending → cancelled', () => {
    assert.ok(validateTransition('task', 'pending', 'cancelled'));
  });

  it('allows active → cancelled', () => {
    assert.ok(validateTransition('task', 'active', 'cancelled'));
  });

  it('allows blocked → cancelled', () => {
    assert.ok(validateTransition('task', 'blocked', 'cancelled'));
  });

  it('rejects blocked → pending (invalid)', () => {
    assert.equal(validateTransition('task', 'blocked', 'pending'), false);
  });

  it('rejects done → active (terminal)', () => {
    assert.equal(validateTransition('task', 'done', 'active'), false);
  });

  it('rejects cancelled → active (terminal)', () => {
    assert.equal(validateTransition('task', 'cancelled', 'active'), false);
  });

  it('done is terminal', () => {
    assert.ok(isTerminalState('task', 'done'));
  });

  it('cancelled is terminal', () => {
    assert.ok(isTerminalState('task', 'cancelled'));
  });

  it('active is not terminal', () => {
    assert.equal(isTerminalState('task', 'active'), false);
  });

  it('initial state is pending', () => {
    assert.equal(getInitialState('task'), 'pending');
  });
});

describe('Decision State Machine', () => {
  it('allows proposed → accepted', () => {
    assert.ok(validateTransition('decision', 'proposed', 'accepted'));
  });

  it('allows proposed → rejected', () => {
    assert.ok(validateTransition('decision', 'proposed', 'rejected'));
  });

  it('allows proposed → deferred', () => {
    assert.ok(validateTransition('decision', 'proposed', 'deferred'));
  });

  it('allows deferred → proposed (revisit)', () => {
    assert.ok(validateTransition('decision', 'deferred', 'proposed'));
  });

  it('allows accepted → superseded', () => {
    assert.ok(validateTransition('decision', 'accepted', 'superseded'));
  });

  it('rejects accepted → rejected (not valid)', () => {
    assert.equal(validateTransition('decision', 'accepted', 'rejected'), false);
  });

  it('superseded is terminal', () => {
    assert.ok(isTerminalState('decision', 'superseded'));
  });

  it('initial state is proposed', () => {
    assert.equal(getInitialState('decision'), 'proposed');
  });
});

describe('Rule/Constraint State Machine', () => {
  for (const type of ['rule', 'constraint']) {
    it(`${type}: allows active → expired`, () => {
      assert.ok(validateTransition(type, 'active', 'expired'));
    });

    it(`${type}: allows active → overridden`, () => {
      assert.ok(validateTransition(type, 'active', 'overridden'));
    });

    it(`${type}: expired is terminal`, () => {
      assert.ok(isTerminalState(type, 'expired'));
    });

    it(`${type}: overridden is terminal`, () => {
      assert.ok(isTerminalState(type, 'overridden'));
    });

    it(`${type}: initial state is active`, () => {
      assert.equal(getInitialState(type), 'active');
    });
  }
});

describe('Goal State Machine', () => {
  it('allows active → on_hold', () => {
    assert.ok(validateTransition('goal', 'active', 'on_hold'));
  });

  it('allows on_hold → active (resume)', () => {
    assert.ok(validateTransition('goal', 'on_hold', 'active'));
  });

  it('allows active → completed', () => {
    assert.ok(validateTransition('goal', 'active', 'completed'));
  });

  it('allows active → abandoned', () => {
    assert.ok(validateTransition('goal', 'active', 'abandoned'));
  });

  it('allows on_hold → abandoned', () => {
    assert.ok(validateTransition('goal', 'on_hold', 'abandoned'));
  });

  it('rejects completed → active (terminal)', () => {
    assert.equal(validateTransition('goal', 'completed', 'active'), false);
  });

  it('initial state is active', () => {
    assert.equal(getInitialState('goal'), 'active');
  });
});

describe('Preference State Machine', () => {
  it('allows active → superseded', () => {
    assert.ok(validateTransition('preference', 'active', 'superseded'));
  });

  it('superseded is terminal', () => {
    assert.ok(isTerminalState('preference', 'superseded'));
  });

  it('initial state is active', () => {
    assert.equal(getInitialState('preference'), 'active');
  });
});

describe('Types Without State Machines', () => {
  for (const type of ['fact', 'habit', 'event', 'state_change', 'relationship']) {
    it(`${type}: validateTransition always returns true`, () => {
      assert.ok(validateTransition(type, 'anything', 'whatever'));
    });

    it(`${type}: getValidTransitions returns empty`, () => {
      assert.deepEqual(getValidTransitions(type, 'any'), []);
    });

    it(`${type}: getInitialState returns null`, () => {
      assert.equal(getInitialState(type), null);
    });
  }
});

describe('Immutable Types', () => {
  it('event is immutable', () => {
    assert.ok(isImmutableType('event'));
  });

  it('state_change is immutable', () => {
    assert.ok(isImmutableType('state_change'));
  });

  it('fact is not immutable', () => {
    assert.equal(isImmutableType('fact'), false);
  });

  it('task is not immutable', () => {
    assert.equal(isImmutableType('task'), false);
  });
});

describe('getAllStates', () => {
  it('task has 5 states', () => {
    assert.equal(getAllStates('task').length, 5);
  });

  it('decision has 5 states', () => {
    assert.equal(getAllStates('decision').length, 5);
  });

  it('fact has 0 states (no FSM)', () => {
    assert.equal(getAllStates('fact').length, 0);
  });
});

describe('mapObservationCategory', () => {
  it('maps known categories directly', () => {
    assert.equal(mapObservationCategory('task'), 'task');
    assert.equal(mapObservationCategory('decision'), 'decision');
    assert.equal(mapObservationCategory('fact'), 'fact');
    assert.equal(mapObservationCategory('constraint'), 'constraint');
    assert.equal(mapObservationCategory('relationship'), 'relationship');
  });

  it('maps extended categories', () => {
    assert.equal(mapObservationCategory('preference'), 'preference');
    assert.equal(mapObservationCategory('goal'), 'goal');
    assert.equal(mapObservationCategory('rule'), 'rule');
  });

  it('falls back to fact for unknown categories', () => {
    assert.equal(mapObservationCategory('banana'), 'fact');
    assert.equal(mapObservationCategory(undefined), 'fact');
    assert.equal(mapObservationCategory(null), 'fact');
  });

  it('handles case insensitivity', () => {
    assert.equal(mapObservationCategory('TASK'), 'task');
    assert.equal(mapObservationCategory('Decision'), 'decision');
  });
});

describe('getValidTransitions', () => {
  it('returns valid targets for task:active', () => {
    const targets = getValidTransitions('task', 'active');
    assert.deepEqual(targets.sort(), ['blocked', 'cancelled', 'done']);
  });

  it('returns empty for terminal states', () => {
    assert.deepEqual(getValidTransitions('task', 'done'), []);
  });

  it('returns empty for unknown state', () => {
    assert.deepEqual(getValidTransitions('task', 'nonexistent'), []);
  });
});
