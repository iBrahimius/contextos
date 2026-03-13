import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  reciprocalRankFusion,
  applyCategoryBoosts,
  applyOriginPenalty,
  applySeedBonus,
  breakRRFTies,
  validateRRFInput,
} from '../src/core/rrf.js';

test('RRF - reciprocalRankFusion', async (t) => {
  await t.test('combines multiple ranked lists by RRF formula', () => {
    const graphResults = [
      { type: 'fact', id: 1, payload: { detail: 'high graph score' } },
      { type: 'decision', id: 2, payload: { detail: 'medium graph score' } },
      { type: 'fact', id: 3, payload: { detail: 'low graph score' } },
    ];

    const vectorResults = [
      { type: 'fact', id: 3, payload: { detail: 'high vector score' } },
      { type: 'fact', id: 1, payload: { detail: 'medium vector score' } },
      { type: 'message', id: 10, payload: { detail: 'low vector score' } },
    ];

    const result = reciprocalRankFusion([graphResults, vectorResults], 60);

    // fact:1 appears in both lists (rank 0 in graph, rank 1 in vector)
    // fact:3 appears in both lists (rank 2 in graph, rank 0 in vector)
    // decision:2 appears only in graph (rank 1)
    // message:10 appears only in vector (rank 2)

    assert.ok(result.length > 0, 'returns results');
    assert.ok(result[0].rrfScore > 0, 'RRF score is positive');
    assert.ok(result[0].score === result[0].rrfScore, 'score is set to rrfScore');

    // fact:1 should have highest RRF score (appears in both with early ranks)
    const fact1Result = result.find((r) => r.type === 'fact' && r.id === 1);
    assert.ok(fact1Result, 'fact:1 in results');
    assert.ok(fact1Result.rank <= 2, 'fact:1 ranks high');
  });

  await t.test('deduplicates items across lists, keeping richer metadata', () => {
    const list1 = [
      { type: 'fact', id: 1, payload: { detail: 'short' } },
    ];
    const list2 = [
      { type: 'fact', id: 1, payload: { detail: 'this is much longer detail' } },
    ];

    const result = reciprocalRankFusion([list1, list2], 60);

    assert.equal(result.length, 1, 'one deduplicated result');
    assert.ok(result[0].payload.detail.includes('longer'), 'keeps richer metadata');
  });

  await t.test('handles empty lists', () => {
    const result = reciprocalRankFusion([], 60);
    assert.equal(result.length, 0, 'empty input returns empty output');
  });

  await t.test('handles single list', () => {
    const singleList = [
      { type: 'fact', id: 1 },
      { type: 'fact', id: 2 },
    ];

    const result = reciprocalRankFusion([singleList], 60);

    assert.equal(result.length, 2, 'preserves all items');
    assert.equal(result[0].id, 1, 'maintains original order for single list');
    assert.equal(result[0].rank, 1, 'rank is set correctly');
  });

  await t.test('uses custom k parameter', () => {
    const list1 = [
      { type: 'fact', id: 1 },
      { type: 'fact', id: 2 },
    ];
    const list2 = [
      { type: 'fact', id: 2 },
      { type: 'fact', id: 1 },
    ];

    const resultK60 = reciprocalRankFusion([list1, list2], 60);
    const resultK20 = reciprocalRankFusion([list1, list2], 20);

    // fact:1 appears at rank 0 in list1 (score 1/61) and rank 1 in list2 (score 1/62)
    // fact:2 appears at rank 1 in list1 (score 1/62) and rank 0 in list2 (score 1/61)
    // Both have same RRF score. With k=20, the reciprocal differences are bigger numerically.
    const fact2K60 = resultK60.find((r) => r.id === 2);
    const fact2K20 = resultK20.find((r) => r.id === 2);

    // 1/21 + 1/22 vs 1/61 + 1/62 — k=20 produces higher absolute scores
    assert.ok(fact2K20.rrfScore > fact2K60.rrfScore, 'lower k produces higher scores due to larger reciprocals');
  });
});

test('RRF - applyCategoryBoosts', async (t) => {
  await t.test('applies category-based boosts to RRF scores', () => {
    const results = [
      { type: 'decision', id: 1, rrfScore: 0.1, score: 0.1 },
      { type: 'fact', id: 2, rrfScore: 0.1, score: 0.1 },
      { type: 'task', id: 3, rrfScore: 0.1, score: 0.1 },
    ];

    const boosted = applyCategoryBoosts(results);

    const decisionResult = boosted.find((r) => r.type === 'decision');
    const factResult = boosted.find((r) => r.type === 'fact');
    const taskResult = boosted.find((r) => r.type === 'task');

    assert.ok(decisionResult.score > factResult.score, 'decisions boosted more than facts');
    assert.ok(factResult.score > taskResult.score, 'facts boosted more than tasks');
    assert.ok(decisionResult.categoryBoost === 1.25, 'decision boost applied');
  });

  await t.test('allows custom boost overrides', () => {
    const results = [
      { type: 'custom', id: 1, rrfScore: 0.1, score: 0.1 },
    ];

    const boosted = applyCategoryBoosts(results, { custom: 2.0 });

    assert.equal(boosted[0].score, 0.2, 'custom boost applied');
  });
});

test('RRF - applyOriginPenalty', async (t) => {
  await t.test('reduces agent-generated content score', () => {
    const agentPayload = { origin: 'agent-system' };
    const userPayload = { origin: 'user-input' };

    const agentScore = applyOriginPenalty(1.0, agentPayload);
    const userScore = applyOriginPenalty(1.0, userPayload);

    assert.equal(agentScore, 0.85, 'agent origin penalized 15%');
    assert.equal(userScore, 1.0, 'user origin not penalized');
  });

  await t.test('handles missing origin gracefully', () => {
    const result = applyOriginPenalty(1.0, {});
    assert.equal(result, 1.0, 'no penalty for missing origin');
  });
});

test('RRF - applySeedBonus', async (t) => {
  await t.test('boosts results linked to seed entities', () => {
    const seedIds = new Set([100, 101]);
    const results = [
      { type: 'fact', id: 1, linkedEntityIds: [100], score: 0.1 },
      { type: 'fact', id: 2, linkedEntityIds: [999], score: 0.1 },
      { type: 'entity', id: 100, score: 0.1 },
    ];

    const boosted = applySeedBonus(results, seedIds);

    const linked = boosted.find((r) => r.id === 1);
    const unlinked = boosted.find((r) => r.id === 2);
    const entity = boosted.find((r) => r.type === 'entity' && r.id === 100);

    assert.ok(linked.seedBonus === 0.01, 'linked result gets bonus');
    assert.ok(!unlinked.seedBonus, 'unlinked result no bonus');
    assert.ok(entity.seedBonus === 0.01, 'seed entity itself gets bonus');
  });

  await t.test('handles empty seed set', () => {
    const results = [{ type: 'fact', id: 1, score: 0.1 }];
    const boosted = applySeedBonus(results, new Set());

    assert.equal(boosted[0].score, 0.1, 'no change with empty seed set');
  });
});

test('RRF - breakRRFTies', async (t) => {
  await t.test('breaks ties by timestamp (newer first)', () => {
    const results = [
      {
        type: 'fact',
        id: 1,
        rrfScore: 0.1,
        timestamp: '2026-03-09T10:00:00Z',
        rank: 1,
      },
      {
        type: 'fact',
        id: 2,
        rrfScore: 0.1,
        timestamp: '2026-03-09T09:00:00Z',
        rank: 2,
      },
    ];

    const sorted = breakRRFTies(results);

    assert.equal(sorted[0].id, 1, 'newer item ranks first');
    assert.equal(sorted[1].id, 2, 'older item ranks second');
    assert.equal(sorted[0].rank, 1, 'ranks updated after sorting');
  });

  await t.test('breaks ties by type priority', () => {
    const results = [
      { type: 'message', id: 1, rrfScore: 0.1, rank: 1 },
      { type: 'decision', id: 2, rrfScore: 0.1, rank: 2 },
      { type: 'fact', id: 3, rrfScore: 0.1, rank: 3 },
    ];

    const sorted = breakRRFTies(results);

    assert.equal(sorted[0].type, 'decision', 'decision ranked first by type');
    assert.equal(sorted[1].type, 'fact', 'fact ranked second by type');
    assert.equal(sorted[2].type, 'message', 'message ranked last by type');
  });
});

test('RRF - validateRRFInput', async (t) => {
  await t.test('filters out invalid items (missing type or id)', () => {
    const lists = [
      [
        { type: 'fact', id: 1 },
        { id: 2 }, // missing type
        { type: 'fact' }, // missing id
      ],
      [
        { type: 'message', id: 3 },
      ],
    ];

    const validated = validateRRFInput(lists);

    assert.equal(validated[0].length, 1, 'invalid items filtered from first list');
    assert.equal(validated[1].length, 1, 'second list unchanged');
  });

  await t.test('removes empty lists after filtering', () => {
    const lists = [
      [{ id: 1 }, { id: 2 }], // all invalid
      [{ type: 'fact', id: 3 }],
    ];

    const validated = validateRRFInput(lists);

    assert.equal(validated.length, 1, 'empty list removed');
    assert.equal(validated[0][0].id, 3, 'valid items preserved');
  });

  await t.test('allows id: 0 (falsy but valid)', () => {
    const lists = [
      [{ type: 'fact', id: 0 }],
    ];

    const validated = validateRRFInput(lists);

    assert.equal(validated[0].length, 1, 'id: 0 is valid');
  });
});

test('RRF - integration: full pipeline', async (t) => {
  await t.test('applies RRF + boosts + tie-breaking in sequence', () => {
    const graphResults = [
      { type: 'decision', id: 1, linkedEntityIds: [100], payload: { origin: 'user' } },
      { type: 'fact', id: 2 },
    ];

    const vectorResults = [
      { type: 'fact', id: 2 },
      { type: 'decision', id: 1 },
    ];

    const seedIds = new Set([100]);

    // Step 1: RRF
    let results = reciprocalRankFusion(
      [graphResults, vectorResults],
      60
    );

    // Step 2: Category boosts
    results = applyCategoryBoosts(results);

    // Step 3: Seed bonus
    results = applySeedBonus(results, seedIds);

    // Step 4: Tie-breaking
    results = breakRRFTies(results);

    assert.ok(results.length > 0, 'final results exist');
    const decision = results.find((r) => r.type === 'decision');
    const fact = results.find((r) => r.type === 'fact');

    // Decision should rank higher due to category boost
    assert.ok(decision.rank < fact.rank, 'decision ranks higher than fact');

    // Decision should have seed bonus (linked to entity 100)
    assert.ok(decision.seedBonus > 0, 'seed bonus applied');
  });
});
