import test from "node:test";
import assert from "node:assert/strict";

import { DECAY_RATES, calculateImportance } from "../src/core/importance-decay.js";

// ── calculateImportance ──────────────────────────────────────────────

test("calculateImportance — event decays fast (~5 day half-life)", () => {
  const at0 = calculateImportance("event", 0);
  const at5 = calculateImportance("event", 5);
  const at10 = calculateImportance("event", 10);

  // At day 0, score = base = 0.5
  assert.ok(Math.abs(at0 - 0.5) < 0.001, `expected 0.5, got ${at0}`);
  // At day 5, roughly half of base (~0.25)
  assert.ok(at5 < at0 * 0.6, `expected significant decay by day 5, got ${at5}`);
  // At day 10, more decayed than day 5
  assert.ok(at10 < at5, `expected more decay at day 10 vs day 5`);
});

test("calculateImportance — state_change decays same as event", () => {
  const eventScore = calculateImportance("event", 5);
  const stateScore = calculateImportance("state_change", 5);
  assert.equal(eventScore, stateScore);
});

test("calculateImportance — fact decays moderately", () => {
  const at0 = calculateImportance("fact", 0);
  const at23 = calculateImportance("fact", 23);
  // Base 0.7, half-life ~23 days
  assert.ok(Math.abs(at0 - 0.7) < 0.001);
  assert.ok(at23 < at0 * 0.6, `expected ~half at day 23, got ${at23}`);
});

test("calculateImportance — task decays slowly", () => {
  const at0 = calculateImportance("task", 0);
  const at35 = calculateImportance("task", 35);
  assert.ok(Math.abs(at0 - 0.8) < 0.001);
  assert.ok(at35 < at0 * 0.6, `expected ~half at day 35`);
});

test("calculateImportance — decision has long tail", () => {
  const at0 = calculateImportance("decision", 0);
  const at69 = calculateImportance("decision", 69);
  assert.ok(Math.abs(at0 - 0.8) < 0.001);
  assert.ok(at69 < at0 * 0.6, `expected ~half at day 69`);
});

test("calculateImportance — rule never decays", () => {
  const at0 = calculateImportance("rule", 0);
  const at365 = calculateImportance("rule", 365);
  const at1000 = calculateImportance("rule", 1000);

  assert.equal(at0, DECAY_RATES.rule.base);
  assert.equal(at365, DECAY_RATES.rule.base);
  assert.equal(at1000, DECAY_RATES.rule.base);
});

test("calculateImportance — goal never decays", () => {
  const at0 = calculateImportance("goal", 0);
  const at365 = calculateImportance("goal", 365);

  assert.equal(at0, DECAY_RATES.goal.base);
  assert.equal(at365, DECAY_RATES.goal.base);
});

test("calculateImportance — habit never decays", () => {
  const at0 = calculateImportance("habit", 0);
  const at365 = calculateImportance("habit", 365);

  assert.equal(at0, DECAY_RATES.habit.base);
  assert.equal(at365, DECAY_RATES.habit.base);
});

test("calculateImportance — constraint decays very slowly", () => {
  const at0 = calculateImportance("constraint", 0);
  const at139 = calculateImportance("constraint", 139);
  assert.ok(Math.abs(at0 - 0.9) < 0.001);
  assert.ok(at139 < at0 * 0.6, `expected ~half at day 139`);
});

test("calculateImportance — preference decays very slowly", () => {
  const at0 = calculateImportance("preference", 0);
  const at347 = calculateImportance("preference", 347);
  assert.ok(Math.abs(at0 - 0.7) < 0.001);
  assert.ok(at347 < at0 * 0.6, `expected ~half at day 347`);
});

test("calculateImportance — relationship decays moderately", () => {
  const at0 = calculateImportance("relationship", 0);
  assert.ok(Math.abs(at0 - 0.6) < 0.001);
  // Should decay over time
  const at100 = calculateImportance("relationship", 100);
  assert.ok(at100 < at0);
});

test("calculateImportance — importance never drops below 0.01", () => {
  const types = Object.keys(DECAY_RATES);
  for (const type of types) {
    const score = calculateImportance(type, 10000); // extreme age
    assert.ok(score >= 0.01, `${type} at day 10000 should be >= 0.01, got ${score}`);
  }
});

test("calculateImportance — unknown type uses fallback defaults", () => {
  const score = calculateImportance("unknown_type", 0);
  // Fallback: base 0.5, rate 0.95
  assert.ok(Math.abs(score - 0.5) < 0.001, `expected 0.5, got ${score}`);
});

test("calculateImportance — all defined types have entries in DECAY_RATES", () => {
  const expectedTypes = [
    "event", "state_change", "fact", "task", "decision",
    "constraint", "preference", "rule", "goal", "habit", "relationship",
  ];
  for (const type of expectedTypes) {
    assert.ok(type in DECAY_RATES, `missing DECAY_RATES entry for: ${type}`);
  }
});
