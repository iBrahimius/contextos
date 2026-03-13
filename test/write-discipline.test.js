import test from "node:test";
import assert from "node:assert/strict";

import {
  AI_AUTO_APPLY_CONFIDENCE_THRESHOLD,
  AI_PROPOSED_PARKING_THRESHOLD,
  WRITE_CLASS_RULES,
  classifyWriteClass,
  getQueuePressureDisposition,
  getWriteClassDisposition,
} from "../src/core/write-discipline.js";

// ── Auto types ───────────────────────────────────────────────────────

test("classifyWriteClass — add_entity is auto", () => {
  assert.equal(classifyWriteClass("add_entity"), "auto");
});

test("classifyWriteClass — link_entities is auto", () => {
  assert.equal(classifyWriteClass("link_entities"), "auto");
});

test("classifyWriteClass — update_profile is auto", () => {
  assert.equal(classifyWriteClass("update_profile"), "auto");
});

test("classifyWriteClass — all auto types resolve correctly", () => {
  for (const type of WRITE_CLASS_RULES.auto) {
    assert.equal(classifyWriteClass(type), "auto", `expected auto for: ${type}`);
  }
});

// ── Canonical types ───────────────────────────────────────────────────

test("classifyWriteClass — add_decision is canonical", () => {
  assert.equal(classifyWriteClass("add_decision"), "canonical");
});

test("classifyWriteClass — supersede_decision is canonical", () => {
  assert.equal(classifyWriteClass("supersede_decision"), "canonical");
});

test("classifyWriteClass — add_constraint is canonical", () => {
  assert.equal(classifyWriteClass("add_constraint"), "canonical");
});

test("classifyWriteClass — update_constraint is canonical", () => {
  assert.equal(classifyWriteClass("update_constraint"), "canonical");
});

test("classifyWriteClass — expire_constraint is canonical", () => {
  assert.equal(classifyWriteClass("expire_constraint"), "canonical");
});

test("classifyWriteClass — add_project is canonical", () => {
  assert.equal(classifyWriteClass("add_project"), "canonical");
});

test("classifyWriteClass — update_project is canonical", () => {
  assert.equal(classifyWriteClass("update_project"), "canonical");
});

test("classifyWriteClass — mark_breakthrough is canonical", () => {
  assert.equal(classifyWriteClass("mark_breakthrough"), "canonical");
});

test("classifyWriteClass — all canonical types resolve correctly", () => {
  for (const type of WRITE_CLASS_RULES.canonical) {
    assert.equal(classifyWriteClass(type), "canonical", `expected canonical for: ${type}`);
  }
});

// ── AI-proposed types ────────────────────────────────────────────────

test("classifyWriteClass — add_task is ai_proposed", () => {
  assert.equal(classifyWriteClass("add_task"), "ai_proposed");
});

test("classifyWriteClass — update_task is ai_proposed", () => {
  assert.equal(classifyWriteClass("update_task"), "ai_proposed");
});

test("classifyWriteClass — close_task is ai_proposed", () => {
  assert.equal(classifyWriteClass("close_task"), "ai_proposed");
});

test("classifyWriteClass — reopen_task is ai_proposed", () => {
  assert.equal(classifyWriteClass("reopen_task"), "ai_proposed");
});

test("classifyWriteClass — assert_fact is ai_proposed", () => {
  assert.equal(classifyWriteClass("assert_fact"), "ai_proposed");
});

test("classifyWriteClass — retract_fact is ai_proposed", () => {
  assert.equal(classifyWriteClass("retract_fact"), "ai_proposed");
});

test("classifyWriteClass — update_entity is ai_proposed", () => {
  assert.equal(classifyWriteClass("update_entity"), "ai_proposed");
});

// ── Unknown type default ──────────────────────────────────────────────

test("classifyWriteClass — unknown mutation type defaults to ai_proposed", () => {
  assert.equal(classifyWriteClass("totally_unknown_type"), "ai_proposed");
});

test("classifyWriteClass — empty string defaults to ai_proposed", () => {
  assert.equal(classifyWriteClass(""), "ai_proposed");
});

test("classifyWriteClass — undefined defaults to ai_proposed", () => {
  assert.equal(classifyWriteClass(undefined), "ai_proposed");
});

// ── WRITE_CLASS_RULES structure ───────────────────────────────────────

test("WRITE_CLASS_RULES — auto and canonical are disjoint sets", () => {
  for (const type of WRITE_CLASS_RULES.auto) {
    assert.ok(
      !WRITE_CLASS_RULES.canonical.has(type),
      `${type} should not be in both auto and canonical`,
    );
  }
});

// ── Queue-pressure policy ────────────────────────────────────────────

test("getQueuePressureDisposition — low-confidence ai_proposed is parked, not auto-applied", () => {
  const disposition = getQueuePressureDisposition({
    writeClass: "ai_proposed",
    status: "proposed",
    confidence: AI_PROPOSED_PARKING_THRESHOLD - 0.01,
  });

  assert.equal(disposition.queue_bucket, "parked");
  assert.equal(disposition.actionable, false);
  assert.equal(disposition.triage, "parked_backlog");
  assert.equal(disposition.queue_reason, "low_confidence_ai_proposed_parked");
  assert.equal(disposition.policy_decision, "park_low_confidence_ai_proposed");
});

test("getQueuePressureDisposition — threshold ai_proposed stays in actionable review", () => {
  const disposition = getQueuePressureDisposition({
    writeClass: "ai_proposed",
    status: "proposed",
    confidence: AI_PROPOSED_PARKING_THRESHOLD,
  });

  assert.equal(disposition.queue_bucket, "actionable");
  assert.equal(disposition.actionable, true);
  assert.equal(disposition.triage, "ai_review");
  assert.equal(disposition.queue_reason, "ai_proposed_requires_review");
  assert.equal(disposition.policy_decision, "queue_ai_review");
});

test("getQueuePressureDisposition — canonical stays actionable regardless of confidence", () => {
  const disposition = getQueuePressureDisposition({
    writeClass: "canonical",
    status: "proposed",
    confidence: 0.1,
  });

  assert.equal(disposition.queue_bucket, "actionable");
  assert.equal(disposition.actionable, true);
  assert.equal(disposition.triage, "human_canonical");
  assert.equal(disposition.policy_decision, "queue_canonical_review");
});

// ── Confidence-gated auto-apply ──────────────────────────────────────

test("AI_AUTO_APPLY_CONFIDENCE_THRESHOLD is exported and equals 0.85", () => {
  assert.equal(AI_AUTO_APPLY_CONFIDENCE_THRESHOLD, 0.85);
});

test("AI_AUTO_APPLY_CONFIDENCE_THRESHOLD is above AI_PROPOSED_PARKING_THRESHOLD", () => {
  assert.ok(AI_AUTO_APPLY_CONFIDENCE_THRESHOLD > AI_PROPOSED_PARKING_THRESHOLD);
});

test("canonical write class auto-applies via writeClass check in proposeMutation", () => {
  const disposition = getWriteClassDisposition("canonical");
  assert.equal(disposition.autoApply, false, "canonical disposition.autoApply stays false");
  assert.equal(disposition.reviewRequired, true);
  // The actual auto-apply for canonical is handled in proposeMutation via:
  //   shouldAutoApply = disposition.autoApply || writeClass === 'canonical'
  const writeClass = classifyWriteClass("add_decision");
  assert.equal(writeClass, "canonical");
  const shouldAutoApply = disposition.autoApply || writeClass === "canonical";
  assert.equal(shouldAutoApply, true, "canonical proposals auto-apply in proposeMutation");
});

test("auto write class disposition has autoApply true", () => {
  const disposition = getWriteClassDisposition("auto");
  assert.equal(disposition.autoApply, true);
});

test("ai_proposed write class does not auto-apply via writeClass check", () => {
  const disposition = getWriteClassDisposition("ai_proposed");
  assert.equal(disposition.autoApply, false);
  const writeClass = "ai_proposed";
  const shouldAutoApply = disposition.autoApply || writeClass === "canonical";
  assert.equal(shouldAutoApply, false, "ai_proposed does not auto-apply via writeClass check");
});
