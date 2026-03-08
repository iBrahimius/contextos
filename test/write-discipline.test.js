import test from "node:test";
import assert from "node:assert/strict";

import { WRITE_CLASS_RULES, classifyWriteClass } from "../src/core/write-discipline.js";

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
