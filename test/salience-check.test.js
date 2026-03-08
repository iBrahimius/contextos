import test from "node:test";
import assert from "node:assert/strict";
import { checkSalience, getSalienceAlert } from "../src/core/salience-check.js";

test("checkSalience — task blocked triggers high salience", () => {
  const mutation = {
    type: "update_task",
    payload: {
      id: "task1",
      title: "Test Task",
      lifecycle_state: "blocked",
    },
  };

  const result = checkSalience(mutation);
  assert.equal(result.salience, "high");
  assert.equal(result.type, "task_blocked");
});

test("checkSalience — new high severity constraint triggers high salience", () => {
  const mutation = {
    type: "add_constraint",
    payload: {
      id: "const1",
      label: "Critical Rule",
      severity: "high",
    },
  };

  const result = checkSalience(mutation);
  assert.equal(result.salience, "high");
  assert.equal(result.type, "constraint_created");
});

test("checkSalience — new critical severity constraint triggers high salience", () => {
  const mutation = {
    type: "add_constraint",
    payload: {
      id: "const2",
      label: "Critical Rule",
      severity: "critical",
    },
  };

  const result = checkSalience(mutation);
  assert.equal(result.salience, "high");
  assert.equal(result.type, "constraint_created");
});

test("checkSalience — new disputed claim triggers medium salience", () => {
  const mutation = {
    type: "add_claim",
    payload: {
      subject_entity_id: "ent1",
      lifecycle_state: "disputed",
    },
  };

  const result = checkSalience(mutation);
  assert.equal(result.salience, "medium");
  assert.equal(result.type, "disputed_claim");
});

test("checkSalience — decision superseded triggers medium salience", () => {
  const mutation = {
    type: "supersede_decision",
    payload: {
      decision_id: "dec1",
    },
  };

  const result = checkSalience(mutation);
  assert.equal(result.salience, "medium");
  assert.equal(result.type, "decision_superseded");
});

test("checkSalience — task approaching deadline triggers high salience", () => {
  const tomorrow = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();

  const mutation = {
    type: "add_task",
    payload: {
      id: "task2",
      title: "Urgent Task",
      deadline: tomorrow,
    },
  };

  const result = checkSalience(mutation);
  assert.equal(result.salience, "high");
  assert.equal(result.type, "task_deadline_approaching");
});

test("checkSalience — task far in future does not trigger salience", () => {
  const nextMonth = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString();

  const mutation = {
    type: "add_task",
    payload: {
      id: "task3",
      title: "Future Task",
      deadline: nextMonth,
    },
  };

  const result = checkSalience(mutation);
  assert.equal(result, null);
});

test("checkSalience — claim on focused entity triggers low salience", () => {
  const mutation = {
    type: "add_claim",
    payload: {
      subject_entity_id: "ent1",
    },
  };

  const context = {
    focusedEntityId: "ent1",
    config: { salience: { enableLowOnFocused: true } },
  };

  const result = checkSalience(mutation, context);
  assert.equal(result.salience, "low");
  assert.equal(result.type, "claim_on_focused_entity");
});

test("checkSalience — low salience can be disabled via config", () => {
  const mutation = {
    type: "add_claim",
    payload: {
      subject_entity_id: "ent1",
    },
  };

  const context = {
    focusedEntityId: "ent1",
    config: { salience: { enableLowOnFocused: false } },
  };

  const result = checkSalience(mutation, context);
  assert.equal(result, null);
});

test("checkSalience — returns null for non-triggering mutations", () => {
  const mutation = {
    type: "add_entity",
    payload: {
      id: "ent2",
      label: "New Entity",
    },
  };

  const result = checkSalience(mutation);
  assert.equal(result, null);
});

test("getSalienceAlert — formats result as alert object", () => {
  const mutation = {
    id: "mut1",
    type: "update_task",
    payload: {
      id: "task1",
      title: "Test Task",
      lifecycle_state: "blocked",
    },
  };

  const alert = getSalienceAlert(mutation);
  assert.equal(alert.type, "task_blocked");
  assert.equal(alert.salience, "high");
  assert.equal(alert.mutationId, "mut1");
  assert.ok(alert.timestamp);
});

test("getSalienceAlert — returns null if no salience trigger", () => {
  const mutation = {
    id: "mut2",
    type: "add_entity",
    payload: { id: "ent3" },
  };

  const alert = getSalienceAlert(mutation);
  assert.equal(alert, null);
});
