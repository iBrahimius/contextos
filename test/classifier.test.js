import test from "node:test";
import assert from "node:assert/strict";

import { ObservationClassifier } from "../src/core/classifier.js";

test("classifier extracts tasks, constraints, facts, and relationships from live text", () => {
  const classifier = new ObservationClassifier();
  const result = classifier.classifyText(`
    ContextOS is a local-first memory system.
    The memory system depends on the retrieval pipeline and storage layer.
    We will use shadcn for the dashboard frontend.
    The proxy layer must block prompt injection.
    Task: add telemetry.
  `);

  const categories = result.observations.map((item) => item.category);
  assert.ok(categories.includes("relationship"));
  assert.ok(categories.includes("task"));
  assert.ok(categories.includes("constraint"));
  assert.ok(categories.includes("decision"));
  assert.ok(categories.includes("fact"));

  const labels = result.entities.map((entity) => entity.label.toLowerCase());
  assert.ok(labels.includes("contextos"));
  assert.ok(labels.includes("memory system"));
  assert.ok(labels.includes("retrieval pipeline"));
});
