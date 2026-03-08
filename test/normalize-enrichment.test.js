import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  normalizeEnrichment,
  normalizeLabel,
  normalizeKind,
  normalizePredicate,
} from "../src/core/normalize-enrichment.js";

describe("normalizePredicate", () => {
  it("passes through allowed predicates", () => {
    assert.equal(normalizePredicate("part_of"), "part_of");
    assert.equal(normalizePredicate("depends_on"), "depends_on");
    assert.equal(normalizePredicate("related_to"), "related_to");
    assert.equal(normalizePredicate("integrates_with"), "integrates_with");
  });

  it("maps known invented predicates", () => {
    assert.equal(normalizePredicate("stored_in_parallel"), "integrates_with");
    assert.equal(normalizePredicate("confirmed_as"), "related_to");
    assert.equal(normalizePredicate("runs_on"), "depends_on");
    assert.equal(normalizePredicate("built_with"), "depends_on");
    assert.equal(normalizePredicate("contains"), "has_part");
    assert.equal(normalizePredicate("belongs_to"), "part_of");
    assert.equal(normalizePredicate("created_by"), "owned_by");
    assert.equal(normalizePredicate("manages"), "owns");
  });

  it("falls back to related_to for unknown predicates", () => {
    assert.equal(normalizePredicate("frobulates"), "related_to");
    assert.equal(normalizePredicate("synergizes_with"), "related_to");
  });

  it("handles null/undefined/empty", () => {
    assert.equal(normalizePredicate(null), "related_to");
    assert.equal(normalizePredicate(undefined), "related_to");
    assert.equal(normalizePredicate(""), "related_to");
  });
});

describe("normalizeKind", () => {
  it("passes through allowed kinds", () => {
    assert.equal(normalizeKind("person"), "person");
    assert.equal(normalizeKind("project"), "project");
    assert.equal(normalizeKind("system"), "system");
    assert.equal(normalizeKind("tool"), "tool");
    assert.equal(normalizeKind("event"), "event");
  });

  it("maps common inventions", () => {
    assert.equal(normalizeKind("technology"), "tool");
    assert.equal(normalizeKind("milestone"), "event");
    assert.equal(normalizeKind("component"), "system");
    assert.equal(normalizeKind("company"), "organization");
    assert.equal(normalizeKind("framework"), "tool");
    assert.equal(normalizeKind("feature"), "concept");
  });

  it("falls back to concept for unknown kinds", () => {
    assert.equal(normalizeKind("widget"), "concept");
    assert.equal(normalizeKind("thingamajig"), "concept");
  });

  it("handles null/undefined", () => {
    assert.equal(normalizeKind(null), "concept");
    assert.equal(normalizeKind(undefined), "concept");
  });
});

describe("normalizeLabel", () => {
  it("passes through clean labels", () => {
    assert.equal(normalizeLabel("ContextOS"), "ContextOS");
    assert.equal(normalizeLabel("Ibrahim"), "Ibrahim");
    assert.equal(normalizeLabel("Haiku classifier"), "Haiku classifier");
    assert.equal(normalizeLabel("Shadow mode"), "Shadow mode");
  });

  it("rejects multi-concept labels (commas)", () => {
    assert.equal(normalizeLabel("phases 0, 1, 1.5"), null);
    assert.equal(normalizeLabel("Cortex, ContextOS"), null);
  });

  it("rejects multi-concept labels (and/or)", () => {
    assert.equal(normalizeLabel("tasks and decisions"), null);
    assert.equal(normalizeLabel("Cortex or ContextOS"), null);
  });

  it("rejects phrase labels (too many words)", () => {
    assert.equal(normalizeLabel("session related files panel view"), null);
    assert.equal(normalizeLabel("need to build the dashboard soon"), null);
  });

  it("allows up to 4 words", () => {
    assert.equal(normalizeLabel("Haiku classification prompt"), "Haiku classification prompt");
    assert.equal(normalizeLabel("ContextOS retrieval engine"), "ContextOS retrieval engine");
    // 4 words is fine
    assert.equal(normalizeLabel("Node.js single runtime decision"), "Node.js single runtime decision");
  });

  it("strips leading noise words", () => {
    assert.equal(normalizeLabel("the dashboard"), "dashboard");
    assert.equal(normalizeLabel("a system"), "system");
    assert.equal(normalizeLabel("an entity graph"), "entity graph");
  });

  it("strips surrounding quotes", () => {
    assert.equal(normalizeLabel('"ContextOS"'), "ContextOS");
    assert.equal(normalizeLabel("'plan.md'"), "plan.md");
    assert.equal(normalizeLabel("`retrieval`"), "retrieval");
  });

  it("rejects too short", () => {
    assert.equal(normalizeLabel("a"), null);
    assert.equal(normalizeLabel(""), null);
    assert.equal(normalizeLabel("X"), null);
  });

  it("handles null/undefined", () => {
    assert.equal(normalizeLabel(null), null);
    assert.equal(normalizeLabel(undefined), null);
  });

  it("rejects slash-separated labels", () => {
    assert.equal(normalizeLabel("input / output"), null);
  });
});

describe("normalizeEnrichment", () => {
  it("cleans a realistic Haiku output", () => {
    // Simulated Haiku response for: "let's review plan.md, phases 0, 1-1.5 done,
    // dashboard is usable, numbers going up"
    const haiku = {
      entities: [
        { label: "plan.md", kind: "system", summary: "A plan document" },
        { label: "dashboard", kind: "system", summary: "Monitoring interface" },
        { label: "phases 0, 1-1.5", kind: "milestone", summary: "Completed dev phases" },
      ],
      observations: [
        {
          category: "fact",
          detail: "Phases 0 through 1.5 have been completed",
          subjectLabel: "phases 0, 1-1.5",
          objectLabel: null,
          predicate: null,
          confidence: 0.9,
        },
        {
          category: "fact",
          detail: "Dashboard can display increasing numeric metrics",
          subjectLabel: "dashboard",
          objectLabel: null,
          predicate: null,
          confidence: 0.85,
        },
        {
          category: "relationship",
          detail: "Dashboard enables monitoring of metric changes",
          subjectLabel: "dashboard",
          objectLabel: "metrics",
          predicate: "enables",
          confidence: 0.9,
        },
      ],
      graphProposals: [],
    };

    const result = normalizeEnrichment(haiku);

    // "phases 0, 1-1.5" should be rejected (comma)
    assert.equal(result.entities.length, 2);
    assert.equal(result.entities[0].label, "plan.md");
    assert.equal(result.entities[1].label, "dashboard");

    // "milestone" should be mapped to "event"
    // Actually plan.md was "system" (allowed), dashboard was "system" (allowed)
    // The rejected entity was the milestone one
    assert.equal(result.stats.entities.rejected, 1);

    // "enables" predicate should be mapped to related_to
    const rel = result.observations.find((o) => o.category === "relationship");
    assert.equal(rel.predicate, "related_to");
    assert.equal(result.stats.observations.predicateMapped, 1);
  });

  it("handles dual-write test case (invented predicates)", () => {
    const haiku = {
      entities: [
        { label: "Cortex", kind: "system", summary: "Memory system" },
        { label: "ContextOS", kind: "project", summary: "New memory system" },
      ],
      observations: [
        {
          category: "relationship",
          detail: "Messages stored in parallel in both systems",
          subjectLabel: "Cortex",
          objectLabel: "ContextOS",
          predicate: "stored_in_parallel",
          confidence: 0.9,
        },
        {
          category: "fact",
          detail: "Dual-write shadow mode is active",
          subjectLabel: "ContextOS",
          objectLabel: null,
          predicate: null,
          confidence: 0.88,
        },
      ],
      graphProposals: [],
    };

    const result = normalizeEnrichment(haiku);

    // Both entities should pass
    assert.equal(result.entities.length, 2);

    // stored_in_parallel → integrates_with
    const rel = result.observations.find((o) => o.category === "relationship");
    assert.equal(rel.predicate, "integrates_with");
  });

  it("handles shadow mode test case (confirmed_as)", () => {
    const haiku = {
      entities: [
        { label: "Shadow mode", kind: "concept", summary: "Dual-write testing mode" },
      ],
      observations: [
        {
          category: "fact",
          detail: "Shadow mode is working correctly",
          subjectLabel: "Shadow mode",
          objectLabel: null,
          predicate: null,
          confidence: 0.9,
        },
        {
          category: "relationship",
          detail: "Shadow mode confirmed as working",
          subjectLabel: "Shadow mode",
          objectLabel: "ContextOS",
          predicate: "confirmed_as",
          confidence: 0.85,
        },
      ],
      graphProposals: [],
    };

    const result = normalizeEnrichment(haiku);
    const rel = result.observations.find((o) => o.category === "relationship");
    assert.equal(rel.predicate, "related_to");
  });

  it("handles session files test case (phrase entity)", () => {
    const haiku = {
      entities: [
        { label: "dashboard", kind: "system", summary: "Dashboard UI" },
        { label: "session related files", kind: "concept", summary: "Files from sessions" },
      ],
      observations: [
        {
          category: "task",
          detail: "Add session files to dashboard",
          subjectLabel: null,
          objectLabel: "dashboard",
          predicate: null,
          confidence: 0.9,
        },
      ],
      graphProposals: [],
    };

    const result = normalizeEnrichment(haiku);
    // "session related files" is 3 words, under limit — should pass
    assert.equal(result.entities.length, 2);
    // Task observation should pass through unchanged (no predicate normalization needed)
    assert.equal(result.observations.length, 1);
    assert.equal(result.observations[0].category, "task");
  });

  it("rejects observations without detail", () => {
    const haiku = {
      entities: [],
      observations: [
        { category: "fact", detail: "", confidence: 0.9 },
        { category: "fact", detail: "OK", confidence: 0.9 }, // too short
        { category: "task", detail: "Build the API endpoint", confidence: 0.85 },
      ],
      graphProposals: [],
    };

    const result = normalizeEnrichment(haiku);
    assert.equal(result.observations.length, 1);
    assert.equal(result.observations[0].detail, "Build the API endpoint");
    assert.equal(result.stats.observations.rejected, 2);
  });

  it("rejects relationships without both subject and object", () => {
    const haiku = {
      entities: [],
      observations: [
        {
          category: "relationship",
          detail: "Something depends on something",
          subjectLabel: "A",
          objectLabel: null,
          predicate: "depends_on",
          confidence: 0.9,
        },
      ],
      graphProposals: [],
    };

    const result = normalizeEnrichment(haiku);
    assert.equal(result.observations.length, 0);
    assert.equal(result.stats.observations.rejected, 1);
  });

  it("returns stats for monitoring", () => {
    const haiku = {
      entities: [
        { label: "ContextOS", kind: "project", summary: "Memory" },
        { label: "phases 0, 1, 2", kind: "milestone", summary: "Phases" },
      ],
      observations: [
        {
          category: "relationship",
          detail: "A uses B",
          subjectLabel: "A",
          objectLabel: "B",
          predicate: "uses",
          confidence: 0.9,
        },
      ],
      graphProposals: [],
    };

    const result = normalizeEnrichment(haiku);
    assert.equal(result.stats.entities.input, 2);
    assert.equal(result.stats.entities.output, 1);
    assert.equal(result.stats.entities.rejected, 1);
    assert.equal(result.stats.observations.predicateMapped, 1);
  });
});
