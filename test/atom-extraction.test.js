import test from "node:test";
import assert from "node:assert/strict";
import {
  buildAtomExtractionPrompt,
  extractAtoms,
  persistAtoms,
} from "../src/core/atom-extraction.js";

// Mock LLM client
class MockLLMClient {
  constructor(response) {
    this.response = response;
    this.callCount = 0;
  }

  async completeJSON(params) {
    this.callCount++;
    return this.response;
  }
}

test("buildAtomExtractionPrompt - includes all observations", () => {
  const observations = [
    { id: 1, timestamp: "2026-01-01T10:00:00Z", text: "First observation", confidence: 0.9 },
    { id: 2, timestamp: "2026-01-01T11:00:00Z", text: "Second observation", confidence: 0.8 },
  ];

  const prompt = buildAtomExtractionPrompt(observations);

  assert(prompt.includes("[ID: 1]"));
  assert(prompt.includes("[ID: 2]"));
  assert(prompt.includes("First observation"));
  assert(prompt.includes("Second observation"));
  assert(prompt.includes("confidence: 0.9"));
  assert(prompt.includes("confidence: 0.8"));
  assert(prompt.includes("fact"));
  assert(prompt.includes("decision"));
  assert(prompt.includes("contradiction"));
});

test("buildAtomExtractionPrompt - empty observations", () => {
  const prompt = buildAtomExtractionPrompt([]);
  assert(typeof prompt === "string");
  assert(prompt.length > 0);
  assert(prompt.includes("JSON"));
});

test("extractAtoms - empty observations returns empty array", async () => {
  const mockClient = new MockLLMClient({
    data: { atoms: [] },
  });

  const result = await extractAtoms([], mockClient);
  assert.deepEqual(result, []);
});

test("extractAtoms - null observations returns empty array", async () => {
  const mockClient = new MockLLMClient({
    data: { atoms: [] },
  });

  const result = await extractAtoms(null, mockClient);
  assert.deepEqual(result, []);
});

test("extractAtoms - validates source observation IDs", async () => {
  const observations = [
    { id: 1, timestamp: "2026-01-01T10:00:00Z", text: "Obs 1", confidence: 0.9 },
    { id: 2, timestamp: "2026-01-01T11:00:00Z", text: "Obs 2", confidence: 0.8 },
  ];

  const mockResponse = {
    data: {
      atoms: [
        {
          type: "fact",
          text: "Valid atom",
          source_observation_ids: [1, 2],
          confidence: 0.95,
        },
        {
          type: "decision",
          text: "Invalid: cites nonexistent observation",
          source_observation_ids: [1, 99],
          confidence: 0.8,
        },
        {
          type: "emotion",
          text: "Another valid atom",
          source_observation_ids: [2],
          confidence: 0.85,
        },
      ],
    },
  };

  const mockClient = new MockLLMClient(mockResponse);
  const atoms = await extractAtoms(observations, mockClient);

  assert.equal(atoms.length, 2);
  assert.equal(atoms[0].text, "Valid atom");
  assert.equal(atoms[1].text, "Another valid atom");
});

test("extractAtoms - handles LLM error gracefully", async () => {
  const observations = [
    { id: 1, timestamp: "2026-01-01T10:00:00Z", text: "Obs", confidence: 0.9 },
  ];

  const errorClient = {
    async completeJSON() {
      throw new Error("LLM service unavailable");
    },
  };

  const atoms = await extractAtoms(observations, errorClient);
  assert.deepEqual(atoms, []);
});

test("extractAtoms - preserves atom types and confidence", async () => {
  const observations = [
    { id: 1, timestamp: "2026-01-01T10:00:00Z", text: "Obs", confidence: 0.9 },
  ];

  const mockResponse = {
    data: {
      atoms: [
        {
          type: "fact",
          text: "A fact",
          source_observation_ids: [1],
          confidence: 0.99,
        },
        {
          type: "contradiction",
          text: "A contradiction",
          source_observation_ids: [1],
          confidence: 0.75,
        },
      ],
    },
  };

  const mockClient = new MockLLMClient(mockResponse);
  const atoms = await extractAtoms(observations, mockClient);

  assert.equal(atoms.length, 2);
  assert.equal(atoms[0].type, "fact");
  assert.equal(atoms[0].confidence, 0.99);
  assert.equal(atoms[1].type, "contradiction");
  assert.equal(atoms[1].confidence, 0.75);
});

test("persistAtoms - writes atoms to database", async () => {
  let runCount = 0;
  const testDb = {
    prepare: (sql) => ({
      run: (...args) => {
        runCount++;
        assert.equal(args[0], 42);
        assert(["fact", "decision"].includes(args[1]));
        assert(typeof args[2] === "string");
        assert(typeof args[3] === "string");
        assert(typeof args[4] === "number");
        return { changes: 1 };
      },
    }),
  };

  const atoms = [
    {
      type: "fact",
      text: "Test fact",
      source_observation_ids: [1, 2],
      confidence: 0.9,
    },
    {
      type: "decision",
      text: "Test decision",
      source_observation_ids: [3],
      confidence: 0.85,
    },
  ];

  await persistAtoms(testDb, 42, atoms);
  assert.equal(runCount, 2);
});

test("persistAtoms - writes to real cluster_atoms table", async () => {
  const { DatabaseSync } = await import("node:sqlite");
  const { SCHEMA } = await import("../src/db/schema.js");

  const db = new DatabaseSync(":memory:");
  db.exec(SCHEMA);

  // Insert prerequisite rows
  db.exec(`INSERT INTO episodes (id, started_at, created_at) VALUES ('ep1', datetime('now'), datetime('now'))`);
  db.exec(`INSERT INTO observation_clusters (id, episode_id, time_span_start, time_span_end) VALUES ('cl1', 'ep1', datetime('now'), datetime('now'))`);

  const atoms = [
    { type: "fact", text: "Alice prefers async", source_observation_ids: [1], confidence: 0.9 },
    { type: "decision", text: "Use SQLite", source_observation_ids: [2, 3], confidence: 0.85 },
  ];

  await persistAtoms(db, "cl1", atoms);

  const rows = db.prepare("SELECT * FROM cluster_atoms WHERE cluster_id = ?").all("cl1");
  assert.equal(rows.length, 2);
  assert.equal(rows[0].atom_type, "fact");
  assert.equal(rows[0].text, "Alice prefers async");
  assert.equal(rows[0].confidence, 0.9);
  assert.equal(rows[1].atom_type, "decision");
  assert.deepEqual(JSON.parse(rows[1].source_observation_ids), [2, 3]);
});
