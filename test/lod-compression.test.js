import test from "node:test";
import assert from "node:assert/strict";
import {
  buildLevelGenerationPrompt,
  generateLevels,
  persistLevels,
} from "../src/core/lod-compression.js";

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

test("buildLevelGenerationPrompt - includes atoms and observations", () => {
  const atoms = [
    { type: "fact", text: "Database chosen", confidence: 0.95 },
    { type: "decision", text: "Use SQLite", confidence: 0.9 },
  ];

  const observations = [
    { id: 1, text: "We selected SQLite for simplicity" },
    { id: 2, text: "No external dependencies needed" },
  ];

  const prompt = buildLevelGenerationPrompt(atoms, observations);

  assert(prompt.includes("[FACT]"));
  assert(prompt.includes("[DECISION]"));
  assert(prompt.includes("Database chosen"));
  assert(prompt.includes("Use SQLite"));
  assert(prompt.includes("[1]"));
  assert(prompt.includes("[2]"));
  assert(prompt.includes("L2"));
  assert(prompt.includes("L1"));
  assert(prompt.includes("L0"));
});

test("buildLevelGenerationPrompt - emphasizes contradiction preservation", () => {
  const atoms = [];
  const observations = [];

  const prompt = buildLevelGenerationPrompt(atoms, observations);

  assert(prompt.includes("don't resolve them"));
  assert(prompt.includes("information"));
});

test("generateLevels - returns structured levels", async () => {
  const atoms = [
    { type: "decision", text: "Use SQLite", confidence: 0.95 },
  ];

  const observations = [
    { id: 1, text: "SQLite chosen for zero dependencies" },
  ];

  const mockResponse = {
    data: {
      l0: "Primary topic: database selection",
      l1: "Key decision: SQLite chosen for simplicity and zero dependencies.",
      l2: "After careful consideration, the team selected SQLite as the persistence layer...",
    },
  };

  const mockClient = new MockLLMClient(mockResponse);
  const levels = await generateLevels(atoms, observations, mockClient);

  assert(levels.l0);
  assert(levels.l1);
  assert(levels.l2);
  assert.equal(mockClient.callCount, 1);
});

test("generateLevels - null atoms returns empty object", async () => {
  const mockClient = new MockLLMClient({ data: {} });
  const levels = await generateLevels(null, [{ id: 1, text: "Obs" }], mockClient);

  assert.deepEqual(levels, {});
});

test("generateLevels - null observations returns empty object", async () => {
  const mockClient = new MockLLMClient({ data: {} });
  const levels = await generateLevels([{ type: "fact", text: "Atom" }], null, mockClient);

  assert.deepEqual(levels, {});
});

test("generateLevels - handles LLM error gracefully", async () => {
  const atoms = [{ type: "fact", text: "Atom" }];
  const observations = [{ id: 1, text: "Obs" }];

  const errorClient = {
    async completeJSON() {
      throw new Error("LLM service timeout");
    },
  };

  const levels = await generateLevels(atoms, observations, errorClient);
  assert.deepEqual(levels, {});
});

test("persistLevels - writes levels to database", async () => {
  const levels = {
    l0: "Headline (150 chars)",
    l1: "Synopsis (1500 chars)",
    l2: "Full narrative (3500 chars)",
  };

  const sourceObsIds = [1, 2, 3];

  let runCalls = [];
  const db = {
    prepare: (sql) => ({
      run: (...args) => {
        runCalls.push({ sql, args });
      },
    }),
  };

  await persistLevels(db, 42, levels, sourceObsIds);

  assert.equal(runCalls.length, 3);

  assert.equal(runCalls[0].args[1], 0);
  assert.equal(runCalls[1].args[1], 1);
  assert.equal(runCalls[2].args[1], 2);
});

test("persistLevels - skips empty level texts", async () => {
  const levels = {
    l0: "Headline",
    l1: "",
    l2: "Full text",
  };

  let runCalls = [];
  const db = {
    prepare: (sql) => ({
      run: (...args) => {
        runCalls.push(args);
      },
    }),
  };

  await persistLevels(db, 42, levels, [1]);

  assert.equal(runCalls.length, 2);
});

test("persistLevels - computes character count correctly", async () => {
  const levels = {
    l0: "A".repeat(100),
    l1: "B".repeat(2000),
    l2: "C".repeat(4000),
  };

  let charCounts = [];
  const db = {
    prepare: (sql) => ({
      run: (...args) => {
        charCounts.push(args[4]);
      },
    }),
  };

  await persistLevels(db, 42, levels);

  assert.equal(charCounts[0], 100);
  assert.equal(charCounts[1], 2000);
  assert.equal(charCounts[2], 4000);
});
