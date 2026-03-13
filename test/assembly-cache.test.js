import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";
import {
  createAssemblyCache,
  buildAssemblyCacheKey,
  serializeCacheValue,
} from "../src/core/assembly-cache.js";
import { persistPatchForMessage } from "./test-helpers.js";

// --- Unit tests for the cache module itself ---

test("assembly-cache: serializeCacheValue produces stable output", () => {
  assert.equal(serializeCacheValue(null), "null");
  assert.equal(serializeCacheValue("hello"), '"hello"');
  assert.equal(serializeCacheValue(42), "42");
  assert.equal(serializeCacheValue({ b: 2, a: 1 }), '{"a":1,"b":2}');
  assert.equal(serializeCacheValue([3, 1, 2]), "[3,1,2]");
  assert.equal(
    serializeCacheValue({ z: [1, { b: 2, a: 1 }], a: null }),
    '{"a":null,"z":[1,{"a":1,"b":2}]}',
  );
});

test("assembly-cache: buildAssemblyCacheKey differentiates packet types", () => {
  const request = { query: "test", tokenBudget: 2000 };
  const keyA = buildAssemblyCacheKey("context-packet", request);
  const keyB = buildAssemblyCacheKey("memory-brief", request);
  assert.notEqual(keyA, keyB);
});

test("assembly-cache: buildAssemblyCacheKey differentiates token budgets", () => {
  const base = { query: "test", intent: "general" };
  const keyA = buildAssemblyCacheKey("context-packet", { ...base, tokenBudget: 2000 });
  const keyB = buildAssemblyCacheKey("context-packet", { ...base, tokenBudget: 4000 });
  assert.notEqual(keyA, keyB);
});

test("assembly-cache: buildAssemblyCacheKey differentiates scope filters", () => {
  const base = { query: "test", tokenBudget: 2000 };
  const keyA = buildAssemblyCacheKey("context-packet", {
    ...base,
    scopeFilter: { scopeKind: "private", scopeId: "user-1" },
  });
  const keyB = buildAssemblyCacheKey("context-packet", {
    ...base,
    scopeFilter: { scopeKind: "project", scopeId: "proj-1" },
  });
  assert.notEqual(keyA, keyB);
});

test("assembly-cache: buildAssemblyCacheKey differentiates queries", () => {
  const base = { tokenBudget: 2000, intent: "general" };
  const keyA = buildAssemblyCacheKey("context-packet", { ...base, query: "what is DNS?" });
  const keyB = buildAssemblyCacheKey("context-packet", { ...base, query: "what is hosting?" });
  assert.notEqual(keyA, keyB);
});

test("assembly-cache: buildAssemblyCacheKey differentiates intents", () => {
  const base = { query: "test", tokenBudget: 2000 };
  const keyA = buildAssemblyCacheKey("context-packet", { ...base, intent: "current_state" });
  const keyB = buildAssemblyCacheKey("context-packet", { ...base, intent: "history_temporal" });
  assert.notEqual(keyA, keyB);
});

test("assembly-cache: identical requests produce identical keys", () => {
  const request = {
    query: "DNS provider",
    tokenBudget: 2000,
    intent: "general",
    scopeFilter: { scopeKind: "project", scopeId: "p1" },
    conversationId: "conv-1",
  };
  const keyA = buildAssemblyCacheKey("context-packet", request);
  const keyB = buildAssemblyCacheKey("context-packet", { ...request });
  assert.equal(keyA, keyB);
});

test("assembly-cache: property order does not affect key", () => {
  const keyA = buildAssemblyCacheKey("context-packet", {
    query: "test",
    tokenBudget: 2000,
    intent: null,
  });
  const keyB = buildAssemblyCacheKey("context-packet", {
    intent: null,
    tokenBudget: 2000,
    query: "test",
  });
  assert.equal(keyA, keyB);
});

test("assembly-cache: open-items keys normalize kind casing and whitespace", () => {
  const keyA = buildAssemblyCacheKey("open-items", { kind: " all " });
  const keyB = buildAssemblyCacheKey("open-items", { kind: "ALL" });
  assert.equal(keyA, keyB);
});

test("assembly-cache: registry-query keys normalize name, query, and filter sets", () => {
  const keyA = buildAssemblyCacheKey("registry-query", {
    name: " Tasks ",
    query: " DNS ",
    filters: {
      tags: ["Infra", "dns", "infra"],
      status: " Active ",
      date_to: " 2026-03-06 ",
      date_from: " 2026-03-01 ",
    },
  });
  const keyB = buildAssemblyCacheKey("registry-query", {
    query: "DNS",
    name: "tasks",
    filters: {
      date_from: "2026-03-01",
      status: "active",
      tags: ["dns", "infra"],
      date_to: "2026-03-06",
    },
  });
  assert.equal(keyA, keyB);
});

// --- Functional tests for the cache store ---

test("assembly-cache: cold miss on first access", () => {
  const cache = createAssemblyCache();
  const result = cache.get("context-packet", { query: "test" }, 1);
  assert.equal(result.status, "miss");
  assert.equal(result.reason, "cold");
  assert.equal(result.payload, null);
});

test("assembly-cache: hit on identical request and same graph version", () => {
  const cache = createAssemblyCache();
  const request = { query: "DNS", tokenBudget: 2000 };
  const payload = { data: "assembled-packet" };

  cache.set("context-packet", request, 5, payload);
  const result = cache.get("context-packet", request, 5);

  assert.equal(result.status, "hit");
  assert.deepEqual(result.payload, payload);
});

test("assembly-cache: miss when graph version changes", () => {
  const cache = createAssemblyCache();
  const request = { query: "DNS", tokenBudget: 2000 };
  const payload = { data: "assembled-packet" };

  cache.set("context-packet", request, 5, payload);
  const result = cache.get("context-packet", request, 6);

  assert.equal(result.status, "miss");
  assert.equal(result.reason, "graph_version_changed");
  assert.equal(result.payload, null);
});

test("assembly-cache: miss when token budget changes", () => {
  const cache = createAssemblyCache();
  const payload = { data: "assembled" };

  cache.set("context-packet", { query: "DNS", tokenBudget: 2000 }, 5, payload);
  const result = cache.get("context-packet", { query: "DNS", tokenBudget: 4000 }, 5);

  assert.equal(result.status, "miss");
  assert.equal(result.reason, "cold");
});

test("assembly-cache: miss when scope filter changes", () => {
  const cache = createAssemblyCache();
  const payload = { data: "assembled" };

  cache.set("context-packet", {
    query: "DNS",
    tokenBudget: 2000,
    scopeFilter: { scopeKind: "private", scopeId: "user-1" },
  }, 5, payload);

  const result = cache.get("context-packet", {
    query: "DNS",
    tokenBudget: 2000,
    scopeFilter: { scopeKind: "project", scopeId: "proj-1" },
  }, 5);

  assert.equal(result.status, "miss");
  assert.equal(result.reason, "cold");
});

test("assembly-cache: no collision between context-packet and memory-brief", () => {
  const cache = createAssemblyCache();
  const request = { query: "test", tokenBudget: 2000 };

  cache.set("context-packet", request, 5, { type: "context" });
  cache.set("memory-brief", request, 5, { type: "brief" });

  const ctxResult = cache.get("context-packet", request, 5);
  const briefResult = cache.get("memory-brief", request, 5);

  assert.equal(ctxResult.status, "hit");
  assert.deepEqual(ctxResult.payload, { type: "context" });
  assert.equal(briefResult.status, "hit");
  assert.deepEqual(briefResult.payload, { type: "brief" });
});

test("assembly-cache: manual invalidation clears all entries", () => {
  const cache = createAssemblyCache();
  const request = { query: "test", tokenBudget: 2000 };

  cache.set("context-packet", request, 5, { data: "cached" });
  assert.equal(cache.get("context-packet", request, 5).status, "hit");

  cache.invalidate();
  assert.equal(cache.get("context-packet", request, 5).status, "miss");
  assert.equal(cache.get("context-packet", request, 5).reason, "cold");
});

test("assembly-cache: stats track hits and misses", () => {
  const cache = createAssemblyCache();
  const request = { query: "test", tokenBudget: 2000 };

  cache.get("context-packet", request, 1); // miss
  cache.set("context-packet", request, 1, { data: "x" });
  cache.get("context-packet", request, 1); // hit
  cache.get("context-packet", request, 2); // miss (graph version)

  const stats = cache.getStats();
  assert.equal(stats.hits, 1);
  assert.equal(stats.misses, 2);
  assert.equal(stats.size, 1);
});

test("assembly-cache: no cross-scope leakage between private and project", () => {
  const cache = createAssemblyCache();
  const baseRequest = { query: "sensitive data", tokenBudget: 2000 };

  cache.set("context-packet", {
    ...baseRequest,
    scopeFilter: { scopeKind: "private", scopeId: "user-secret" },
  }, 5, { data: "private-data" });

  // Different scope must not return the private data
  const projectResult = cache.get("context-packet", {
    ...baseRequest,
    scopeFilter: { scopeKind: "project", scopeId: "proj-public" },
  }, 5);

  assert.equal(projectResult.status, "miss");
  assert.equal(projectResult.payload, null);
});

test("assembly-cache: different entity/conversation IDs produce distinct keys", () => {
  const cache = createAssemblyCache();

  cache.set("context-packet", {
    query: "test",
    tokenBudget: 2000,
    conversationId: "conv-A",
  }, 5, { data: "A" });

  const resultB = cache.get("context-packet", {
    query: "test",
    tokenBudget: 2000,
    conversationId: "conv-B",
  }, 5);

  assert.equal(resultB.status, "miss");
});

// --- Integration test: context packet cache via ContextOS ---

async function makeHarness() {
  const rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-assembly-cache-"));
  await fs.mkdir(path.join(rootDir, "data"), { recursive: true });
  const contextOS = new ContextOS({ rootDir, autoBackfillEmbeddings: false });
  contextOS.enqueueMessageEmbedding = () => null;
  contextOS.enqueueObservationEmbedding = () => null;

  return {
    rootDir,
    contextOS,
    async close() {
      await contextOS.close();
      await fs.rm(rootDir, { recursive: true, force: true });
    },
  };
}

test("integration: contextPacket cache hit on identical request, same graph version", async () => {
  const harness = await makeHarness();
  try {
    const request = { query: "", tokenBudget: 2000 };

    const first = await harness.contextOS.contextPacket(request);
    assert.ok(first.diagnostics.cache_status.startsWith("miss:"));

    const second = await harness.contextOS.contextPacket(request);
    assert.equal(second.diagnostics.cache_status, "hit");

    // Payload structure should match (except timestamp/diagnostics)
    assert.equal(second.intent, first.intent);
    assert.equal(second.graph_version, first.graph_version);
    assert.deepEqual(second.stable_prefix, first.stable_prefix);
    assert.deepEqual(second.active_state, first.active_state);
  } finally {
    await harness.close();
  }
});

test("integration: contextPacket cache miss after graph version bump", async () => {
  const harness = await makeHarness();
  try {
    const request = { query: "", tokenBudget: 2000 };

    const first = await harness.contextOS.contextPacket(request);
    assert.ok(first.diagnostics.cache_status.startsWith("miss:"));

    // Bump graph version explicitly (simulates entity/relationship mutation)
    harness.contextOS.database.bumpGraphVersion();
    harness.contextOS.graph.graphVersion = harness.contextOS.database.getGraphVersion();

    const second = await harness.contextOS.contextPacket(request);
    assert.ok(second.diagnostics.cache_status.startsWith("miss:"));
    assert.notEqual(second.graph_version, first.graph_version);
  } finally {
    await harness.close();
  }
});

test("integration: contextPacket cache miss on changed token budget", async () => {
  const harness = await makeHarness();
  try {
    const first = await harness.contextOS.contextPacket({ query: "", tokenBudget: 2000 });
    assert.ok(first.diagnostics.cache_status.startsWith("miss:"));

    const second = await harness.contextOS.contextPacket({ query: "", tokenBudget: 4000 });
    assert.ok(second.diagnostics.cache_status.startsWith("miss:"));
  } finally {
    await harness.close();
  }
});

test("integration: contextPacket cache miss on changed scope filter", async () => {
  const harness = await makeHarness();
  try {
    const first = await harness.contextOS.contextPacket({
      query: "",
      tokenBudget: 2000,
      scopeFilter: { scopeKind: "private", scopeId: "user-1" },
    });
    assert.ok(first.diagnostics.cache_status.startsWith("miss:"));

    const second = await harness.contextOS.contextPacket({
      query: "",
      tokenBudget: 2000,
      scopeFilter: { scopeKind: "project", scopeId: "proj-1" },
    });
    assert.ok(second.diagnostics.cache_status.startsWith("miss:"));
  } finally {
    await harness.close();
  }
});

test("integration: manual invalidation clears contextPacket cache", async () => {
  const harness = await makeHarness();
  try {
    const request = { query: "", tokenBudget: 2000 };

    await harness.contextOS.contextPacket(request);
    const cached = await harness.contextOS.contextPacket(request);
    assert.equal(cached.diagnostics.cache_status, "hit");

    harness.contextOS._assemblyCache.invalidate();

    const afterInvalidate = await harness.contextOS.contextPacket(request);
    assert.ok(afterInvalidate.diagnostics.cache_status.startsWith("miss:"));
  } finally {
    await harness.close();
  }
});

test("integration: listOpenItems cache hit on equivalent kind request, same graph version", async () => {
  const harness = await makeHarness();
  try {
    const first = harness.contextOS.listOpenItems(" all ");
    assert.ok(first.diagnostics.cache_status.startsWith("miss:"));

    const second = harness.contextOS.listOpenItems("ALL");
    assert.equal(second.diagnostics.cache_status, "hit");
    assert.deepEqual(second.tasks, first.tasks);
    assert.deepEqual(second.decisions, first.decisions);
    assert.deepEqual(second.constraints, first.constraints);
  } finally {
    await harness.close();
  }
});

test("integration: listOpenItems cache miss after graph version bump", async () => {
  const harness = await makeHarness();
  try {
    harness.contextOS.listOpenItems("tasks");
    harness.contextOS.database.bumpGraphVersion();
    harness.contextOS.graph.graphVersion = harness.contextOS.database.getGraphVersion();

    const second = harness.contextOS.listOpenItems(" tasks ");
    assert.ok(second.diagnostics.cache_status.startsWith("miss:"));
  } finally {
    await harness.close();
  }
});

test("integration: queryRegistry cache hit on normalized request, same graph version", async () => {
  const harness = await makeHarness();
  try {
    const first = harness.contextOS.queryRegistry({
      name: " Tasks ",
      query: " DNS ",
      filters: {
        tags: ["infra", "DNS", "infra"],
        status: " Active ",
      },
    });
    assert.ok(first.diagnostics.cache_status.startsWith("miss:"));

    const second = harness.contextOS.queryRegistry({
      name: "tasks",
      query: "DNS",
      filters: {
        status: "active",
        tags: ["dns", "infra"],
      },
    });
    assert.equal(second.diagnostics.cache_status, "hit");
    assert.deepEqual(second.results, first.results);
    assert.equal(second.total, first.total);
  } finally {
    await harness.close();
  }
});

test("integration: queryRegistry cache miss after graph version bump", async () => {
  const harness = await makeHarness();
  try {
    harness.contextOS.queryRegistry({
      name: "tasks",
      query: "DNS",
      filters: { status: "active", tags: ["infra"] },
    });
    harness.contextOS.database.bumpGraphVersion();
    harness.contextOS.graph.graphVersion = harness.contextOS.database.getGraphVersion();

    const second = harness.contextOS.queryRegistry({
      name: " tasks ",
      query: " DNS ",
      filters: { tags: ["infra", "infra"], status: "ACTIVE" },
    });
    assert.ok(second.diagnostics.cache_status.startsWith("miss:"));
  } finally {
    await harness.close();
  }
});

test("integration: memoryBrief cache hit on identical request, same graph version", async () => {
  const harness = await makeHarness();
  try {
    const request = { tokenBudget: 1800 };

    const first = await harness.contextOS.memoryBrief(request);
    const second = await harness.contextOS.memoryBrief(request);

    // Memory brief itself doesn't have cache_status in its payload,
    // but the underlying contextPacket call should be cached.
    // We verify by checking that the assembly cache has entries.
    const stats = harness.contextOS._assemblyCache.getStats();
    assert.ok(stats.hits >= 1, "Expected at least one cache hit");
    assert.equal(second.graph_version, first.graph_version);
    assert.deepEqual(second.profile, first.profile);
  } finally {
    await harness.close();
  }
});

test("integration: memoryBrief cache miss after graph version bump", async () => {
  const harness = await makeHarness();
  try {
    const request = { tokenBudget: 1800 };

    await harness.contextOS.memoryBrief(request);
    harness.contextOS._assemblyCache.resetStats();

    // Bump graph version explicitly
    harness.contextOS.database.bumpGraphVersion();
    harness.contextOS.graph.graphVersion = harness.contextOS.database.getGraphVersion();

    await harness.contextOS.memoryBrief(request);
    const stats = harness.contextOS._assemblyCache.getStats();
    // After graph bump, all cache accesses should be misses
    assert.equal(stats.hits, 0, "Expected zero hits after graph version bump");
    assert.ok(stats.misses >= 1, "Expected at least one miss after graph version bump");
  } finally {
    await harness.close();
  }
});
