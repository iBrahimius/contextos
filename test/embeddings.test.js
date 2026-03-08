import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";

import { ContextOS } from "../src/core/context-os.js";
import { EMBEDDING_DIMENSIONS, EMBEDDING_MODEL_PATH, embedText } from "../src/core/embeddings.js";
import { estimateTokens } from "../src/core/utils.js";
import { handleRequest } from "../src/http/router.js";

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-embeddings-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  return root;
}

function insertSeedMessage(contextOS, conversationId, content, ingestId) {
  return contextOS.database.insertMessage({
    conversationId,
    role: "user",
    direction: "inbound",
    actorId: "seed",
    originKind: "import",
    content,
    tokenCount: estimateTokens(content),
    raw: { seeded: true },
    ingestId,
  });
}

async function waitFor(check, { timeoutMs = 30000, intervalMs = 25 } = {}) {
  const startedAt = performance.now();

  while (performance.now() - startedAt < timeoutMs) {
    const value = await check();
    if (value) {
      return value;
    }

    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }

  throw new Error(`Timed out after ${timeoutMs}ms`);
}

test("embedText loads the real embedding model and returns a 768-dim Float32Array", async () => {
  await fs.access(EMBEDDING_MODEL_PATH);

  const vector = await embedText("Move DNS to Cloudflare for the edge network.");

  assert.ok(vector instanceof Float32Array);
  assert.equal(vector.length, EMBEDDING_DIMENSIONS);
});

test("ingestMessage stays non-blocking while the embedding is stored asynchronously", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({
    rootDir,
    autoBackfillEmbeddings: false,
    
  });
  const conversation = contextOS.database.createConversation("Embedding Ingest");

  try {
    const startedAt = performance.now();
    const record = await contextOS.ingestMessage({
      conversationId: conversation.id,
      conversationTitle: conversation.title,
      role: "user",
      direction: "inbound",
      content: "We decided to move DNS to Cloudflare for the edge network.",
    });
    const latencyMs = performance.now() - startedAt;

    assert.ok(latencyMs < 100, `Expected ingest to stay under 100ms, got ${latencyMs.toFixed(2)}ms`);

    const stored = await waitFor(() => contextOS.database.getMessageEmbedding(record.message.id));
    assert.ok(stored);
    assert.equal(stored.embedding.length, EMBEDDING_DIMENSIONS);
  } finally {
    contextOS.database.close();
    contextOS.telemetry.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("backfillEmbeddings updates /api/status coverage for existing messages", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({
    rootDir,
    autoBackfillEmbeddings: false,
    
  });
  const conversation = contextOS.database.createConversation("Embedding Status");
  const server = http.createServer((request, response) => handleRequest(contextOS, rootDir, request, response));

  try {
    insertSeedMessage(contextOS, conversation.id, "We decided to move DNS to Cloudflare for the edge network.", "embed_status_1");
    insertSeedMessage(contextOS, conversation.id, "The rollout checklist needs another DNS validation pass.", "embed_status_2");

    const result = await contextOS.backfillEmbeddings({
      batchSize: 10,
      rateLimitPerSecond: 10,
    });
    assert.equal(result.total, 2);
    assert.equal(result.embedded, 2);
    assert.equal(result.coverage, 100);

    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    const { port } = server.address();
    const response = await fetch(`http://127.0.0.1:${port}/api/status`);
    const payload = await response.json();

    assert.equal(response.status, 200);
    assert.deepEqual(payload.embeddings, {
      embedded: 2,
      total: 2,
      coverage: 100,
    });
  } finally {
    await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
    contextOS.database.close();
    contextOS.telemetry.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});

test("hybrid retrieval ranks the semantic decision above a keyword-only partner mention", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({
    rootDir,
    autoBackfillEmbeddings: false,
    
  });
  const conversation = contextOS.database.createConversation("Semantic Retrieval");

  try {
    const decisionMessage = insertSeedMessage(
      contextOS,
      conversation.id,
      "We decided to move forward with the RHI partner integration for the enterprise launch.",
      "semantic_decision",
    );
    const keywordOnlyMessage = insertSeedMessage(
      contextOS,
      conversation.id,
      "The partner worksheet template still needs new column names.",
      "semantic_keyword_only",
    );
    insertSeedMessage(
      contextOS,
      conversation.id,
      "DNS monitoring will stay in Datadog during the migration.",
      "semantic_irrelevant",
    );

    await contextOS.backfillEmbeddings({
      batchSize: 10,
      rateLimitPerSecond: 10,
    });

    const retrieval = await contextOS.retrieve({
      conversationId: conversation.id,
      queryText: "what did we decide about the partner?",
    });
    const messageResults = retrieval.items.filter((item) => item.type === "message");

    assert.ok(messageResults.length >= 2);
    assert.equal(messageResults[0].id, decisionMessage.id);
    assert.ok(messageResults.findIndex((item) => item.id === keywordOnlyMessage.id) > 0);
  } finally {
    contextOS.database.close();
    contextOS.telemetry.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});
