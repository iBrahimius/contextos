import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

import { ContextOS } from "../src/core/context-os.js";
import { persistHeuristicPatchForMessage } from "./test-helpers.js";

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  return root;
}

function listTables(sqlite) {
  return sqlite.prepare(`
    SELECT name
    FROM sqlite_master
    WHERE type = 'table'
  `).all().map((row) => row.name);
}

test("content and telemetry data are split across two SQLite databases", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const conversation = contextOS.database.createConversation("Two DB Split");

  await contextOS.ingestMessage({
    conversationId: conversation.id,
    conversationTitle: conversation.title,
    role: "user",
    direction: "inbound",
    content: "The memory system depends on the retrieval pipeline and storage layer.",
  });

  persistHeuristicPatchForMessage(contextOS, contextOS.database.listMessages(conversation.id).at(-1));

  const retrieval = await contextOS.retrieve({
    conversationId: conversation.id,
    queryText: "memory system",
  });

  assert.ok(retrieval.queryId);

  const contentPath = path.join(rootDir, "data", "contextos.db");
  const telemetryPath = path.join(rootDir, "data", "contextos_telemetry.db");

  await fs.access(contentPath);
  await fs.access(telemetryPath);

  const contentDb = new DatabaseSync(contentPath);
  const telemetryDb = new DatabaseSync(telemetryPath);

  try {
    const contentTables = new Set(listTables(contentDb));
    const telemetryTables = new Set(listTables(telemetryDb));

    assert.ok(contentTables.has("messages"));
    assert.ok(contentTables.has("entities"));
    assert.ok(!contentTables.has("model_runs"));
    assert.ok(!contentTables.has("retrieval_queries"));
    assert.ok(!contentTables.has("retrieval_results"));
    assert.ok(!contentTables.has("retrieval_hints"));
    assert.ok(!contentTables.has("proxy_events"));

    assert.ok(telemetryTables.has("model_runs"));
    assert.ok(telemetryTables.has("retrieval_queries"));
    assert.ok(telemetryTables.has("retrieval_results"));
    assert.ok(!telemetryTables.has("messages"));
    assert.ok(!telemetryTables.has("entities"));
    assert.ok(!telemetryTables.has("graph_proposals"));

    const messageCount = contentDb.prepare(`SELECT COUNT(*) AS count FROM messages`).get().count;
    const entityCount = contentDb.prepare(`SELECT COUNT(*) AS count FROM entities`).get().count;
    const modelRunCount = telemetryDb.prepare(`SELECT COUNT(*) AS count FROM model_runs`).get().count;
    const retrievalCount = telemetryDb.prepare(`SELECT COUNT(*) AS count FROM retrieval_queries`).get().count;
    const retrievalResultCount = telemetryDb.prepare(`SELECT COUNT(*) AS count FROM retrieval_results`).get().count;

    assert.ok(messageCount >= 1);
    assert.ok(entityCount >= 1);
    assert.equal(modelRunCount, 0);
    assert.equal(retrievalCount, 1);
    assert.ok(retrievalResultCount >= 1);
  } finally {
    contentDb.close();
    telemetryDb.close();
  }
});
