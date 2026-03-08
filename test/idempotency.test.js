import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";
import { persistHeuristicPatchForMessage } from "./test-helpers.js";

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  return root;
}

test("ingest is idempotent when the same ingestId is replayed", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const conversation = contextOS.database.createConversation("Idempotency");
  const ingestId = "ing_retry_demo";

  const first = await contextOS.ingestMessage({
    conversationId: conversation.id,
    conversationTitle: conversation.title,
    role: "user",
    direction: "inbound",
    ingestId,
    content: "The memory system depends on the retrieval pipeline and storage layer.",
  });

  persistHeuristicPatchForMessage(contextOS, first);

  const countsAfterFirst = {
    messages: contextOS.database.prepare(`SELECT COUNT(*) AS count FROM messages`).get().count,
    observations: contextOS.database.prepare(`SELECT COUNT(*) AS count FROM observations`).get().count,
    entities: contextOS.database.prepare(`SELECT COUNT(*) AS count FROM entities`).get().count,
  };

  const second = await contextOS.ingestMessage({
    conversationId: conversation.id,
    conversationTitle: conversation.title,
    role: "user",
    direction: "inbound",
    ingestId,
    content: "The memory system depends on the retrieval pipeline and storage layer.",
  });

  const countsAfterSecond = {
    messages: contextOS.database.prepare(`SELECT COUNT(*) AS count FROM messages`).get().count,
    observations: contextOS.database.prepare(`SELECT COUNT(*) AS count FROM observations`).get().count,
    entities: contextOS.database.prepare(`SELECT COUNT(*) AS count FROM entities`).get().count,
  };

  const stored = contextOS.database.prepare(`
    SELECT COUNT(*) AS count
    FROM messages
    WHERE ingest_id = ?
  `).get(ingestId);

  assert.equal(stored.count, 1);
  assert.equal(first.message.id, second.message.id);
  assert.equal(countsAfterSecond.messages, countsAfterFirst.messages);
  assert.equal(countsAfterSecond.observations, countsAfterFirst.observations);
  assert.equal(countsAfterSecond.entities, countsAfterFirst.entities);
});
