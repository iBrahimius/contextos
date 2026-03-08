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

test("messages persist origin lineage and retrieval penalizes agent restatements", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const conversation = contextOS.database.createConversation("Origin Lineage");

  const userCapture = await contextOS.ingestMessage({
    conversationId: conversation.id,
    conversationTitle: conversation.title,
    role: "user",
    direction: "inbound",
    content: "The memory system depends on the retrieval pipeline.",
  });

  const agentCapture = await contextOS.ingestMessage({
    conversationId: conversation.id,
    conversationTitle: conversation.title,
    role: "assistant",
    direction: "outbound",
    content: "The memory system depends on the retrieval pipeline.",
    sourceMessageId: userCapture.message.id,
  });

  const importedCapture = await contextOS.ingestMessage({
    conversationId: conversation.id,
    conversationTitle: conversation.title,
    role: "system",
    direction: "inbound",
    originKind: "import",
    content: "Imported design note: ContextOS uses SQLite for persistence.",
  });

  persistHeuristicPatchForMessage(contextOS, userCapture);
  persistHeuristicPatchForMessage(contextOS, agentCapture);
  persistHeuristicPatchForMessage(contextOS, importedCapture);

  const storedMessages = contextOS.database.prepare(`
    SELECT id, origin_kind, source_message_id
    FROM messages
    WHERE id IN (?, ?, ?)
  `).all(userCapture.message.id, agentCapture.message.id, importedCapture.message.id);
  const storedById = new Map(storedMessages.map((row) => [row.id, row]));

  assert.equal(storedById.get(userCapture.message.id).origin_kind, "user");
  assert.equal(storedById.get(agentCapture.message.id).origin_kind, "agent");
  assert.equal(storedById.get(agentCapture.message.id).source_message_id, userCapture.message.id);
  assert.equal(storedById.get(importedCapture.message.id).origin_kind, "import");

  const result = await contextOS.retrieve({
    conversationId: conversation.id,
    queryText: "memory system",
  });

  const userObservation = result.items.find(
    (item) =>
      item.payload?.category === "relationship" &&
      item.payload.origin_kind === "user" &&
      item.summary === "memory system depends on retrieval pipeline",
  );
  const agentObservation = result.items.find(
    (item) =>
      item.payload?.category === "relationship" &&
      item.payload.origin_kind === "agent" &&
      item.summary === "memory system depends on retrieval pipeline",
  );

  assert.ok(userObservation);
  assert.ok(agentObservation);
  assert.ok(userObservation.score > agentObservation.score);
});
