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

function isKnowledgeItem(item) {
  return Boolean(item.payload?.category || item.payload?.claim_type);
}

test("scope model stores message scope and filters retrieval evidence without hiding entities", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const conversation = contextOS.database.createConversation("Scope Model");

  const privateCapture = await contextOS.ingestMessage({
    conversationId: conversation.id,
    conversationTitle: conversation.title,
    role: "user",
    direction: "inbound",
    content: "The memory system depends on the retrieval pipeline.",
    scopeKind: "private",
  });

  const sharedCapture = await contextOS.ingestMessage({
    conversationId: conversation.id,
    conversationTitle: conversation.title,
    role: "user",
    direction: "inbound",
    content: "The memory system stores data in the knowledge base.",
    scopeKind: "shared",
  });

  persistHeuristicPatchForMessage(contextOS, privateCapture);
  persistHeuristicPatchForMessage(contextOS, sharedCapture);

  const storedMessages = contextOS.database.prepare(`
    SELECT id, scope_kind
    FROM messages
    WHERE id IN (?, ?)
  `).all(privateCapture.message.id, sharedCapture.message.id);

  const scopeByMessageId = new Map(storedMessages.map((row) => [row.id, row.scope_kind]));
  assert.equal(scopeByMessageId.get(privateCapture.message.id), "private");
  assert.equal(scopeByMessageId.get(sharedCapture.message.id), "shared");

  const sharedScoped = await contextOS.retrieve({
    conversationId: conversation.id,
    queryText: "memory system",
    scopeFilter: "shared",
  });
  const unfiltered = await contextOS.retrieve({
    conversationId: conversation.id,
    queryText: "memory system",
  });

  const sharedScopedMessageIds = new Set(
    sharedScoped.items
      .filter(isKnowledgeItem)
      .map((item) => item.payload.message_id)
      .filter(Boolean),
  );
  const unfilteredMessageIds = new Set(
    unfiltered.items
      .filter(isKnowledgeItem)
      .map((item) => item.payload.message_id)
      .filter(Boolean),
  );

  assert.ok(sharedScopedMessageIds.has(sharedCapture.message.id));
  assert.ok(!sharedScopedMessageIds.has(privateCapture.message.id));
  assert.ok(unfilteredMessageIds.has(sharedCapture.message.id));
  assert.ok(unfilteredMessageIds.has(privateCapture.message.id));

  assert.ok(sharedScoped.expandedEntities.some((entity) => entity.label.toLowerCase() === "memory system"));
});
