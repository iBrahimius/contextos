import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";
import { handleRequest } from "../src/http/router.js";
import { persistPatchForMessage } from "./test-helpers.js";

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  return root;
}

test("actor identity is stored across ingests and API writes", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const conversation = contextOS.database.createConversation("Actor Identity");

  const authored = await contextOS.ingestMessage({
    conversationId: conversation.id,
    conversationTitle: conversation.title,
    actorId: "user:alice",
    role: "user",
    direction: "inbound",
    content: "ContextOS must keep SQLite local.",
  });

  const defaulted = await contextOS.ingestMessage({
    conversationId: conversation.id,
    conversationTitle: conversation.title,
    role: "user",
    direction: "inbound",
    content: "The memory system depends on the retrieval pipeline.",
  });

  persistPatchForMessage(contextOS, authored, {
    entities: [
      { label: "ContextOS", kind: "project", summary: "Local-first memory system." },
      { label: "SQLite", kind: "technology", summary: "Embedded database used by ContextOS." },
    ],
    observations: [
      {
        category: "constraint",
        detail: "ContextOS must keep SQLite local.",
        subjectLabel: "ContextOS",
        confidence: 0.92,
      },
    ],
    graphProposals: [
      {
        proposalType: "fact",
        subjectLabel: "ContextOS",
        detail: "SQLite remains local for ContextOS.",
        confidence: 0.45,
        reason: "Needs review before broadening the rule.",
      },
    ],
    retrieveHints: [],
    complexityAdjustments: [],
  });

  const storedMessage = contextOS.database.prepare(`
    SELECT actor_id
    FROM messages
    WHERE id = ?
  `).get(authored.message.id);
  const storedObservation = contextOS.database.prepare(`
    SELECT actor_id
    FROM observations
    WHERE message_id = ?
    LIMIT 1
  `).get(authored.message.id);
  const storedProposal = contextOS.database.prepare(`
    SELECT actor_id
    FROM graph_proposals
    WHERE message_id = ?
    LIMIT 1
  `).get(authored.message.id);
  const defaultMessage = contextOS.database.prepare(`
    SELECT actor_id
    FROM messages
    WHERE id = ?
  `).get(defaulted.message.id);

  assert.equal(storedMessage.actor_id, "user:alice");
  assert.equal(storedObservation.actor_id, "user:alice");
  assert.equal(storedProposal.actor_id, "user:alice");
  assert.equal(defaultMessage.actor_id, "system");

  const server = http.createServer((request, response) => handleRequest(contextOS, rootDir, request, response));
  await new Promise((resolve) => server.listen(0, resolve));

  try {
    const { port } = server.address();
    const response = await fetch(`http://127.0.0.1:${port}/api/messages`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({
        conversationId: conversation.id,
        actorId: "user:api",
        role: "user",
        direction: "inbound",
        content: "The proxy layer captures prompt injection attempts.",
      }),
    });

    assert.equal(response.status, 201);
    const payload = await response.json();
    const apiMessage = contextOS.database.prepare(`
      SELECT actor_id
      FROM messages
      WHERE id = ?
    `).get(payload.message.id);

    assert.equal(apiMessage.actor_id, "user:api");
  } finally {
    await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  }
});
