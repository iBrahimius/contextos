import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";
import { persistPatchForMessage } from "./test-helpers.js";

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  return root;
}

test("explicit patch persistence stores retrieval hints without ingest-time model stages", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const conversation = contextOS.database.createConversation("Model Pipeline");

  const answerCapture = await contextOS.ingestMessage({
    conversationId: conversation.id,
    conversationTitle: conversation.title,
    role: "assistant",
    direction: "outbound",
    content: "The memory system depends on the retrieval pipeline and storage layer.",
  });

  const patch = {
    ...contextOS.buildHeuristicPatch(answerCapture.message.content),
    retrieveHints: [
      {
        seed: "memory system",
        expandTo: "retrieval pipeline",
        reason: "Depends on edge observed live",
        weight: 1.1,
        ttlTurns: 4,
      },
    ],
    graphProposals: [
      {
        proposalType: "relationship",
        subjectLabel: "memory system",
        predicate: "depends_on",
        objectLabel: "storage layer",
        detail: "memory system depends on storage layer",
        confidence: 0.45,
        reason: "Low-confidence explicit follow-up",
      },
    ],
  };
  persistPatchForMessage(contextOS, answerCapture, patch);

  await contextOS.ingestMessage({
    conversationId: conversation.id,
    conversationTitle: conversation.title,
    role: "user",
    direction: "inbound",
    content: "What does the memory system depend on?",
  });

  const labels = contextOS.graph.listEntities().map((entity) => entity.label);
  assert.ok(labels.includes("memory system"));
  assert.ok(!labels.includes("What does the memory system"));

  const modelRuns = contextOS.telemetry.listRecentModelRuns(10);
  assert.equal(modelRuns.length, 0);

  const hints = contextOS.telemetry.listActiveRetrievalHints(10);
  assert.ok(hints.some((hint) => hint.seed_label === "memory system" && hint.expand_label === "retrieval pipeline"));

  const proposals = contextOS.database.listRecentGraphProposals(10);
  assert.ok(proposals.some((proposal) => proposal.message_id === answerCapture.message.id && proposal.status === "pending"));
});
