import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";
import { estimateTokens } from "../src/core/utils.js";

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-retrieval-quality-"));
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

function insertObservationRecord(contextOS, conversationId, {
  messageId,
  category,
  detail,
  confidence = 0.9,
  subjectEntityId = null,
  objectEntityId = null,
  predicate = null,
  metadata = null,
}) {
  const observation = contextOS.database.insertObservation({
    conversationId,
    messageId,
    actorId: "seed",
    category,
    predicate,
    subjectEntityId,
    objectEntityId,
    detail,
    confidence,
    sourceSpan: detail,
    metadata,
    scopeKind: "private",
    scopeId: null,
  });
  contextOS.graph.updateGraphVersion(observation.graphVersion);

  if (category === "task") {
    contextOS.database.insertTask({
      observationId: observation.id,
      entityId: subjectEntityId ?? objectEntityId ?? null,
      title: detail,
      status: "open",
      priority: metadata?.priority ?? "medium",
    });
  }

  if (category === "decision") {
    contextOS.database.insertDecision({
      observationId: observation.id,
      entityId: subjectEntityId ?? objectEntityId ?? null,
      title: detail,
      rationale: metadata?.rationale ?? null,
    });
  }

  if (category === "constraint") {
    contextOS.database.insertConstraint({
      observationId: observation.id,
      entityId: subjectEntityId ?? objectEntityId ?? null,
      detail,
      severity: metadata?.severity ?? "high",
    });
  }

  if (category === "fact") {
    contextOS.database.insertFact({
      observationId: observation.id,
      entityId: subjectEntityId ?? objectEntityId ?? null,
      detail,
    });
  }

  return observation;
}

function seedGoldenQueryData(contextOS, conversationId) {
  const cloudflare = contextOS.graph.ensureEntity({ label: "Cloudflare", kind: "service" });
  const dns = contextOS.graph.ensureEntity({ label: "DNS", kind: "concept" });
  const ibrahim = contextOS.graph.ensureEntity({ label: "Ibrahim", kind: "person" });
  const pythonCortex = contextOS.graph.ensureEntity({ label: "Python Cortex", kind: "system" });
  const contextOsEntity = contextOS.graph.ensureEntity({ label: "ContextOS", kind: "system" });
  const modeld = contextOS.graph.ensureEntity({ label: "modeld", kind: "component" });
  const injectorRejector = contextOS.graph.ensureEntity({ label: "Injector/Rejector", kind: "component" });

  const dnsDecisionMessage = insertSeedMessage(
    contextOS,
    conversationId,
    "Decision log: Cloudflare is our standard provider for DNS management.",
    "golden_dns_decision",
  );
  insertSeedMessage(
    contextOS,
    conversationId,
    "Let me check the DNS settings.",
    "golden_dns_noise",
  );
  insertObservationRecord(contextOS, conversationId, {
    messageId: dnsDecisionMessage.id,
    category: "decision",
    detail: "We use Cloudflare for all DNS management",
    subjectEntityId: cloudflare.id,
    objectEntityId: dns.id,
    metadata: { rationale: "Single provider across environments" },
  });

  const ibrahimMessage = insertSeedMessage(
    contextOS,
    conversationId,
    "Ibrahim communication notes: one question at a time works best, but start by showing the full picture.",
    "golden_ibrahim_preferences",
  );
  insertSeedMessage(
    contextOS,
    conversationId,
    "Ibrahim is busy right now.",
    "golden_ibrahim_noise",
  );
  insertObservationRecord(contextOS, conversationId, {
    messageId: ibrahimMessage.id,
    category: "fact",
    detail: "Ibrahim prefers one question at a time to fully focus and give stronger answers",
    subjectEntityId: ibrahim.id,
  });
  insertObservationRecord(contextOS, conversationId, {
    messageId: ibrahimMessage.id,
    category: "fact",
    detail: "Show full picture first (summary + all questions), then focus on one question at a time",
    subjectEntityId: ibrahim.id,
  });

  const cortexDecisionMessage = insertSeedMessage(
    contextOS,
    conversationId,
    "Architecture decision: retire Python Cortex and move the bridge responsibilities into ContextOS Node.js.",
    "golden_cortex_decision",
  );
  insertSeedMessage(
    contextOS,
    conversationId,
    "Cortex is down again.",
    "golden_cortex_noise",
  );
  insertObservationRecord(contextOS, conversationId, {
    messageId: cortexDecisionMessage.id,
    category: "decision",
    detail: "Kill Python Cortex — one runtime, one language. ContextOS Node.js replaces the Python MCP bridge entirely",
    subjectEntityId: pythonCortex.id,
    objectEntityId: contextOsEntity.id,
    metadata: { rationale: "Remove the Python bridge" },
  });

  const modeldMessage = insertSeedMessage(
    contextOS,
    conversationId,
    "modeld notes: Qwen 3 8B runs through Ollama, with async Haiku enrichment after ingest.",
    "golden_modeld_facts",
  );
  insertObservationRecord(contextOS, conversationId, {
    messageId: modeldMessage.id,
    category: "fact",
    detail: "modeld uses Qwen 3 8B via Ollama for extraction and alias resolution",
    subjectEntityId: modeld.id,
  });
  insertObservationRecord(contextOS, conversationId, {
    messageId: modeldMessage.id,
    category: "fact",
    detail: "Haiku classifier runs asynchronously for enrichment after ingest",
    subjectEntityId: modeld.id,
  });

  const securityMessage = insertSeedMessage(
    contextOS,
    conversationId,
    "Security defaults note: both proxy directions block on timeout.",
    "golden_security_defaults",
  );
  insertObservationRecord(contextOS, conversationId, {
    messageId: securityMessage.id,
    category: "constraint",
    detail: "Both inbound and outbound proxy directions default to BLOCK on timeout — security defaults to closed",
    subjectEntityId: injectorRejector.id,
    metadata: { severity: "high" },
  });
}

function topThreeContainsObservation(results, category, detail) {
  return results.items
    .slice(0, 3)
    .some((item) => item.payload?.category === category && item.summary === detail);
}

test("golden retrieval queries surface the expected observations in the top 3", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({
    rootDir,
    autoBackfillEmbeddings: false,
    
  });
  const conversation = contextOS.database.createConversation("Retrieval Quality");

  try {
    seedGoldenQueryData(contextOS, conversation.id);

    await contextOS.backfillEmbeddings({
      batchSize: 20,
      rateLimitPerSecond: 20,
    });

    const dnsResults = await contextOS.retrieve({
      conversationId: conversation.id,
      queryText: "What DNS provider do we use?",
    });
    assert.ok(topThreeContainsObservation(
      dnsResults,
      "decision",
      "We use Cloudflare for all DNS management",
    ));

    const ibrahimResults = await contextOS.retrieve({
      conversationId: conversation.id,
      queryText: "Ibrahim's communication preferences",
    });
    assert.ok(topThreeContainsObservation(
      ibrahimResults,
      "fact",
      "Ibrahim prefers one question at a time to fully focus and give stronger answers",
    ));
    assert.ok(topThreeContainsObservation(
      ibrahimResults,
      "fact",
      "Show full picture first (summary + all questions), then focus on one question at a time",
    ));

    const cortexResults = await contextOS.retrieve({
      conversationId: conversation.id,
      queryText: "Why did we kill Python Cortex?",
    });
    assert.ok(topThreeContainsObservation(
      cortexResults,
      "decision",
      "Kill Python Cortex — one runtime, one language. ContextOS Node.js replaces the Python MCP bridge entirely",
    ));

    const modeldResults = await contextOS.retrieve({
      conversationId: conversation.id,
      queryText: "What model does modeld use?",
    });
    assert.ok(topThreeContainsObservation(
      modeldResults,
      "fact",
      "modeld uses Qwen 3 8B via Ollama for extraction and alias resolution",
    ));

    const securityResults = await contextOS.retrieve({
      conversationId: conversation.id,
      queryText: "Security defaults",
    });
    assert.ok(topThreeContainsObservation(
      securityResults,
      "constraint",
      "Both inbound and outbound proxy directions default to BLOCK on timeout — security defaults to closed",
    ));
  } finally {
    contextOS.database.close();
    contextOS.telemetry.close();
    await fs.rm(rootDir, { recursive: true, force: true });
  }
});
