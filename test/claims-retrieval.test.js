import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  await fs.mkdir(path.join(root, "docs"), { recursive: true });
  return root;
}

test("claims returned when matching entity exists", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const conversation = contextOS.database.createConversation("Claims Test");

  const dnsProvider = contextOS.graph.ensureEntity({ label: "DNS Provider", kind: "infrastructure" });
  const cloudflare = contextOS.graph.ensureEntity({ label: "Cloudflare", kind: "vendor" });

  const message = contextOS.database.insertMessage({
    conversationId: conversation.id,
    role: "user",
    direction: "inbound",
    content: "We use Cloudflare as our DNS provider.",
    tokenCount: 10,
  });

  const observation = contextOS.database.insertObservation({
    messageId: message.id,
    conversationId: conversation.id,
    category: "fact",
    detail: "Cloudflare is the DNS provider",
    subjectEntityId: dnsProvider.id,
    objectEntityId: cloudflare.id,
    scopeKind: "private",
  });

  const claim = contextOS.database.insertClaim({
    observationId: observation.id,
    conversationId: conversation.id,
    claimType: "fact",
    subjectEntityId: dnsProvider.id,
    objectEntityId: cloudflare.id,
    valueText: "Cloudflare",
    confidence: 0.95,
    lifecycleState: "active",
    importanceScore: 0.8,
    scopeKind: "private",
  });

  assert.ok(claim.id);
  assert.equal(claim.lifecycle_state, "active");

  const result = await contextOS.retrieve({
    conversationId: conversation.id,
    queryText: "DNS provider",
  });

  const claimResults = result.items.filter((r) => r.type === "claim");
  assert.ok(claimResults.length > 0, "Should have claim results");
  const foundClaim = claimResults.find((r) => r.id === claim.id);
  assert.ok(foundClaim, "Should find the inserted claim");
  assert.equal(foundClaim.summary, "Cloudflare");
});

test("candidate claims ≥0.7 confidence included, <0.7 excluded", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const conversation = contextOS.database.createConversation("Candidate Test");

  const entity = contextOS.graph.ensureEntity({ label: "TestEntity", kind: "test" });

  const message = contextOS.database.insertMessage({
    conversationId: conversation.id,
    role: "user",
    direction: "inbound",
    content: "Test content",
    tokenCount: 3,
  });

  const observation = contextOS.database.insertObservation({
    messageId: message.id,
    conversationId: conversation.id,
    category: "fact",
    detail: "Test detail",
    subjectEntityId: entity.id,
    scopeKind: "private",
  });

  // Create separate observations for each claim (unique constraint)
  const observation2 = contextOS.database.insertObservation({
    messageId: message.id,
    conversationId: conversation.id,
    category: "fact",
    detail: "Test detail 2",
    subjectEntityId: entity.id,
    scopeKind: "private",
  });

  const highConfidence = contextOS.database.insertClaim({
    observationId: observation.id,
    conversationId: conversation.id,
    claimType: "fact",
    subjectEntityId: entity.id,
    valueText: "High confidence",
    confidence: 0.8,
    lifecycleState: "candidate",
    importanceScore: 0.5,
    scopeKind: "private",
  });

  const lowConfidence = contextOS.database.insertClaim({
    observationId: observation2.id,
    conversationId: conversation.id,
    claimType: "fact",
    subjectEntityId: entity.id,
    valueText: "Low confidence",
    confidence: 0.5,
    lifecycleState: "candidate",
    importanceScore: 0.5,
    scopeKind: "private",
  });

  const result = await contextOS.retrieve({
    conversationId: conversation.id,
    queryText: "TestEntity",
  });

  const claimResults = result.items.filter((r) => r.type === "claim");
  const hasHigh = claimResults.some((r) => r.id === highConfidence.id);
  const hasLow = claimResults.some((r) => r.id === lowConfidence.id);

  assert.ok(hasHigh, "High confidence candidate should be included");
  assert.equal(hasLow, false, "Low confidence candidate should be excluded");
});

test("active claims score > candidate claims", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const conversation = contextOS.database.createConversation("Scoring Test");

  const entity = contextOS.graph.ensureEntity({ label: "ScoringEntity", kind: "test" });

  const message = contextOS.database.insertMessage({
    conversationId: conversation.id,
    role: "user",
    direction: "inbound",
    content: "Test",
    tokenCount: 2,
  });

  const observation = contextOS.database.insertObservation({
    messageId: message.id,
    conversationId: conversation.id,
    category: "fact",
    detail: "Detail",
    subjectEntityId: entity.id,
    scopeKind: "private",
  });

  const observation2 = contextOS.database.insertObservation({
    messageId: message.id,
    conversationId: conversation.id,
    category: "fact",
    detail: "Detail 2",
    subjectEntityId: entity.id,
    scopeKind: "private",
  });

  const activeClaim = contextOS.database.insertClaim({
    observationId: observation.id,
    conversationId: conversation.id,
    claimType: "fact",
    subjectEntityId: entity.id,
    valueText: "Active",
    confidence: 0.8,
    lifecycleState: "active",
    importanceScore: 0.5,
    scopeKind: "private",
  });

  const candidateClaim = contextOS.database.insertClaim({
    observationId: observation2.id,
    conversationId: conversation.id,
    claimType: "fact",
    subjectEntityId: entity.id,
    valueText: "Candidate",
    confidence: 0.8,
    lifecycleState: "candidate",
    importanceScore: 0.5,
    scopeKind: "private",
  });

  const claimResults = contextOS.retrieval.retrieveClaims([
    { id: entity.id, label: entity.label },
  ]);
  const active = claimResults.find((r) => r.id === activeClaim.id);
  const candidate = claimResults.find((r) => r.id === candidateClaim.id);

  assert.ok(active, "Active claim should be in results");
  assert.ok(candidate, "Candidate claim should be in results");
  assert.ok(active.score > candidate.score, "Active score should be higher");
});

test("clean active truth still outranks a higher-confidence candidate", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const conversation = contextOS.database.createConversation("Candidate Uncertainty Test");

  const entity = contextOS.graph.ensureEntity({ label: "RankingEntity", kind: "test" });

  const message = contextOS.database.insertMessage({
    conversationId: conversation.id,
    role: "user",
    direction: "inbound",
    content: "Ranking check",
    tokenCount: 2,
  });

  const activeObservation = contextOS.database.insertObservation({
    messageId: message.id,
    conversationId: conversation.id,
    category: "fact",
    detail: "Settled truth",
    subjectEntityId: entity.id,
    scopeKind: "private",
  });
  const candidateObservation = contextOS.database.insertObservation({
    messageId: message.id,
    conversationId: conversation.id,
    category: "fact",
    detail: "Still uncertain",
    subjectEntityId: entity.id,
    scopeKind: "private",
  });

  const activeClaim = contextOS.database.insertClaim({
    observationId: activeObservation.id,
    conversationId: conversation.id,
    claimType: "fact",
    subjectEntityId: entity.id,
    valueText: "Settled truth",
    confidence: 0.8,
    lifecycleState: "active",
    importanceScore: 0.5,
    scopeKind: "private",
  });
  const candidateClaim = contextOS.database.insertClaim({
    observationId: candidateObservation.id,
    conversationId: conversation.id,
    claimType: "fact",
    subjectEntityId: entity.id,
    valueText: "Still uncertain",
    confidence: 0.99,
    lifecycleState: "candidate",
    importanceScore: 0.5,
    scopeKind: "private",
  });

  const claimResults = contextOS.retrieval.retrieveClaims([{ id: entity.id, label: entity.label }]);
  const active = claimResults.find((result) => result.id === activeClaim.id);
  const candidate = claimResults.find((result) => result.id === candidateClaim.id);

  assert.ok(active);
  assert.ok(candidate);
  assert.ok(candidate.payload.truth.effective_confidence > active.payload.truth.effective_confidence);
  assert.ok(active.score > candidate.score, "Candidate uncertainty must not outrank clean active truth");

  await contextOS.close();
  await fs.rm(rootDir, { recursive: true, force: true });
});

test("claims appear in final retrieve() output", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const conversation = contextOS.database.createConversation("Output Test");

  const pricing = contextOS.graph.ensureEntity({ label: "Product Pricing", kind: "business" });

  const message = contextOS.database.insertMessage({
    conversationId: conversation.id,
    role: "user",
    direction: "inbound",
    content: "Priced at €34.99",
    tokenCount: 5,
  });

  const observation = contextOS.database.insertObservation({
    messageId: message.id,
    conversationId: conversation.id,
    category: "fact",
    detail: "Price €34.99",
    subjectEntityId: pricing.id,
    scopeKind: "private",
  });

  const claim = contextOS.database.insertClaim({
    observationId: observation.id,
    conversationId: conversation.id,
    claimType: "fact",
    subjectEntityId: pricing.id,
    valueText: "€34.99",
    confidence: 0.9,
    lifecycleState: "active",
    importanceScore: 0.7,
    scopeKind: "private",
  });

  const result = await contextOS.retrieve({
    conversationId: conversation.id,
    queryText: "product pricing",
  });

  const claimResults = result.items.filter((r) => r.type === "claim");
  assert.ok(claimResults.length > 0, "Should have claim results");

  const found = claimResults.find((r) => r.id === claim.id);
  assert.ok(found, "Specific claim should be in results");
  assert.ok(found.rank, "Claim should have rank");
  assert.ok(Number.isFinite(found.score), "Score should be numeric");
  assert.equal(found.source, "claims");
});

test("claim-backed facts dedupe overlapping legacy fact results", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const conversation = contextOS.database.createConversation("Dedup Test");

  const pricing = contextOS.graph.ensureEntity({ label: "Product Pricing", kind: "business" });

  const message = contextOS.database.insertMessage({
    conversationId: conversation.id,
    role: "user",
    direction: "inbound",
    content: "The product price is €34.99.",
    tokenCount: 6,
  });

  const observation = contextOS.database.insertObservation({
    messageId: message.id,
    conversationId: conversation.id,
    category: "fact",
    detail: "The product price is €34.99.",
    subjectEntityId: pricing.id,
    scopeKind: "private",
  });

  const factId = contextOS.database.insertFact({
    observationId: observation.id,
    entityId: pricing.id,
    detail: "The product price is €34.99.",
  });

  const claim = contextOS.database.insertClaim({
    observationId: observation.id,
    conversationId: conversation.id,
    claimType: "fact",
    subjectEntityId: pricing.id,
    valueText: "The product price is €34.99.",
    confidence: 0.95,
    lifecycleState: "active",
    importanceScore: 0.8,
    scopeKind: "private",
  });

  const result = await contextOS.retrieve({
    conversationId: conversation.id,
    queryText: "product pricing",
  });

  assert.ok(result.items.some((item) => item.type === "claim" && item.id === claim.id));
  assert.equal(
    result.items.some((item) => item.type === "fact" && item.id === factId),
    false,
    "Overlapping legacy fact should be removed when claim exists",
  );
});

test("dedupe tolerates punctuation/whitespace differences between claim and legacy fact", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const conversation = contextOS.database.createConversation("Dedup Normalization Test");

  const dnsProvider = contextOS.graph.ensureEntity({ label: "DNS Provider", kind: "infrastructure" });

  const message = contextOS.database.insertMessage({
    conversationId: conversation.id,
    role: "user",
    direction: "inbound",
    content: "Cloudflare is our DNS provider",
    tokenCount: 5,
  });

  const observation = contextOS.database.insertObservation({
    messageId: message.id,
    conversationId: conversation.id,
    category: "fact",
    detail: "Cloudflare is our DNS provider",
    subjectEntityId: dnsProvider.id,
    scopeKind: "private",
  });

  const factId = contextOS.database.insertFact({
    observationId: observation.id,
    entityId: dnsProvider.id,
    detail: "Cloudflare   is our DNS provider!!!",
  });

  const claim = contextOS.database.insertClaim({
    observationId: observation.id,
    conversationId: conversation.id,
    claimType: "fact",
    subjectEntityId: dnsProvider.id,
    valueText: "Cloudflare is our DNS provider",
    confidence: 0.92,
    lifecycleState: "active",
    importanceScore: 0.7,
    scopeKind: "private",
  });

  const result = await contextOS.retrieve({
    conversationId: conversation.id,
    queryText: "dns provider",
  });

  assert.ok(result.items.some((item) => item.type === "claim" && item.id === claim.id));
  assert.equal(
    result.items.some((item) => item.type === "fact" && item.id === factId),
    false,
    "Normalized overlap should still dedupe the legacy fact result",
  );
});


test("retrieveClaims exposes conflict state and aggregated support confidence", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const conversation = contextOS.database.createConversation("Conflict Retrieval Test");

  const dns = contextOS.graph.ensureEntity({ label: "DNS", kind: "infrastructure" });
  const cloudflare = contextOS.graph.ensureEntity({ label: "Cloudflare", kind: "vendor" });
  const route53 = contextOS.graph.ensureEntity({ label: "Route53", kind: "vendor" });

  const message1 = contextOS.database.insertMessage({
    conversationId: conversation.id,
    role: "user",
    direction: "inbound",
    content: "Cloudflare is the DNS provider.",
    tokenCount: 6,
  });
  const message2 = contextOS.database.insertMessage({
    conversationId: conversation.id,
    role: "user",
    direction: "inbound",
    content: "Confirmed again: Cloudflare.",
    tokenCount: 4,
  });
  const message3 = contextOS.database.insertMessage({
    conversationId: conversation.id,
    role: "user",
    direction: "inbound",
    content: "Actually Route53 handles DNS.",
    tokenCount: 5,
  });

  const observation1 = contextOS.database.insertObservation({
    messageId: message1.id,
    conversationId: conversation.id,
    category: "fact",
    predicate: "provider",
    detail: "Cloudflare",
    subjectEntityId: dns.id,
    objectEntityId: cloudflare.id,
    scopeKind: "private",
  });
  const observation2 = contextOS.database.insertObservation({
    messageId: message2.id,
    conversationId: conversation.id,
    category: "fact",
    predicate: "provider",
    detail: "Cloudflare",
    subjectEntityId: dns.id,
    objectEntityId: cloudflare.id,
    scopeKind: "private",
  });
  const observation3 = contextOS.database.insertObservation({
    messageId: message3.id,
    conversationId: conversation.id,
    category: "fact",
    predicate: "provider",
    detail: "Route53",
    subjectEntityId: dns.id,
    objectEntityId: route53.id,
    scopeKind: "private",
  });

  const cloudflareClaim1 = contextOS.database.insertClaim({
    observationId: observation1.id,
    conversationId: conversation.id,
    messageId: message1.id,
    claimType: "fact",
    subjectEntityId: dns.id,
    objectEntityId: cloudflare.id,
    predicate: "provider",
    valueText: "Cloudflare",
    confidence: 0.7,
    lifecycleState: "disputed",
    facetKey: `fact|${dns.id}|provider|${cloudflare.id}|cloudflare`,
    resolutionKey: `fact:${dns.id}:provider`,
    scopeKind: "private",
  });
  const cloudflareClaim2 = contextOS.database.insertClaim({
    observationId: observation2.id,
    conversationId: conversation.id,
    messageId: message2.id,
    claimType: "fact",
    subjectEntityId: dns.id,
    objectEntityId: cloudflare.id,
    predicate: "provider",
    valueText: "Cloudflare",
    confidence: 0.8,
    lifecycleState: "disputed",
    facetKey: `fact|${dns.id}|provider|${cloudflare.id}|cloudflare`,
    resolutionKey: `fact:${dns.id}:provider`,
    scopeKind: "private",
  });
  const route53Claim = contextOS.database.insertClaim({
    observationId: observation3.id,
    conversationId: conversation.id,
    messageId: message3.id,
    claimType: "fact",
    subjectEntityId: dns.id,
    objectEntityId: route53.id,
    predicate: "provider",
    valueText: "Route53",
    confidence: 0.85,
    lifecycleState: "disputed",
    facetKey: `fact|${dns.id}|provider|${route53.id}|route53`,
    resolutionKey: `fact:${dns.id}:provider`,
    scopeKind: "private",
  });

  const results = contextOS.retrieval.retrieveClaims([{ id: dns.id, label: dns.label }]);
  const cloudflareResult = results.find((result) => result.summary === "Cloudflare");
  const route53Result = results.find((result) => result.summary === "Route53");

  assert.equal(results.length, 2);
  assert.ok(cloudflareResult);
  assert.ok(route53Result);
  assert.equal(cloudflareResult.payload.truth.support_count, 2);
  assert.equal(cloudflareResult.payload.truth.has_conflict, true);
  assert.equal(cloudflareResult.payload.truth.aggregated_confidence, 0.94);
  assert.deepEqual(cloudflareResult.payload.truth.support_claim_ids.sort(), [cloudflareClaim1.id, cloudflareClaim2.id].sort());
  assert.deepEqual(cloudflareResult.payload.truth.current_claim_ids.sort(), [cloudflareClaim1.id, cloudflareClaim2.id].sort());
  assert.deepEqual(cloudflareResult.payload.truth.conflicting_claim_ids, [route53Claim.id]);
  assert.equal(cloudflareResult.payload.truth.representative_claim_id, cloudflareClaim2.id);
  assert.equal(route53Result.payload.truth.has_conflict, true);
  assert.equal(route53Result.payload.truth.conflict_set_id, cloudflareResult.payload.truth.conflict_set_id);

  await contextOS.close();
  await fs.rm(rootDir, { recursive: true, force: true });
});
