import { EMBEDDING_DIMENSIONS, embedText } from "../src/core/embeddings.js";

let embeddingSupportPromise = null;

export function persistPatchForMessage(contextOS, captureOrMessage, patch, overrides = {}) {
  const message = captureOrMessage?.message ?? captureOrMessage;

  return contextOS.persistKnowledgePatch({
    conversationId: overrides.conversationId ?? message.conversationId,
    messageId: message.id,
    patch,
    modelRuns: overrides.modelRuns ?? {},
    actorId: overrides.actorId ?? message.actorId ?? "system",
    scopeKind: overrides.scopeKind ?? message.scopeKind ?? "private",
    scopeId: overrides.scopeId ?? message.scopeId ?? null,
  });
}

export function persistHeuristicPatchForMessage(contextOS, captureOrMessage, overrides = {}) {
  const message = captureOrMessage?.message ?? captureOrMessage;
  const patch = overrides.patch ?? contextOS.buildHeuristicPatch(message.content);

  return persistPatchForMessage(contextOS, message, patch, overrides);
}

export async function embeddingsAvailable() {
  if (!embeddingSupportPromise) {
    embeddingSupportPromise = embedText("embedding probe").then(
      (vector) => vector instanceof Float32Array && vector.length === EMBEDDING_DIMENSIONS,
      () => false,
    );
  }

  return embeddingSupportPromise;
}
