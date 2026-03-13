/**
 * Shared skip guards for tests that depend on runtime services.
 * Import and use with node:test's `skip` option:
 *
 *   import { skipUnlessEmbeddings } from "./skip-guards.js";
 *   test("needs embeddings", { skip: skipUnlessEmbeddings }, async () => { ... });
 */
import { existsSync } from "node:fs";
import { EMBEDDING_MODEL_PATH } from "../src/core/embeddings.js";

export const hasEmbeddingModel = existsSync(EMBEDDING_MODEL_PATH);

/**
 * Pass as `{ skip: skipUnlessEmbeddings }` to skip when embedding model is absent.
 * Returns false (don't skip) when model exists, or a reason string when it doesn't.
 */
export const skipUnlessEmbeddings = hasEmbeddingModel
  ? false
  : `embedding model not found at ${EMBEDDING_MODEL_PATH}`;

/**
 * Check if ContextOS is running on localhost:4183 (for live-service tests).
 * This is async — call once at module level and use the result.
 */
export async function checkContextOSRunning(url = "http://localhost:4183") {
  try {
    const res = await fetch(`${url}/api/health`, { signal: AbortSignal.timeout(2000) });
    return res.ok;
  } catch {
    return false;
  }
}
