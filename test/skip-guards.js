/**
 * Shared skip guards for tests that depend on runtime services.
 * Import and use with node:test's `skip` option:
 *
 *   import { skipUnlessEmbeddings } from "./skip-guards.js";
 *   test("needs embeddings", { skip: skipUnlessEmbeddings }, async () => { ... });
 */
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { EMBEDDING_MODEL_PATH } from "../src/core/embeddings.js";

const require = createRequire(import.meta.url);

export const hasEmbeddingModel = existsSync(EMBEDDING_MODEL_PATH);
export const hasEmbeddingRuntime = (() => {
  try {
    require.resolve("node-llama-cpp");
    return true;
  } catch {
    return false;
  }
})();

/**
 * Pass as `{ skip: skipUnlessEmbeddings }` to skip when the embedding model or
 * runtime package is unavailable.
 * Returns false (don't skip) when embeddings can run, or a reason string when
 * the environment is missing a required dependency.
 */
export const skipUnlessEmbeddings = hasEmbeddingModel && hasEmbeddingRuntime
  ? false
  : hasEmbeddingModel
    ? "node-llama-cpp package is unavailable"
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
