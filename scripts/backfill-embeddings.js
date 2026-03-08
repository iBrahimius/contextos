import { ContextOS } from "../src/core/context-os.js";

const rootDir = process.cwd();
const contextOS = new ContextOS({
  rootDir,
  autoBackfillEmbeddings: false,
});

try {
  const before = contextOS.database.getEmbeddingCoverage();
  console.log(
    `[embeddings] Starting backfill in ${rootDir} (${before.embedded}/${before.total} embedded, ${before.coverage}% coverage)`,
  );

  const result = await contextOS.backfillEmbeddings({
    batchSize: 10,
    rateLimitPerSecond: 10,
    logProgress: true,
  });

  console.log(
    `[embeddings] Completed backfill: ${result.embedded}/${result.total} embedded, ${result.coverage}% coverage`,
  );
  console.log(`[embeddings] Stored ${result.processed} new embeddings`);
} finally {
  contextOS.database.close();
  contextOS.telemetry.close();
}
