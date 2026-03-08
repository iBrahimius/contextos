import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  return root;
}

test("graph version increments monotonically across graph mutations", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });

  assert.equal(contextOS.database.getGraphVersion(), 0);
  assert.equal(contextOS.graph.getGraphVersion(), 0);

  const memorySystem = contextOS.graph.ensureEntity({ label: "memory system", kind: "component" });
  assert.equal(contextOS.database.getGraphVersion(), 1);
  assert.equal(contextOS.graph.getGraphVersion(), 1);

  const retrievalPipeline = contextOS.graph.ensureEntity({ label: "retrieval pipeline", kind: "component" });
  const versionAfterSecondEntity = contextOS.database.getGraphVersion();
  assert.ok(versionAfterSecondEntity > 1);

  contextOS.graph.connect({
    subjectEntityId: memorySystem.id,
    predicate: "depends_on",
    objectEntityId: retrievalPipeline.id,
    weight: 1,
  });

  const versionAfterRelationship = contextOS.database.getGraphVersion();
  assert.ok(versionAfterRelationship > versionAfterSecondEntity);
  assert.equal(contextOS.graph.getGraphVersion(), versionAfterRelationship);

  const retrieval = await contextOS.retrieve({
    queryText: "memory system",
  });
  assert.equal(retrieval.graphVersion, versionAfterRelationship);

  const versionBeforeRapidMutations = contextOS.database.getGraphVersion();
  contextOS.graph.ensureEntity({ label: "dashboard service", kind: "component" });
  const versionAfterThirdEntity = contextOS.database.getGraphVersion();
  contextOS.graph.ensureEntity({ label: "proxy layer", kind: "component" });
  const versionAfterFourthEntity = contextOS.database.getGraphVersion();

  assert.ok(versionAfterThirdEntity > versionBeforeRapidMutations);
  assert.ok(versionAfterFourthEntity > versionAfterThirdEntity);
  assert.equal(contextOS.graph.getGraphVersion(), versionAfterFourthEntity);
});
