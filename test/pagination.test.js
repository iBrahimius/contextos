import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";
import { decodeCursor, encodeCursor } from "../src/core/pagination.js";

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  return root;
}

test("pagination cursors round-trip and invalid cursors return null", () => {
  const cursor = encodeCursor({ value: "2026-03-06T12:00:00.000Z" });

  assert.deepEqual(decodeCursor(cursor), { value: "2026-03-06T12:00:00.000Z" });
  assert.equal(decodeCursor(null), null);
  assert.equal(decodeCursor("garbage"), null);
});

test("messages paginate with cursor-based results and preserve flat-array compatibility", async () => {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const conversation = contextOS.database.createConversation("Pagination");

  for (let index = 0; index < 5; index += 1) {
    await contextOS.ingestMessage({
      conversationId: conversation.id,
      conversationTitle: conversation.title,
      role: "user",
      direction: "inbound",
      content: `message ${index + 1}`,
    });

    await new Promise((resolve) => setTimeout(resolve, 5));
  }

  const firstPage = contextOS.database.listMessages(conversation.id, { limit: 2 });
  assert.equal(firstPage.items.length, 2);
  assert.equal(firstPage.hasMore, true);
  assert.ok(firstPage.nextCursor);
  assert.deepEqual(firstPage.items.map((item) => item.content), ["message 1", "message 2"]);

  const secondPage = contextOS.database.listMessages(conversation.id, {
    cursor: firstPage.nextCursor,
    limit: 2,
  });
  assert.equal(secondPage.items.length, 2);
  assert.equal(secondPage.hasMore, true);
  assert.ok(secondPage.nextCursor);
  assert.deepEqual(secondPage.items.map((item) => item.content), ["message 3", "message 4"]);

  const thirdPage = contextOS.database.listMessages(conversation.id, {
    cursor: secondPage.nextCursor,
    limit: 2,
  });
  assert.equal(thirdPage.items.length, 1);
  assert.equal(thirdPage.hasMore, false);
  assert.equal(thirdPage.nextCursor, null);
  assert.deepEqual(thirdPage.items.map((item) => item.content), ["message 5"]);

  const allMessages = contextOS.database.listMessages(conversation.id);
  assert.ok(Array.isArray(allMessages));
  assert.equal(allMessages.length, 5);
  assert.deepEqual(allMessages.map((item) => item.content), [
    "message 1",
    "message 2",
    "message 3",
    "message 4",
    "message 5",
  ]);
});
