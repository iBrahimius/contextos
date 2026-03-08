import test from "node:test";
import assert from "node:assert/strict";

import { PreconsciousBuffer } from "../src/core/preconscious.js";

// ── push / poll / peek ───────────────────────────────────────────────

test("PreconsciousBuffer — push adds alerts with buffered_at and delivered=false", () => {
  const buf = new PreconsciousBuffer();
  buf.push({ type: "task_blocked", detail: "DNS issue" });

  assert.equal(buf.buffer.length, 1);
  assert.equal(buf.buffer[0].type, "task_blocked");
  assert.equal(buf.buffer[0].detail, "DNS issue");
  assert.equal(buf.buffer[0].delivered, false);
  assert.ok(typeof buf.buffer[0].buffered_at === "string");
});

test("PreconsciousBuffer — poll returns undelivered alerts", () => {
  const buf = new PreconsciousBuffer();
  buf.push({ type: "task_blocked", detail: "A" });
  buf.push({ type: "new_constraint", detail: "B" });

  const results = buf.poll();
  assert.equal(results.length, 2);
  assert.equal(results[0].type, "task_blocked");
  assert.equal(results[1].type, "new_constraint");
});

test("PreconsciousBuffer — poll marks alerts as delivered", () => {
  const buf = new PreconsciousBuffer();
  buf.push({ type: "task_blocked" });
  buf.push({ type: "decision_superseded" });

  buf.poll();

  assert.equal(buf.buffer[0].delivered, true);
  assert.equal(buf.buffer[1].delivered, true);
});

test("PreconsciousBuffer — second poll returns only new alerts", () => {
  const buf = new PreconsciousBuffer();
  buf.push({ type: "alert_1" });
  buf.poll(); // marks alert_1 as delivered

  buf.push({ type: "alert_2" });
  const results = buf.poll();

  assert.equal(results.length, 1);
  assert.equal(results[0].type, "alert_2");
});

test("PreconsciousBuffer — peek returns count of undelivered without marking delivered", () => {
  const buf = new PreconsciousBuffer();
  buf.push({ type: "a" });
  buf.push({ type: "b" });
  buf.push({ type: "c" });

  const count = buf.peek();
  assert.equal(count, 3);

  // Alerts should still be undelivered
  assert.equal(buf.buffer.every((a) => !a.delivered), true);
});

test("PreconsciousBuffer — peek after poll reflects only new alerts", () => {
  const buf = new PreconsciousBuffer();
  buf.push({ type: "x" });
  buf.poll(); // delivered

  buf.push({ type: "y" });
  assert.equal(buf.peek(), 1);
});

test("PreconsciousBuffer — clear empties the buffer", () => {
  const buf = new PreconsciousBuffer();
  buf.push({ type: "a" });
  buf.push({ type: "b" });

  buf.clear();

  assert.equal(buf.buffer.length, 0);
  assert.equal(buf.peek(), 0);
  assert.deepEqual(buf.poll(), []);
});

// ── buffer overflow ──────────────────────────────────────────────────

test("PreconsciousBuffer — overflow evicts oldest alert", () => {
  const buf = new PreconsciousBuffer(3);

  buf.push({ type: "first" });
  buf.push({ type: "second" });
  buf.push({ type: "third" });
  buf.push({ type: "fourth" }); // should evict "first"

  assert.equal(buf.buffer.length, 3);
  assert.equal(buf.buffer[0].type, "second");
  assert.equal(buf.buffer[2].type, "fourth");
});

test("PreconsciousBuffer — overflow with maxSize=1 keeps only latest", () => {
  const buf = new PreconsciousBuffer(1);

  buf.push({ type: "old" });
  buf.push({ type: "new" });

  assert.equal(buf.buffer.length, 1);
  assert.equal(buf.buffer[0].type, "new");
});

test("PreconsciousBuffer — at exactly maxSize, no eviction occurs", () => {
  const buf = new PreconsciousBuffer(2);

  buf.push({ type: "a" });
  buf.push({ type: "b" });

  assert.equal(buf.buffer.length, 2);
  assert.equal(buf.buffer[0].type, "a");
  assert.equal(buf.buffer[1].type, "b");
});

// ── default maxSize ───────────────────────────────────────────────────

test("PreconsciousBuffer — default maxSize is 50", () => {
  const buf = new PreconsciousBuffer();
  assert.equal(buf.maxSize, 50);
});

test("PreconsciousBuffer — poll sets lastPollTimestamp", () => {
  const buf = new PreconsciousBuffer();
  assert.equal(buf.lastPollTimestamp, null);

  buf.push({ type: "x" });
  buf.poll();

  assert.ok(typeof buf.lastPollTimestamp === "string");
});
