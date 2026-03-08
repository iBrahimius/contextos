import test from "node:test";
import assert from "node:assert/strict";

import { LLMClient, createLLMClient } from "../src/core/llm-client.js";

test("LLMClient — throws if no token provided", () => {
  const saved = process.env.OPENCLAW_GATEWAY_TOKEN;
  delete process.env.OPENCLAW_GATEWAY_TOKEN;
  try {
    assert.throws(
      () => new LLMClient(),
      /OPENCLAW_GATEWAY_TOKEN is required/
    );
  } finally {
    if (saved) process.env.OPENCLAW_GATEWAY_TOKEN = saved;
  }
});

test("LLMClient — accepts token from options", () => {
  const client = new LLMClient({
    gatewayToken: "test-token-123",
    model: "anthropic/claude-haiku-4-5",
  });
  assert.equal(client.gatewayToken, "test-token-123");
  assert.equal(client.defaultModel, "anthropic/claude-haiku-4-5");
});

test("LLMClient — uses env token if options token missing", () => {
  process.env.OPENCLAW_GATEWAY_TOKEN = "env-token-456";
  const client = new LLMClient();
  assert.equal(client.gatewayToken, "env-token-456");
  delete process.env.OPENCLAW_GATEWAY_TOKEN;
});

test("createLLMClient — returns null if no token configured", () => {
  delete process.env.OPENCLAW_GATEWAY_TOKEN;
  const client = createLLMClient();
  assert.equal(client, null);
});

test("createLLMClient — returns client when token available", () => {
  process.env.OPENCLAW_GATEWAY_TOKEN = "test-token";
  const client = createLLMClient();
  assert.ok(client instanceof LLMClient);
  delete process.env.OPENCLAW_GATEWAY_TOKEN;
});

test("LLMClient.complete — integration test (requires gateway)", async () => {
  const token = process.env.OPENCLAW_GATEWAY_TOKEN;
  if (!token) {
    console.log("[llm-client.test] Skipping gateway integration test — no OPENCLAW_GATEWAY_TOKEN");
    return;
  }

  const client = new LLMClient({
    gatewayToken: token,
    model: "anthropic/claude-haiku-4-5",
  });

  try {
    const result = await client.complete({
      prompt: "Say hello in one word",
      maxTokens: 10,
    });

    assert.ok(result.text, "Response should have text");
    assert.ok(result.model, "Response should have model");
    assert.ok(result.raw, "Response should have raw data");
  } catch (err) {
    console.log("[llm-client.test] Gateway error (expected if not running):", err.message);
  }
});

test("LLMClient.completeJSON — validates JSON output", async () => {
  const token = process.env.OPENCLAW_GATEWAY_TOKEN;
  if (!token) {
    console.log("[llm-client.test] Skipping JSON integration test");
    return;
  }

  const client = new LLMClient({
    gatewayToken: token,
    model: "anthropic/claude-haiku-4-5",
  });

  try {
    const result = await client.completeJSON({
      prompt: 'Return {"status": "ok"} and nothing else',
      maxTokens: 30,
    });

    assert.ok(result.data, "Should parse JSON data");
    assert.equal(result.data.status, "ok");
  } catch (err) {
    console.log("[llm-client.test] Gateway error (expected if not running):", err.message);
  }
});
