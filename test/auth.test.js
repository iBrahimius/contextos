import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";

import { ContextOS } from "../src/core/context-os.js";
import { handleRequest } from "../src/http/router.js";

const AUTH_TOKEN = "shared-secret-token";

async function makeRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "contextos-auth-"));
  await fs.mkdir(path.join(root, "data"), { recursive: true });
  return root;
}

async function createHarness() {
  const rootDir = await makeRoot();
  const contextOS = new ContextOS({ rootDir });
  const server = http.createServer((request, response) => handleRequest(contextOS, rootDir, request, response));

  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();

  return {
    baseUrl: `http://127.0.0.1:${port}`,
    async close() {
      await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
      contextOS.database.close();
      contextOS.telemetry.close();
      await fs.rm(rootDir, { recursive: true, force: true });
    },
  };
}

async function withHarness(callback) {
  const harness = await createHarness();

  try {
    return await callback(harness);
  } finally {
    await harness.close();
  }
}

async function withAuthToken(token, callback) {
  const previous = process.env.CONTEXTOS_AUTH_TOKEN;

  if (token === undefined) {
    delete process.env.CONTEXTOS_AUTH_TOKEN;
  } else {
    process.env.CONTEXTOS_AUTH_TOKEN = token;
  }

  try {
    return await callback();
  } finally {
    if (previous === undefined) {
      delete process.env.CONTEXTOS_AUTH_TOKEN;
    } else {
      process.env.CONTEXTOS_AUTH_TOKEN = previous;
    }
  }
}

test("Request without token when auth is configured -> 401", async () => {
  await withAuthToken(AUTH_TOKEN, async () => withHarness(async (harness) => {
    const response = await fetch(`${harness.baseUrl}/api/status`);

    assert.equal(response.status, 401);
  }));
});

test("Request with wrong token when auth is configured -> 403", async () => {
  await withAuthToken(AUTH_TOKEN, async () => withHarness(async (harness) => {
    const response = await fetch(`${harness.baseUrl}/api/status`, {
      headers: { authorization: "Bearer wrong-token" },
    });

    assert.equal(response.status, 403);
  }));
});

test("Request with valid token -> 200", async () => {
  await withAuthToken(AUTH_TOKEN, async () => withHarness(async (harness) => {
    const response = await fetch(`${harness.baseUrl}/api/status`, {
      headers: { authorization: `Bearer ${AUTH_TOKEN}` },
    });

    assert.equal(response.status, 200);
    assert.match(
      response.headers.get("access-control-allow-headers") ?? "",
      /authorization/i,
    );
  }));
});

test("GET /api/health accessible without token -> 200", async () => {
  await withAuthToken(AUTH_TOKEN, async () => withHarness(async (harness) => {
    const response = await fetch(`${harness.baseUrl}/api/health`);

    assert.equal(response.status, 200);
  }));
});

test("OPTIONS preflight accessible without token -> 204", async () => {
  await withAuthToken(AUTH_TOKEN, async () => withHarness(async (harness) => {
    const response = await fetch(`${harness.baseUrl}/api/status`, {
      method: "OPTIONS",
      headers: {
        origin: "http://localhost:3000",
        "access-control-request-method": "GET",
        "access-control-request-headers": "authorization, content-type",
      },
    });

    assert.equal(response.status, 204);
    assert.match(
      response.headers.get("access-control-allow-headers") ?? "",
      /authorization/i,
    );
  }));
});

test("Auth disabled (no env var) -> all endpoints accessible", async () => {
  await withAuthToken(undefined, async () => withHarness(async (harness) => {
    const statusResponse = await fetch(`${harness.baseUrl}/api/status`);
    assert.equal(statusResponse.status, 200);

    const createConversationResponse = await fetch(`${harness.baseUrl}/api/conversations`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ title: "Auth disabled conversation" }),
    });

    assert.equal(createConversationResponse.status, 201);
    const payload = await createConversationResponse.json();
    assert.equal(payload.title, "Auth disabled conversation");
  }));
});

test("401 response includes correct error message", async () => {
  await withAuthToken(AUTH_TOKEN, async () => withHarness(async (harness) => {
    const response = await fetch(`${harness.baseUrl}/api/status`);
    const payload = await response.json();

    assert.equal(response.status, 401);
    assert.deepEqual(payload, { error: "Authentication required" });
  }));
});

test("403 response includes correct error message", async () => {
  await withAuthToken(AUTH_TOKEN, async () => withHarness(async (harness) => {
    const response = await fetch(`${harness.baseUrl}/api/status`, {
      headers: { authorization: "Bearer wrong-token" },
    });
    const payload = await response.json();

    assert.equal(response.status, 403);
    assert.deepEqual(payload, { error: "Invalid authentication token" });
  }));
});
