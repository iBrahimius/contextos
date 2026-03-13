import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { ContextOS } from "./core/context-os.js";
import { handleRequest } from "./http/router.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, "..");
const port = Number(process.env.PORT ?? 4181);
const host = process.env.HOST ?? "127.0.0.1";

// --- PID lockfile: prevents scripts from wiping data/ while server is running ---
const dataDir = path.join(rootDir, "data");
const lockFile = path.join(dataDir, "contextos.pid");
fs.mkdirSync(dataDir, { recursive: true });
fs.writeFileSync(lockFile, `${process.pid}\n`, "utf8");

function cleanupLock() {
  try {
    const content = fs.readFileSync(lockFile, "utf8").trim();
    if (content === String(process.pid)) fs.unlinkSync(lockFile);
  } catch { /* already gone */ }
}
process.on("exit", cleanupLock);
process.on("SIGINT", () => { cleanupLock(); process.exit(0); });
process.on("SIGTERM", () => { cleanupLock(); process.exit(0); });
// --- end lockfile ---

const contextOS = new ContextOS({ rootDir, deferInit: true });

const server = http.createServer((request, response) => {
  handleRequest(contextOS, rootDir, request, response).catch((error) => {
    response.writeHead(500, {
      "Content-Type": "application/json; charset=utf-8",
    });
    response.end(JSON.stringify({ error: error.message, stack: error.stack }, null, 2));
  });
});

server.listen(port, host, () => {
  console.log(`ContextOS listening on http://${host}:${port}`);
  contextOS.init().then(() => {
    console.log("ContextOS initialization complete");
  }).catch((error) => {
    console.error("ContextOS initialization failed:", error.message);
  });
});
