import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { ContextOS } from "./core/context-os.js";
import { handleRequest } from "./http/router.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, "..");
const port = Number(process.env.PORT ?? 4181);
const host = process.env.HOST ?? "127.0.0.1";

const contextOS = new ContextOS({ rootDir });

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
});
