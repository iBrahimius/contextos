import fs from "node:fs/promises";
import path from "node:path";

import { checksum, estimateTokens } from "./utils.js";

function chunkMarkdown(content) {
  const lines = content.split(/\r?\n/);
  const chunks = [];
  let heading = "Document";
  let buffer = [];

  function flush() {
    const text = buffer.join("\n").trim();
    if (!text) {
      buffer = [];
      return;
    }

    chunks.push({ heading, content: text });
    buffer = [];
  }

  for (const line of lines) {
    const headingMatch = line.match(/^#{1,6}\s+(.*)$/);
    if (headingMatch) {
      flush();
      heading = headingMatch[1].trim();
      continue;
    }

    buffer.push(line);
    if (buffer.join("\n").length > 900) {
      flush();
    }
  }

  flush();
  return chunks;
}

async function walkMarkdownFiles(root) {
  const entries = await fs.readdir(root, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    const fullPath = path.join(root, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await walkMarkdownFiles(fullPath)));
      continue;
    }

    if (entry.isFile() && entry.name.endsWith(".md")) {
      files.push(fullPath);
    }
  }

  return files;
}

export class DocumentIndexer {
  constructor({ database, graph, classifier }) {
    this.database = database;
    this.graph = graph;
    this.classifier = classifier;
  }

  async indexDirectory(root, { scopeKind = "shared", scopeId = null } = {}) {
    const files = await walkMarkdownFiles(root);
    const indexed = [];

    for (const filePath of files) {
      const content = await fs.readFile(filePath, "utf8");
      const documentId = this.database.upsertDocument({
        filePath,
        checksum: checksum(content),
      });

      const chunks = chunkMarkdown(content);
      chunks.forEach((chunk, ordinal) => {
        const chunkId = this.database.insertDocumentChunk({
          documentId,
          ordinal,
          heading: chunk.heading,
          content: chunk.content,
          tokenCount: estimateTokens(chunk.content),
          scopeKind,
          scopeId,
        });

        const classified = this.classifier.classifyText(chunk.content);
        const entityIds = [];

        for (const entity of classified.entities) {
          const resolved = this.graph.ensureEntity(entity);
          entityIds.push(resolved.id);
          this.database.linkChunkEntity({ chunkId, entityId: resolved.id, score: 1 });
        }

        for (const observation of classified.observations.filter((item) => item.category === "relationship")) {
          const subject = observation.subjectLabel ? this.graph.ensureEntity({ label: observation.subjectLabel }) : null;
          const object = observation.objectLabel ? this.graph.ensureEntity({ label: observation.objectLabel }) : null;
          if (subject && object) {
            this.graph.connect({
              subjectEntityId: subject.id,
              predicate: observation.predicate,
              objectEntityId: object.id,
              weight: observation.confidence,
              provenanceMessageId: null,
              metadata: { source: "document", path: filePath, heading: chunk.heading },
            });
          }
        }
      });

      indexed.push({ filePath, chunkCount: chunks.length });
    }

    return indexed;
  }
}
