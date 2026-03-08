#!/usr/bin/env node
/**
 * Import Cortex events into ContextOS.
 *
 * Reads raw events from ~/.cortex/cortex.db and POSTs them to ContextOS
 * with origin_kind='import'. ContextOS re-classifies from scratch.
 *
 * Field mapping:
 *   Cortex event_id     → ingestId (dedup key — safe to re-run)
 *   Cortex role          → role (user/assistant)
 *   Cortex content       → content
 *   Cortex timestamp     → captured_at (converted from Unix float to ISO)
 *   Cortex source        → actorId mapping (telegram→user:telegram, llm→agent:assistant, etc.)
 *   Cortex session_id    → conversationId (groups by day)
 *   (hardcoded)          → originKind: 'import'
 *   (hardcoded)          → scopeKind: 'private'
 *   (derived from role)  → direction: inbound/outbound
 *
 * Safety:
 *   - ingestId dedup: running twice is a no-op
 *   - origin_kind='import': all imported events permanently tagged
 *   - Skips empty content
 *   - Skips NO_REPLY / HEARTBEAT_OK noise
 *   - Logs every POST result
 *   - Dry-run mode with --dry-run flag
 *
 * Usage:
 *   node scripts/import-cortex.js                    # live import
 *   node scripts/import-cortex.js --dry-run           # preview only
 *   node scripts/import-cortex.js --cortex-db /path   # custom DB path
 *   node scripts/import-cortex.js --target http://...  # custom ContextOS URL
 */

import { DatabaseSync } from "node:sqlite";
import { existsSync } from "node:fs";
import { join } from "node:path";

// --- Config ---

const args = process.argv.slice(2);
const DRY_RUN = args.includes("--dry-run");
const cortexDbIdx = args.indexOf("--cortex-db");
const targetIdx = args.indexOf("--target");

const CORTEX_DB = cortexDbIdx >= 0 ? args[cortexDbIdx + 1] : join(process.env.HOME, ".cortex", "cortex.db");
const TARGET_URL = targetIdx >= 0 ? args[targetIdx + 1] : "http://127.0.0.1:4183";
const BATCH_DELAY_MS = 50; // Small delay between POSTs to avoid overwhelming the server

// --- Noise filter ---

const SKIP_CONTENT = new Set([
  "NO_REPLY",
  "HEARTBEAT_OK",
]);

function shouldSkip(content) {
  const trimmed = content.trim();
  if (!trimmed) return true;
  if (SKIP_CONTENT.has(trimmed)) return true;
  // Skip very short system noise (< 3 chars)
  if (trimmed.length < 3) return true;
  return false;
}

// --- Field mapping ---

function mapActorId(role, source) {
  if (role === "user") {
    // source is the channel: telegram, webchat, manual
    return `user:${source || "unknown"}`;
  }
  // assistant
  return "agent:assistant";
}

function mapDirection(role) {
  return role === "user" ? "inbound" : "outbound";
}

function unixToIso(timestamp) {
  return new Date(timestamp * 1000).toISOString();
}

function mapConversationTitle(sessionId, source) {
  // session_id is like "s_2026-03-05"
  const date = sessionId.replace("s_", "");
  return `${source || "imported"} session ${date}`;
}

// --- Main ---

async function main() {
  console.log("=== ContextOS Import from Cortex ===");
  console.log(`  Cortex DB: ${CORTEX_DB}`);
  console.log(`  Target:    ${TARGET_URL}`);
  console.log(`  Dry run:   ${DRY_RUN}`);
  console.log();

  if (!existsSync(CORTEX_DB)) {
    console.error(`ERROR: Cortex DB not found at ${CORTEX_DB}`);
    process.exit(1);
  }

  // Verify ContextOS is reachable
  if (!DRY_RUN) {
    try {
      const health = await fetch(`${TARGET_URL}/api/health`);
      if (!health.ok) throw new Error(`HTTP ${health.status}`);
      const data = await health.json();
      console.log(`ContextOS health: ${data.status} (${data.counts.messages} messages already)`);
    } catch (err) {
      console.error(`ERROR: Cannot reach ContextOS at ${TARGET_URL}: ${err.message}`);
      process.exit(1);
    }
  }

  // Read all events from Cortex
  const db = new DatabaseSync(CORTEX_DB, { readOnly: true });
  const events = db.prepare(
    "SELECT event_id, event_type, timestamp, session_id, role, content, source, raw_json FROM events WHERE event_type = 'message' ORDER BY timestamp ASC"
  ).all();
  db.close();

  console.log(`Found ${events.length} message events in Cortex`);
  console.log();

  let imported = 0;
  let skipped = 0;
  let duplicates = 0;
  let errors = 0;

  for (const event of events) {
    // Filter noise
    if (shouldSkip(event.content)) {
      skipped++;
      continue;
    }

    const payload = {
      ingestId: `cortex_${event.event_id}`,  // Prefix to avoid collision with live events
      role: event.role,
      direction: mapDirection(event.role),
      originKind: "import",
      actorId: mapActorId(event.role, event.source),
      content: event.content,
      conversationId: `cortex_${event.session_id}`,  // Prefix to separate from live conversations
      conversationTitle: mapConversationTitle(event.session_id, event.source),
      scopeKind: "private",
      skipClassification: false,  // Let ContextOS classify fresh
      raw: {
        importedFrom: "cortex",
        originalEventId: event.event_id,
        originalTimestamp: event.timestamp,
        originalSource: event.source,
        originalSessionId: event.session_id,
      },
    };

    if (DRY_RUN) {
      console.log(`[DRY] ${event.event_id} | ${event.role} | ${event.source} | ${event.content.slice(0, 80)}...`);
      imported++;
      continue;
    }

    try {
      const response = await fetch(`${TARGET_URL}/api/messages`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(30000),  // 30s timeout per event (classification takes time)
      });

      if (response.ok) {
        imported++;
        if (imported % 50 === 0) {
          console.log(`  Imported ${imported}/${events.length - skipped}...`);
        }
      } else if (response.status === 409) {
        // Conflict = duplicate ingestId, already imported
        duplicates++;
      } else {
        const body = await response.text().catch(() => "");
        console.error(`  ERROR ${response.status} for ${event.event_id}: ${body.slice(0, 200)}`);
        errors++;
      }
    } catch (err) {
      console.error(`  ERROR for ${event.event_id}: ${err.message}`);
      errors++;
    }

    // Small delay to not overwhelm the server
    await new Promise(r => setTimeout(r, BATCH_DELAY_MS));
  }

  console.log();
  console.log("=== Import Complete ===");
  console.log(`  Total events:  ${events.length}`);
  console.log(`  Imported:      ${imported}`);
  console.log(`  Skipped:       ${skipped} (noise/empty)`);
  console.log(`  Duplicates:    ${duplicates} (already existed)`);
  console.log(`  Errors:        ${errors}`);

  if (!DRY_RUN) {
    const health = await fetch(`${TARGET_URL}/api/health`);
    const data = await health.json();
    console.log();
    console.log("=== ContextOS Post-Import ===");
    console.log(`  Messages:      ${data.counts.messages}`);
    console.log(`  Entities:      ${data.counts.entities}`);
    console.log(`  Relationships: ${data.counts.relationships}`);
    console.log(`  Graph version: ${data.graph_version}`);
  }
}

main().catch(err => {
  console.error(`FATAL: ${err.message}`);
  process.exit(1);
});
