#!/usr/bin/env node

/**
 * backfill-quality-audit.js
 *
 * Post-backfill quality check. Answers: "Is Haiku being too conservative?"
 *
 * Three checks:
 * 1. Random sample of "empty" messages (0 observations) — human reviews for missed facts
 * 2. Random sample of "extracted" messages — human reviews for accuracy
 * 3. Observation density by time period — detects flat/suspicious patterns
 *
 * Usage:
 *   node scripts/backfill-quality-audit.js [--empty=30] [--extracted=20]
 *
 * Output: audit report to stdout + saves to data/backfill-quality-audit.json
 */

import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DB_PATH = path.join(__dirname, "../data/contextos.db");
const OUTPUT_PATH = path.join(__dirname, "../data/backfill-quality-audit.json");

function parseArgs(argv) {
  const args = { empty: 30, extracted: 20 };
  for (const arg of argv.slice(2)) {
    if (arg.startsWith("--empty=")) args.empty = parseInt(arg.slice(8), 10);
    if (arg.startsWith("--extracted=")) args.extracted = parseInt(arg.slice(12), 10);
  }
  return args;
}

function main() {
  const args = parseArgs(process.argv);
  const db = new DatabaseSync(DB_PATH, { readOnly: true });

  // ── Overall stats ────────────────────────────────────────────

  const totalMessages = db.prepare("SELECT COUNT(*) as n FROM messages WHERE content IS NOT NULL AND length(content) > 10").get().n;
  const withObs = db.prepare("SELECT COUNT(DISTINCT m.id) as n FROM messages m INNER JOIN observations o ON o.message_id = m.id").get().n;
  const withoutObs = totalMessages - withObs;
  const obsTotal = db.prepare("SELECT COUNT(*) as n FROM observations").get().n;
  const backfillObs = db.prepare("SELECT COUNT(*) as n FROM observations WHERE actor_id = 'haiku-classifier' OR actor_id = 'backfill-v2.5'").get().n;

  console.log("═".repeat(60));
  console.log("📊 BACKFILL QUALITY AUDIT");
  console.log("═".repeat(60));
  console.log(`Total messages (>10 chars):  ${totalMessages}`);
  console.log(`Messages with observations:  ${withObs} (${(withObs / totalMessages * 100).toFixed(1)}%)`);
  console.log(`Messages without:            ${withoutObs} (${(withoutObs / totalMessages * 100).toFixed(1)}%)`);
  console.log(`Total observations:          ${obsTotal}`);
  console.log(`Backfill observations:       ${backfillObs}`);
  console.log(`Obs/message ratio:           ${(obsTotal / totalMessages).toFixed(2)}`);
  console.log();

  // ── Check 1: Sample "empty" messages ─────────────────────────

  console.log(`── CHECK 1: Random sample of ${args.empty} "empty" messages ──`);
  console.log("(Human review: do any contain extractable facts that Haiku missed?)\n");

  const emptyMessages = db.prepare(`
    SELECT m.id, m.content, m.captured_at, m.role
    FROM messages m
    LEFT JOIN observations o ON o.message_id = m.id
    WHERE o.id IS NULL
    AND m.content IS NOT NULL
    AND length(m.content) > 20
    ORDER BY RANDOM()
    LIMIT ?
  `).all(args.empty);

  let emptyWithPotentialFacts = 0;
  for (let i = 0; i < emptyMessages.length; i++) {
    const msg = emptyMessages[i];
    const content = msg.content.slice(0, 200).replace(/\n/g, " ");
    const hasNumbers = /\d+[.,]\d+|\$|€|£/.test(msg.content);
    const hasDecisionWords = /\b(decided|agreed|chose|locked in|shipped|confirmed)\b/i.test(msg.content);
    const hasFactPatterns = /\b(is|are|was|uses?|runs?|costs?|port|version)\b.*\b\d+\b/i.test(msg.content);
    const suspicious = hasNumbers || hasDecisionWords || hasFactPatterns;

    if (suspicious) emptyWithPotentialFacts++;

    const flag = suspicious ? "⚠️  REVIEW" : "   ok";
    console.log(`${flag} [${msg.role}] ${msg.captured_at?.slice(0, 10) ?? "?"}: ${content}`);
    if (i < emptyMessages.length - 1) console.log();
  }

  console.log(`\n📋 Auto-flagged ${emptyWithPotentialFacts}/${emptyMessages.length} as potentially containing facts`);
  console.log("   (Flagging heuristic: contains numbers/currency, decision words, or fact patterns)");
  console.log();

  // ── Check 2: Sample "extracted" messages ─────────────────────

  console.log(`── CHECK 2: Random sample of ${args.extracted} "extracted" messages ──`);
  console.log("(Human review: are observations accurate? Any hallucinations?)\n");

  const extractedMessages = db.prepare(`
    SELECT m.id, m.content, m.captured_at, m.role,
           GROUP_CONCAT(o.category || ': ' || substr(o.detail, 1, 100), ' | ') as obs_summary,
           COUNT(o.id) as obs_count,
           AVG(o.confidence) as avg_conf
    FROM messages m
    INNER JOIN observations o ON o.message_id = m.id
    GROUP BY m.id
    ORDER BY RANDOM()
    LIMIT ?
  `).all(args.extracted);

  for (let i = 0; i < extractedMessages.length; i++) {
    const msg = extractedMessages[i];
    const content = msg.content.slice(0, 150).replace(/\n/g, " ");
    console.log(`📝 [${msg.role}] ${msg.captured_at?.slice(0, 10) ?? "?"}: ${content}`);
    console.log(`   → ${msg.obs_count} obs, avg conf ${msg.avg_conf?.toFixed(2) ?? "?"}`);
    console.log(`   → ${msg.obs_summary?.slice(0, 200) ?? "(none)"}`);
    if (i < extractedMessages.length - 1) console.log();
  }
  console.log();

  // ── Check 3: Observation density by time period ──────────────

  console.log("── CHECK 3: Observation density by week ──\n");

  const weeklyDensity = db.prepare(`
    SELECT 
      strftime('%Y-W%W', m.captured_at) as week,
      COUNT(DISTINCT m.id) as messages,
      COUNT(DISTINCT CASE WHEN o.id IS NOT NULL THEN m.id END) as msgs_with_obs,
      COUNT(o.id) as observations
    FROM messages m
    LEFT JOIN observations o ON o.message_id = m.id
    WHERE m.content IS NOT NULL AND length(m.content) > 10
    GROUP BY week
    ORDER BY week ASC
  `).all();

  console.log("Week          | Messages | With Obs | Obs Total | Density");
  console.log("-".repeat(62));

  for (const week of weeklyDensity) {
    const density = week.messages > 0 ? (week.msgs_with_obs / week.messages * 100).toFixed(1) : "0.0";
    const bar = "█".repeat(Math.round(parseFloat(density) / 5));
    console.log(
      `${week.week.padEnd(14)}| ${String(week.messages).padStart(8)} | ${String(week.msgs_with_obs).padStart(8)} | ${String(week.observations).padStart(9)} | ${density.padStart(5)}% ${bar}`
    );
  }

  // ── Flag suspicious patterns ─────────────────────────────────

  console.log("\n── ASSESSMENT ──\n");

  const overallDensity = withObs / totalMessages;
  if (overallDensity < 0.10) {
    console.log("🔴 LOW: <10% of messages have observations. Haiku may be too conservative.");
    console.log("   Action: Review CHECK 1 samples. If >20% have missed facts, tune the prompt.");
  } else if (overallDensity < 0.25) {
    console.log("🟡 MODERATE: 10-25% of messages have observations. Plausible for conversation data.");
    console.log("   Action: Review CHECK 1 auto-flagged items. Spot check for missed decisions.");
  } else if (overallDensity < 0.50) {
    console.log("🟢 HEALTHY: 25-50% of messages have observations. Expected range.");
  } else {
    console.log("⚠️  HIGH: >50% of messages have observations. Check for over-extraction / false positives.");
    console.log("   Action: Review CHECK 2 samples for hallucinated or trivial observations.");
  }

  // ── Save audit report ────────────────────────────────────────

  const report = {
    timestamp: new Date().toISOString(),
    totalMessages,
    withObservations: withObs,
    withoutObservations: withoutObs,
    observationDensity: overallDensity,
    totalObservations: obsTotal,
    backfillObservations: backfillObs,
    weeklyDensity,
    emptyAutoFlagged: emptyWithPotentialFacts,
    emptySampleSize: emptyMessages.length,
    extractedSampleSize: extractedMessages.length,
  };

  fs.writeFile(OUTPUT_PATH, JSON.stringify(report, null, 2)).then(() => {
    console.log(`\n✅ Report saved to data/backfill-quality-audit.json`);
  });

  db.close();
}

main();
