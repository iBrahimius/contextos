import { readFileSync, writeFileSync, existsSync } from "node:fs";
import test from "node:test";
import assert from "node:assert/strict";

import {
  ROUTE_PROFILES,
  classifyRoute,
  isValidRouteLabel,
  scoreQuery,
} from "./retrieval-quality.helpers.js";
import { checkContextOSRunning } from "./skip-guards.js";

const CONTEXTOS_URL = process.env.CONTEXTOS_URL ?? "http://localhost:4183";
const contextOSRunning = await checkContextOSRunning(CONTEXTOS_URL);
const skipUnlessLive = contextOSRunning ? false : `ContextOS not running at ${CONTEXTOS_URL}`;
const GOLDEN_SET_PATH = new URL("./golden-retrieval-set.json", import.meta.url).pathname;
const HISTORY_PATH = new URL("./retrieval-quality-history.json", import.meta.url).pathname;

const goldenSet = JSON.parse(readFileSync(GOLDEN_SET_PATH, "utf8"));

async function queryRecall(queryText, tokenBudget = 4000) {
  const response = await fetch(`${CONTEXTOS_URL}/api/recall`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: queryText, token_budget: tokenBudget }),
  });
  if (!response.ok) {
    throw new Error(`Recall failed: ${response.status} ${await response.text()}`);
  }
  return response.json();
}

function summarizeGroup(results) {
  const count = results.length;
  const avgPrecision = count ? results.reduce((sum, result) => sum + result.precisionAt5, 0) / count : 0;
  const avgMRR = count ? results.reduce((sum, result) => sum + result.reciprocalRank, 0) / count : 0;
  const passRate = count ? results.filter((result) => result.top1InTop3).length / count : 0;
  const artifactLeakRate = count ? results.filter((result) => result.artifactViolations.length > 0).length / count : 0;

  return {
    count,
    avgPrecision: Number(avgPrecision.toFixed(3)),
    avgMRR: Number(avgMRR.toFixed(3)),
    passRate: Number(passRate.toFixed(3)),
    artifactLeakRate: Number(artifactLeakRate.toFixed(3)),
  };
}

test("golden retrieval fixture metadata is route-aware", () => {
  assert.ok(Array.isArray(goldenSet.queries) && goldenSet.queries.length > 0, "queries fixture is empty");

  for (const query of goldenSet.queries) {
    assert.ok(isValidRouteLabel(query.route), `${query.id} has invalid route label ${query.route}`);
    assert.ok(query.target_families?.primary, `${query.id} missing target_families.primary`);
    assert.ok(Array.isArray(query.target_families?.secondary), `${query.id} missing target_families.secondary`);
    assert.equal(query.artifact_exclusions?.boundary ?? null, "hard_exclude", `${query.id} must hard-exclude artifacts`);
    assert.ok(
      (query.artifact_exclusions?.families ?? []).includes("operational"),
      `${query.id} must exclude operational artifacts`,
    );
  }
});

test("deterministic route classification cases stay aligned with fixture labels", () => {
  const cases = goldenSet.route_classification_cases ?? [];
  assert.ok(cases.length > 0, "route classification cases are missing");

  for (const fixtureCase of cases) {
    const route = classifyRoute(fixtureCase.query);
    assert.equal(
      route,
      fixtureCase.expected_route,
      `${fixtureCase.id}: expected ${fixtureCase.expected_route}, got ${route} for query: ${fixtureCase.query}`,
    );
  }
});

test("artifact-boundary audit scenarios are enforced at scoring layer", () => {
  const scenarios = goldenSet.artifact_audit_scenarios ?? [];
  assert.ok(scenarios.length > 0, "artifact audit scenarios are missing");

  for (const scenario of scenarios) {
    const score = scoreQuery(scenario.fixture_query, { evidence: scenario.mock_evidence });
    assert.ok(
      score.artifactViolations.length >= scenario.minimum_artifact_violations,
      `${scenario.id}: expected at least ${scenario.minimum_artifact_violations} artifact violation(s), got ${score.artifactViolations.length}`,
    );
    assert.ok(
      (score.familyCounts.operational ?? 0) >= scenario.minimum_operational_results,
      `${scenario.id}: expected at least ${scenario.minimum_operational_results} operational result(s), got ${score.familyCounts.operational ?? 0}`,
    );
  }
});

test("Retrieval Quality — Golden Set", { skip: skipUnlessLive }, async () => {
  const results = [];
  const startTime = Date.now();

  for (const query of goldenSet.queries) {
    const recallResult = await queryRecall(query.query);
    const score = scoreQuery(query, recallResult);
    results.push(score);

    console.log(`\n  ${score.id} (${score.intent} → ${score.route})`);
    console.log(`    P@5: ${score.precisionAt5.toFixed(2)} | MRR: ${score.reciprocalRank.toFixed(2)} | Top1 in Top3: ${score.top1InTop3}`);
    console.log(`    Families: ${JSON.stringify(score.familyCounts)}`);
    console.log(`    Found: [${score.foundTerms.join(", ")}]`);
    if (score.missedTerms.length) {
      console.log(`    Missed: [${score.missedTerms.join(", ")}]`);
    }
    if (score.absentViolations.length) {
      console.log(`    ⚠️  Absent violations: [${score.absentViolations.join(", ")}]`);
    }
    if (score.artifactViolations.length) {
      console.log(`    🚫 Artifact violations: [${score.artifactViolations.join(", ")}]`);
    }
    console.log(`    Top-5 types: [${score.top5Types.join(", ")}]`);
    console.log(`    Top-5 summaries:`);
    for (const summary of score.top5Summaries) {
      console.log(`      - ${summary}`);
    }
  }

  assert.ok(results.length > 0, "No results collected — did the benchmark run?");

  const totalDurationMs = Date.now() - startTime;
  const avgPrecision = results.reduce((sum, result) => sum + result.precisionAt5, 0) / results.length;
  const avgMRR = results.reduce((sum, result) => sum + result.reciprocalRank, 0) / results.length;
  const passRate = results.filter((result) => result.top1InTop3).length / results.length;
  const artifactLeakRate = results.filter((result) => result.artifactViolations.length > 0).length / results.length;
  const latencies = results.filter((result) => result.latencyMs != null).map((result) => result.latencyMs);
  const sortedLatencies = [...latencies].sort((left, right) => left - right);
  const p50Latency = sortedLatencies.length ? sortedLatencies[Math.floor(sortedLatencies.length * 0.5)] : null;
  const p95Latency = sortedLatencies.length ? sortedLatencies[Math.floor(sortedLatencies.length * 0.95)] : null;

  const byIntent = {};
  for (const intent of [...new Set(results.map((result) => result.intent))]) {
    byIntent[intent] = summarizeGroup(results.filter((result) => result.intent === intent));
  }

  const byRoute = {};
  for (const route of Object.keys(ROUTE_PROFILES)) {
    const routeResults = results.filter((result) => result.route === route);
    if (!routeResults.length) {
      continue;
    }
    byRoute[route] = summarizeGroup(routeResults);
  }

  const aggregate = {
    timestamp: new Date().toISOString(),
    label: process.env.GOLDEN_LABEL ?? "baseline",
    queryCount: results.length,
    avgPrecisionAt5: Number(avgPrecision.toFixed(3)),
    avgMRR: Number(avgMRR.toFixed(3)),
    passRate: Number(passRate.toFixed(3)),
    artifactLeakRate: Number(artifactLeakRate.toFixed(3)),
    p50LatencyMs: p50Latency,
    p95LatencyMs: p95Latency,
    totalDurationMs,
    byIntent,
    byRoute,
    perQuery: results.map((result) => ({
      id: result.id,
      intent: result.intent,
      route: result.route,
      precisionAt5: Number(result.precisionAt5.toFixed(3)),
      mrr: Number(result.reciprocalRank.toFixed(3)),
      pass: result.top1InTop3,
      typeMatch: result.typeMatch,
      artifactViolations: result.artifactViolations,
      familyCounts: result.familyCounts,
    })),
  };

  console.log("\n" + "=".repeat(72));
  console.log("RETRIEVAL QUALITY SUMMARY");
  console.log("=".repeat(72));
  console.log(`  Label:              ${aggregate.label}`);
  console.log(`  Queries:            ${aggregate.queryCount}`);
  console.log(`  Avg P@5:            ${aggregate.avgPrecisionAt5}`);
  console.log(`  Avg MRR:            ${aggregate.avgMRR}`);
  console.log(`  Pass rate:          ${aggregate.passRate} (top-1 in top-3)`);
  console.log(`  Artifact leak rate: ${aggregate.artifactLeakRate}`);
  console.log(`  Latency P50:        ${aggregate.p50LatencyMs}ms`);
  console.log(`  Latency P95:        ${aggregate.p95LatencyMs}ms`);
  console.log("");

  console.log("  By intent:");
  for (const [intent, stats] of Object.entries(byIntent)) {
    console.log(
      `    ${intent.padEnd(8)} — P@5: ${stats.avgPrecision.toFixed(3)} | MRR: ${stats.avgMRR.toFixed(3)} | Pass: ${stats.passRate.toFixed(3)} | Artifact leaks: ${stats.artifactLeakRate.toFixed(3)} (n=${stats.count})`,
    );
  }

  console.log("  By route:");
  for (const [route, stats] of Object.entries(byRoute)) {
    console.log(
      `    ${route.padEnd(21)} — P@5: ${stats.avgPrecision.toFixed(3)} | MRR: ${stats.avgMRR.toFixed(3)} | Pass: ${stats.passRate.toFixed(3)} | Artifact leaks: ${stats.artifactLeakRate.toFixed(3)} (n=${stats.count})`,
    );
  }
  console.log("=".repeat(72));

  let history = [];
  if (existsSync(HISTORY_PATH)) {
    try {
      history = JSON.parse(readFileSync(HISTORY_PATH, "utf8"));
    } catch {
      history = [];
    }
  }
  history = history.filter((entry) => entry.label !== aggregate.label);
  history.push(aggregate);
  writeFileSync(HISTORY_PATH, JSON.stringify(history, null, 2));
  console.log(`\nResults saved to ${HISTORY_PATH}`);

  if (history.length > 1 && aggregate.label !== "baseline") {
    const baseline = history.find((entry) => entry.label === "baseline") ?? history[history.length - 2];
    const precisionDelta = aggregate.avgPrecisionAt5 - baseline.avgPrecisionAt5;
    const mrrDelta = aggregate.avgMRR - baseline.avgMRR;

    console.log(`\n  vs ${baseline.label}:`);
    console.log(`    P@5:  ${precisionDelta >= 0 ? "+" : ""}${precisionDelta.toFixed(3)}`);
    console.log(`    MRR:  ${mrrDelta >= 0 ? "+" : ""}${mrrDelta.toFixed(3)}`);

    assert.ok(
      precisionDelta >= -0.02,
      `Precision@5 regressed by ${Math.abs(precisionDelta).toFixed(3)} vs ${baseline.label}. Previous: ${baseline.avgPrecisionAt5}, Current: ${aggregate.avgPrecisionAt5}`,
    );
    assert.ok(
      mrrDelta >= -0.02,
      `MRR regressed by ${Math.abs(mrrDelta).toFixed(3)} vs ${baseline.label}. Previous: ${baseline.avgMRR}, Current: ${aggregate.avgMRR}`,
    );
  }
});
