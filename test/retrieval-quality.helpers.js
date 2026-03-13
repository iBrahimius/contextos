const ROUTE_ORDER = [
  "current_state",
  "history_temporal",
  "why_explanatory",
  "entity_state",
  "mixed",
  "operational_diagnostics",
];

export const ROUTE_PROFILES = {
  current_state: {
    primary: "canonical",
    secondary: ["conversational"],
    artifactBoundary: "hard_exclude",
  },
  history_temporal: {
    primary: "conversational",
    secondary: ["canonical"],
    artifactBoundary: "hard_exclude",
  },
  why_explanatory: {
    primary: "conversational",
    secondary: ["canonical"],
    artifactBoundary: "hard_exclude",
  },
  entity_state: {
    primary: "canonical",
    secondary: ["conversational"],
    artifactBoundary: "hard_exclude",
  },
  mixed: {
    primary: "canonical",
    secondary: ["conversational"],
    artifactBoundary: "hard_exclude",
  },
  operational_diagnostics: {
    primary: "operational",
    secondary: [],
    artifactBoundary: "open",
  },
};

const OPERATIONAL_PATTERNS = [
  /\bbenchmark\b/i,
  /\bdiagnostic\b/i,
  /\baudit\b/i,
  /\bfixture\b/i,
  /\btelemetry\b/i,
  /\bdebug\b/i,
  /\blaunchd\b/i,
  /contextos(?:_|\.db)/i,
  /retrieval-quality/i,
  /golden-retrieval/i,
  /\/test\//i,
];

const MIXED_PATTERNS = [
  /\bhow should i handle it\b/i,
  /\bknown weaknesses\b/i,
  /\btradeoffs\b/i,
  /\bwhat are the tradeoffs\b/i,
  /\bwhat should i do\b/i,
];

const ENTITY_PATTERNS = [
  /^(?:who|what) is\b/i,
  /^(?:tell me about|what do we know about|brief(?:ing)? on|overview of)\b/i,
  /\bwhat trademarks does\b/i,
];

const WHY_PATTERNS = [
  /^(?:why\b|rationale\b|reason (?:for|behind)\b|how come\b|what motivated\b|what was the reasoning behind\b)/i,
];

const HISTORY_PATTERNS = [
  /^(?:what happened\b|when did\b|when was\b|timeline of\b|history of\b|what changes were made\b|what did we build\b)/i,
  /\b(?:recently|last week|yesterday|today|this week|phase \d+)\b/i,
  /\bwhat was decided about\b/i,
];

const CURRENT_STATE_PATTERNS = [
  /^(?:what(?:'s| is| are)\b|what do we use\b|what .* do we use\b|what email service\b|what hosting platform\b|who owns\b|current(?: status)?\b|status of\b)/i,
  /\bdo we use\b/i,
  /\bcurrently active\b/i,
  /\bstatus of\b/i,
  /\bcurrent\b/i,
];

export function isValidRouteLabel(route) {
  return ROUTE_ORDER.includes(route);
}

export function classifyRoute(queryText) {
  const query = String(queryText ?? "").trim();
  if (!query) {
    return "mixed";
  }

  if (MIXED_PATTERNS.some((pattern) => pattern.test(query))) {
    return "mixed";
  }

  if (WHY_PATTERNS.some((pattern) => pattern.test(query))) {
    return "why_explanatory";
  }

  if (HISTORY_PATTERNS.some((pattern) => pattern.test(query))) {
    return "history_temporal";
  }

  if (ENTITY_PATTERNS.some((pattern) => pattern.test(query))) {
    return "entity_state";
  }

  if (CURRENT_STATE_PATTERNS.some((pattern) => pattern.test(query))) {
    return "current_state";
  }

  return "mixed";
}

export function itemText(item) {
  return [
    item?.content ?? "",
    item?.claim?.resolution_key ?? "",
    item?.event_id ?? "",
    item?.source ?? "",
  ]
    .join(" ")
    .toLowerCase();
}

export function itemSummary(item) {
  return String(item?.content ?? "").slice(0, 120);
}

export function itemType(item) {
  const rk = item?.claim?.resolution_key ?? "";
  const content = String(item?.content ?? "");
  if (rk.includes("decision")) return "decision";
  if (rk.includes("task")) return "task";
  if (rk.includes("constraint")) return "constraint";
  if (rk.includes("fact")) return "fact";
  if (/"label"\s*:\s*".+?"/.test(content) && /"kind"\s*:\s*".+?"/.test(content)) return "entity";
  if (item?.role === "user") return "user_message";
  if (item?.role === "assistant") return "assistant_message";
  return "unknown";
}

export function inferTargetFamily(item) {
  if (item?.target_family) {
    return item.target_family;
  }

  const haystack = [
    item?.content ?? "",
    item?.source ?? "",
    item?.event_id ?? "",
    item?.claim?.resolution_key ?? "",
  ].join(" ");

  if (OPERATIONAL_PATTERNS.some((pattern) => pattern.test(haystack))) {
    return "operational";
  }

  if (item?.claim) {
    return "canonical";
  }

  if (["user", "assistant", "system"].includes(item?.role)) {
    return "conversational";
  }

  return "unknown";
}

export function buildArtifactTermList(query) {
  const explicitTerms = query?.artifact_exclusions?.terms ?? [];
  const defaultTerms = query?.route === "operational_diagnostics"
    ? []
    : ["telemetry", "benchmark", "diagnostic", "audit", "contextos.db", "launchd"];
  return [...new Set([...explicitTerms, ...defaultTerms].filter(Boolean))];
}

export function inspectArtifactViolations(query, items) {
  const sourcePatterns = (query?.artifact_exclusions?.sources ?? []).map((value) => new RegExp(value, "i"));
  const forbiddenFamilies = new Set(query?.artifact_exclusions?.families ?? []);
  const terms = buildArtifactTermList(query).map((term) => String(term).toLowerCase());
  const violations = [];

  for (const item of items ?? []) {
    const text = itemText(item);
    const family = inferTargetFamily(item);

    if (forbiddenFamilies.has(family)) {
      violations.push(`family:${family}`);
    }

    for (const pattern of sourcePatterns) {
      if (pattern.test(String(item?.source ?? ""))) {
        violations.push(`source:${item?.source}`);
      }
    }

    for (const term of terms) {
      if (text.includes(term)) {
        violations.push(`term:${term}`);
      }
    }
  }

  return [...new Set(violations)];
}

export function scoreQuery(query, recallResult) {
  const items = recallResult?.evidence ?? [];
  const top5 = items.slice(0, 5);
  const top3 = items.slice(0, 3);
  const top10 = items.slice(0, 10);
  const top5Combined = top5.map((result) => itemText(result)).join(" ");

  const expectedTerms = query.expected_in_top5 ?? [];
  const foundTerms = expectedTerms.filter((term) => top5Combined.includes(term.toLowerCase()));
  const precisionAt5 = expectedTerms.length > 0 ? foundTerms.length / expectedTerms.length : 0;

  const top1Text = top5.length > 0 ? itemText(top5[0]) : "";
  const top1Match = query.expected_top1_contains
    ? top1Text.includes(String(query.expected_top1_contains).toLowerCase())
    : true;

  const top1InTop3 = query.expected_top1_contains
    ? top3.some((result) => itemText(result).includes(String(query.expected_top1_contains).toLowerCase()))
    : true;

  let reciprocalRank = 0;
  if (query.expected_top1_contains) {
    const searchTerm = String(query.expected_top1_contains).toLowerCase();
    for (let index = 0; index < top10.length; index += 1) {
      if (itemText(top10[index]).includes(searchTerm)) {
        reciprocalRank = 1 / (index + 1);
        break;
      }
    }
  }

  const absentTerms = query.expected_absent ?? [];
  const absentViolations = absentTerms.filter((term) => top5Combined.includes(String(term).toLowerCase()));

  const typeMatch = top5.length > 0
    && query.expected_top1_type
    && itemType(top5[0]) === query.expected_top1_type;

  const top5Families = top5.map((result) => inferTargetFamily(result));
  const familyCounts = top5Families.reduce((accumulator, family) => {
    accumulator[family] = (accumulator[family] ?? 0) + 1;
    return accumulator;
  }, {});

  const primaryFamily = query?.target_families?.primary ?? null;
  const secondaryFamilies = query?.target_families?.secondary ?? [];
  const primaryFamilyHits = primaryFamily ? (familyCounts[primaryFamily] ?? 0) : 0;
  const secondaryFamilyHits = secondaryFamilies.reduce((sum, family) => sum + (familyCounts[family] ?? 0), 0);
  const artifactViolations = inspectArtifactViolations(query, top5);

  return {
    id: query.id,
    intent: query.intent,
    route: query.route,
    query: query.query,
    precisionAt5,
    reciprocalRank,
    top1Match,
    top1InTop3,
    typeMatch,
    absentViolations,
    artifactViolations,
    foundTerms,
    missedTerms: expectedTerms.filter((term) => !foundTerms.map((found) => found.toLowerCase()).includes(term.toLowerCase())),
    resultCount: items.length,
    top5Types: top5.map((result) => itemType(result)),
    top5Families,
    familyCounts,
    primaryFamilyHits,
    secondaryFamilyHits,
    top5Summaries: top5.map((result) => itemSummary(result)),
    latencyMs: recallResult?.latencyMs ?? null,
  };
}
