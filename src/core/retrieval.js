import { performance } from "node:perf_hooks";

import { embedText } from "./embeddings.js";
import { scoreHintOutcome } from "./hint-policy.js";
import { analyzeClaimsTruthSet } from "./claim-resolution.js";
import { getWeight } from "./relation-types.js";
import { estimateTokens } from "./utils.js";
import { cosineSimilarity } from "./vector-math.js";
import {
  reciprocalRankFusion,
  applyCategoryBoosts,
  applyOriginPenalty as rrfApplyOriginPenalty,
  applySeedBonus,
  breakRRFTies,
  validateRRFInput,
} from "./rrf.js";

const HINT_BASE_WEIGHT = 0.72;
const GRAPH_SCORE_WEIGHT = 0.4;
const VECTOR_SCORE_WEIGHT = 0.6;
const OBSERVATION_CATEGORY_BOOSTS = {
  decision: 0.25,
  constraint: 0.20,
  fact: 0.15,
  task: 0.10,
  relationship: 0.05,
};
const QUERY_NOISE_TOKENS = new Set([
  "about",
  "all",
  "are",
  "did",
  "do",
  "does",
  "for",
  "from",
  "had",
  "has",
  "have",
  "how",
  "our",
  "that",
  "the",
  "their",
  "them",
  "they",
  "this",
  "use",
  "used",
  "using",
  "was",
  "were",
  "what",
  "when",
  "where",
  "which",
  "who",
  "why",
  "with",
  "work",
  "worked",
  "works",
  "we",
]);

function clamp(value, minimum, maximum) {
  return Math.min(maximum, Math.max(minimum, value));
}

function normalizeText(value) {
  return String(value ?? "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function scoreHintSeed(queryText, seedLabel) {
  const query = normalizeText(queryText);
  const seed = normalizeText(seedLabel);
  if (!query || !seed) {
    return 0;
  }

  if (query.includes(seed) || seed.includes(query)) {
    return 2.4;
  }

  const queryTokens = new Set(query.split(/[^a-z0-9]+/).filter((token) => token.length > 2));
  const seedTokens = seed.split(/[^a-z0-9]+/).filter((token) => token.length > 2);
  const overlap = seedTokens.filter((token) => queryTokens.has(token)).length;

  return overlap * 0.65;
}

function inferHintPredicate(reason) {
  const detail = normalizeText(reason);
  if (detail.includes("depends on")) {
    return "depends_on";
  }
  if (detail.includes("part of")) {
    return "part_of";
  }
  if (detail.includes("integrates with")) {
    return "integrates_with";
  }
  if (detail.includes("stores in")) {
    return "stores_in";
  }
  if (detail.includes("indexes")) {
    return "indexes";
  }
  if (detail.includes("retrieves")) {
    return "retrieves";
  }
  if (detail.includes("captures")) {
    return "captures";
  }

  return "related_to";
}

function stemToken(token) {
  let value = String(token ?? "").toLowerCase();
  for (const suffix of ["ings", "ing", "edly", "ed", "es", "s"]) {
    if (value.length > 5 && value.endsWith(suffix)) {
      value = value.slice(0, -suffix.length);
      break;
    }
  }
  if (value.length > 6 && value.endsWith("ion")) {
    value = value.slice(0, -3);
  }
  return value;
}

function normalizeTextTokens(text) {
  return new Set(
    String(text ?? "")
      .toLowerCase()
      .split(/[^a-z0-9]+/)
      .filter((token) => token.length > 2)
      .map((token) => stemToken(token))
      .filter(Boolean)
  );
}

function lexicalQueryTokens(text) {
  return [...normalizeTextTokens(text)].filter((token) => !QUERY_NOISE_TOKENS.has(token));
}

const QUERY_SYNONYM_MAP = new Map([
  ["rhi", ["rumor has it"]],
  ["rumor", ["rhi"]],
  ["dns", ["domain", "domains", "dns management"]],
  ["provider", ["management", "managed", "all domains"]],
  ["price", ["pricing", "priced", "per unit", "shipping"]],
  ["pricing", ["price", "per unit", "shipping"]],
  ["cogs", ["cost", "manufacturing cost", "production cost", "unit cost", "cost per unit"]],
  ["trademark", ["registered", "registration", "boip", "euipo", "benelux", "eu trademark"]],
  ["trademarks", ["trademark", "registered", "boip", "euipo", "benelux", "eu trademark"]],
  ["holdings", ["registered", "registration"]],
  ["policy", ["decision", "decided"]],
  ["decided", ["decision", "policy"]],
  ["local", ["ollama", "qwen", "classifier"]],
  ["llm", ["ollama", "qwen", "classifier"]],
  ["shipping", ["ship", "public", "public facing"]],
  ["public", ["public facing", "visibility", "written"]],
  ["facing", ["public facing", "visibility"]],
]);

const META_RESULT_MARKERS = [
  "retrieval",
  "benchmark",
  "golden set",
  "golden query",
  "pass rate",
  "mrr",
  "p@",
  "top hit",
  "top result",
  "query scored",
  "queries",
  "misses cluster",
  "acceptance criteria",
  "scored",
  "ranking",
  "ranked",
  "coverage",
];

const ARTIFACT_TEXT_MARKERS = [
  "retrieval-quality",
  "retrieval quality",
  "benchmark summary",
  "golden-retrieval-set",
  "retrieval-quality-history",
  '"avgprecisionat5"',
  '"avgmrr"',
  '"passrate"',
  '"top5summaries"',
  '"expected_top1_contains"',
  '"expected_in_top5"',
  '"expected_absent"',
  '"priority":',
  '"status":',
  '"detail":',
  '"title":',
  '"tasks":',
  '"decisions":',
  '"constraints":',
  '"queries":',
  "```json",
  "fix/retrieval-quality",
];

const ARTIFACT_PATH_MARKERS = [
  "retrieval-quality",
  "golden-retrieval-set",
  "retrieval-quality-history",
  "benchmark",
  "audit",
  "tasks.json",
  "todo.json",
  "registry",
];

export const RETRIEVAL_ROUTE_PROFILES = {
  current_state: {
    primaryTargetFamily: "canonical",
    secondaryTargetFamilies: ["conversational"],
    artifactBoundary: "hard_exclude",
  },
  history_temporal: {
    primaryTargetFamily: "conversational",
    secondaryTargetFamilies: ["canonical"],
    artifactBoundary: "hard_exclude",
  },
  why_explanatory: {
    primaryTargetFamily: "conversational",
    secondaryTargetFamilies: ["canonical"],
    artifactBoundary: "hard_exclude",
  },
  general: {
    primaryTargetFamily: "canonical",
    secondaryTargetFamilies: ["conversational"],
    artifactBoundary: "hard_exclude",
  },
};

const ROUTE_SOURCE_KEYS = {
  current_state: {
    primary: ["claims", "graphCanonical", "registryLexical"],
    secondary: ["graphConversational", "vectorObservations", "vectorMessages", "ftsConversational"],
  },
  history_temporal: {
    primary: ["graphConversational", "vectorMessages", "vectorObservations", "ftsConversational"],
    secondary: ["graphCanonical", "claims", "registryLexical"],
  },
  why_explanatory: {
    primary: ["graphConversational", "graphCanonical", "claims", "vectorObservations", "ftsConversational"],
    secondary: ["vectorMessages", "registryLexical", "clusterLevels"],
  },
  general: {
    primary: ["graphCanonical", "claims", "registryLexical", "graphConversational", "vectorObservations"],
    secondary: ["vectorMessages", "ftsConversational", "clusterLevels"],
  },
};

const ROUTE_SOURCE_LIMITS = {
  primary: 40,
  secondary: 18,
};

const ROUTE_MIN_PRIMARY_RESULTS = {
  current_state: 6,
  history_temporal: 5,
  why_explanatory: 6,
  general: 6,
};

const WHY_ROUTE_PATTERNS = [
  /^(?:why\b|rationale\b|reason (?:for|behind)\b|how come\b|what motivated\b|what was the reasoning behind\b)/i,
];

const HISTORY_ROUTE_PATTERNS = [
  /^(?:what happened\b|when did\b|when was\b|timeline of\b|history of\b|what changes were made\b|what changed\b|what did we build\b)/i,
  /\bwhat was decided about\b/i,
];

const CURRENT_STATE_ROUTE_PATTERNS = [
  /^(?:what(?:'s| is| are)\b|what do we use\b|what .* do we use\b|what email service\b|what hosting platform\b|who owns\b|current(?: status)?\b|status of\b)/i,
  /\bdo we use\b/i,
  /\bcurrently active\b/i,
];

const GENERAL_ROUTE_PATTERNS = [
  /\bhow should i handle it\b/i,
  /\bknown weaknesses\b/i,
  /\btradeoffs\b/i,
  /\bwhat are the tradeoffs\b/i,
  /\bwhat should i do\b/i,
  /\bwhat changed and why\b/i,
];

const TEMPORAL_QUERY_PATTERNS = [
  /\btoday\b/i,
  /\byesterday\b/i,
  /\bthis week\b/i,
  /\blast week\b/i,
  /\bthis month\b/i,
  /\blast month\b/i,
  /\brecently\b/i,
  /\bwhen (?:did|was)\b/i,
];

const OPERATIONAL_PROVENANCE_PATTERNS = [
  { kind: "benchmark", pattern: /\bbenchmark\b/i },
  { kind: "audit", pattern: /\baudit\b/i },
  { kind: "diagnostic", pattern: /\bdiagnostic\b/i },
  { kind: "debug", pattern: /\bdebug\b/i },
  { kind: "fixture", pattern: /\bfixture\b/i },
  { kind: "test", pattern: /(?:^|[\/_-])test(?:s|ing)?(?:$|[\/_.-])/i },
  { kind: "telemetry", pattern: /\btelemetry\b/i },
  { kind: "golden", pattern: /\bgolden(?:[-_\s]set|[-_\s]retrieval)?\b/i },
];

const _DIRECT_ENTITY_BLACKLIST = new Set([
  "cost",
  "costs",
  "cogs",
  "current",
  "email",
  "history",
  "hosting",
  "how",
  "owner",
  "ownership",
  "platform",
  "price",
  "pricing",
  "provider",
  "status",
  "timeline",
  "use",
  "uses",
  "using",
  "what",
  "when",
  "where",
  "which",
  "why",
]);

function expandQueryText(queryText) {
  const normalized = normalizeText(queryText);
  const additions = new Set();
  for (const token of lexicalQueryTokens(queryText)) {
    for (const synonym of QUERY_SYNONYM_MAP.get(token) ?? []) {
      additions.add(synonym);
    }
  }

  return additions.size ? `${normalized} ${[...additions].join(" ")}` : normalized;
}

export function buildQueryProfile(queryText) {
  const normalized = normalizeText(queryText);
  const expandedQuery = expandQueryText(queryText);
  const tokens = new Set(lexicalQueryTokens(expandedQuery));
  const isQuestion = /^(what|who|which|where|when|why|how)/.test(normalized) || normalized.includes("?");
  const wantsDecision = tokens.has("decid") || tokens.has("decide") || tokens.has("decision") || tokens.has("policy");
  const wantsBehavior = tokens.has("pattern") || tokens.has("handle") || tokens.has("behavior") || tokens.has("public") || tokens.has("visibility") || tokens.has("perfectionism");
  const wantsFact = isQuestion && !wantsBehavior;

  return {
    normalized,
    expandedQuery,
    tokens,
    isQuestion,
    wantsFact,
    wantsDecision,
    wantsBehavior,
    penalizeMetaResults: wantsFact || wantsDecision,
  };
}

function startOfUtcDay(date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
}

function _endOfUtcDay(date) {
  const next = startOfUtcDay(date);
  next.setUTCDate(next.getUTCDate() + 1);
  next.setUTCMilliseconds(next.getUTCMilliseconds() - 1);
  return next;
}

function startOfUtcWeek(date) {
  const start = startOfUtcDay(date);
  const dayIndex = (start.getUTCDay() + 6) % 7;
  start.setUTCDate(start.getUTCDate() - dayIndex);
  return start;
}

function _endOfUtcWeek(date) {
  const next = startOfUtcWeek(date);
  next.setUTCDate(next.getUTCDate() + 7);
  next.setUTCMilliseconds(next.getUTCMilliseconds() - 1);
  return next;
}

function startOfUtcMonth(date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1));
}

function _endOfUtcMonth(date) {
  const next = startOfUtcMonth(date);
  next.setUTCMonth(next.getUTCMonth() + 1);
  next.setUTCMilliseconds(next.getUTCMilliseconds() - 1);
  return next;
}

function isoTimestamp(date) {
  return date instanceof Date && Number.isFinite(date.getTime())
    ? date.toISOString()
    : null;
}

function collectRouteSignals(queryText) {
  const normalizedQuery = String(queryText ?? "").trim();
  const signals = [];

  if (WHY_ROUTE_PATTERNS.some((pattern) => pattern.test(normalizedQuery))) {
    signals.push({ route: "why_explanatory", confidence: 0.95, reason: "matched_why_query" });
  }
  if (HISTORY_ROUTE_PATTERNS.some((pattern) => pattern.test(normalizedQuery)) || TEMPORAL_QUERY_PATTERNS.some((pattern) => pattern.test(normalizedQuery))) {
    signals.push({ route: "history_temporal", confidence: 0.92, reason: "matched_history_or_temporal_query" });
  }
  if (CURRENT_STATE_ROUTE_PATTERNS.some((pattern) => pattern.test(normalizedQuery))) {
    signals.push({ route: "current_state", confidence: 0.88, reason: "matched_current_state_query" });
  }
  if (GENERAL_ROUTE_PATTERNS.some((pattern) => pattern.test(normalizedQuery))) {
    signals.push({ route: "general", confidence: 0.7, reason: "matched_general_query" });
  }

  return signals;
}

function getTimezoneFormatter(timezone, options = {}) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    ...options,
  });
}

function getZonedDateParts(date, timezone) {
  const formatter = getTimezoneFormatter(timezone);
  const values = Object.fromEntries(
    formatter.formatToParts(date)
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value])
  );

  return {
    year: Number(values.year),
    month: Number(values.month),
    day: Number(values.day),
  };
}

function getTimeZoneOffsetMs(date, timezone) {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  const values = Object.fromEntries(
    formatter.formatToParts(date)
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value])
  );
  const asUtc = Date.UTC(
    Number(values.year),
    Number(values.month) - 1,
    Number(values.day),
    Number(values.hour),
    Number(values.minute),
    Number(values.second),
    0,
  );
  return asUtc - date.getTime();
}

function zonedLocalToUtc({ year, month, day, hour = 0, minute = 0, second = 0, millisecond = 0 }, timezone) {
  let utcGuess = Date.UTC(year, month - 1, day, hour, minute, second, millisecond);

  for (let attempt = 0; attempt < 4; attempt += 1) {
    const offset = getTimeZoneOffsetMs(new Date(utcGuess), timezone);
    const adjusted = Date.UTC(year, month - 1, day, hour, minute, second, millisecond) - offset;
    if (adjusted === utcGuess) {
      break;
    }
    utcGuess = adjusted;
  }

  return new Date(utcGuess);
}

function normalizeLocalDayRange(parts, timezone) {
  const start = zonedLocalToUtc({ ...parts, hour: 0, minute: 0, second: 0, millisecond: 0 }, timezone);
  const nextDay = shiftLocalDate(parts, 1);
  const nextDayStart = zonedLocalToUtc({ ...nextDay, hour: 0, minute: 0, second: 0, millisecond: 0 }, timezone);
  const end = new Date(nextDayStart.getTime() - 1);
  return {
    startAt: isoTimestamp(start),
    endAt: isoTimestamp(end),
  };
}

function shiftLocalDate(parts, deltaDays) {
  const shifted = new Date(Date.UTC(parts.year, parts.month - 1, parts.day + deltaDays));
  return {
    year: shifted.getUTCFullYear(),
    month: shifted.getUTCMonth() + 1,
    day: shifted.getUTCDate(),
  };
}

function getLocalWeekdayIndex(parts) {
  return new Date(Date.UTC(parts.year, parts.month - 1, parts.day)).getUTCDay();
}

function detectTemporalExpression(queryText) {
  const query = String(queryText ?? "").trim();
  if (!query) {
    return null;
  }

  const phrasePatterns = [
    /\btoday\b/i,
    /\byesterday\b/i,
    /\blast week\b/i,
    /\blast month\b/i,
    /\bthis week\b/i,
    /\bthis month\b/i,
    /\brecently\b/i,
    /\bwhen (?:did|was)\b/i,
    /\b\d{4}-\d{2}-\d{2}\s*(?:to|through|until|-)\s*\d{4}-\d{2}-\d{2}\b/i,
    /\b\d{4}-\d{2}-\d{2}\b/i,
  ];

  for (const pattern of phrasePatterns) {
    const match = query.match(pattern);
    if (match) {
      return match[0];
    }
  }

  return null;
}

function parseExplicitDateToken(value) {
  const match = String(value ?? "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) {
    return null;
  }

  return {
    year: Number(match[1]),
    month: Number(match[2]),
    day: Number(match[3]),
  };
}

export function parseTemporalWindow(queryText, { now = new Date(), timezone = "Europe/Amsterdam" } = {}) {
  const query = String(queryText ?? "").trim();
  if (!query) {
    return null;
  }

  const expression = detectTemporalExpression(query);
  if (!expression) {
    return null;
  }

  const anchor = now instanceof Date ? now : new Date(now);
  const localNow = getZonedDateParts(anchor, timezone);

  if (/^when (?:did|was)\b/i.test(expression)) {
    return {
      kind: "point",
      startAt: null,
      endAt: null,
      timezone,
      confidence: 0.66,
      derivedFrom: ["when_was_lookup"],
      expression,
      parseStatus: "parsed",
    };
  }

  if (/\btoday\b/i.test(expression)) {
    return {
      kind: "relative_range",
      ...normalizeLocalDayRange(localNow, timezone),
      timezone,
      confidence: 0.95,
      derivedFrom: ["today"],
      expression,
      parseStatus: "parsed",
    };
  }

  if (/\byesterday\b/i.test(expression)) {
    return {
      kind: "relative_range",
      ...normalizeLocalDayRange(shiftLocalDate(localNow, -1), timezone),
      timezone,
      confidence: 0.95,
      derivedFrom: ["yesterday"],
      expression,
      parseStatus: "parsed",
    };
  }

  if (/\bthis week\b/i.test(expression)) {
    const dayIndex = (getLocalWeekdayIndex(localNow) + 6) % 7;
    const thisWeekStart = shiftLocalDate(localNow, -dayIndex);
    const thisWeekEnd = shiftLocalDate(thisWeekStart, 6);
    return {
      kind: "relative_range",
      startAt: normalizeLocalDayRange(thisWeekStart, timezone).startAt,
      endAt: normalizeLocalDayRange(thisWeekEnd, timezone).endAt,
      timezone,
      confidence: 0.94,
      derivedFrom: ["this_week"],
      expression,
      parseStatus: "parsed",
    };
  }

  if (/\blast week\b/i.test(expression)) {
    const dayIndex = (getLocalWeekdayIndex(localNow) + 6) % 7;
    const previousWeekStart = shiftLocalDate(localNow, -(dayIndex + 7));
    const previousWeekEnd = shiftLocalDate(previousWeekStart, 6);
    return {
      kind: "relative_range",
      startAt: normalizeLocalDayRange(previousWeekStart, timezone).startAt,
      endAt: normalizeLocalDayRange(previousWeekEnd, timezone).endAt,
      timezone,
      confidence: 0.94,
      derivedFrom: ["last_week"],
      expression,
      parseStatus: "parsed",
    };
  }

  if (/\bthis month\b/i.test(expression)) {
    const thisMonthStart = { year: localNow.year, month: localNow.month, day: 1 };
    const nextMonth = localNow.month === 12
      ? { year: localNow.year + 1, month: 1, day: 1 }
      : { year: localNow.year, month: localNow.month + 1, day: 1 };
    const thisMonthEnd = shiftLocalDate(nextMonth, -1);
    return {
      kind: "relative_range",
      startAt: normalizeLocalDayRange(thisMonthStart, timezone).startAt,
      endAt: normalizeLocalDayRange(thisMonthEnd, timezone).endAt,
      timezone,
      confidence: 0.93,
      derivedFrom: ["this_month"],
      expression,
      parseStatus: "parsed",
    };
  }

  if (/\blast month\b/i.test(expression)) {
    const previousMonthAnchor = localNow.month === 1
      ? { year: localNow.year - 1, month: 12, day: 1 }
      : { year: localNow.year, month: localNow.month - 1, day: 1 };
    const nextMonth = previousMonthAnchor.month === 12
      ? { year: previousMonthAnchor.year + 1, month: 1, day: 1 }
      : { year: previousMonthAnchor.year, month: previousMonthAnchor.month + 1, day: 1 };
    const previousMonthEnd = shiftLocalDate(nextMonth, -1);
    return {
      kind: "relative_range",
      startAt: normalizeLocalDayRange(previousMonthAnchor, timezone).startAt,
      endAt: normalizeLocalDayRange(previousMonthEnd, timezone).endAt,
      timezone,
      confidence: 0.93,
      derivedFrom: ["last_month"],
      expression,
      parseStatus: "parsed",
    };
  }

  if (/\brecently\b/i.test(expression)) {
    const recentStart = shiftLocalDate(localNow, -14);
    return {
      kind: "relative_range",
      startAt: normalizeLocalDayRange(recentStart, timezone).startAt,
      endAt: normalizeLocalDayRange(localNow, timezone).endAt,
      timezone,
      confidence: 0.35,
      derivedFrom: ["recently"],
      expression,
      parseStatus: "parsed",
      soft: true,
    };
  }

  const rangeMatch = query.match(/\b(\d{4}-\d{2}-\d{2})\s*(?:to|through|until|-)\s*(\d{4}-\d{2}-\d{2})\b/i);
  if (rangeMatch) {
    const start = parseExplicitDateToken(rangeMatch[1]);
    const end = parseExplicitDateToken(rangeMatch[2]);
    if (!start || !end) {
      return null;
    }
    return {
      kind: "range",
      startAt: normalizeLocalDayRange(start, timezone).startAt,
      endAt: normalizeLocalDayRange(end, timezone).endAt,
      timezone,
      confidence: 0.92,
      derivedFrom: [rangeMatch[1], rangeMatch[2]],
      expression: rangeMatch[0],
      parseStatus: "parsed",
    };
  }

  const dateMatch = query.match(/\b(\d{4}-\d{2}-\d{2})\b/);
  if (dateMatch) {
    const parsed = parseExplicitDateToken(dateMatch[1]);
    if (!parsed) {
      return null;
    }
    return {
      kind: "range",
      ...normalizeLocalDayRange(parsed, timezone),
      timezone,
      confidence: 0.9,
      derivedFrom: [dateMatch[1]],
      expression: dateMatch[1],
      parseStatus: "parsed",
    };
  }

  return null;
}

export function inferOperationalArtifactKind(value = {}) {
  const payload = value?.payload ?? value ?? {};
  const provenanceFields = [
    payload.path,
    payload.file_path,
    payload.filePath,
    payload.message_ingest_id,
    payload.messageIngestId,
    payload.ingest_id,
    payload.ingestId,
    payload.origin_kind,
    payload.originKind,
    value?.source,
    value?.event_id,
  ]
    .map((entry) => normalizeText(entry))
    .filter(Boolean);

  if (!provenanceFields.length) {
    return null;
  }

  for (const field of provenanceFields) {
    for (const descriptor of OPERATIONAL_PROVENANCE_PATTERNS) {
      if (descriptor.pattern.test(field)) {
        return descriptor.kind;
      }
    }
  }

  return null;
}

function decorateRetrievalResult(result, fallbackTargetFamily) {
  const artifactKind = inferOperationalArtifactKind(result);
  return {
    ...result,
    targetFamily: artifactKind ? "operational" : fallbackTargetFamily,
    artifactKind,
  };
}

export function planRetrievalRoute(queryText, { queryProfile = null, seedEntities = [], now = new Date(), timezone = "Europe/Amsterdam" } = {}) {
  const profile = queryProfile ?? buildQueryProfile(queryText);
  const temporalExpression = detectTemporalExpression(queryText);
  const temporal = parseTemporalWindow(queryText, { now, timezone });
  const signals = collectRouteSignals(queryText);
  const hasExplanatoryPhrase = signals.some((signal) => signal.route === "why_explanatory")
    || WHY_ROUTE_PATTERNS.some((pattern) => pattern.test(String(queryText ?? "").trim()));
  const hasCurrentStatePhrase = signals.some((signal) => signal.route === "current_state")
    || CURRENT_STATE_ROUTE_PATTERNS.some((pattern) => pattern.test(String(queryText ?? "").trim()))
    || (profile.wantsFact && !temporalExpression && !hasExplanatoryPhrase);
  const queryFeatures = {
    has_temporal_phrase: Boolean(temporalExpression),
    has_explanatory_phrase: hasExplanatoryPhrase,
    has_current_state_phrase: Boolean(hasCurrentStatePhrase),
    resolved_entities: seedEntities.length,
  };

  let route = "general";
  let routeReason = GENERAL_ROUTE_PATTERNS.some((pattern) => pattern.test(String(queryText ?? "").trim()))
    ? "general_pattern"
    : "no_specialized_route";
  let confidence = 0.45;
  let fallbackUsed = false;
  let fallbackReason = null;

  if (queryFeatures.has_temporal_phrase && queryFeatures.has_explanatory_phrase) {
    route = "why_explanatory";
    routeReason = temporal ? "temporal_explanatory_constraint" : "temporal_parse_failed_explanatory";
    confidence = temporal ? 0.97 : 0.82;
  } else if (queryFeatures.has_temporal_phrase) {
    route = "history_temporal";
    routeReason = temporal ? "temporal_constraint" : "temporal_parse_failed";
    confidence = temporal ? 0.95 : 0.78;
  } else if (queryFeatures.has_current_state_phrase) {
    route = "current_state";
    routeReason = profile.wantsFact ? "current_truth_intent" : "current_state_pattern";
    confidence = profile.wantsFact ? 0.84 : 0.8;
  } else if (queryFeatures.has_explanatory_phrase) {
    route = "why_explanatory";
    routeReason = "explanatory_intent";
    confidence = 0.91;
  } else if (signals.some((signal) => signal.route === "general")) {
    fallbackUsed = true;
    fallbackReason = "ambiguous_general_query";
  }

  if (route === "general") {
    fallbackUsed = true;
    fallbackReason = fallbackReason ?? (queryFeatures.has_temporal_phrase ? "no_specialized_route_after_parse_failure" : "no_specialized_route");
  }

  const profileConfig = RETRIEVAL_ROUTE_PROFILES[route] ?? RETRIEVAL_ROUTE_PROFILES.general;

  return {
    route,
    routeReason,
    confidence: Number(confidence.toFixed(2)),
    fallbackUsed,
    fallbackReason,
    reasons: uniqueValues([routeReason, ...signals.map((signal) => signal.reason)]),
    sourceSignals: signals.map((signal) => ({
      route: signal.route,
      confidence: Number(signal.confidence.toFixed(2)),
      reason: signal.reason,
    })),
    queryFeatures,
    targetFamilies: {
      primary: profileConfig.primaryTargetFamily,
      secondary: [...profileConfig.secondaryTargetFamilies],
    },
    artifactBoundary: profileConfig.artifactBoundary,
    allowOperational: false,
    temporalExpression,
    temporalParseStatus: queryFeatures.has_temporal_phrase
      ? (temporal?.parseStatus ?? "failed")
      : "not_applicable",
    temporal: temporal ?? {
      kind: "unbounded",
      startAt: null,
      endAt: null,
      timezone,
      confidence: 0,
      derivedFrom: [],
      expression: temporalExpression,
      parseStatus: queryFeatures.has_temporal_phrase ? "failed" : "not_applicable",
    },
  };
}

export function classifyRetrievalRoute(queryText, options = {}) {
  return planRetrievalRoute(queryText, options).route;
}

function isMessageLikeType(type) {
  return type.endsWith("_message") || type === "message";
}

function answerTypeWeight(result, profile) {
  if (!profile) {
    return 1;
  }

  const claimType = normalizeText(result.payload?.claim_type ?? "");
  const type = normalizeText(result.type);
  const messageLike = isMessageLikeType(type);

  if (profile.wantsBehavior) {
    if (messageLike) {
      return 1.12;
    }
    if (type === "chunk" || type === "entity") {
      return 0.92;
    }
    if (type === "task") {
      return 0.88;
    }
    return 1.05;
  }

  if (profile.wantsDecision) {
    if (type === "decision" || claimType === "decision") {
      return 1.42;
    }
    if (type === "constraint") {
      return 1.16;
    }
    if (type === "claim") {
      return 1.18;
    }
    if (messageLike) {
      return 0.72;
    }
    if (type === "chunk" || type === "entity" || type === "task") {
      return 0.8;
    }
    return 0.94;
  }

  if (profile.wantsFact) {
    if (type === "fact" || type === "constraint") {
      return 1.24;
    }
    if (type === "decision") {
      return 1.08;
    }
    if (type === "claim") {
      return 1.3;
    }
    if (messageLike) {
      return 0.68;
    }
    if (type === "chunk" || type === "entity") {
      return 0.82;
    }
    if (type === "task") {
      return 0.76;
    }
  }

  return 1;
}

function metaResultPenalty(result, profile) {
  if (!profile?.penalizeMetaResults) {
    return 1;
  }

  const payload = result.payload ?? {};
  const summary = normalizeText(result.summary ?? payload.detail ?? payload.content ?? "");
  const content = normalizeText(payload.content ?? payload.detail ?? payload.value_text ?? "");
  const path = normalizeText(payload.path ?? payload.file_path ?? payload.filePath ?? "");
  const artifactText = `${summary} ${content}`.trim();
  if (!artifactText && !path) {
    return 1;
  }

  const looksLikeJsonArtifact = artifactText.startsWith('{"entities"')
    || artifactText.startsWith('```json')
    || artifactText.includes('{"tasks":')
    || artifactText.includes('{"decisions":')
    || artifactText.includes('{"constraints":');
  const hasArtifactPath = ARTIFACT_PATH_MARKERS.some((marker) => path.includes(marker));
  const hasArtifactText = ARTIFACT_TEXT_MARKERS.some((marker) => artifactText.includes(marker));
  const hasMetaMarkers = META_RESULT_MARKERS.filter((marker) => artifactText.includes(marker)).length;

  if (looksLikeJsonArtifact || hasArtifactPath || hasArtifactText) {
    return 0.22;
  }

  if (hasMetaMarkers >= 2) {
    return 0.32;
  }

  return hasMetaMarkers === 1 ? 0.45 : 1;
}

export function applyQueryIntentWeighting(results, queryProfile) {
  return (results ?? []).map((result) => {
    const lexicalBoost = scoreLexicalMatch(queryProfile.expandedQuery, result.summary ?? result.payload?.detail ?? result.payload?.content ?? "");
    const lexicalMultiplier = lexicalBoost > 0 ? 1 + Math.min(0.28, lexicalBoost * 0.035) : 1;
    const typeMultiplier = answerTypeWeight(result, queryProfile);
    const metaMultiplier = metaResultPenalty(result, queryProfile);

    return {
      ...result,
      queryLexicalBoost: lexicalBoost,
      score: Number(result.score ?? 0) * lexicalMultiplier * typeMultiplier * metaMultiplier,
    };
  });
}

function tokensLooselyMatch(left, right) {
  if (!left || !right) {
    return false;
  }
  if (left === right) {
    return true;
  }
  if (left.length >= 4 && right.length >= 4 && (left.startsWith(right) || right.startsWith(left))) {
    return true;
  }
  return false;
}

function scoreLexicalMatch(queryText, candidateText) {
  const queryTokens = lexicalQueryTokens(queryText);
  if (!queryTokens.length) {
    return 0;
  }

  const candidateTokens = [...normalizeTextTokens(candidateText)];
  if (!candidateTokens.length) {
    return 0;
  }

  const overlap = queryTokens.filter((token) => candidateTokens.some((candidate) => tokensLooselyMatch(token, candidate))).length;
  if (!overlap) {
    return 0;
  }

  const normalizedCandidate = normalizeText(candidateText);
  const normalizedQuery = normalizeText(queryText);
  const phraseBonus = normalizedCandidate.includes(normalizedQuery) ? 0.8 : 0;

  return overlap * 1.4 + (overlap / queryTokens.length) * 2.2 + phraseBonus;
}

function createRegistryLexicalResults(database, queryText, resolvedScopeFilter) {
  const expandedQueryText = expandQueryText(queryText);
  const candidates = [
    ...database.prepare(`
      SELECT id, entity_id, detail, created_at
      FROM facts
      ORDER BY created_at DESC
      LIMIT 4000
    `).all().map((item) => ({
      type: "fact",
      id: item.id,
      entityId: item.entity_id ?? null,
      summary: item.detail,
      payload: item,
      tokenCount: estimateTokens(item.detail),
      hintIds: [],
    })),
    ...database.prepare(`
      SELECT id, entity_id, title, rationale, created_at
      FROM decisions
      ORDER BY created_at DESC
      LIMIT 1500
    `).all().map((item) => ({
      type: "decision",
      id: item.id,
      entityId: item.entity_id ?? null,
      summary: item.rationale ? `${item.title} — ${item.rationale}` : item.title,
      payload: item,
      tokenCount: estimateTokens(item.rationale ? `${item.title} ${item.rationale}` : item.title),
      hintIds: [],
    })),
    ...database.prepare(`
      SELECT id, entity_id, detail, severity, created_at
      FROM constraints
      ORDER BY created_at DESC
      LIMIT 1500
    `).all().map((item) => ({
      type: "constraint",
      id: item.id,
      entityId: item.entity_id ?? null,
      summary: item.detail,
      payload: item,
      tokenCount: estimateTokens(item.detail),
      hintIds: [],
    })),
    ...database.prepare(`
      SELECT id, subject_entity_id, object_entity_id, value_text, claim_type, lifecycle_state, importance_score, created_at,
             scope_kind, scope_id
      FROM claims
      WHERE lifecycle_state IN ('active', 'candidate')
      ORDER BY created_at DESC
      LIMIT 4000
    `).all()
      .filter((item) => matchesScopeFilter(item, resolvedScopeFilter, "private"))
      .map((item) => ({
        type: "claim",
        id: item.id,
        entityId: item.subject_entity_id ?? item.object_entity_id ?? null,
        summary: item.value_text,
        payload: item,
        tokenCount: estimateTokens(item.value_text),
        hintIds: [],
      })),
  ];

  return candidates
    .map((item) => ({
      ...decorateRetrievalResult(item, "canonical"),
      score: scoreLexicalMatch(expandedQueryText, item.summary),
    }))
    .filter((item) => item.score > 0)
    .sort((left, right) => right.score - left.score)
    .slice(0, 60);
}

function normalizeDedupText(text) {
  return String(text ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function jaccardSimilarity(tokens1, tokens2) {
  const intersection = new Set([...tokens1].filter((x) => tokens2.has(x)));
  const union = new Set([...tokens1, ...tokens2]);
  return union.size === 0 ? 0 : intersection.size / union.size;
}

function getRegistryEntityId(result) {
  return result.payload?.subject_entity_id
    ?? result.payload?.entity_id
    ?? result.payload?.object_entity_id
    ?? result.entityId
    ?? null;
}

function getRegistryText(result) {
  return result.payload?.value_text
    ?? result.payload?.title
    ?? result.payload?.detail
    ?? result.summary
    ?? "";
}

function claimOverlapsRegistryItem(claim, registryItem) {
  const claimType = claim.payload?.claim_type ?? claim.type;
  const claimEntityId = claim.payload?.subject_entity_id
    ?? claim.payload?.object_entity_id
    ?? claim.entityId
    ?? null;
  const registryEntityId = getRegistryEntityId(registryItem);

  if (!claimType || !claimEntityId || !registryEntityId) {
    return false;
  }

  if (claimType !== registryItem.type || claimEntityId !== registryEntityId) {
    return false;
  }

  const claimText = claim.payload?.value_text ?? claim.summary ?? "";
  const registryText = getRegistryText(registryItem);
  const normalizedClaimText = normalizeDedupText(claimText);
  const normalizedRegistryText = normalizeDedupText(registryText);

  if (!normalizedClaimText || !normalizedRegistryText) {
    return false;
  }

  if (normalizedClaimText === normalizedRegistryText) {
    return true;
  }

  return jaccardSimilarity(
    normalizeTextTokens(normalizedClaimText),
    normalizeTextTokens(normalizedRegistryText),
  ) >= 0.8;
}

function deduplicateClaimsAgainstRegistry(results, routePlan = null) {
  const claims = [];
  const registryResults = [];

  for (const result of results) {
    if (result.type === "claim") {
      claims.push(result);
    } else {
      registryResults.push(result);
    }
  }

  if (!claims.length) {
    return results;
  }

  const filteredRegistryResults = registryResults.filter((registryItem) => {
    const overlappingClaim = claims.find((claim) => claimOverlapsRegistryItem(claim, registryItem));
    if (!overlappingClaim) {
      return true;
    }

    if (isTemporalPointLookup(routePlan)
      && parseTimestampMs(getResultTimestamp(registryItem)) !== null
      && (isDecisionBearingResult(registryItem) || hasExplicitTemporalAnchor(registryItem))) {
      return true;
    }

    return false;
  });

  return [...filteredRegistryResults, ...claims];
}

function dedupeResults(results) {
  const deduped = new Map();

  for (const result of results) {
    const key = `${result.type}:${result.id}`;
    const existing = deduped.get(key);

    if (!existing) {
      deduped.set(key, {
        ...result,
        hintIds: uniqueValues(result.hintIds ?? []),
      });
      continue;
    }

    const existingScore = Number(existing.score ?? 0);
    const nextScore = Number(result.score ?? 0);
    const preferred = nextScore > existingScore ? result : existing;

    deduped.set(key, {
      ...preferred,
      hintIds: mergeHintIds(existing.hintIds ?? [], result.hintIds ?? []),
    });
  }

  return [...deduped.values()];
}

function uniqueValues(values) {
  return [...new Set(values.filter(Boolean))];
}

function normalizeScopeFilter(scopeFilter) {
  if (!scopeFilter) {
    return null;
  }

  if (typeof scopeFilter === "string") {
    return {
      scopeKind: scopeFilter,
      scopeId: null,
    };
  }

  return {
    scopeKind: scopeFilter.scopeKind ?? null,
    scopeId: scopeFilter.scopeId ?? null,
  };
}

function provenanceHintIds(provenance, entityId) {
  return provenance.get(entityId)?.hintIds ?? [];
}

function mergeHintIds(...lists) {
  return uniqueValues(lists.flat());
}

function applyOriginPenalty(score, payload) {
  if (payload?.origin_kind === "agent") {
    return score * 0.85;
  }

  return score;
}

const SCOPE_ORDER = ["private", "project", "shared", "public"];

function scopeRank(scopeKind, defaultScopeKind = "private") {
  const index = SCOPE_ORDER.indexOf(scopeKind ?? defaultScopeKind);
  return index >= 0 ? index : SCOPE_ORDER.indexOf(defaultScopeKind);
}

function matchesScopeFilter(row, scopeFilter, defaultScopeKind = "private") {
  const filter = normalizeScopeFilter(scopeFilter);
  if (!filter?.scopeKind) {
    return true;
  }

  const rowScopeKind = row?.scopeKind ?? row?.scope_kind ?? defaultScopeKind;
  if (scopeRank(rowScopeKind, defaultScopeKind) < scopeRank(filter.scopeKind, defaultScopeKind)) {
    return false;
  }

  if (rowScopeKind === "project" && filter.scopeKind === "project" && filter.scopeId) {
    return (row?.scopeId ?? row?.scope_id ?? null) === filter.scopeId;
  }

  return true;
}

function getResultMessageId(result) {
  if (result.type === "message") {
    return result.id;
  }

  return result.payload?.message_id ?? result.payload?.messageId ?? null;
}

function createVectorResult(message, vectorScore) {
  return decorateRetrievalResult({
    type: "message",
    id: message.id,
    entityId: null,
    score: VECTOR_SCORE_WEIGHT * vectorScore,
    summary: message.content,
    payload: message,
    tokenCount: Number(message.tokenCount ?? estimateTokens(message.content)),
    hintIds: [],
    vectorScore,
    graphScore: 0,
  }, "conversational");
}

function createClusterLevelResult(clusterId, level, text, vectorScore) {
  return decorateRetrievalResult({
    type: `cluster_l${level}`,
    id: `cl:${clusterId}:${level}`,
    entityId: null,
    score: VECTOR_SCORE_WEIGHT * vectorScore,
    summary: text,
    payload: {
      clusterId,
      level,
      text,
    },
    tokenCount: estimateTokens(text),
    hintIds: [],
    vectorScore,
    graphScore: 0,
  }, "conversational");
}

function isObservationResult(result) {
  return Boolean(result?.id && result?.payload?.category && result?.payload?.detail);
}

function getObservationCategoryBoost(category) {
  return OBSERVATION_CATEGORY_BOOSTS[String(category ?? "").trim().toLowerCase()] ?? 0;
}

function normalizeImportanceScore(value) {
  if (value === null || value === undefined) {
    return 1.0;
  }

  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : 1.0;
}

function daysSinceTimestamp(timestamp, nowMs = Date.now()) {
  const resolved = Date.parse(String(timestamp ?? ""));
  if (!Number.isFinite(resolved)) {
    return Number.POSITIVE_INFINITY;
  }

  return Math.max(0, (nowMs - resolved) / (1000 * 60 * 60 * 24));
}

export function messageImportanceProxy(messageTimestamp, nowMs = Date.now()) {
  const daysOld = daysSinceTimestamp(messageTimestamp, nowMs);
  if (!Number.isFinite(daysOld)) {
    return 1.0;
  }

  return Math.max(0.3, 1 - daysOld / 30);
}

export function recencyBoost(daysOld) {
  if (!Number.isFinite(daysOld)) {
    return 1.0;
  }

  if (daysOld < 1) return 1.15;
  if (daysOld < 3) return 1.08;
  if (daysOld < 7) return 1.03;
  return 1.0;
}

function getResultObservationId(result) {
  if (!result || result.type === "message") {
    return null;
  }

  return result.payload?.observation_id
    ?? result.payload?.observationId
    ?? (isObservationResult(result) ? result.id : null);
}

function getResultTimestamp(result) {
  if (!result) {
    return null;
  }

  if (result.type === "message") {
    return result.payload?.capturedAt
      ?? result.payload?.captured_at
      ?? result.payload?.createdAt
      ?? result.payload?.created_at
      ?? null;
  }

  if (getResultObservationId(result)) {
    return result.payload?.created_at
      ?? result.payload?.createdAt
      ?? result.payload?.message_captured_at
      ?? result.payload?.messageCapturedAt
      ?? null;
  }

  return null;
}

function getDaysOld(result, nowMs = Date.now()) {
  return daysSinceTimestamp(getResultTimestamp(result), nowMs);
}

function parseTimestampMs(value) {
  const parsed = Date.parse(String(value ?? ""));
  return Number.isFinite(parsed) ? parsed : null;
}

function hasBoundedTemporalWindow(temporal) {
  return Boolean(temporal?.startAt && temporal?.endAt);
}

function isResultInTemporalWindow(result, temporal) {
  if (!hasBoundedTemporalWindow(temporal)) {
    return false;
  }

  const timestampMs = parseTimestampMs(getResultTimestamp(result));
  if (timestampMs === null) {
    return false;
  }

  const startMs = parseTimestampMs(temporal.startAt);
  const endMs = parseTimestampMs(temporal.endAt);
  if (startMs === null || endMs === null) {
    return false;
  }

  return timestampMs >= startMs && timestampMs <= endMs;
}

function countUniqueResultsAcrossLists(lists) {
  return dedupeResults((lists ?? []).flat()).length;
}

function trimCandidateList(results, phase = "primary") {
  const limit = ROUTE_SOURCE_LIMITS[phase] ?? ROUTE_SOURCE_LIMITS.primary;
  return (results ?? []).slice(0, limit);
}

function applyTemporalWindowToResults(results, temporal) {
  if (!hasBoundedTemporalWindow(temporal)) {
    return {
      results,
      filteredCount: 0,
      inRangeCount: 0,
      outOfRangeCount: 0,
      excludedUntimestampedCount: 0,
      outOfRangeSupportCount: 0,
      noInRangeEvidence: false,
      fallbackSupportUsed: false,
    };
  }

  const timestamped = [];
  const untimestamped = [];

  for (const result of results ?? []) {
    if (parseTimestampMs(getResultTimestamp(result)) === null) {
      untimestamped.push(result);
    } else {
      timestamped.push(result);
    }
  }

  const inRange = timestamped.filter((result) => isResultInTemporalWindow(result, temporal));
  const outOfRange = timestamped.filter((result) => !isResultInTemporalWindow(result, temporal));

  if (!inRange.length) {
    const support = outOfRange
      .sort((left, right) => (parseTimestampMs(getResultTimestamp(right)) ?? 0) - (parseTimestampMs(getResultTimestamp(left)) ?? 0))
      .slice(0, Math.min(4, outOfRange.length))
      .map((result) => ({
        ...result,
        temporalSupportLabel: "out_of_range_support",
      }));

    return {
      results: support,
      filteredCount: timestamped.length,
      inRangeCount: 0,
      outOfRangeCount: outOfRange.length,
      excludedUntimestampedCount: untimestamped.length,
      outOfRangeSupportCount: support.length,
      noInRangeEvidence: true,
      fallbackSupportUsed: support.length > 0,
    };
  }

  return {
    results: inRange,
    filteredCount: outOfRange.length,
    inRangeCount: inRange.length,
    outOfRangeCount: outOfRange.length,
    excludedUntimestampedCount: untimestamped.length,
    outOfRangeSupportCount: 0,
    noInRangeEvidence: false,
    fallbackSupportUsed: false,
  };
}

function countResultsByFamily(results) {
  return (results ?? []).reduce((accumulator, result) => {
    const family = result.targetFamily ?? "unknown";
    accumulator[family] = (accumulator[family] ?? 0) + 1;
    return accumulator;
  }, {});
}

function isRationaleBearingResult(result) {
  const summary = normalizeText(result.summary ?? "");
  const rationale = normalizeText(result.payload?.rationale ?? "");
  return Boolean(rationale)
    || /\b(?:because|so that|tradeoff|reason|why|motivated|instead of|in favor of|to avoid|to reduce)\b/i.test(summary);
}

function isTemporalPointLookup(routePlan) {
  return routePlan?.route === "history_temporal"
    && routePlan?.temporal?.kind === "point"
    && !hasBoundedTemporalWindow(routePlan?.temporal);
}

function isDecisionBearingResult(result) {
  const type = normalizeText(result?.type ?? "");
  const claimType = normalizeText(result?.payload?.claim_type ?? "");
  const text = normalizeText([
    result?.summary,
    result?.payload?.content,
    result?.payload?.detail,
    result?.payload?.value_text,
    result?.payload?.title,
    result?.payload?.rationale,
    result?.payload?.message_content,
  ].filter(Boolean).join(" "));

  return type === "decision"
    || claimType === "decision"
    || /\b(?:decide|decided|decision|choose|chose|chosen|approved|selected)\b/i.test(text);
}

function hasExplicitTemporalAnchor(result) {
  const text = normalizeText([
    result?.summary,
    result?.payload?.content,
    result?.payload?.detail,
    result?.payload?.message_content,
  ].filter(Boolean).join(" "));
  return parseTimestampMs(getResultTimestamp(result)) !== null
    || /\b20\d{2}-\d{2}-\d{2}\b/.test(text);
}

function pointTemporalSortBucket(result) {
  const timestamped = parseTimestampMs(getResultTimestamp(result)) !== null;
  const canonical = result?.targetFamily === "canonical";
  const answerBearingCanonical = canonical
    && ["claim", "decision", "fact", "constraint", "task"].includes(normalizeText(result?.type ?? ""));

  if (timestamped && (isDecisionBearingResult(result) || hasExplicitTemporalAnchor(result))) {
    return 0;
  }
  if (answerBearingCanonical) {
    return 1;
  }
  if (canonical) {
    return 2;
  }
  if (timestamped) {
    return 3;
  }
  return 4;
}

function applyRouteWeighting(results, routePlan, queryProfile = null) {
  const temporalModifierActive = hasBoundedTemporalWindow(routePlan.temporal);
  const temporalPointLookup = isTemporalPointLookup(routePlan);
  return (results ?? []).map((result) => {
    let multiplier = 1;
    const family = result.targetFamily ?? "unknown";
    const type = normalizeText(result.type ?? "");
    const messageLike = isMessageLikeType(type);
    const decisionBearing = isDecisionBearingResult(result);
    const anchored = hasExplicitTemporalAnchor(result);

    if (routePlan.route === "current_state") {
      multiplier *= family === "canonical" ? 1.18 : 0.82;
      if (result.type === "claim" || result.type === "fact") {
        multiplier *= 1.14;
      }
    } else if (routePlan.route === "history_temporal") {
      if (temporalPointLookup) {
        if (queryProfile?.wantsDecision) {
          if (decisionBearing) {
            multiplier *= messageLike && anchored ? 2.8 : 1.46;
          } else if (type === "fact" || messageLike) {
            multiplier *= 0.2;
          } else if (type === "entity" || type === "chunk") {
            multiplier *= 0.84;
          }
        }

        if (anchored) {
          multiplier *= 1.08;
        }
      } else {
        multiplier *= family === "conversational" ? 1.12 : 0.92;
        if (temporalModifierActive) {
          multiplier *= isResultInTemporalWindow(result, routePlan.temporal) ? 1.32 : 0.68;
        }
      }
    } else if (routePlan.route === "why_explanatory") {
      if (isRationaleBearingResult(result)) {
        multiplier *= 1.34;
      }
      if (result.type === "decision") {
        multiplier *= 1.18;
      }
      if (temporalModifierActive) {
        multiplier *= isResultInTemporalWindow(result, routePlan.temporal) ? 1.16 : 0.74;
      }
    } else if (routePlan.route === "general") {
      multiplier *= family === "canonical" ? 1.04 : 0.98;
    }

    return {
      ...result,
      score: Number(result.score ?? 0) * multiplier,
    };
  });
}

function sortResultsForRoute(results, routePlan) {
  const sorted = [...(results ?? [])];
  const pointTemporalLookup = isTemporalPointLookup(routePlan);
  const chronologyRoute = (routePlan.route === "history_temporal" && !pointTemporalLookup)
    || (routePlan.route === "why_explanatory" && hasBoundedTemporalWindow(routePlan.temporal));

  if (pointTemporalLookup) {
    return sorted.sort((left, right) => {
      const leftSupport = left.temporalSupportLabel === "out_of_range_support" ? 1 : 0;
      const rightSupport = right.temporalSupportLabel === "out_of_range_support" ? 1 : 0;
      if (leftSupport !== rightSupport) {
        return leftSupport - rightSupport;
      }

      const leftBucket = pointTemporalSortBucket(left);
      const rightBucket = pointTemporalSortBucket(right);
      if (leftBucket !== rightBucket) {
        return leftBucket - rightBucket;
      }

      const scoreDelta = Number(right.score ?? 0) - Number(left.score ?? 0);
      if (scoreDelta !== 0) {
        return scoreDelta;
      }

      const leftTimestamp = parseTimestampMs(getResultTimestamp(left));
      const rightTimestamp = parseTimestampMs(getResultTimestamp(right));
      if (leftTimestamp !== null && rightTimestamp !== null && leftTimestamp !== rightTimestamp) {
        return leftTimestamp - rightTimestamp;
      }
      if (leftTimestamp !== null || rightTimestamp !== null) {
        return leftTimestamp === null ? 1 : -1;
      }

      return 0;
    });
  }

  if (!chronologyRoute) {
    return sorted.sort((left, right) => right.score - left.score);
  }

  return sorted.sort((left, right) => {
    const leftSupport = left.temporalSupportLabel === "out_of_range_support" ? 1 : 0;
    const rightSupport = right.temporalSupportLabel === "out_of_range_support" ? 1 : 0;
    if (leftSupport !== rightSupport) {
      return leftSupport - rightSupport;
    }

    const leftTimestamp = parseTimestampMs(getResultTimestamp(left));
    const rightTimestamp = parseTimestampMs(getResultTimestamp(right));
    if (leftTimestamp !== null && rightTimestamp !== null && leftTimestamp !== rightTimestamp) {
      return leftTimestamp - rightTimestamp;
    }
    if (leftTimestamp !== null || rightTimestamp !== null) {
      return leftTimestamp === null ? 1 : -1;
    }

    return right.score - left.score;
  });
}

function buildCandidateSourceCounts(results) {
  return (results ?? []).reduce((counts, result) => {
    if (result.type === "claim") {
      counts.claims += 1;
    } else if (result.type === "message") {
      counts.messages += 1;
    } else if (result.type === "chunk") {
      counts.documents += 1;
    } else if (result.type === "decision" || result.type === "task") {
      counts.events += 1;
    } else {
      counts.observations += 1;
    }
    return counts;
  }, {
    claims: 0,
    observations: 0,
    messages: 0,
    events: 0,
    documents: 0,
  });
}

function buildRouteAwareRrfLists(routePlan, sourceLists) {
  const sourceKeys = ROUTE_SOURCE_KEYS[routePlan.route] ?? ROUTE_SOURCE_KEYS.general;
  const diagnostics = {
    sourceListCounts: {},
    selectedSourceListCounts: {},
    candidateSourceCounts: {
      claims: 0,
      observations: 0,
      messages: 0,
      events: 0,
      documents: 0,
    },
    excludedOperationalCandidates: 0,
    temporalFilteredCandidates: 0,
    excludedUntimestampedCount: 0,
    inRangeCandidates: 0,
    timestampedTemporalCandidates: 0,
    outOfRangeCandidates: 0,
    outOfRangeSupportCount: 0,
    noInRangeEvidence: false,
    fallbackApplied: false,
    fallbackReason: null,
  };

  const materializePhase = (keys, phase) => keys.map((key) => {
    const list = sourceLists[key] ?? [];
    diagnostics.sourceListCounts[key] = list.length;

    let eligible = [];
    for (const result of list) {
      if (!routePlan.allowOperational && result.targetFamily === "operational") {
        diagnostics.excludedOperationalCandidates += 1;
        continue;
      }

      if (routePlan.allowOperational && result.targetFamily !== "operational") {
        continue;
      }

      eligible.push(result);
    }

    const temporalModifierActive = routePlan.route === "history_temporal"
      || (routePlan.route === "why_explanatory" && hasBoundedTemporalWindow(routePlan.temporal));

    if (temporalModifierActive) {
      const temporalSlice = applyTemporalWindowToResults(eligible, routePlan.temporal);
      diagnostics.temporalFilteredCandidates += temporalSlice.filteredCount;
      diagnostics.inRangeCandidates += temporalSlice.inRangeCount;
      diagnostics.outOfRangeCandidates += temporalSlice.outOfRangeCount;
      diagnostics.excludedUntimestampedCount += temporalSlice.excludedUntimestampedCount;
      diagnostics.outOfRangeSupportCount += temporalSlice.outOfRangeSupportCount;
      diagnostics.timestampedTemporalCandidates += temporalSlice.inRangeCount + temporalSlice.outOfRangeCount;
      if (temporalSlice.fallbackSupportUsed) {
        diagnostics.fallbackApplied = true;
        diagnostics.fallbackReason = diagnostics.fallbackReason ?? "temporal_window_empty_using_out_of_range_support";
      }
      eligible = temporalSlice.results;
    }

    const trimmed = trimCandidateList(eligible, phase);
    diagnostics.selectedSourceListCounts[key] = trimmed.length;
    return trimmed;
  }).filter((list) => list.length > 0);

  const primaryLists = materializePhase(sourceKeys.primary, "primary");
  const primaryCount = countUniqueResultsAcrossLists(primaryLists);
  const threshold = ROUTE_MIN_PRIMARY_RESULTS[routePlan.route] ?? ROUTE_MIN_PRIMARY_RESULTS.general;

  let lists = primaryLists;
  if (sourceKeys.secondary.length > 0 && primaryCount < threshold) {
    diagnostics.fallbackApplied = true;
    diagnostics.fallbackReason = diagnostics.fallbackReason ?? `primary_candidates_${primaryCount}_below_${threshold}`;
    lists = [...primaryLists, ...materializePhase(sourceKeys.secondary, "secondary")];
  }

  diagnostics.noInRangeEvidence = hasBoundedTemporalWindow(routePlan.temporal)
    && diagnostics.inRangeCandidates === 0
    && diagnostics.timestampedTemporalCandidates > 0;
  diagnostics.candidateSourceCounts = buildCandidateSourceCounts(
    dedupeResults(lists.flat())
  );

  return {
    lists,
    diagnostics,
  };
}

function getImportanceScore(result, database, importanceCache, nowMs) {
  if (result?.type === "message") {
    return messageImportanceProxy(getResultTimestamp(result), nowMs);
  }

  const observationId = getResultObservationId(result);
  if (!observationId || typeof database?.getObservationImportanceScore !== "function") {
    return 1.0;
  }

  if (!importanceCache.has(observationId)) {
    importanceCache.set(
      observationId,
      normalizeImportanceScore(database.getObservationImportanceScore(observationId)),
    );
  }

  return importanceCache.get(observationId) ?? 1.0;
}

function createObservationVectorResult(obs, seedEntityIdSet = new Set()) {
  const categoryBoost = getObservationCategoryBoost(obs.category);
  const seedBonus = (
    (obs.subject_entity_id && seedEntityIdSet.has(obs.subject_entity_id))
    || (obs.object_entity_id && seedEntityIdSet.has(obs.object_entity_id))
  )
    ? 0.15
    : 0;

  return decorateRetrievalResult({
    type: obs.category,
    id: obs.id,
    entityId: obs.subject_entity_id ?? obs.object_entity_id ?? null,
    score: applyOriginPenalty(VECTOR_SCORE_WEIGHT * obs.vectorScore + categoryBoost + seedBonus, obs),
    summary: obs.detail,
    payload: obs,
    tokenCount: estimateTokens(obs.detail),
    hintIds: [],
    vectorScore: obs.vectorScore,
    graphScore: 0,
    categoryBoost,
    seedBonus,
  }, "conversational");
}

export function applyImportanceWeighting(results, { database = null, nowMs = Date.now() } = {}) {
  const importanceCache = new Map();

  return (results ?? []).map((result) => {
    const importance = getImportanceScore(result, database, importanceCache, nowMs);
    const daysOld = getDaysOld(result, nowMs);
    const boost = recencyBoost(daysOld);

    return {
      ...result,
      importanceScore: importance,
      recencyBoost: boost,
      score: Number(result.score ?? 0) * importance * boost,
    };
  });
}

function _mergeHybridResults(graphResults, vectorResults, observationVectorResults = [], clusterLevelResults = []) {
  const vectorScoresByMessageId = new Map(
    vectorResults.map((result) => [result.id, Number(result.vectorScore ?? 0)]),
  );
  const observationVectorsById = new Map(
    observationVectorResults.map((result) => [result.id, result]),
  );
  const _clusterLevelResultsById = new Map(
    clusterLevelResults.map((result) => [result.id, result]),
  );
  const coveredMessageIds = new Set();
  const coveredObservationIds = new Set();
  const coveredClusterIds = new Set();
  const merged = [];

  for (const result of graphResults) {
    const graphScore = Number(result.score ?? 0);
    const messageId = getResultMessageId(result);
    const messageVectorScore = messageId ? vectorScoresByMessageId.get(messageId) : undefined;
    const observationVector = isObservationResult(result)
      ? observationVectorsById.get(result.id)
      : null;

    if (typeof messageVectorScore === "number" && messageId) {
      coveredMessageIds.add(messageId);
    }

    if (observationVector) {
      coveredObservationIds.add(result.id);
      const observationVectorScore = Number(observationVector.vectorScore ?? 0);
      const hybridVectorScore = Math.max(Number(messageVectorScore ?? 0), observationVectorScore);
      const categoryBoost = Number(observationVector.categoryBoost ?? 0);
      const seedBonus = Number(observationVector.seedBonus ?? 0);
      merged.push({
        ...result,
        graphScore,
        vectorScore: hybridVectorScore,
        messageVectorScore: Number(messageVectorScore ?? 0),
        observationVectorScore,
        categoryBoost,
        seedBonus,
        score: GRAPH_SCORE_WEIGHT * graphScore
          + applyOriginPenalty(VECTOR_SCORE_WEIGHT * hybridVectorScore + categoryBoost + seedBonus, result.payload),
      });
      continue;
    }

    if (typeof messageVectorScore === "number") {
      merged.push({
        ...result,
        graphScore,
        vectorScore: messageVectorScore,
        score: GRAPH_SCORE_WEIGHT * graphScore + VECTOR_SCORE_WEIGHT * messageVectorScore,
      });
      continue;
    }

    merged.push({
      ...result,
      graphScore,
      vectorScore: 0,
      score: GRAPH_SCORE_WEIGHT * graphScore,
    });
  }

  for (const result of vectorResults) {
    if (coveredMessageIds.has(result.id)) {
      continue;
    }

    merged.push(result);
  }

  for (const result of observationVectorResults) {
    if (coveredObservationIds.has(result.id)) {
      continue;
    }

    merged.push(result);
  }

  for (const result of clusterLevelResults) {
    if (coveredClusterIds.has(result.id)) {
      continue;
    }

    coveredClusterIds.add(result.id);
    merged.push(result);
  }

  return merged;
}

export class RetrievalEngine {
  constructor({ graph, database, telemetry, classifier, vectorIndex = null }) {
    this.graph = graph;
    this.database = database;
    this.telemetry = telemetry;
    this.classifier = classifier;
    this.vectorIndex = vectorIndex;
    this.lastQueryByConversation = new Map();
  }

  findSeedEntities(queryText, activeHints = []) {
    const directMatches = this.graph.matchQuery(queryText).slice(0, 12);
    const extracted = this.classifier.classifyText(queryText).entities;
    const extractedMatches = extracted.flatMap((entity) => this.graph.matchQuery(entity.label));
    const hintedMatches = activeHints
      .map((hint) => {
        const entity = hint.seed_entity_id
          ? this.graph.getEntity(hint.seed_entity_id)
          : this.graph.findEntityByLabel(hint.seed_label);
        const score = scoreHintSeed(queryText, hint.seed_label);

        if (!entity || score <= 0) {
          return null;
        }

        return {
          entity,
          score: 1.6 + score,
        };
      })
      .filter(Boolean);

    const merged = new Map();
    for (const match of [...directMatches, ...extractedMatches, ...hintedMatches]) {
      const current = merged.get(match.entity.id);
      if (!current || match.score > current.score) {
        merged.set(match.entity.id, match);
      }
    }

    return [...merged.values()]
      .sort((left, right) => right.score - left.score)
      .map((entry) => entry.entity);
  }

  findRelevantHints({ conversationId = null, queryText, seedEntities, activeHints }) {
    const seedIds = new Set(seedEntities.map((entity) => entity.id));
    const seedLabels = new Set(seedEntities.map((entity) => normalizeText(entity.label)).filter(Boolean));
    const relevant = new Map();

    for (const row of activeHints) {
      const sourceEntity = row.seed_entity_id
        ? this.graph.getEntity(row.seed_entity_id)
        : this.graph.findEntityByLabel(row.seed_label);
      const targetEntity = row.expand_entity_id
        ? this.graph.getEntity(row.expand_entity_id)
        : this.graph.findEntityByLabel(row.expand_label);
      const queryScore = scoreHintSeed(queryText, row.seed_label);
      const seedLabel = normalizeText(row.seed_label);

      if (!sourceEntity || !targetEntity || sourceEntity.id === targetEntity.id) {
        continue;
      }

      if (!seedIds.has(sourceEntity.id) && !seedLabels.has(seedLabel) && queryScore <= 0) {
        continue;
      }

      const ttlTurns = Math.max(1, Number(row.ttl_turns ?? 1));
      const turnsRemaining = Math.max(0, Number(row.turns_remaining ?? ttlTurns));
      const freshness = clamp(turnsRemaining / ttlTurns, 0.25, 1);
      const scopeBoost = conversationId && row.conversation_id === conversationId ? 1.15 : 1;
      const matchBoost = clamp(1 + queryScore * 0.12, 1, 1.35);
      const weight = clamp(Number(row.weight ?? 0.75) * freshness * scopeBoost * matchBoost, 0.15, 3);
      const hint = {
        id: row.id,
        seedEntityId: sourceEntity.id,
        seedLabel: sourceEntity.label,
        expandEntityId: targetEntity.id,
        expandLabel: targetEntity.label,
        predicate: inferHintPredicate(row.reason),
        reason: row.reason,
        conversationId: row.conversation_id ?? null,
        baseWeight: Number(row.weight ?? 0.75),
        baseTtlTurns: ttlTurns,
        weight,
        ttlTurns,
        turnsRemaining,
      };
      const key = `${hint.seedEntityId}:${hint.expandEntityId}:${hint.predicate}`;
      const current = relevant.get(key);
      if (!current || hint.weight > current.weight) {
        relevant.set(key, hint);
      }
    }

    return [...relevant.values()].sort((left, right) => right.weight - left.weight);
  }

  expandSeeds(seedEntities, retrievalHints = []) {
    const visited = new Map();
    const provenance = new Map();
    const expansionPath = [];
    const queue = seedEntities.map((entity) => ({ entityId: entity.id, score: 1, depth: 0 }));
    const hintsBySeed = new Map();

    for (const entity of seedEntities) {
      visited.set(entity.id, 1);
      provenance.set(entity.id, {
        entityId: entity.id,
        parentEntityId: null,
        source: "seed",
        hintIds: [],
        depth: 0,
      });
    }

    for (const hint of retrievalHints) {
      if (!hintsBySeed.has(hint.seedEntityId)) {
        hintsBySeed.set(hint.seedEntityId, []);
      }

      hintsBySeed.get(hint.seedEntityId).push(hint);
    }

    while (queue.length) {
      queue.sort((left, right) => right.score - left.score);
      const current = queue.shift();
      const entity = this.graph.getEntity(current.entityId);
      if (!entity) {
        continue;
      }

      const maxDepth = 1 + Math.min(4, Math.floor(Math.log2(entity.complexityScore + 1)) + 1);
      if (current.depth >= maxDepth) {
        continue;
      }

      for (const neighbor of this.graph.neighbors(entity.id)) {
        const predicateWeight = getWeight(neighbor.relationship.predicate);
        const complexityMultiplier = 1 + Math.min(1.5, neighbor.entity.complexityScore * 0.15);
        const nextScore = current.score * predicateWeight * complexityMultiplier;
        const threshold = 0.18 / complexityMultiplier;

        if (nextScore < threshold) {
          continue;
        }

        const bestScore = visited.get(neighbor.entity.id) ?? 0;
        if (nextScore <= bestScore) {
          continue;
        }

        visited.set(neighbor.entity.id, nextScore);
        const inheritedHintIds = provenanceHintIds(provenance, entity.id);
        provenance.set(neighbor.entity.id, {
          entityId: neighbor.entity.id,
          parentEntityId: entity.id,
          source: inheritedHintIds.length ? "hint_chain" : "graph",
          hintIds: inheritedHintIds,
          depth: current.depth + 1,
        });
        queue.push({
          entityId: neighbor.entity.id,
          score: nextScore,
          depth: current.depth + 1,
        });
        expansionPath.push({
          from: entity.id,
          to: neighbor.entity.id,
          predicate: neighbor.relationship.predicate,
          depth: current.depth + 1,
          score: Number(nextScore.toFixed(3)),
          source: "graph",
        });
      }

      for (const hint of hintsBySeed.get(entity.id) ?? []) {
        const target = this.graph.getEntity(hint.expandEntityId);
        if (!target) {
          continue;
        }

        const complexityMultiplier = 1 + Math.min(1.75, target.complexityScore * 0.18);
        const nextScore = current.score * HINT_BASE_WEIGHT * hint.weight * complexityMultiplier;
        const threshold = 0.1 / complexityMultiplier;

        if (nextScore < threshold) {
          continue;
        }

        const bestScore = visited.get(target.id) ?? 0;
        if (nextScore <= bestScore) {
          continue;
        }

        visited.set(target.id, nextScore);
        const inheritedHintIds = provenanceHintIds(provenance, entity.id);
        const nextHintIds = mergeHintIds(inheritedHintIds, [hint.id]);
        provenance.set(target.id, {
          entityId: target.id,
          parentEntityId: entity.id,
          source: "hint",
          hintIds: nextHintIds,
          depth: current.depth + 1,
        });
        queue.push({
          entityId: target.id,
          score: nextScore,
          depth: current.depth + 1,
        });
        expansionPath.push({
          from: entity.id,
          to: target.id,
          predicate: hint.predicate,
          depth: current.depth + 1,
          score: Number(nextScore.toFixed(3)),
          source: "hint",
          hintId: hint.id,
          reason: hint.reason,
          turnsRemaining: hint.turnsRemaining,
        });
      }
    }

    return { visited, provenance, expansionPath };
  }

  collectHintIds(provenance, entityIds) {
    return uniqueValues(entityIds.flatMap((entityId) => provenanceHintIds(provenance, entityId)));
  }

  learnHintPolicy({ conversationId = null, queryId, hints, graphBaselineVisited, provenance, results }) {
    const resultsByHint = new Map();
    const uniqueEntityIdsByHint = new Map();
    const appliedHintIds = new Set();

    for (const result of results) {
      for (const hintId of result.hintIds ?? []) {
        if (!resultsByHint.has(hintId)) {
          resultsByHint.set(hintId, []);
        }
        resultsByHint.get(hintId).push(result);
      }
    }

    for (const [entityId, entry] of provenance.entries()) {
      for (const hintId of entry.hintIds ?? []) {
        if (!uniqueEntityIdsByHint.has(hintId)) {
          uniqueEntityIdsByHint.set(hintId, new Set());
        }

        if (!graphBaselineVisited.has(entityId)) {
          uniqueEntityIdsByHint.get(hintId).add(entityId);
        }
        appliedHintIds.add(hintId);
      }
    }

    const outcomes = [];
    for (const hint of hints) {
      const applied = appliedHintIds.has(hint.id);
      const attributedResults = (resultsByHint.get(hint.id) ?? []).sort((left, right) => left.rank - right.rank);
      const uniqueEntityIds = uniqueEntityIdsByHint.get(hint.id) ?? new Set();
      const uniqueAttributedResults = attributedResults.filter((result) => {
        if (!result.entityId) {
          return uniqueEntityIds.size > 0;
        }

        return uniqueEntityIds.has(result.entityId);
      });
      const graphReachableWithoutHint = graphBaselineVisited.has(hint.expandEntityId);
      const scored = scoreHintOutcome({
        hint,
        applied,
        graphReachableWithoutHint,
        attributedResults,
        uniqueAttributedResults,
        uniqueEntityIds,
      });

      this.telemetry.recordRetrievalHintOutcome({
        hintId: hint.id,
        queryId,
        conversationId: conversationId ?? hint.conversationId ?? null,
        applied,
        rewarded: scored.rewarded,
        decayed: scored.decayed,
        reward: scored.reward,
        penalty: scored.penalty,
        nextWeight: scored.nextWeight,
        nextTtlTurns: scored.nextTtlTurns,
        detail: {
          seedLabel: hint.seedLabel,
          expandLabel: hint.expandLabel,
          queryHintWeight: hint.weight,
          applied,
          metrics: scored.metrics,
          topSummaries: attributedResults.slice(0, 3).map((result) => result.summary),
        },
      });

      outcomes.push({
        hintId: hint.id,
        seedLabel: hint.seedLabel,
        expandLabel: hint.expandLabel,
        applied,
        reward: scored.reward,
        penalty: scored.penalty,
        netReward: scored.netReward,
        nextWeight: scored.nextWeight,
        nextTtlTurns: scored.nextTtlTurns,
        metrics: scored.metrics,
      });
    }

    return outcomes.sort((left, right) => right.netReward - left.netReward);
  }

  applyMissHeuristic(conversationId, seedEntities) {
    const previous = this.lastQueryByConversation.get(conversationId);
    if (!previous || !seedEntities.length) {
      return [];
    }

    const misses = [];
    for (const seed of seedEntities) {
      if (previous.expandedEntityIds.includes(seed.id)) {
        continue;
      }

      const linked = previous.seedEntityIds.some((priorId) => this.graph.findPath(priorId, seed.id, 2));
      if (linked) {
        this.graph.registerMiss(seed.id);
        misses.push(seed.id);
      }
    }

    return misses;
  }

  retrieveClaims(seedEntities, scopeFilter = null) {
    const seedEntityIds = seedEntities.map((entity) => entity.id);
    if (!seedEntityIds.length) {
      return [];
    }

    const claims = this.database.listClaimsForEntities(
      seedEntityIds,
      scopeFilter,
      {
        states: ["active", "candidate", "disputed"],
        types: null,
        limit: 100,
      }
    );
    const truthAnalysis = analyzeClaimsTruthSet(claims);

    const seedVisited = new Map();
    for (const entity of seedEntities) {
      seedVisited.set(entity.id, 0.8);
    }

    const representativeClaims = new Map();
    for (const variant of truthAnalysis.variants) {
      if (!variant.current_claim_ids.length || !variant.representative_claim_id) {
        continue;
      }

      const representative = claims.find((claim) => claim.id === variant.representative_claim_id);
      if (representative) {
        representativeClaims.set(representative.id, representative);
      }
    }

    return [...representativeClaims.values()].map((claim) => {
      const subjectEntityId = claim.subject_entity_id;
      const objectEntityId = claim.object_entity_id;
      const entityForScore = seedEntityIds.includes(subjectEntityId) ? subjectEntityId : objectEntityId;
      const entityGraphScore = seedVisited.get(entityForScore) ?? 0.5;
      const truth = truthAnalysis.byClaimId.get(claim.id) ?? null;
      const effectiveConfidence = Number(truth?.effective_confidence ?? claim.confidence ?? 0.7);
      const extractionConfidence = Number(truth?.extraction_confidence ?? claim.confidence ?? 0.7);
      const stateDiscount = claim.lifecycle_state === "active"
        ? 1.0
        : claim.lifecycle_state === "candidate"
          ? 0.8
          : 0.7;
      const categoryBoost = Number(claim.importance_score ?? 1.0);
      const score = entityGraphScore * effectiveConfidence * stateDiscount * categoryBoost;

      let summary = "";
      if (claim.claim_type === "fact") {
        summary = claim.value_text || claim.detail || "";
      } else if (claim.claim_type === "decision") {
        summary = claim.value_text || claim.detail || "";
      } else if (claim.claim_type === "relationship") {
        const subLabel = claim.subjectLabel || claim.subject_entity_id || "";
        const predicate = claim.predicate || "";
        const objLabel = claim.objectLabel || claim.object_entity_id || "";
        summary = `${subLabel} ${predicate} ${objLabel}`.trim();
      } else {
        summary = claim.value_text || claim.detail || claim.claim_type;
      }

      return decorateRetrievalResult({
        type: "claim",
        id: claim.id,
        entityId: entityForScore,
        score,
        summary,
        payload: {
          ...claim,
          truth: truth ? {
            ...truth,
            extraction_confidence: extractionConfidence,
            effective_confidence: effectiveConfidence,
          } : null,
        },
        tokenCount: estimateTokens(summary),
        hintIds: [],
        source: "claims",
      }, "canonical");
    });
  }

  async retrieve({ conversationId = null, queryText, scopeFilter = null }) {
    const start = performance.now();
    const queryProfile = buildQueryProfile(queryText);
    const resolvedScopeFilter = normalizeScopeFilter(scopeFilter);
    const activeHints = this.telemetry.listActiveRetrievalHints(128);
    const seedEntities = this.findSeedEntities(queryProfile.expandedQuery, activeHints);
    const seedEntityIdSet = new Set(seedEntities.map((entity) => entity.id));
    const routePlan = planRetrievalRoute(queryText, {
      queryProfile,
      seedEntities,
    });
    const relevantHints = this.findRelevantHints({
      conversationId,
      queryText,
      seedEntities,
      activeHints,
    });
    const graphBaseline = relevantHints.length ? this.expandSeeds(seedEntities, []) : { visited: new Map() };
    const { visited, provenance, expansionPath } = this.expandSeeds(seedEntities, relevantHints);
    const expandedEntityIds = [...visited.keys()];

    const observations = this.database.listObservationsForEntities(expandedEntityIds, resolvedScopeFilter).map((item) => decorateRetrievalResult({
      type: item.category,
      id: item.id,
      entityId: item.subject_entity_id ?? item.object_entity_id ?? null,
      score: applyOriginPenalty(visited.get(item.subject_entity_id) ?? visited.get(item.object_entity_id) ?? 0.4, item),
      summary: item.detail,
      payload: item,
      tokenCount: estimateTokens(item.detail),
      hintIds: this.collectHintIds(provenance, [item.subject_entity_id, item.object_entity_id]),
    }, "conversational"));

    const tasks = this.database.listTasksForEntities(expandedEntityIds, resolvedScopeFilter).map((item) => decorateRetrievalResult({
      type: "task",
      id: item.id,
      entityId: item.entity_id ?? null,
      score: applyOriginPenalty((visited.get(item.entity_id) ?? 0.35) + 0.15, item),
      summary: item.title,
      payload: item,
      tokenCount: estimateTokens(item.title),
      hintIds: this.collectHintIds(provenance, [item.entity_id]),
    }, "canonical"));

    const decisions = this.database.listDecisionsForEntities(expandedEntityIds, resolvedScopeFilter).map((item) => decorateRetrievalResult({
      type: "decision",
      id: item.id,
      entityId: item.entity_id ?? null,
      score: applyOriginPenalty((visited.get(item.entity_id) ?? 0.3) + 0.1, item),
      summary: item.rationale ? `${item.title} — ${item.rationale}` : item.title,
      payload: item,
      tokenCount: estimateTokens(item.rationale ? `${item.title} ${item.rationale}` : item.title),
      hintIds: this.collectHintIds(provenance, [item.entity_id]),
    }, "canonical"));

    const constraints = this.database.listConstraintsForEntities(expandedEntityIds, resolvedScopeFilter).map((item) => decorateRetrievalResult({
      type: "constraint",
      id: item.id,
      entityId: item.entity_id ?? null,
      score: applyOriginPenalty((visited.get(item.entity_id) ?? 0.3) + 0.18, item),
      summary: item.detail,
      payload: item,
      tokenCount: estimateTokens(item.detail),
      hintIds: this.collectHintIds(provenance, [item.entity_id]),
    }, "canonical"));

    const facts = this.database.listFactsForEntities(expandedEntityIds, resolvedScopeFilter).map((item) => decorateRetrievalResult({
      type: "fact",
      id: item.id,
      entityId: item.entity_id ?? null,
      score: applyOriginPenalty((visited.get(item.entity_id) ?? 0.3) + 0.08, item),
      summary: item.detail,
      payload: item,
      tokenCount: estimateTokens(item.detail),
      hintIds: this.collectHintIds(provenance, [item.entity_id]),
    }, "canonical"));

    const linkedChunks = this.database.listChunksForEntities(expandedEntityIds, resolvedScopeFilter).map((item) => decorateRetrievalResult({
      type: "chunk",
      id: item.id,
      entityId: item.entityId ?? null,
      score: (visited.get(item.entityId) ?? 0.25) + Number(item.score ?? 0),
      summary: `${item.path}#${item.ordinal}`,
      payload: item,
      tokenCount: Number(item.tokenCount ?? estimateTokens(item.content)),
      hintIds: this.collectHintIds(provenance, [item.entityId]),
    }, "conversational"));

    const ftsQuery = queryProfile.expandedQuery
      .split(/\s+/)
      .map((token) => token.replace(/[^A-Za-z0-9]/g, "").trim())
      .filter((token) => token.length > 2)
      .join(" OR ");

    const ftsChunks = (ftsQuery ? this.database.searchChunks(ftsQuery, resolvedScopeFilter) : []).map((item) => decorateRetrievalResult({
      type: "chunk",
      id: item.id,
      entityId: null,
      score: Math.max(0.2, 1 - Number(item.rank ?? 1)),
      summary: `${item.path}#${item.ordinal}`,
      payload: item,
      tokenCount: Number(item.tokenCount ?? estimateTokens(item.content)),
      hintIds: [],
    }, "conversational"));

    const ftsObservations = (ftsQuery
      ? this.database.searchObservations(ftsQuery, resolvedScopeFilter)
      : []
    ).map((obs) => decorateRetrievalResult({
      type: obs.category,
      id: obs.id,
      entityId: obs.subject_entity_id ?? obs.object_entity_id ?? null,
      score: applyOriginPenalty(
        Math.max(0.3, 1 - Number(obs.rank ?? 1)) + getObservationCategoryBoost(obs.category),
        obs,
      ),
      summary: obs.detail,
      payload: obs,
      tokenCount: estimateTokens(obs.detail),
      hintIds: [],
    }, "conversational"));

    const entityResults = expandedEntityIds
      .map((entityId) => this.graph.getEntity(entityId))
      .filter(Boolean)
      .map((entity) => decorateRetrievalResult({
        type: "entity",
        id: entity.id,
        entityId: entity.id,
        score: visited.get(entity.id) ?? 0.3,
        summary: `${entity.label} (${entity.kind})`,
        payload: entity,
        tokenCount: estimateTokens(entity.summary ?? entity.label),
        hintIds: provenanceHintIds(provenance, entity.id),
      }, "canonical"));

    const queryEmbedding = String(queryText ?? "").trim()
      ? await embedText(queryText)
      : null;
    const indexedVectorMatches = queryEmbedding && this.vectorIndex?.size
      ? this.vectorIndex.query(
          queryEmbedding,
          100,
          0.3,
          resolvedScopeFilter ? (item) => matchesScopeFilter(item, resolvedScopeFilter) : null,
        )
      : null;
    const vectorResults = indexedVectorMatches
      ? indexedVectorMatches
          .filter((result) => result.type === "message")
          .map((result) => {
            const message = this.database.getMessage(result.id);
            return message ? createVectorResult(message, result.score) : null;
          })
          .filter(Boolean)
      : queryEmbedding
        ? this.database.listEmbeddedMessages(resolvedScopeFilter)
            .map((message) => ({
              ...message,
              vectorScore: cosineSimilarity(queryEmbedding, message.embedding),
            }))
            .filter((message) => Number.isFinite(message.vectorScore) && message.vectorScore > 0)
            .sort((left, right) => right.vectorScore - left.vectorScore)
            .slice(0, 50)
            .map((message) => createVectorResult(message, message.vectorScore))
        : [];
    const observationVectorResults = indexedVectorMatches
      ? indexedVectorMatches
          .filter((result) => result.type === "observation")
          .map((result) => {
            const observation = this.database.getObservation(result.id);
            return observation
              ? createObservationVectorResult({
                ...observation,
                vectorScore: result.score,
              }, seedEntityIdSet)
              : null;
          })
          .filter(Boolean)
      : queryEmbedding
        ? this.database.listEmbeddedObservations(resolvedScopeFilter)
            .map((obs) => ({
              ...obs,
              vectorScore: cosineSimilarity(queryEmbedding, obs.embedding),
            }))
            .filter((obs) => Number.isFinite(obs.vectorScore) && obs.vectorScore > 0)
            .sort((left, right) => right.vectorScore - left.vectorScore)
            .slice(0, 50)
            .map((obs) => createObservationVectorResult(obs, seedEntityIdSet))
        : [];

    // LOD-aware cluster level search (scatter phase)
    const clusterLevelResults = indexedVectorMatches
      ? indexedVectorMatches
          .filter((result) => result.type.startsWith("cluster_l"))
          .map((result) => {
            const match = result.type.match(/^cluster_l(\d+)$/);
            if (!match) return null;
            const level = parseInt(match[1], 10);
            const clusterIdStr = result.id.split(":")[1];
            if (!clusterIdStr) return null;
            const levelData = this.database.getClusterLevel(clusterIdStr, level);
            return levelData ? createClusterLevelResult(clusterIdStr, level, levelData.text, result.score) : null;
          })
          .filter(Boolean)
      : [];

    // Claims retrieval: entity-linked, typed, lifecycle-managed knowledge items
    const claimsResults = this.retrieveClaims(seedEntities, resolvedScopeFilter);
    const registryLexicalResults = createRegistryLexicalResults(this.database, queryText, resolvedScopeFilter);

    const sourceLists = {
      graphCanonical: dedupeResults([
        ...entityResults,
        ...constraints,
        ...tasks,
        ...decisions,
        ...facts,
      ]),
      graphConversational: dedupeResults([
        ...observations,
        ...linkedChunks,
      ]),
      vectorMessages: dedupeResults(vectorResults),
      vectorObservations: dedupeResults(observationVectorResults),
      ftsConversational: dedupeResults([
        ...ftsChunks,
        ...ftsObservations,
      ]),
      clusterLevels: dedupeResults(clusterLevelResults),
      claims: dedupeResults(claimsResults),
      registryLexical: dedupeResults(registryLexicalResults),
    };

    const { lists: routeRrfListsRaw, diagnostics: routeSelectionDiagnostics } = buildRouteAwareRrfLists(routePlan, sourceLists);

    // RRF: Combine signals by rank position, not raw score magnitude
    const rrfLists = validateRRFInput(routeRrfListsRaw);

    // Apply RRF fusion across signals
    let rrfResults = rrfLists.length > 0
      ? reciprocalRankFusion(rrfLists, 60)
      : [];

    // Apply category boosts (post-RRF)
    rrfResults = applyCategoryBoosts(rrfResults);

    // Apply seed entity bonuses (post-RRF)
    rrfResults = applySeedBonus(rrfResults, seedEntityIdSet);

    // Apply origin penalty post-RRF (agent content penalized vs user content)
    rrfResults = rrfResults.map((item) => ({
      ...item,
      score: rrfApplyOriginPenalty(item.score, item.payload),
    }));

    // Reweight toward answer-bearing items for explicit factual/policy queries
    rrfResults = applyQueryIntentWeighting(rrfResults, queryProfile);

    // Apply route-specific weighting after route planning and source gating.
    rrfResults = applyRouteWeighting(rrfResults, routePlan, queryProfile);

    // Break ties using secondary criteria (timestamp, type priority)
    rrfResults = breakRRFTies(rrfResults);

    // Deduplicate claims against registry items (claims are less authoritative)
    rrfResults = deduplicateClaimsAgainstRegistry(rrfResults, routePlan);

    // Deduplicate and apply importance weighting
    const allResults = applyImportanceWeighting(
      dedupeResults(rrfResults),
      { database: this.database },
    )
      .sort((left, right) => right.score - left.score);

    const rankedResults = sortResultsForRoute(allResults, routePlan)
      .map((result, index) => ({
        ...result,
        rank: index + 1,
      }));

    const tokensConsumed = rankedResults.reduce((sum, result) => sum + result.tokenCount, 0);
    const latencyMs = Math.round(performance.now() - start);
    const missEntityIds = conversationId ? this.applyMissHeuristic(conversationId, seedEntities) : [];
    const finalFamilyCounts = countResultsByFamily(rankedResults.slice(0, 10));
    const temporalParseStatus = routePlan.temporalParseStatus === "parsed" && routeSelectionDiagnostics.noInRangeEvidence
      ? (routeSelectionDiagnostics.outOfRangeSupportCount > 0 ? "fallback_support" : "empty_window")
      : routePlan.temporalParseStatus;
    const diagnostics = {
      route: routePlan.route,
      route_reason: routePlan.routeReason,
      route_confidence: routePlan.confidence,
      fallback_used: routePlan.fallbackUsed || routeSelectionDiagnostics.fallbackApplied,
      fallback_reason: routeSelectionDiagnostics.fallbackReason ?? routePlan.fallbackReason ?? null,
      candidate_source_counts: routeSelectionDiagnostics.candidateSourceCounts,
      query_features: routePlan.queryFeatures,
      temporal_parse_status: temporalParseStatus,
      temporal_expression: routePlan.temporalExpression ?? null,
      temporal_window: hasBoundedTemporalWindow(routePlan.temporal)
        ? {
          start_at: routePlan.temporal.startAt,
          end_at: routePlan.temporal.endAt,
        }
        : null,
      temporal_timezone: routePlan.temporal?.timezone ?? null,
      excluded_untimestamped_count: routeSelectionDiagnostics.excludedUntimestampedCount,
      out_of_range_support_count: routeSelectionDiagnostics.outOfRangeSupportCount,
      routeConfidence: routePlan.confidence,
      routeReasons: routePlan.reasons,
      sourceSignals: routePlan.sourceSignals,
      targetFamilies: routePlan.targetFamilies,
      artifactBoundary: routePlan.artifactBoundary,
      temporal: routePlan.temporal,
      fallbackApplied: routeSelectionDiagnostics.fallbackApplied,
      fallbackReason: routeSelectionDiagnostics.fallbackReason,
      sourceListCounts: routeSelectionDiagnostics.sourceListCounts,
      selectedSourceListCounts: routeSelectionDiagnostics.selectedSourceListCounts,
      excludedOperationalCandidates: routeSelectionDiagnostics.excludedOperationalCandidates,
      temporalFilteredCandidates: routeSelectionDiagnostics.temporalFilteredCandidates,
      inRangeCandidates: routeSelectionDiagnostics.inRangeCandidates,
      noInRangeEvidence: routeSelectionDiagnostics.noInRangeEvidence,
      resultFamilyCounts: finalFamilyCounts,
    };

    const queryId = this.telemetry.logRetrieval({
      conversationId,
      queryText,
      latencyMs,
      seedEntityIds: seedEntities.map((entity) => entity.id),
      expandedEntityIds,
      expansionPath,
      itemsReturned: rankedResults.length,
      tokensConsumed,
      missEntityIds,
      results: rankedResults,
    });
    const hintOutcomes = relevantHints.length
      ? this.learnHintPolicy({
        conversationId,
        queryId,
        hints: relevantHints,
        graphBaselineVisited: graphBaseline.visited,
        provenance,
        results: rankedResults,
      })
      : [];

    if (conversationId) {
      this.lastQueryByConversation.set(conversationId, {
        seedEntityIds: seedEntities.map((entity) => entity.id),
        expandedEntityIds,
      });
    }

    const graphVersion = this.database.getGraphVersion();

    return {
      queryId,
      latencyMs,
      graphVersion,
      seedEntities,
      expandedEntities: expandedEntityIds.map((entityId) => this.graph.getEntity(entityId)).filter(Boolean),
      expansionPath,
      retrievalHints: relevantHints,
      hintOutcomes,
      items: rankedResults,
      tokensConsumed,
      missEntityIds,
      diagnostics,
      routePlan,
    };
  }
}
