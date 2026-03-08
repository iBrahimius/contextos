function tokenize(value) {
  return String(value ?? "")
    .toLowerCase()
    .match(/[a-z0-9]+/g) ?? [];
}

function uniqueTokens(value) {
  return [...new Set(tokenize(value))];
}

function listEntities(entityGraph) {
  if (!entityGraph) {
    return [];
  }

  if (typeof entityGraph.listEntities === "function") {
    return entityGraph.listEntities();
  }

  if (Array.isArray(entityGraph.entities)) {
    return entityGraph.entities;
  }

  return [];
}

const INTENT_RULES = [
  {
    intent: "why",
    pattern: /^(why\b|rationale\b|reason (?:for|behind)\b|how come\b|what motivated\b)/i,
  },
  {
    intent: "history",
    pattern: /^(what happened\b|when did\b|timeline of\b|history of\b|how did .* change\b)/i,
  },
  {
    intent: "next-action",
    pattern: /^(what should i\b|what do i do now\b|what should i work on next\b|what(?:'s| is) next\b|what are my priorities\b|priorities\b|any blockers\b|blockers\b)/i,
  },
  {
    intent: "entity-briefing",
    pattern: /^(tell me about\b|what do (?:we|i) know about\b|brief(?:ing)? on\b|overview of\b)/i,
  },
  {
    intent: "current-state",
    pattern: /^(what(?:'s| is| are)\b|current(?: status)?(?: of)?\b|status of\b|who (?:owns|is responsible)\b|active constraints\b)/i,
  },
  {
    intent: "general",
    pattern: /^(search for\b|find\b|look up\b|show me\b)|\b(?:architecture|setup)\b/i,
  },
];

export const INTENT_STRATEGIES = {
  "current-state": {
    steps: ["registry_lookup", "graph_expand"],
    claimTypes: ["fact", "decision", "preference", "rule", "constraint", "state_change"],
    claimStates: ["active"],
    evidenceRatio: 0.2,
    messageRatio: 0.1,
  },
  history: {
    steps: ["hybrid_retrieval"],
    claimTypes: [],
    claimStates: ["active", "superseded", "archived"],
    evidenceRatio: 0.5,
    messageRatio: 0.4,
  },
  why: {
    steps: ["registry_lookup", "evidence_chain"],
    claimTypes: ["decision", "constraint", "fact", "rule"],
    claimStates: ["active", "superseded"],
    evidenceRatio: 0.4,
    messageRatio: 0.35,
  },
  "entity-briefing": {
    steps: ["entity_lookup", "graph_expand", "claim_scan"],
    claimTypes: [],
    claimStates: ["active", "candidate"],
    evidenceRatio: 0.35,
    messageRatio: 0.25,
  },
  "next-action": {
    steps: ["registry_lookup"],
    claimTypes: ["task", "goal", "constraint", "decision"],
    claimStates: ["active", "candidate"],
    evidenceRatio: 0.15,
    messageRatio: 0.1,
  },
  general: {
    steps: ["hybrid_retrieval"],
    claimTypes: [],
    claimStates: ["active", "candidate", "disputed"],
    evidenceRatio: 0.45,
    messageRatio: 0.35,
  },
};

const INTENT_CATEGORIES = Object.keys(INTENT_STRATEGIES);

export function isKnownEntityQuery(query, entityGraph) {
  const queryTokens = uniqueTokens(query);
  if (!queryTokens.length) {
    return false;
  }

  return listEntities(entityGraph).some((entity) => {
    const labelTokens = uniqueTokens(entity?.label ?? "");
    if (!labelTokens.length) {
      return false;
    }

    const overlap = labelTokens.filter((token) => queryTokens.includes(token)).length;
    return (overlap / labelTokens.length) > 0.6;
  });
}

export async function classifyIntent(query, entityGraph, llmClassifier) {
  const normalizedQuery = String(query ?? "").trim();
  if (!normalizedQuery) {
    return { intent: "general", source: "default" };
  }

  for (const rule of INTENT_RULES) {
    if (rule.pattern.test(normalizedQuery)) {
      return { intent: rule.intent, source: "rules" };
    }
  }

  if (isKnownEntityQuery(normalizedQuery, entityGraph)) {
    return { intent: "entity-briefing", source: "entity-match" };
  }

  if (!llmClassifier || typeof llmClassifier.classify !== "function") {
    return { intent: "general", source: "default" };
  }

  try {
    const result = await llmClassifier.classify(normalizedQuery, INTENT_CATEGORIES);
    const intent = typeof result === "string" ? result : result?.intent;
    if (INTENT_CATEGORIES.includes(intent)) {
      return { intent, source: "haiku" };
    }
  } catch {
    return { intent: "general", source: "default" };
  }

  return { intent: "general", source: "default" };
}
