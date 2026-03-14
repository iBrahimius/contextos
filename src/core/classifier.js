import { sentenceSplit, unique } from "./utils.js";

const DOMAIN_SUFFIXES = [
  "system",
  "engine",
  "pipeline",
  "layer",
  "graph",
  "dashboard",
  "proxy",
  "storage",
  "index",
  "indexer",
  "memory",
  "retrieval",
  "telemetry",
  "frontend",
  "backend",
  "classifier",
  "conversation",
  "agent",
  "component",
  "project",
];

const KNOWN_TERMS = [
  "ContextOS",
  "SQLite",
  "FTS5",
  "Apple Metal",
  "Metal",
  "shadcn",
  "OpenClaw",
  "openclaw",
  "entity graph",
  "retrieval pipeline",
  "storage layer",
  "embedding engine",
  "proxy layer",
  "memory system",
  "document indexing",
  "prompt injection",
];

const LEADING_NOISE = new Set([
  "a",
  "an",
  "and",
  "also",
  "are",
  "capture",
  "captures",
  "every",
  "for",
  "full",
  "ignore",
  "if",
  "integrate",
  "integrates",
  "logs",
  "design",
  "task",
  "decision",
  "depends",
  "on",
  "query",
  "querying",
  "retrieving",
  "retrieve",
  "retrieves",
  "surface",
  "surfaces",
  "should",
  "store",
  "stores",
  "must",
  "the",
  "this",
  "that",
  "these",
  "those",
  "they",
  "we",
  "use",
  "using",
  "with",
  "without",
  "reveal",
  "add",
  "block",
  "because",
  "called",
]);

const GENERIC_LABELS = new Set([
  "agent",
  "component",
  "concept",
  "conversation",
  "dashboard",
  "every",
  "frontend",
  "backend",
  "graph",
  "hidden",
  "human",
  "ignore",
  "memory",
  "pipeline",
  "prompt",
  "project",
  "proxy",
  "querying",
  "ram",
  "retrieval",
  "system",
  "telemetry",
  "the",
]);

const RELATION_PATTERNS = [
  { predicate: "part_of", regex: /\b(part of|inside|within)\b/i, weight: 0.95 },
  { predicate: "depends_on", regex: /\b(depends on|requires|relies on|needs)\b/i, weight: 0.95 },
  { predicate: "integrates_with", regex: /\b(integrates with|connects to|works with)\b/i, weight: 0.9 },
  { predicate: "stores_in", regex: /\b(store|stores|stored|persist|persists|persisted)\b.*\b(in|into)\b/i, weight: 0.88 },
  { predicate: "captures", regex: /\b(capture|captures|record|records|log|logs|intercept|intercepts)\b/i, weight: 0.82 },
  { predicate: "indexes", regex: /\b(index|indexes|indexing|chunk|chunks|chunking)\b/i, weight: 0.8 },
  { predicate: "retrieves", regex: /\b(retrieve|retrieves|retrieval|surface|surfaces|expand|expands)\b/i, weight: 0.82 },
  { predicate: "related_to", regex: /\b(related|linked|connected|associated)\b/i, weight: 0.6 },
];

function cleanLabel(label) {
  const tokens = label
    .replace(/^[^A-Za-z0-9]+|[^A-Za-z0-9]+$/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .filter(Boolean);

  while (tokens.length > 1 && LEADING_NOISE.has(tokens[0].toLowerCase())) {
    tokens.shift();
  }

  while (tokens.length > 1 && /^(and|or|because)$/i.test(tokens[tokens.length - 1])) {
    tokens.pop();
  }

  return tokens.join(" ").trim();
}

function guessKind(label) {
  if (/^ContextOS$/i.test(label)) {
    return "project";
  }

  if (/SQLite|FTS5|Metal|OpenClaw|openclaw|shadcn/i.test(label)) {
    return "technology";
  }

  if (/(system|engine|pipeline|layer|graph|dashboard|proxy|storage|retrieval|telemetry|frontend|backend|classifier|agent|component)/i.test(label)) {
    return "component";
  }

  if (/task|constraint|decision|fact/i.test(label)) {
    return "capability";
  }

  return "concept";
}

function collectMatches(regex, text) {
  const matches = [];
  for (const match of text.matchAll(regex)) {
    if (match.index === undefined) {
      continue;
    }

    matches.push({ text: cleanLabel(match[1] ?? match[0]), index: match.index });
  }
  return matches;
}

function extractEntitiesWithOffsets(text) {
  const candidates = [];

  const quoted = collectMatches(/["'`]{1}([^"'`]{2,80})["'`]{1}/g, text);
  candidates.push(...quoted);

  for (const term of KNOWN_TERMS) {
    for (const match of text.matchAll(new RegExp(`\\b(${term.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")})\\b`, "gi"))) {
      candidates.push({ text: cleanLabel(match[1]), index: match.index ?? 0 });
    }
  }

  const domainPattern = new RegExp(
    `\\b((?:[A-Za-z0-9][A-Za-z0-9/+.-]*\\s+){0,3}(?:${DOMAIN_SUFFIXES.join("|")}))s?\\b`,
    "gi",
  );
  candidates.push(...collectMatches(domainPattern, text));

  const capitalized = collectMatches(/\b([A-Z][A-Za-z0-9/+.-]*(?:\s+[A-Z][A-Za-z0-9/+.-]*){0,2})\b/g, text);
  candidates.push(...capitalized);

  const listCandidates = collectMatches(
    /\b([a-z][a-z0-9/+.-]*(?:\s+[a-z][a-z0-9/+.-]*){0,3}\s+(?:graph|pipeline|layer|engine|dashboard|proxy|system|telemetry|indexer))\b/gi,
    text,
  );
  candidates.push(...listCandidates);

  const uniqueMatches = new Map();
  for (const candidate of candidates) {
    const label = cleanLabel(candidate.text);
    if (!label || label.length < 3) {
      continue;
    }

    const lower = label.toLowerCase();
    if (GENERIC_LABELS.has(lower)) {
      continue;
    }

    const key = lower;
    if (!uniqueMatches.has(key)) {
      uniqueMatches.set(key, { label, kind: guessKind(label), index: candidate.index });
    }
  }

  return [...uniqueMatches.values()].sort((left, right) => left.index - right.index);
}

function deriveRelationship(sentence, entities) {
  if (entities.length < 2) {
    return [];
  }

  const relations = [];

  for (const pattern of RELATION_PATTERNS) {
    if (!pattern.regex.test(sentence)) {
      continue;
    }

    if (/should also surface/i.test(sentence) && entities.length >= 2) {
      const [seed, ...rest] = entities;
      for (const entity of rest) {
        relations.push({
          category: "relationship",
          predicate: pattern.predicate === "retrieves" ? "related_to" : pattern.predicate,
          subjectLabel: seed.label,
          objectLabel: entity.label,
          detail: `${seed.label} ${pattern.predicate.replace(/_/g, " ")} ${entity.label}`,
          confidence: pattern.weight,
        });
      }
      continue;
    }

    for (let index = 0; index < entities.length - 1; index += 1) {
      relations.push({
        category: "relationship",
        predicate: pattern.predicate,
        subjectLabel: entities[index].label,
        objectLabel: entities[index + 1].label,
        detail: `${entities[index].label} ${pattern.predicate.replace(/_/g, " ")} ${entities[index + 1].label}`,
        confidence: pattern.weight,
      });
    }
  }

  // Removed: catch-all related_to fallback at 0.45 confidence.
  // This produced 8K+ junk relationship proposals with null subjects/objects
  // and empty details. Real relationships come from Haiku classification
  // or the regex patterns above — the fallback added only noise.

  return relations;
}

/**
 * Task classification — tightened to reduce false positives.
 *
 * `task` is the ONLY category that creates an action item (promotes to `tasks` table).
 * False positives here show up as fake items on the dashboard — high cost.
 * See CLASSIFIER-RATIONALE.md for full reasoning.
 *
 * Filters:
 * 1. Must contain an action keyword (same as before)
 * 2. Must NOT be past tense / descriptive (already done)
 * 3. Must NOT be inside a markdown table row (status tables aren't tasks)
 * 4. Must NOT be meta-commentary about tasks ("this is a task", "Codex task")
 * 5. Must have imperative/future intent (we need to, should, will, let's, TODO)
 */
function inferTasks(sentence, primaryEntity) {
  const tasks = [];

  // Step 1: Must contain an action keyword
  if (!/\b(todo|implement|build|add|track|log|index|capture|design|prototype)\b/i.test(sentence)) {
    return tasks;
  }

  // Step 2: Skip past-tense / completed descriptions
  if (/\b(built|implemented|added|tracked|logged|indexed|captured|designed|prototyped|shipped|completed|done|finished)\b/i.test(sentence)) {
    return tasks;
  }

  // Step 3: Skip markdown table rows (pipeline/status tables)
  if (/^\s*\|/.test(sentence)) {
    return tasks;
  }

  // Step 4: Skip meta-commentary about tasks (talking ABOUT tasks, not defining them)
  if (/\b(this is a|that's a|it's a|was a|clean|meatiest|next|the real)\b.*\btask\b/i.test(sentence)) {
    return tasks;
  }
  if (/\btask\b.*\b(yet|already|session|agent|codex)\b/i.test(sentence)) {
    return tasks;
  }

  // Step 5: Require imperative/future intent
  const trimmed = sentence.trim();
  const hasIntent = /\b(need to|should|must|will|let's|lets|want to|going to|plan to|TODO|FIXME|HACK)\b/i.test(sentence)
    || /^(implement|build|add|create|design|track|capture|set up|wire|replace|write)\b/i.test(trimmed)
    || /^(task|todo|fixme|hack)\s*:/i.test(trimmed);  // "Task: add telemetry" style

  if (!hasIntent) {
    return tasks;
  }

  tasks.push({
    category: "task",
    subjectLabel: primaryEntity?.label ?? null,
    detail: sentence,
    confidence: 0.78,
    metadata: {
      priority: /\bmust|required|correctness first\b/i.test(sentence) ? "high" : "medium",
    },
  });

  return tasks;
}

function inferConstraints(sentence, primaryEntity) {
  if (!/\b(must|cannot|no |without|first time right|correctness first|local-first)\b/i.test(sentence)) {
    return [];
  }

  return [
    {
      category: "constraint",
      subjectLabel: primaryEntity?.label ?? null,
      detail: sentence,
      confidence: 0.86,
      metadata: {
        severity: /\b(cannot|must|first time right|correctness first)\b/i.test(sentence) ? "high" : "medium",
      },
    },
  ];
}

function inferDecisions(sentence, primaryEntity) {
  if (!/\b(decide|decision|going with|we will|chosen|use shadcn|stores everything in|lives in ram)\b/i.test(sentence)) {
    return [];
  }

  return [
    {
      category: "decision",
      subjectLabel: primaryEntity?.label ?? null,
      detail: sentence,
      confidence: 0.7,
    },
  ];
}

function inferFacts(sentence, primaryEntity) {
  if (!/\b(is|are|has|have|stores|lives|works|tracks|logs|uses)\b/i.test(sentence)) {
    return [];
  }

  return [
    {
      category: "fact",
      subjectLabel: primaryEntity?.label ?? null,
      detail: sentence,
      confidence: 0.62,
    },
  ];
}

export class ObservationClassifier {
  classifyText(text) {
    const sentences = sentenceSplit(text);
    const entityMap = new Map();
    const observations = [];

    for (const sentence of sentences) {
      const entities = extractEntitiesWithOffsets(sentence);
      for (const entity of entities) {
        if (!entityMap.has(entity.label.toLowerCase())) {
          entityMap.set(entity.label.toLowerCase(), entity);
        }
      }

      const primaryEntity = entities[0] ?? null;
      const sentenceObservations = [
        ...deriveRelationship(sentence, entities),
        ...inferTasks(sentence, primaryEntity),
        ...inferConstraints(sentence, primaryEntity),
        ...inferDecisions(sentence, primaryEntity),
        ...inferFacts(sentence, primaryEntity),
      ];

      for (const observation of sentenceObservations) {
        observations.push({
          ...observation,
          sourceSpan: sentence,
        });
      }
    }

    const entities = unique([...entityMap.values()].map((entity) => entity.label)).map((label) => entityMap.get(label.toLowerCase()) ?? entityMap.get(label));

    return {
      entities,
      observations,
    };
  }
}
