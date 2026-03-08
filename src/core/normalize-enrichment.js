/**
 * normalize-enrichment.js — Post-processing for Haiku classification output.
 *
 * Deterministic cleanup applied AFTER the LLM returns, BEFORE persisting.
 * Fixes three known Haiku failure modes:
 *   1. Invented predicates (maps to nearest allowed or related_to)
 *   2. Bad entity labels (phrases, multi-concept, too long)
 *   3. Invalid entity kinds (maps to concept)
 *
 * This is intentionally NOT in the prompt — prompts are probabilistic,
 * this code runs on 100% of output, every time.
 */

import { RELATION_REGISTRY } from "./relation-types.js";

// --- Allowed values ---

const ALLOWED_KINDS = new Set([
  "person", "project", "system", "concept", "tool",
  "organization", "location", "event",
]);

const ALLOWED_PREDICATES = new Set(Object.keys(RELATION_REGISTRY));

// Fuzzy mapping for common invented predicates → allowed ones
const PREDICATE_MAP = {
  // Common Haiku inventions observed in testing
  stored_in_parallel: "integrates_with",
  confirmed_as: "related_to",
  runs_on: "depends_on",
  built_with: "depends_on",
  built_on: "depends_on",
  written_in: "depends_on",
  uses: "depends_on",
  used_by: "depended_on_by",
  contains: "has_part",
  included_in: "part_of",
  belongs_to: "part_of",
  hosts: "stores",
  hosted_by: "stores_in",
  produces: "captures",
  produced_by: "captured_by",
  consumes: "retrieves",
  consumed_by: "retrieved_by",
  feeds: "captures",
  fed_by: "captured_by",
  triggers: "enables",
  triggered_by: "depended_on_by",
  enables: "related_to", // not in registry, map to generic
  replaces: "related_to",
  replaced_by: "related_to",
  blocks: "related_to",
  blocked_by: "related_to",
  created_by: "owned_by",
  manages: "owns",
  managed_by: "owned_by",
  implements: "part_of",
  extends: "related_to",
};

// --- Entity label rules ---

const MAX_LABEL_WORDS = 4;
const MIN_LABEL_LENGTH = 2;
const MAX_LABEL_LENGTH = 60;

// Labels containing these are multi-concept or noise
const LABEL_REJECT_CHARS = [",", ";", " and ", " or ", " / "];

// Leading/trailing noise words
const LABEL_STRIP_WORDS = new Set([
  "a", "an", "the", "this", "that", "these", "those",
  "some", "all", "any", "each", "every",
]);

// --- Normalization functions ---

/**
 * Normalize a predicate to an allowed value.
 * Returns the predicate if allowed, maps known inventions, or falls back to related_to.
 */
export function normalizePredicate(predicate) {
  if (!predicate || typeof predicate !== "string") return "related_to";
  const p = predicate.trim().toLowerCase();
  if (ALLOWED_PREDICATES.has(p)) return p;
  if (PREDICATE_MAP[p]) return PREDICATE_MAP[p];
  return "related_to";
}

/**
 * Normalize an entity kind to an allowed value.
 * Returns the kind if allowed, or maps to concept.
 */
export function normalizeKind(kind) {
  if (!kind || typeof kind !== "string") return "concept";
  const k = kind.trim().toLowerCase();
  if (ALLOWED_KINDS.has(k)) return k;

  // Common mappings
  const KIND_MAP = {
    technology: "tool",
    tech: "tool",
    framework: "tool",
    library: "tool",
    language: "tool",
    component: "system",
    service: "system",
    platform: "system",
    application: "system",
    app: "system",
    milestone: "event",
    phase: "event",
    company: "organization",
    team: "organization",
    group: "organization",
    place: "location",
    city: "location",
    country: "location",
    idea: "concept",
    principle: "concept",
    pattern: "concept",
    strategy: "concept",
    feature: "concept",
  };

  return KIND_MAP[k] ?? "concept";
}

/**
 * Clean an entity label. Returns null if the label should be rejected.
 */
export function normalizeLabel(label) {
  if (!label || typeof label !== "string") return null;

  let cleaned = label.trim();

  // Strip surrounding quotes
  cleaned = cleaned.replace(/^["'`]+|["'`]+$/g, "").trim();

  // Reject if contains multi-concept markers
  for (const ch of LABEL_REJECT_CHARS) {
    if (cleaned.includes(ch)) return null;
  }

  // Strip leading/trailing noise words
  const words = cleaned.split(/\s+/);
  while (words.length > 1 && LABEL_STRIP_WORDS.has(words[0].toLowerCase())) {
    words.shift();
  }
  while (words.length > 1 && LABEL_STRIP_WORDS.has(words[words.length - 1].toLowerCase())) {
    words.pop();
  }
  cleaned = words.join(" ");

  // Reject too short or too long
  if (cleaned.length < MIN_LABEL_LENGTH || cleaned.length > MAX_LABEL_LENGTH) return null;

  // Reject if too many words (phrase, not a name)
  if (words.length > MAX_LABEL_WORDS) return null;

  return cleaned;
}

/**
 * Normalize a full enrichment payload (entities, observations, graphProposals).
 * Returns a cleaned copy — does not mutate the input.
 *
 * @param {object} enrichment - { entities, observations, graphProposals }
 * @returns {{ entities: Array, observations: Array, graphProposals: Array, stats: object }}
 */
export function normalizeEnrichment(enrichment) {
  const stats = {
    entities: { input: 0, output: 0, rejected: 0, kindMapped: 0 },
    observations: { input: 0, output: 0, rejected: 0, predicateMapped: 0 },
    graphProposals: { input: 0, output: 0 },
  };

  // --- Entities ---
  const rawEntities = Array.isArray(enrichment.entities) ? enrichment.entities : [];
  stats.entities.input = rawEntities.length;

  const entities = [];
  const validLabels = new Set();

  for (const entity of rawEntities) {
    const label = normalizeLabel(entity.label);
    if (!label) {
      stats.entities.rejected++;
      continue;
    }

    const originalKind = entity.kind;
    const kind = normalizeKind(entity.kind);
    if (kind !== originalKind) stats.entities.kindMapped++;

    entities.push({
      label,
      kind,
      summary: entity.summary ?? null,
    });
    validLabels.add(label.toLowerCase());
  }
  stats.entities.output = entities.length;

  // --- Observations ---
  const rawObservations = Array.isArray(enrichment.observations) ? enrichment.observations : [];
  stats.observations.input = rawObservations.length;

  const observations = [];

  for (const obs of rawObservations) {
    // Category must be valid
    const category = obs.category;
    if (!category || typeof category !== "string") {
      stats.observations.rejected++;
      continue;
    }

    // Detail must exist
    if (!obs.detail || typeof obs.detail !== "string" || obs.detail.trim().length < 3) {
      stats.observations.rejected++;
      continue;
    }

    const normalized = {
      category: category.toLowerCase(),
      detail: obs.detail.trim(),
      subjectLabel: obs.subjectLabel ?? null,
      objectLabel: obs.objectLabel ?? null,
      predicate: null,
      confidence: typeof obs.confidence === "number" ? obs.confidence : 0.7,
    };

    // For relationship observations, normalize predicate
    if (category === "relationship") {
      const originalPredicate = obs.predicate;
      normalized.predicate = normalizePredicate(obs.predicate);
      if (normalized.predicate !== originalPredicate) {
        stats.observations.predicateMapped++;
      }

      // Relationship must have both subject and object
      if (!normalized.subjectLabel || !normalized.objectLabel) {
        stats.observations.rejected++;
        continue;
      }
    }

    observations.push(normalized);
  }
  stats.observations.output = observations.length;

  // --- Graph proposals ---
  const rawProposals = Array.isArray(enrichment.graphProposals) ? enrichment.graphProposals : [];
  stats.graphProposals.input = rawProposals.length;

  const graphProposals = rawProposals.map((p) => ({
    ...p,
    predicate: p.predicate ? normalizePredicate(p.predicate) : undefined,
  }));
  stats.graphProposals.output = graphProposals.length;

  return { entities, observations, graphProposals, stats };
}
