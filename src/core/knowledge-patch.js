import { clamp, unique } from "./utils.js";

export const ENTITY_KINDS = new Set([
  "person",
  "project",
  "organization",
  "location",
  "event",
  "tool",
  "component",
  "technology",
  "concept",
  "capability",
  "system",
]);

export const OBSERVATION_CATEGORIES = new Set([
  "relationship",
  "task",
  "decision",
  "constraint",
  "fact",
]);

export const RELATIONSHIP_PREDICATES = new Set([
  "part_of",
  "depends_on",
  "integrates_with",
  "created_by",
  "owned_by",
  "manages",
  "implements",
  "extends",
  "replaces",
  "blocks",
  "enables",
  "stores_in",
  "captures",
  "indexes",
  "retrieves",
  "related_to",
]);

export function normalizeLabel(value) {
  if (typeof value !== "string") {
    return null;
  }

  const cleaned = value
    .replace(/[\r\n\t]+/g, " ")
    .replace(/\s+/g, " ")
    .replace(/^[`"'([{<\s]+|[`"')\]}>.,:;!?/\s]+$/g, "")
    .trim();

  return cleaned || null;
}

function normalizeEntityKind(kind, label) {
  if (ENTITY_KINDS.has(kind)) {
    return kind;
  }

  if (/SQLite|FTS5|Metal|shadcn/i.test(label)) {
    return "technology";
  }

  if (/ContextOS/i.test(label)) {
    return "project";
  }

  if (/(system|engine|pipeline|layer|graph|dashboard|proxy|telemetry|frontend|backend|agent|storage)/i.test(label)) {
    return "component";
  }

  return "concept";
}

function dedupeEntities(entities) {
  const merged = new Map();

  for (const entity of entities) {
    const key = entity.label.toLowerCase();
    const current = merged.get(key);
    if (!current) {
      merged.set(key, entity);
      continue;
    }

    merged.set(key, {
      ...current,
      kind: current.kind === "concept" ? entity.kind : current.kind,
      summary: current.summary ?? entity.summary,
      aliases: unique([...(current.aliases ?? []), ...(entity.aliases ?? [])]),
      metadata: { ...(current.metadata ?? {}), ...(entity.metadata ?? {}) },
    });
  }

  return [...merged.values()];
}

function dedupeObservations(observations) {
  const seen = new Set();
  return observations.filter((observation) => {
    const key = [
      observation.category,
      observation.predicate ?? "",
      observation.subjectLabel ?? "",
      observation.objectLabel ?? "",
      observation.detail,
    ].join("|");

    if (seen.has(key)) {
      return false;
    }

    seen.add(key);
    return true;
  });
}

function dedupeByKey(items, makeKey) {
  const seen = new Set();
  return items.filter((item) => {
    const key = makeKey(item);
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

function normalizeEntity(entity) {
  const label = normalizeLabel(entity?.label);
  if (!label) {
    return null;
  }

  return {
    label,
    kind: normalizeEntityKind(entity?.kind, label),
    summary: normalizeLabel(entity?.summary),
    aliases: unique((entity?.aliases ?? []).map((alias) => normalizeLabel(alias)).filter(Boolean)).filter((alias) => alias !== label),
    metadata: entity?.metadata ?? null,
  };
}

function normalizeObservation(observation) {
  const category = observation?.category;
  if (!OBSERVATION_CATEGORIES.has(category)) {
    return null;
  }

  const detail = normalizeLabel(observation?.detail);
  if (!detail) {
    return null;
  }

  const normalized = {
    category,
    predicate: null,
    subjectLabel: normalizeLabel(observation?.subjectLabel),
    objectLabel: normalizeLabel(observation?.objectLabel),
    detail,
    confidence: clamp(Number(observation?.confidence ?? 0.5), 0, 1),
    sourceSpan: normalizeLabel(observation?.sourceSpan ?? detail),
    metadata: observation?.metadata ?? null,
  };

  if (category === "relationship") {
    if (!RELATIONSHIP_PREDICATES.has(observation?.predicate)) {
      return null;
    }

    if (!normalized.subjectLabel || !normalized.objectLabel) {
      return null;
    }

    normalized.predicate = observation.predicate;
  }

  return normalized;
}

function normalizeRetrieveHint(hint) {
  const seed = normalizeLabel(hint?.seed);
  const expandTo = normalizeLabel(hint?.expandTo);
  const reason = normalizeLabel(hint?.reason);

  if (!seed || !expandTo || !reason) {
    return null;
  }

  return {
    seed,
    expandTo,
    reason,
    weight: clamp(Number(hint?.weight ?? 0.75), 0.1, 3),
    ttlTurns: Math.max(1, Math.round(Number(hint?.ttlTurns ?? 6))),
  };
}

function normalizeGraphProposal(proposal) {
  const proposalType = normalizeLabel(proposal?.proposalType);
  if (!proposalType) {
    return null;
  }

  return {
    proposalType,
    subjectLabel: normalizeLabel(proposal?.subjectLabel),
    predicate: proposal?.predicate && RELATIONSHIP_PREDICATES.has(proposal.predicate) ? proposal.predicate : null,
    objectLabel: normalizeLabel(proposal?.objectLabel),
    detail: normalizeLabel(proposal?.detail),
    confidence: clamp(Number(proposal?.confidence ?? 0.5), 0, 1),
    reason: normalizeLabel(proposal?.reason),
    payload: proposal?.payload ?? null,
  };
}

function normalizeComplexityAdjustment(adjustment) {
  const entity = normalizeLabel(adjustment?.entity);
  const reason = normalizeLabel(adjustment?.reason);
  const delta = Number(adjustment?.delta ?? 0);

  if (!entity || !Number.isFinite(delta) || delta === 0) {
    return null;
  }

  return {
    entity,
    delta,
    missIncrement: Math.max(0, Math.round(Number(adjustment?.missIncrement ?? 0))),
    reason,
  };
}

export function validateKnowledgePatch(input = {}) {
  return {
    entities: dedupeEntities((input.entities ?? []).map(normalizeEntity).filter(Boolean)),
    observations: dedupeObservations((input.observations ?? []).map(normalizeObservation).filter(Boolean)),
    retrieveHints: dedupeByKey(
      (input.retrieveHints ?? []).map(normalizeRetrieveHint).filter(Boolean),
      (hint) => `${hint.seed}|${hint.expandTo}|${hint.reason}`,
    ),
    graphProposals: dedupeByKey(
      (input.graphProposals ?? []).map(normalizeGraphProposal).filter(Boolean),
      (proposal) => [
        proposal.proposalType,
        proposal.subjectLabel ?? "",
        proposal.predicate ?? "",
        proposal.objectLabel ?? "",
        proposal.detail ?? "",
      ].join("|"),
    ),
    complexityAdjustments: dedupeByKey(
      (input.complexityAdjustments ?? []).map(normalizeComplexityAdjustment).filter(Boolean),
      (adjustment) => `${adjustment.entity}|${adjustment.reason ?? ""}|${adjustment.delta}`,
    ),
  };
}

export function applyAliasesToPatch(patch, aliases = []) {
  const aliasMap = new Map();

  for (const alias of aliases) {
    const source = normalizeLabel(alias?.label);
    const canonical = normalizeLabel(alias?.canonicalLabel);
    if (!source || !canonical || source.toLowerCase() === canonical.toLowerCase()) {
      continue;
    }
    aliasMap.set(source.toLowerCase(), canonical);
  }

  if (!aliasMap.size) {
    return patch;
  }

  const canonicalize = (label) => {
    const normalized = normalizeLabel(label);
    if (!normalized) {
      return null;
    }

    return aliasMap.get(normalized.toLowerCase()) ?? normalized;
  };

  const entityMap = new Map();
  for (const entity of patch.entities) {
    const canonicalLabel = canonicalize(entity.label);
    const key = canonicalLabel.toLowerCase();
    const aliasesForEntity = unique([
      ...(entity.aliases ?? []),
      canonicalLabel.toLowerCase() === entity.label.toLowerCase() ? null : entity.label,
    ].filter(Boolean));

    const current = entityMap.get(key);
    if (!current) {
      entityMap.set(key, {
        ...entity,
        label: canonicalLabel,
        aliases: aliasesForEntity,
      });
      continue;
    }

    entityMap.set(key, {
      ...current,
      aliases: unique([...(current.aliases ?? []), ...aliasesForEntity]),
      summary: current.summary ?? entity.summary,
    });
  }

  const remapObservation = (observation) => ({
    ...observation,
    subjectLabel: canonicalize(observation.subjectLabel),
    objectLabel: canonicalize(observation.objectLabel),
  });

  return validateKnowledgePatch({
    ...patch,
    entities: [...entityMap.values()],
    observations: patch.observations.map(remapObservation),
    retrieveHints: patch.retrieveHints.map((hint) => ({
      ...hint,
      seed: canonicalize(hint.seed),
      expandTo: canonicalize(hint.expandTo),
    })),
    graphProposals: patch.graphProposals.map((proposal) => ({
      ...proposal,
      subjectLabel: canonicalize(proposal.subjectLabel),
      objectLabel: canonicalize(proposal.objectLabel),
    })),
    complexityAdjustments: patch.complexityAdjustments.map((adjustment) => ({
      ...adjustment,
      entity: canonicalize(adjustment.entity),
    })),
  });
}
