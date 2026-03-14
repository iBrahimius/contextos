import fs from "node:fs/promises";

const COMMENT_RE = /<!--\s*(.*?)\s*-->/g;
const REF_RE = /ref:(decision|task|constraint|entity)\/([A-Za-z0-9:_-]+)(?:\s*=\s*([A-Za-z0-9_-]+))?/g;
const CODE_FENCE_RE = /^```/;
const REGISTRY_TYPES = ["decision", "task", "constraint", "entity"];
const MIN_SUGGESTION_TERM_LENGTH = 3;

function normalizeRefId(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function normalizeStatus(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  return normalized || null;
}

function normalizeRefType(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  return REGISTRY_TYPES.includes(normalized) ? normalized : null;
}

function extractLineExcerpt(line) {
  return String(line ?? "").trim().slice(0, 160);
}

function collectLines(content) {
  return String(content ?? "").replace(/\r/g, "").split("\n");
}

function getRegistryRows(database, type) {
  if (type === "entity") {
    if (typeof database.listEntities === "function") {
      return database.listEntities();
    }

    if (typeof database.queryRegistry === "function") {
      return database.queryRegistry("entities", { query: "", filters: {} });
    }

    return [];
  }

  if (typeof database.queryRegistry === "function") {
    return database.queryRegistry(`${type}s`, { query: "", filters: {} });
  }

  return [];
}

function getClaimMap(database) {
  if (typeof database.listCurrentClaims !== "function") {
    return new Map();
  }

  const claims = database.listCurrentClaims({
    types: ["decision", "constraint", "task"],
    limit: 200,
  });

  const map = new Map();
  for (const claim of claims) {
    const observationId = claim?.observation_id ?? claim?.observationId ?? null;
    if (!observationId) {
      continue;
    }

    map.set(observationId, claim);
  }

  return map;
}

function canonicalStatus(type, value) {
  const normalized = normalizeStatus(value);
  if (!normalized) {
    return null;
  }

  if (type === "task" && normalized === "open") {
    return "active";
  }

  return normalized;
}

function deriveRegistryStatus(type, row, claim = null) {
  if (type === "entity") {
    return null;
  }

  if (type === "task") {
    return canonicalStatus(type, claim?.value_text ?? row?.status ?? "open") ?? "active";
  }

  const lifecycleState = normalizeStatus(claim?.lifecycle_state ?? claim?.lifecycleState);
  const claimValue = normalizeStatus(claim?.value_text ?? claim?.valueText);

  if (type === "decision") {
    if (lifecycleState && lifecycleState !== "active") {
      return lifecycleState;
    }

    if (claimValue === "superseded") {
      return "superseded";
    }

    return "active";
  }

  if (type === "constraint") {
    if (claimValue === "expired" || claimValue === "overridden") {
      return claimValue;
    }

    if (lifecycleState && lifecycleState !== "active") {
      return lifecycleState;
    }

    return "active";
  }

  return normalizeStatus(row?.status ?? claimValue ?? lifecycleState);
}

function buildPrimaryName(type, row) {
  if (type === "entity") {
    return row?.label ?? row?.slug ?? row?.id ?? "";
  }

  if (type === "decision") {
    return row?.title ?? row?.detail ?? row?.id ?? "";
  }

  if (type === "task") {
    return row?.title ?? row?.detail ?? row?.id ?? "";
  }

  if (type === "constraint") {
    return row?.detail ?? row?.title ?? row?.id ?? "";
  }

  return row?.id ?? "";
}

function buildSearchTerms(type, row, preferredRefId) {
  const terms = new Set();
  const values = [
    preferredRefId,
    row?.slug,
    row?.label,
    row?.title,
    row?.detail,
  ];

  for (const value of values) {
    const raw = String(value ?? "").trim();
    if (!raw) {
      continue;
    }

    const lowered = raw.toLowerCase();
    if (lowered.length >= MIN_SUGGESTION_TERM_LENGTH) {
      terms.add(lowered);
    }

    const normalized = normalizeRefId(raw);
    if (normalized.length >= MIN_SUGGESTION_TERM_LENGTH) {
      terms.add(normalized.replace(/_/g, " "));
    }
  }

  if (type === "entity") {
    const label = String(row?.label ?? "").trim();
    if (label.length >= MIN_SUGGESTION_TERM_LENGTH) {
      terms.add(label.toLowerCase());
    }
  }

  return [...terms];
}

function computePreferredRefIds(rowsByType) {
  const countsByType = new Map();
  for (const type of REGISTRY_TYPES) {
    countsByType.set(type, new Map());
    for (const row of rowsByType[type]) {
      const candidate = normalizeRefId(
        type === "entity"
          ? (row?.slug ?? row?.label ?? row?.id)
          : buildPrimaryName(type, row),
      ) || normalizeRefId(row?.id);

      const counts = countsByType.get(type);
      counts.set(candidate, (counts.get(candidate) ?? 0) + 1);
    }
  }

  const preferredRefIds = new Map();
  for (const type of REGISTRY_TYPES) {
    const counts = countsByType.get(type);
    for (const row of rowsByType[type]) {
      const candidate = normalizeRefId(
        type === "entity"
          ? (row?.slug ?? row?.label ?? row?.id)
          : buildPrimaryName(type, row),
      ) || normalizeRefId(row?.id);

      const preferredRefId = (counts.get(candidate) ?? 0) === 1
        ? candidate
        : String(row?.id ?? candidate);

      preferredRefIds.set(`${type}:${row?.id}`, preferredRefId);
    }
  }

  return preferredRefIds;
}

function createRegistryEntry(type, row, claimMap, preferredRefIds) {
  const claim = claimMap.get(row?.observationId) ?? null;
  const preferredRefId = preferredRefIds.get(`${type}:${row?.id}`)
    || normalizeRefId(type === "entity" ? (row?.slug ?? row?.label ?? row?.id) : buildPrimaryName(type, row))
    || String(row?.id ?? "");

  const keys = new Set([
    String(row?.id ?? "").trim(),
    normalizeRefId(row?.id),
    preferredRefId,
    normalizeRefId(row?.slug),
    normalizeRefId(row?.label),
    normalizeRefId(row?.title),
    normalizeRefId(row?.detail),
    normalizeRefId(row?.name),
  ].filter(Boolean));

  return {
    type,
    id: String(row?.id ?? preferredRefId),
    preferredRefId,
    status: deriveRegistryStatus(type, row, claim),
    label: row?.label ?? null,
    slug: row?.slug ?? null,
    title: row?.title ?? null,
    detail: row?.detail ?? null,
    summary: row?.summary ?? null,
    rationale: row?.rationale ?? null,
    observationId: row?.observationId ?? null,
    entityId: row?.entityId ?? null,
    entityLabel: row?.entityLabel ?? null,
    keys: [...keys],
    searchTerms: buildSearchTerms(type, row, preferredRefId),
    raw: row,
  };
}

export function createRefRegistrySnapshot(database) {
  if (!database) {
    throw new Error("A database instance is required");
  }

  const rowsByType = {
    entity: getRegistryRows(database, "entity"),
    decision: getRegistryRows(database, "decision"),
    task: getRegistryRows(database, "task"),
    constraint: getRegistryRows(database, "constraint"),
  };
  const claimMap = getClaimMap(database);
  const preferredRefIds = computePreferredRefIds(rowsByType);

  return {
    entity: rowsByType.entity.map((row) => createRegistryEntry("entity", row, claimMap, preferredRefIds)),
    decision: rowsByType.decision.map((row) => createRegistryEntry("decision", row, claimMap, preferredRefIds)),
    task: rowsByType.task.map((row) => createRegistryEntry("task", row, claimMap, preferredRefIds)),
    constraint: rowsByType.constraint.map((row) => createRegistryEntry("constraint", row, claimMap, preferredRefIds)),
  };
}

function buildLookup(snapshot) {
  const lookup = new Map();

  for (const type of REGISTRY_TYPES) {
    const typeLookup = new Map();
    for (const entry of snapshot[type] ?? []) {
      for (const key of entry.keys ?? []) {
        if (!key) {
          continue;
        }

        typeLookup.set(normalizeRefId(key), entry);
      }
    }

    lookup.set(type, typeLookup);
  }

  return lookup;
}

function buildSuggestionCandidates(snapshot) {
  const candidates = [];

  for (const type of REGISTRY_TYPES) {
    for (const entry of snapshot[type] ?? []) {
      for (const term of entry.searchTerms ?? []) {
        const normalizedTerm = String(term ?? "").trim().toLowerCase();
        if (normalizedTerm.length < MIN_SUGGESTION_TERM_LENGTH) {
          continue;
        }

        candidates.push({
          type,
          entry,
          term: normalizedTerm,
          termLength: normalizedTerm.length,
        });
      }
    }
  }

  return candidates.sort((left, right) => right.termLength - left.termLength);
}

function findRegistryEntry(lookup, type, refId) {
  const normalizedType = normalizeRefType(type);
  if (!normalizedType) {
    return null;
  }

  return lookup.get(normalizedType)?.get(normalizeRefId(refId)) ?? null;
}

function resolveRegistryEntry(database, lookup, type, refId) {
  const entry = findRegistryEntry(lookup, type, refId);
  if (entry) {
    return entry;
  }

  if (type === "entity" && typeof database?.findEntityByName === "function") {
    const entity = database.findEntityByName(refId);
    if (!entity) {
      return null;
    }

    return {
      type: "entity",
      id: String(entity.id),
      preferredRefId: normalizeRefId(entity.slug ?? entity.label ?? entity.id) || String(entity.id),
      status: null,
    };
  }

  return null;
}

function isTagOnlyLine(line) {
  const stripped = String(line ?? "")
    .replace(COMMENT_RE, "")
    .trim();
  return stripped.length === 0;
}

function buildCoveredLines(tags, lines) {
  const coveredLines = new Set();
  for (const tag of tags) {
    coveredLines.add(tag.line);
    if (tag.line > 1 && isTagOnlyLine(lines[tag.line - 1])) {
      coveredLines.add(tag.line - 1);
    }
  }

  return coveredLines;
}

function normalizeSearchText(value) {
  return String(value ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function lineHasMatch(line, term) {
  const rawLine = String(line ?? "").toLowerCase();
  const normalizedLine = normalizeSearchText(line);
  const normalizedTerm = normalizeSearchText(term);
  return rawLine.includes(String(term ?? "").toLowerCase()) || normalizedLine.includes(normalizedTerm);
}

function buildMissingTagSuggestion(entry, status = null) {
  const suffix = status ? ` =${status}` : "";
  return `<!-- ref:${entry.type}/${entry.preferredRefId}${suffix} -->`;
}

export function extractRefTags(content, { filePath = "<content>" } = {}) {
  const tags = [];
  const lines = collectLines(content);
  let inCodeFence = false;

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    if (CODE_FENCE_RE.test(line.trim())) {
      inCodeFence = !inCodeFence;
      continue;
    }

    if (inCodeFence) {
      continue;
    }

    COMMENT_RE.lastIndex = 0;
    let commentMatch = COMMENT_RE.exec(line);
    while (commentMatch) {
      const commentBody = commentMatch[1] ?? "";
      REF_RE.lastIndex = 0;
      let refMatch = REF_RE.exec(commentBody);
      while (refMatch) {
        const [, type, id, status] = refMatch;
        tags.push({
          type,
          id,
          status: normalizeStatus(status),
          file: filePath,
          line: index + 1,
          raw: refMatch[0],
        });
        refMatch = REF_RE.exec(commentBody);
      }
      commentMatch = COMMENT_RE.exec(line);
    }
  }

  return tags;
}

export function lintRefTags({ content, database, filePath = "<content>", snapshot = null } = {}) {
  if (typeof content !== "string") {
    throw new Error("content must be a string");
  }

  const registrySnapshot = snapshot ?? createRefRegistrySnapshot(database);
  const lookup = buildLookup(registrySnapshot);
  const candidates = buildSuggestionCandidates(registrySnapshot);
  const tags = extractRefTags(content, { filePath });
  const conflicts = [];
  const staleRefs = [];
  const orphans = [];
  const suggestions = [];
  const lines = collectLines(content);
  const coveredLines = buildCoveredLines(tags, lines);
  const taggedEntries = new Set();

  for (const tag of tags) {
    const entry = resolveRegistryEntry(database, lookup, tag.type, tag.id);

    if (!entry) {
      conflicts.push({
        file: filePath,
        line: tag.line,
        tag,
        description: `${tag.type}/${tag.id} does not exist in the live registry`,
      });
      continue;
    }

    taggedEntries.add(`${entry.type}:${entry.id}`);

    if (tag.status && entry.status && canonicalStatus(tag.type, tag.status) !== canonicalStatus(entry.type, entry.status)) {
      staleRefs.push({
        file: filePath,
        line: tag.line,
        tag,
        registryStatus: entry.status,
        description: `${tag.type}/${tag.id} is tagged as ${tag.status}, but the registry status is ${entry.status}`,
        suggestion: buildMissingTagSuggestion(entry, entry.status),
      });
    }
  }

  let inCodeFence = false;
  const seenOrphans = new Set();
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    const lineNumber = index + 1;

    if (CODE_FENCE_RE.test(line.trim())) {
      inCodeFence = !inCodeFence;
      continue;
    }

    if (inCodeFence || coveredLines.has(lineNumber)) {
      continue;
    }

    const excerpt = extractLineExcerpt(line);
    if (!excerpt || excerpt.length < MIN_SUGGESTION_TERM_LENGTH) {
      continue;
    }

    const matchedTerms = new Set();
    for (const candidate of candidates) {
      if (taggedEntries.has(`${candidate.type}:${candidate.entry.id}`)) {
        continue;
      }

      if (!lineHasMatch(line, candidate.term)) {
        continue;
      }

      const normalizedTerm = normalizeSearchText(candidate.term);
      if (matchedTerms.has(normalizedTerm)) {
        continue;
      }

      const orphanKey = `${lineNumber}:${candidate.type}:${candidate.entry.id}`;
      if (seenOrphans.has(orphanKey)) {
        continue;
      }

      const confidence = normalizeRefId(candidate.term) === candidate.entry.preferredRefId
        ? "high"
        : "medium";

      orphans.push({
        file: filePath,
        line: lineNumber,
        type: candidate.type,
        id: candidate.entry.preferredRefId,
        registryId: candidate.entry.id,
        excerpt,
        description: `${candidate.type}/${candidate.entry.preferredRefId} is mentioned here but has no ref tag`,
      });
      suggestions.push({
        file: filePath,
        line: lineNumber,
        textExcerpt: excerpt,
        matchType: candidate.type,
        matchId: candidate.entry.preferredRefId,
        confidence,
        suggestedTag: buildMissingTagSuggestion(candidate.entry),
      });
      seenOrphans.add(orphanKey);
      matchedTerms.add(normalizedTerm);
    }
  }

  return {
    ok: conflicts.length === 0 && staleRefs.length === 0 && orphans.length === 0,
    file: filePath,
    tags,
    conflicts,
    staleRefs,
    orphans,
    suggestions,
    counts: {
      tags: tags.length,
      conflicts: conflicts.length,
      staleRefs: staleRefs.length,
      orphans: orphans.length,
      suggestions: suggestions.length,
    },
  };
}

export async function lintRefTagsPath({ path, database, snapshot = null } = {}) {
  const filePath = String(path ?? "").trim();
  if (!filePath) {
    throw new Error("path is required");
  }

  try {
    const content = await fs.readFile(filePath, "utf8");
    return lintRefTags({ content, database, filePath, snapshot });
  } catch (error) {
    if (error?.code === "ENOENT") {
      error.statusCode = 404;
    }
    throw error;
  }
}

export { normalizeRefId };
