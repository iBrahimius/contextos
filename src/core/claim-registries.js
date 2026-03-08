function normalizeEntityIds(entityIds) {
  if (!Array.isArray(entityIds)) {
    return null;
  }

  const normalized = entityIds
    .map((value) => String(value ?? "").trim())
    .filter(Boolean)
    .sort();

  return normalized.length ? normalized : null;
}

function normalizeFilter(filter = {}) {
  return {
    entityIds: normalizeEntityIds(filter.entityIds),
    scopeFilter: filter.scopeFilter ?? null,
    limit: filter.limit ?? undefined,
  };
}

function serializeValue(value) {
  if (Array.isArray(value)) {
    return `[${value.map((item) => serializeValue(item)).join(",")}]`;
  }

  if (value && typeof value === "object") {
    const keys = Object.keys(value).sort();
    return `{${keys.map((key) => `${JSON.stringify(key)}:${serializeValue(value[key])}`).join(",")}}`;
  }

  return JSON.stringify(value ?? null);
}

function buildCacheKey(registryName, filter) {
  return `${registryName}:${serializeValue(normalizeFilter(filter))}`;
}

function toRegistryEntry(claim) {
  return {
    entityId: claim.subject_entity_id ?? null,
    status: claim.value_text ?? null,
    predicate: claim.predicate ?? null,
    confidence: Number(claim.confidence ?? 0),
    claimId: claim.id,
    createdAt: claim.created_at ?? claim.createdAt ?? null,
    claimType: claim.claim_type ?? null,
  };
}

function groupClaimsByEntity(claims) {
  const groups = new Map();

  for (const claim of claims) {
    const entry = toRegistryEntry(claim);
    const entityId = entry.entityId;

    if (!groups.has(entityId)) {
      groups.set(entityId, []);
    }

    groups.get(entityId).push(entry);
  }

  return [...groups.entries()].map(([entityId, claimsForEntity]) => ({
    entityId,
    claims: claimsForEntity,
  }));
}

function getClaimsForTypes(db, types, filter = {}) {
  const normalizedFilter = normalizeFilter(filter);
  return db.listCurrentClaims({
    types,
    entityIds: normalizedFilter.entityIds,
    scopeFilter: normalizedFilter.scopeFilter,
    limit: normalizedFilter.limit,
  });
}

function buildRegistry(db, types, filter = {}) {
  return groupClaimsByEntity(getClaimsForTypes(db, types, filter));
}

export function getTaskRegistry(db, filter = {}) {
  return buildRegistry(db, ["task"], filter);
}

export function getDecisionRegistry(db, filter = {}) {
  return buildRegistry(db, ["decision"], filter);
}

export function getGoalRegistry(db, filter = {}) {
  return buildRegistry(db, ["goal"], filter);
}

export function getRuleRegistry(db, filter = {}) {
  return buildRegistry(db, ["rule", "constraint"], filter);
}

export function getRegistrySnapshot(db, filter = {}) {
  return {
    tasks: getTaskRegistry(db, filter),
    decisions: getDecisionRegistry(db, filter),
    goals: getGoalRegistry(db, filter),
    rules: getRuleRegistry(db, filter),
  };
}

export function createCachedRegistry(ttlMs = 5000) {
  const cache = new Map();
  const normalizedTtlMs = Math.max(0, Number(ttlMs) || 0);

  function getCachedValue(registryName, loader, db, filter = {}) {
    const now = Date.now();
    const cacheKey = buildCacheKey(registryName, filter);
    const cached = cache.get(cacheKey);

    if (cached && (now - cached.lastFetch) <= normalizedTtlMs) {
      return cached.value;
    }

    const value = loader(db, filter);
    cache.set(cacheKey, { value, lastFetch: now });
    return value;
  }

  return {
    getTaskRegistry(db, filter = {}) {
      return getCachedValue("tasks", getTaskRegistry, db, filter);
    },
    getDecisionRegistry(db, filter = {}) {
      return getCachedValue("decisions", getDecisionRegistry, db, filter);
    },
    getGoalRegistry(db, filter = {}) {
      return getCachedValue("goals", getGoalRegistry, db, filter);
    },
    getRuleRegistry(db, filter = {}) {
      return getCachedValue("rules", getRuleRegistry, db, filter);
    },
    getRegistrySnapshot(db, filter = {}) {
      return getCachedValue("snapshot", getRegistrySnapshot, db, filter);
    },
    invalidate() {
      cache.clear();
    },
  };
}
