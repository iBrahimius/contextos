/**
 * Assembly Cache (REQ-38)
 *
 * Graph-version-aware in-memory cache for assembled packets.
 * Freshness is keyed on monotonic graph version — no TTL-first model.
 *
 * Cache key: packetType + normalized request shape (query, intent, scopeFilter, tokenBudget, conversationId)
 * Invalidation: graph version mismatch = miss
 */

function serializeCacheValue(value) {
  if (value == null) {
    return "null";
  }

  if (Array.isArray(value)) {
    return `[${value.map((item) => serializeCacheValue(item)).join(",")}]`;
  }

  if (typeof value === "object") {
    const keys = Object.keys(value).sort();
    return `{${keys.map((key) => `${JSON.stringify(key)}:${serializeCacheValue(value[key])}`).join(",")}}`;
  }

  return JSON.stringify(value);
}

function normalizeScopeForKey(scopeFilter) {
  if (!scopeFilter) {
    return null;
  }

  return {
    scopeKind: scopeFilter.scopeKind ?? scopeFilter.scope_kind ?? null,
    scopeId: scopeFilter.scopeId ?? scopeFilter.scope_id ?? null,
  };
}

function normalizeOpenItemsKind(kind) {
  const normalized = String(kind ?? "all").trim().toLowerCase();
  return normalized || "all";
}

function normalizeRegistryFiltersForKey(filters) {
  if (!filters || typeof filters !== "object" || Array.isArray(filters)) {
    return {};
  }

  const normalized = {};

  for (const key of Object.keys(filters).sort()) {
    const value = filters[key];

    if (key === "tags") {
      normalized.tags = Array.isArray(value)
        ? [...new Set(value.map((tag) => String(tag ?? "").trim().toLowerCase()).filter(Boolean))].sort()
        : [];
      continue;
    }

    if (key === "status" || key === "date_from" || key === "date_to") {
      normalized[key] = String(value ?? "").trim().toLowerCase();
      continue;
    }

    normalized[key] = value;
  }

  return normalized;
}

function normalizeRequestForKey(packetType, request = {}) {
  if (packetType === "open-items") {
    return {
      packetType,
      kind: normalizeOpenItemsKind(request.kind),
    };
  }

  if (packetType === "registry-query") {
    return {
      packetType,
      name: String(request.name ?? "").trim().toLowerCase(),
      query: String(request.query ?? request.queryText ?? request.query_text ?? "").trim(),
      filters: normalizeRegistryFiltersForKey(request.filters),
    };
  }

  return {
    packetType,
    conversationId: request.conversationId ?? request.conversation_id ?? null,
    query: String(request.query ?? request.queryText ?? request.query_text ?? "").trim(),
    intent: request.intent ?? null,
    scopeFilter: normalizeScopeForKey(request.scopeFilter ?? request.scope_filter ?? null),
    tokenBudget: request.tokenBudget ?? request.token_budget ?? null,
  };
}

function buildAssemblyCacheKey(packetType, request) {
  return serializeCacheValue(normalizeRequestForKey(packetType, request));
}

export function createAssemblyCache() {
  const store = new Map();
  let stats = { hits: 0, misses: 0 };

  function get(packetType, request, currentGraphVersion) {
    const key = buildAssemblyCacheKey(packetType, request);
    const entry = store.get(key);

    if (!entry) {
      stats.misses++;
      return { status: "miss", reason: "cold", payload: null };
    }

    if (entry.graphVersion !== currentGraphVersion) {
      stats.misses++;
      return { status: "miss", reason: "graph_version_changed", payload: null };
    }

    stats.hits++;
    return { status: "hit", reason: null, payload: entry.payload };
  }

  function set(packetType, request, graphVersion, payload) {
    const key = buildAssemblyCacheKey(packetType, request);
    store.set(key, { graphVersion, payload, cachedAt: Date.now() });
  }

  function invalidate() {
    store.clear();
  }

  function getStats() {
    return { ...stats, size: store.size };
  }

  function resetStats() {
    stats = { hits: 0, misses: 0 };
  }

  return { get, set, invalidate, getStats, resetStats };
}

// Exported for testing
export { buildAssemblyCacheKey, serializeCacheValue, normalizeRequestForKey };
