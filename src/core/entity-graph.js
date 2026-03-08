import { slugify } from "./utils.js";

function normalizeEntity(row) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    slug: row.slug,
    label: row.label,
    kind: row.kind,
    summary: row.summary,
    complexityScore: Number(row.complexity_score ?? row.complexityScore ?? 1),
    mentionCount: Number(row.mention_count ?? row.mentionCount ?? 0),
    missCount: Number(row.miss_count ?? row.missCount ?? 0),
    scopeKind: row.scope_kind ?? row.scopeKind ?? "shared",
    scopeId: row.scope_id ?? row.scopeId ?? null,
    ownerId: row.owner_id ?? row.ownerId ?? "system",
    metadata: row.metadata_json ? JSON.parse(row.metadata_json) : row.metadata ?? null,
    lastSeenAt: row.last_seen_at ?? row.lastSeenAt ?? null,
  };
}

function normalizeRelationship(row) {
  return {
    id: row.id,
    subjectEntityId: row.subject_entity_id ?? row.subjectEntityId,
    predicate: row.predicate,
    objectEntityId: row.object_entity_id ?? row.objectEntityId,
    weight: Number(row.weight ?? 1),
    provenanceMessageId: row.provenance_message_id ?? row.provenanceMessageId ?? null,
    metadata: row.metadata_json ? JSON.parse(row.metadata_json) : row.metadata ?? null,
    createdAt: row.created_at ?? row.createdAt ?? null,
  };
}

export class EntityGraph {
  constructor(database) {
    this.database = database;
    this.graphVersion = 0;
    this.entities = new Map();
    this.aliasToEntity = new Map();
    this.outgoing = new Map();
    this.incoming = new Map();
  }

  load() {
    this.entities.clear();
    this.aliasToEntity.clear();
    this.outgoing.clear();
    this.incoming.clear();

    for (const row of this.database.listEntities()) {
      const entity = normalizeEntity(row);
      this.entities.set(entity.id, entity);
      this.aliasToEntity.set(entity.slug, entity.id);
      this.aliasToEntity.set(entity.label.toLowerCase(), entity.id);
    }

    for (const relationship of this.database.listRelationships().map(normalizeRelationship)) {
      this.pushEdge(this.outgoing, relationship.subjectEntityId, relationship);
      this.pushEdge(this.incoming, relationship.objectEntityId, relationship);
    }

    this.graphVersion = this.database.getGraphVersion();
  }

  pushEdge(bucket, entityId, relationship) {
    if (!bucket.has(entityId)) {
      bucket.set(entityId, []);
    }

    const edges = bucket.get(entityId);
    const existingIndex = edges.findIndex((edge) => edge.id === relationship.id);
    if (existingIndex >= 0) {
      edges[existingIndex] = relationship;
      return;
    }

    edges.push(relationship);
  }

  listEntities() {
    return [...this.entities.values()];
  }

  updateGraphVersion(graphVersion) {
    this.graphVersion = Math.max(this.graphVersion, Number(graphVersion ?? 0));
  }

  getGraphVersion() {
    return this.graphVersion;
  }

  getEntity(id) {
    return this.entities.get(id) ?? null;
  }

  findEntityByLabel(label) {
    const key = slugify(label);
    const id = this.aliasToEntity.get(key) ?? this.aliasToEntity.get(label.toLowerCase());
    return id ? this.getEntity(id) : null;
  }

  ensureEntity({ label, kind = "concept", summary = null, aliases = [], metadata = null }) {
    const existing = this.database.findEntityBySlugOrAlias(label);

    if (existing) {
      const updatedRow = this.database.updateEntity(existing.id, {
        kind: existing.kind === "concept" ? kind : existing.kind,
        summary: existing.summary ?? summary,
        aliases,
        metadata: { ...(existing.metadata_json ? JSON.parse(existing.metadata_json) : {}), ...(metadata ?? {}) },
        mentionIncrement: 1,
      });
      const updated = normalizeEntity(updatedRow);
      this.updateGraphVersion(updatedRow.graphVersion);
      this.entities.set(updated.id, updated);
      this.aliasToEntity.set(updated.slug, updated.id);
      this.aliasToEntity.set(updated.label.toLowerCase(), updated.id);
      for (const alias of aliases) {
        this.aliasToEntity.set(slugify(alias), updated.id);
        this.aliasToEntity.set(alias.toLowerCase(), updated.id);
      }
      return updated;
    }

    const createdRow = this.database.insertEntity({ label, kind, summary, aliases, metadata });
    const created = normalizeEntity(createdRow);
    this.updateGraphVersion(createdRow.graphVersion);
    this.entities.set(created.id, created);
    this.aliasToEntity.set(created.slug, created.id);
    this.aliasToEntity.set(created.label.toLowerCase(), created.id);
    for (const alias of aliases) {
      this.aliasToEntity.set(slugify(alias), created.id);
      this.aliasToEntity.set(alias.toLowerCase(), created.id);
    }
    return created;
  }

  connect({ subjectEntityId, predicate, objectEntityId, weight = 1, provenanceMessageId = null, metadata = null }) {
    if (!subjectEntityId || !objectEntityId || subjectEntityId === objectEntityId) {
      return null;
    }

    const relationshipRow = this.database.insertRelationship({
      subjectEntityId,
      predicate,
      objectEntityId,
      weight,
      provenanceMessageId,
      metadata,
    });
    const relationship = normalizeRelationship(relationshipRow);
    this.updateGraphVersion(relationshipRow.graphVersion);

    this.pushEdge(this.outgoing, relationship.subjectEntityId, relationship);
    this.pushEdge(this.incoming, relationship.objectEntityId, relationship);
    return relationship;
  }

  neighbors(entityId) {
    const outgoing = (this.outgoing.get(entityId) ?? []).map((relationship) => ({
      direction: "outgoing",
      relationship,
      entity: this.getEntity(relationship.objectEntityId),
    }));

    const incoming = (this.incoming.get(entityId) ?? []).map((relationship) => ({
      direction: "incoming",
      relationship,
      entity: this.getEntity(relationship.subjectEntityId),
    }));

    return [...outgoing, ...incoming].filter((item) => item.entity);
  }

  matchQuery(queryText) {
    const query = queryText.toLowerCase();
    const querySlug = slugify(queryText);
    const queryTokens = querySlug.split("-").filter((token) => token.length > 2);

    return this.listEntities()
      .map((entity) => {
        let score = 0;
        if (query.includes(entity.label.toLowerCase()) || querySlug.includes(entity.slug)) {
          score += 3;
        }

        const labelTokens = entity.slug.split("-");
        const overlap = queryTokens.filter((token) => labelTokens.includes(token)).length;
        score += overlap;

        return { entity, score };
      })
      .filter((entry) => entry.score > 0)
      .sort((left, right) => right.score - left.score || right.entity.complexityScore - left.entity.complexityScore);
  }

  registerMiss(entityId, amount = 1) {
    const current = this.getEntity(entityId);
    if (!current) {
      return null;
    }

    const updatedRow = this.database.updateEntity(entityId, {
      missIncrement: amount,
      complexityDelta: amount * 0.4,
    });
    const updated = normalizeEntity(updatedRow);
    this.updateGraphVersion(updatedRow.graphVersion);

    this.entities.set(updated.id, updated);
    return updated;
  }

  adjustComplexity(entityId, delta, missIncrement = 0) {
    const current = this.getEntity(entityId);
    if (!current) {
      return null;
    }

    const updatedRow = this.database.updateEntity(entityId, {
      complexityDelta: delta,
      missIncrement,
    });
    const updated = normalizeEntity(updatedRow);
    this.updateGraphVersion(updatedRow.graphVersion);

    this.entities.set(updated.id, updated);
    return updated;
  }

  findPath(fromEntityId, toEntityId, maxDepth = 2) {
    if (fromEntityId === toEntityId) {
      return [fromEntityId];
    }

    const queue = [{ entityId: fromEntityId, depth: 0, path: [fromEntityId] }];
    const seen = new Set([fromEntityId]);

    while (queue.length) {
      const current = queue.shift();
      if (current.depth >= maxDepth) {
        continue;
      }

      for (const neighbor of this.neighbors(current.entityId)) {
        if (!neighbor.entity || seen.has(neighbor.entity.id)) {
          continue;
        }

        const path = [...current.path, neighbor.entity.id];
        if (neighbor.entity.id === toEntityId) {
          return path;
        }

        seen.add(neighbor.entity.id);
        queue.push({
          entityId: neighbor.entity.id,
          depth: current.depth + 1,
          path,
        });
      }
    }

    return null;
  }

  graphSnapshot(limit = 16) {
    const nodes = this.listEntities()
      .sort((left, right) => right.complexityScore - left.complexityScore || right.mentionCount - left.mentionCount)
      .slice(0, limit);
    const nodeIds = new Set(nodes.map((node) => node.id));
    const edges = [];

    for (const node of nodes) {
      for (const neighbor of this.neighbors(node.id)) {
        if (nodeIds.has(neighbor.entity.id)) {
          edges.push({
            source: node.id,
            target: neighbor.entity.id,
            predicate: neighbor.relationship.predicate,
            weight: neighbor.relationship.weight,
          });
        }
      }
    }

    return { nodes, edges };
  }
}
