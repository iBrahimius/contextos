import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { DatabaseSync } from "node:sqlite";

import { SCHEMA } from "./schema.js";
import { paginateResults, paginationClause } from "../core/pagination.js";
import { validateLifecycleTransition } from "../core/claim-resolution.js";
import { isValidLifecycleState } from "../core/claim-types.js";
import { AI_PROPOSED_PARKING_THRESHOLD } from "../core/write-discipline.js";
import { createId, nowIso, parseJson, slugify, stableJson } from "../core/utils.js";

const SCOPE_ORDER = ["private", "project", "shared", "public"];
const DEFAULT_EMBEDDING_MODEL = "embeddinggemma-300m";

function normalizeQueueBucket(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  return ["actionable", "parked", "not_queued"].includes(normalized) ? normalized : null;
}

function cleanGraphProposalTextValue(value) {
  if (value === null || value === undefined) {
    return null;
  }

  const normalized = String(value).trim();
  return normalized || null;
}

function normalizeGraphProposalComparisonValue(value) {
  const cleaned = cleanGraphProposalTextValue(value);
  return cleaned ? cleaned.toLowerCase() : null;
}

function serializeEmbedding(embedding) {
  if (!embedding) {
    return null;
  }

  const vector = embedding instanceof Float32Array
    ? embedding
    : new Float32Array(Array.from(embedding));

  return Buffer.from(vector.buffer, vector.byteOffset, vector.byteLength);
}

function deserializeEmbedding(blob) {
  if (!blob) {
    return null;
  }

  const bytes = blob instanceof Uint8Array ? blob : new Uint8Array(blob);
  if (!bytes.byteLength || bytes.byteLength % Float32Array.BYTES_PER_ELEMENT !== 0) {
    return null;
  }

  return new Float32Array(
    bytes.buffer,
    bytes.byteOffset,
    bytes.byteLength / Float32Array.BYTES_PER_ELEMENT,
  ).slice();
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

function scopeRank(scopeKind, defaultScopeKind) {
  const kind = scopeKind ?? defaultScopeKind;
  const rank = SCOPE_ORDER.indexOf(kind);
  return rank >= 0 ? rank : SCOPE_ORDER.indexOf(defaultScopeKind);
}

function scopeMatches(row, scopeFilter, defaultScopeKind) {
  const filter = normalizeScopeFilter(scopeFilter);
  if (!filter?.scopeKind) {
    return true;
  }

  const rowScopeKind = row.scope_kind ?? defaultScopeKind;
  if (scopeRank(rowScopeKind, defaultScopeKind) < scopeRank(filter.scopeKind, defaultScopeKind)) {
    return false;
  }

  if (rowScopeKind === "project" && filter.scopeKind === "project" && filter.scopeId && row.scope_id !== filter.scopeId) {
    return false;
  }

  return true;
}

function isPaginationRequest(value) {
  return Boolean(value)
    && typeof value === "object"
    && !Array.isArray(value)
    && ("cursor" in value || "limit" in value);
}

function clampLimit(limit, fallback = null) {
  const numericLimit = Number(limit);
  if (!Number.isFinite(numericLimit)) {
    return fallback;
  }

  return Math.min(Math.max(1, Math.trunc(numericLimit)), 200);
}

function resolveListMode(optionsOrLimit, defaultLimit = null) {
  if (isPaginationRequest(optionsOrLimit)) {
    return {
      mode: "pagination",
      cursor: optionsOrLimit.cursor ?? null,
      limit: optionsOrLimit.limit ?? 50,
    };
  }

  if (typeof optionsOrLimit === "number") {
    return {
      mode: "legacy",
      limit: clampLimit(optionsOrLimit, defaultLimit ?? 50),
    };
  }

  if (defaultLimit !== null) {
    return {
      mode: "legacy",
      limit: defaultLimit,
    };
  }

  return {
    mode: "all",
    limit: null,
  };
}

function resolveScopeAndPagination(scopeFilterOrOptions = null, maybeOptions = null) {
  if (isPaginationRequest(scopeFilterOrOptions) && maybeOptions === null) {
    return {
      scopeFilter: null,
      options: scopeFilterOrOptions,
    };
  }

  return {
    scopeFilter: scopeFilterOrOptions,
    options: maybeOptions,
  };
}

function likeSearchTerm(query) {
  const normalized = String(query ?? "").trim().toLowerCase();
  return normalized ? `%${normalized}%` : null;
}

function normalizeImportanceValue(value) {
  if (value === null || value === undefined) {
    return 1.0;
  }

  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : 1.0;
}

function normalizeLifecycleStateWrite({ currentClaim = null, nextState, supersededByClaimId = undefined }) {
  const normalizedState = typeof nextState === "string" ? nextState.trim() : nextState;
  if (!normalizedState) {
    return currentClaim?.lifecycle_state ?? "candidate";
  }

  if (!isValidLifecycleState(normalizedState)) {
    throw new Error(`Invalid claim lifecycle state: ${normalizedState}`);
  }

  const effectiveSupersededByClaimId = supersededByClaimId === undefined
    ? (currentClaim?.superseded_by_claim_id ?? null)
    : supersededByClaimId;

  if (normalizedState === "active" && effectiveSupersededByClaimId) {
    return "superseded";
  }

  const currentState = currentClaim?.lifecycle_state ?? null;
  if (currentState && !validateLifecycleTransition(currentState, normalizedState)) {
    throw new Error(`Invalid claim lifecycle transition: ${currentState} -> ${normalizedState}`);
  }

  return normalizedState;
}

function normalizeBackfillStatus(value) {
  const normalized = String(value ?? "").trim();
  if (["claim_created", "no_claim", "failed"].includes(normalized)) {
    return normalized;
  }

  throw new Error(`Invalid claim backfill status: ${value}`);
}

function isDateOnly(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value ?? "").trim());
}

function normalizeDateStart(value) {
  if (!value) {
    return null;
  }

  const text = String(value).trim();
  if (!text) {
    return null;
  }

  if (isDateOnly(text)) {
    return `${text}T00:00:00.000Z`;
  }

  const parsed = new Date(text);
  return Number.isNaN(parsed.valueOf()) ? null : parsed.toISOString();
}

function normalizeDateEnd(value) {
  if (!value) {
    return null;
  }

  const text = String(value).trim();
  if (!text) {
    return null;
  }

  if (isDateOnly(text)) {
    return `${text}T23:59:59.999Z`;
  }

  const parsed = new Date(text);
  return Number.isNaN(parsed.valueOf()) ? null : parsed.toISOString();
}

function applyCreatedAtFilters(clauses, params, column, filters = {}) {
  const dateFrom = normalizeDateStart(filters.date_from);
  if (dateFrom) {
    clauses.push(`${column} >= ?`);
    params.push(dateFrom);
  }

  const dateTo = normalizeDateEnd(filters.date_to);
  if (dateTo) {
    clauses.push(`${column} <= ?`);
    params.push(dateTo);
  }
}

function applyMetadataTagFilters(clauses, params, column, filters = {}) {
  const tags = Array.isArray(filters.tags)
    ? filters.tags.map((tag) => String(tag ?? "").trim().toLowerCase()).filter(Boolean)
    : [];

  for (const tag of tags) {
    clauses.push(`LOWER(COALESCE(${column}, '')) LIKE ?`);
    params.push(`%${tag}%`);
  }
}

function applyTextSearch(clauses, params, query, columns) {
  const search = likeSearchTerm(query);
  if (!search) {
    return;
  }

  clauses.push(`(${columns.map((column) => `LOWER(COALESCE(${column}, '')) LIKE ?`).join(" OR ")})`);
  for (let index = 0; index < columns.length; index += 1) {
    params.push(search);
  }
}

const SCHEMA_MIGRATIONS = [
  {
    table: "messages",
    column: "ingest_id",
    definition: "TEXT",
  },
  {
    table: "messages",
    column: "origin_kind",
    definition: `TEXT NOT NULL DEFAULT 'user' CHECK (origin_kind IN ('user', 'agent', 'system', 'import'))`,
  },
  {
    table: "messages",
    column: "source_message_id",
    definition: "TEXT REFERENCES messages(id) ON DELETE SET NULL",
  },
  {
    table: "messages",
    column: "actor_id",
    definition: "TEXT NOT NULL DEFAULT 'system'",
  },
  {
    table: "messages",
    column: "scope_kind",
    definition: `TEXT NOT NULL DEFAULT 'private' CHECK (scope_kind IN ('private', 'project', 'shared', 'public'))`,
  },
  {
    table: "messages",
    column: "scope_id",
    definition: "TEXT",
  },
  {
    table: "messages",
    column: "embedding",
    definition: "BLOB",
  },
  {
    table: "entities",
    column: "scope_kind",
    definition: `TEXT NOT NULL DEFAULT 'shared' CHECK (scope_kind IN ('private', 'project', 'shared', 'public'))`,
  },
  {
    table: "entities",
    column: "scope_id",
    definition: "TEXT",
  },
  {
    table: "entities",
    column: "owner_id",
    definition: "TEXT NOT NULL DEFAULT 'system'",
  },
  {
    table: "observations",
    column: "actor_id",
    definition: "TEXT NOT NULL DEFAULT 'system'",
  },
  {
    table: "observations",
    column: "scope_kind",
    definition: `TEXT NOT NULL DEFAULT 'private' CHECK (scope_kind IN ('private', 'project', 'shared', 'public'))`,
  },
  {
    table: "observations",
    column: "scope_id",
    definition: "TEXT",
  },
  {
    table: "graph_proposals",
    column: "actor_id",
    definition: "TEXT NOT NULL DEFAULT 'system'",
  },
  {
    table: "graph_proposals",
    column: "scope_kind",
    definition: `TEXT NOT NULL DEFAULT 'private' CHECK (scope_kind IN ('private', 'project', 'shared', 'public'))`,
  },
  {
    table: "graph_proposals",
    column: "scope_id",
    definition: "TEXT",
  },
  {
    table: "claims",
    column: "importance_score",
    definition: "REAL NOT NULL DEFAULT 1.0",
  },
  {
    table: "graph_proposals",
    column: "reviewed_by_actor",
    definition: "TEXT",
  },
  {
    table: "graph_proposals",
    column: "reviewed_at",
    definition: "TEXT",
  },
  {
    table: "document_chunks",
    column: "scope_kind",
    definition: `TEXT NOT NULL DEFAULT 'shared' CHECK (scope_kind IN ('private', 'project', 'shared', 'public'))`,
  },
  {
    table: "document_chunks",
    column: "scope_id",
    definition: "TEXT",
  },
  {
    table: "observations",
    column: "compressed_into",
    definition: "TEXT DEFAULT NULL",
  },
  {
    table: "graph_proposals",
    column: "write_class",
    definition: "TEXT NOT NULL DEFAULT 'ai_proposed'",
  },
];

const SCHEMA_INDEXES = [
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_ingest_id ON messages(ingest_id) WHERE ingest_id IS NOT NULL`,
  // Hot-path query performance (issue #9)
  `CREATE INDEX IF NOT EXISTS idx_messages_captured_at ON messages(captured_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_observations_created_at ON observations(created_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_tasks_entity_created ON tasks(entity_id, created_at DESC) WHERE entity_id IS NOT NULL`,
  `CREATE INDEX IF NOT EXISTS idx_decisions_entity_created ON decisions(entity_id, created_at DESC) WHERE entity_id IS NOT NULL`,
  `CREATE INDEX IF NOT EXISTS idx_constraints_entity_created ON constraints(entity_id, created_at DESC) WHERE entity_id IS NOT NULL`,
  `CREATE INDEX IF NOT EXISTS idx_facts_entity_created ON facts(entity_id, created_at DESC) WHERE entity_id IS NOT NULL`,
  `CREATE INDEX IF NOT EXISTS idx_chunk_entities_entity ON chunk_entities(entity_id)`,
  `CREATE INDEX IF NOT EXISTS idx_claims_observation ON claims(observation_id)`,
  `CREATE INDEX IF NOT EXISTS idx_graph_proposals_created ON graph_proposals(created_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_graph_proposals_write_class ON graph_proposals(write_class, status)`,
  `CREATE INDEX IF NOT EXISTS idx_graph_proposals_confidence ON graph_proposals(confidence, status)`,
  `CREATE INDEX IF NOT EXISTS idx_graph_proposals_dedup ON graph_proposals(proposal_type, subject_label, object_label, status)`,
];

export class ContextDatabase {
  constructor(dbPath) {
    this.dbPath = dbPath;
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    this.sqlite = new DatabaseSync(dbPath);
    this.closed = false;
    this._inTransaction = false;

    // Enable WAL mode for concurrent reads, better write performance, and crash resilience.
    // PRAGMA journal_mode is persistent — only needs to be set once per database file,
    // but re-issuing is a no-op if already WAL.
    this.sqlite.exec("PRAGMA journal_mode = WAL");
    this.sqlite.exec("PRAGMA busy_timeout = 5000");

    // Snapshot the DB file's inode at open time for WAL integrity checks.
    // If the inode changes later, someone replaced/deleted the file while we had it open.
    try {
      const stat = fs.statSync(dbPath);
      this._openIno = stat.ino;
      this._openDev = stat.dev;
    } catch {
      this._openIno = null;
      this._openDev = null;
    }

    const t0 = performance.now();
    this.applyTableMigrations();
    const t1 = performance.now();
    this.sqlite.exec(SCHEMA);
    const t2 = performance.now();
    this.statements = new Map();
    this._stmtMaxSize = 500;
    this.applyIndexMigrations();
    const t3 = performance.now();
    this.backfillObservationFts();
    const t4 = performance.now();
    this.backfillClaimsFts();
    const t5 = performance.now();

    console.log(`[db] Startup complete in ${(t5 - t0).toFixed(0)}ms (migrations: ${(t1 - t0).toFixed(0)}ms, schema: ${(t2 - t1).toFixed(0)}ms, indexes: ${(t3 - t2).toFixed(0)}ms, fts-backfill: ${(t5 - t3).toFixed(0)}ms)`);
  }

  hasTable(tableName) {
    return Boolean(this.sqlite.prepare(`
      SELECT name
      FROM sqlite_master
      WHERE type = 'table' AND name = ?
      LIMIT 1
    `).get(tableName));
  }

  hasColumn(tableName, columnName) {
    if (!this.hasTable(tableName)) {
      return false;
    }

    return this.sqlite
      .prepare(`PRAGMA table_info(${tableName})`)
      .all()
      .some((column) => column.name === columnName);
  }

  ensureColumn(tableName, columnName, definition) {
    if (!this.hasTable(tableName) || this.hasColumn(tableName, columnName)) {
      return;
    }

    this.sqlite.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${definition}`);
  }

  applyTableMigrations() {
    for (const migration of SCHEMA_MIGRATIONS) {
      this.ensureColumn(migration.table, migration.column, migration.definition);
    }
  }

  applyIndexMigrations() {
    for (const indexSql of SCHEMA_INDEXES) {
      this.sqlite.exec(indexSql);
    }
  }

  backfillObservationFts() {
    if (!this.hasTable("observations") || !this.hasTable("observation_fts") || !this.hasTable("observation_fts_content")) {
      return;
    }

    const observationCount = Number(this.prepare(`SELECT COUNT(*) AS count FROM observations`).get()?.count ?? 0);
    const ftsCount = Number(this.prepare(`SELECT COUNT(*) AS count FROM observation_fts_content`).get()?.count ?? 0);
    if (ftsCount >= observationCount) {
      return;
    }

    this.prepare(`
      INSERT INTO observation_fts (observation_id, category, content)
      SELECT o.id, o.category, o.detail
      FROM observations o
      WHERE o.id NOT IN (SELECT c0 FROM observation_fts_content)
    `).run();
  }

  upsertClaimEmbedding({ claimId, embedding, model = DEFAULT_EMBEDDING_MODEL }) {
    const serialized = serializeEmbedding(embedding);
    if (!serialized) {
      return null;
    }

    const createdAt = nowIso();

    this.prepare(`
      INSERT INTO claim_embeddings (claim_id, embedding, model, created_at)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(claim_id) DO UPDATE SET
        embedding = excluded.embedding,
        model = excluded.model,
        created_at = excluded.created_at
    `).run(claimId, serialized, model, createdAt);

    return { claimId, model, createdAt };
  }

  getClaimEmbedding(claimId) {
    const row = this.prepare(`
      SELECT
        claim_id AS claimId,
        embedding,
        model,
        created_at AS createdAt
      FROM claim_embeddings
      WHERE claim_id = ?
      LIMIT 1
    `).get(claimId);

    if (!row) {
      return null;
    }

    return {
      ...row,
      embedding: deserializeEmbedding(row.embedding),
    };
  }

  listClaimsMissingEmbeddings(limit = null) {
    const resolvedLimit = clampLimit(limit);
    const params = resolvedLimit === null ? [] : [resolvedLimit];
    const limitClause = resolvedLimit === null ? "" : "\n      LIMIT ?\n    ";

    return this.prepare(`
      SELECT c.id, c.value_text AS detail, c.claim_type
      FROM claims c
      LEFT JOIN claim_embeddings ce ON ce.claim_id = c.id
      WHERE ce.claim_id IS NULL
        AND c.value_text IS NOT NULL
        AND length(c.value_text) > 10
        AND c.lifecycle_state IN ('active', 'candidate', 'disputed')
      ORDER BY c.created_at DESC${limitClause}
    `).all(...params);
  }

  listEmbeddedClaims(scopeFilter = null) {
    if (!this.hasTable("claim_embeddings")) {
      return [];
    }

    return this.prepare(`
      SELECT
        c.id,
        c.claim_type,
        c.value_text,
        c.lifecycle_state,
        c.confidence,
        c.importance_score,
        c.subject_entity_id,
        c.object_entity_id,
        c.predicate,
        c.scope_kind,
        c.scope_id,
        c.created_at,
        ce.embedding,
        ce.model
      FROM claim_embeddings ce
      JOIN claims c ON c.id = ce.claim_id
      WHERE c.lifecycle_state IN ('active', 'candidate', 'disputed')
        AND c.superseded_by_claim_id IS NULL
      ORDER BY c.created_at DESC
    `)
      .all()
      .filter((row) => scopeMatches(row, scopeFilter, "private"))
      .map((row) => ({
        ...row,
        embedding: deserializeEmbedding(row.embedding),
      }));
  }

  pruneOrphanedClaimEmbeddings() {
    if (!this.hasTable("claim_embeddings")) {
      return 0;
    }

    const result = this.prepare(`
      DELETE FROM claim_embeddings
      WHERE claim_id NOT IN (SELECT id FROM claims)
    `).run();
    return result.changes;
  }

  backfillClaimsFts() {
    if (!this.hasTable("claims") || !this.hasTable("claims_fts") || !this.hasTable("claims_fts_content")) {
      return;
    }

    const claimCount = Number(this.prepare(`
      SELECT COUNT(*) AS count
      FROM claims
      WHERE value_text IS NOT NULL
    `).get()?.count ?? 0);
    const ftsCount = Number(this.prepare(`SELECT COUNT(*) AS count FROM claims_fts_content`).get()?.count ?? 0);
    if (ftsCount >= claimCount) {
      return;
    }

    this.prepare(`
      INSERT INTO claims_fts (claim_id, claim_type, content)
      SELECT c.id, c.claim_type, c.value_text
      FROM claims c
      WHERE c.value_text IS NOT NULL
        AND c.id NOT IN (SELECT c0 FROM claims_fts_content)
    `).run();
  }

  getGraphVersion() {
    const row = this.prepare(`
      SELECT value
      FROM system_state
      WHERE key = 'graph_version'
      LIMIT 1
    `).get();

    return Number(row?.value ?? 0);
  }

  bumpGraphVersion() {
    const timestamp = nowIso();
    return this.withTransaction(() => {
      this.prepare(`
        INSERT OR IGNORE INTO system_state (key, value, updated_at)
        VALUES ('graph_version', '0', ?)
      `).run(timestamp);

      this.prepare(`
        UPDATE system_state
        SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT),
            updated_at = ?
        WHERE key = 'graph_version'
      `).run(timestamp);

      return this.getGraphVersion();
    });
  }

  ensureReviewState() {
    this.prepare(`
      INSERT OR IGNORE INTO review_state (
        id,
        last_review_at,
        mutations_since_last_review,
        review_in_progress
      )
      VALUES (1, NULL, 0, 0)
    `).run();
  }

  getReviewState() {
    this.ensureReviewState();

    const row = this.prepare(`
      SELECT
        last_review_at AS lastReviewAt,
        mutations_since_last_review AS mutationsSinceLastReview,
        review_in_progress AS reviewInProgress
      FROM review_state
      WHERE id = 1
      LIMIT 1
    `).get();

    return {
      lastReviewAt: row?.lastReviewAt ?? null,
      mutationsSinceLastReview: Number(row?.mutationsSinceLastReview ?? 0),
      reviewInProgress: Boolean(row?.reviewInProgress),
    };
  }

  updateReviewState({
    lastReviewAt = undefined,
    mutationsSinceLastReview = undefined,
    reviewInProgress = undefined,
  } = {}) {
    this.ensureReviewState();

    const assignments = [];
    const params = [];

    if (lastReviewAt !== undefined) {
      assignments.push(`last_review_at = ?`);
      params.push(lastReviewAt);
    }

    if (mutationsSinceLastReview !== undefined) {
      assignments.push(`mutations_since_last_review = ?`);
      params.push(Math.max(0, Math.trunc(Number(mutationsSinceLastReview) || 0)));
    }

    if (reviewInProgress !== undefined) {
      assignments.push(`review_in_progress = ?`);
      params.push(reviewInProgress ? 1 : 0);
    }

    if (!assignments.length) {
      return this.getReviewState();
    }

    params.push(1);
    this.prepare(`
      UPDATE review_state
      SET ${assignments.join(", ")}
      WHERE id = ?
    `).run(...params);

    return this.getReviewState();
  }

  incrementReviewMutationCount(amount = 1) {
    const normalizedAmount = Math.max(0, Math.trunc(Number(amount) || 0));
    if (!normalizedAmount) {
      return this.getReviewState();
    }

    this.ensureReviewState();
    this.prepare(`
      UPDATE review_state
      SET mutations_since_last_review = mutations_since_last_review + ?
      WHERE id = 1
    `).run(normalizedAmount);

    return this.getReviewState();
  }

  resetReviewInProgress() {
    return this.updateReviewState({ reviewInProgress: false });
  }

  tryStartReviewRun() {
    this.ensureReviewState();

    const result = this.prepare(`
      UPDATE review_state
      SET review_in_progress = 1
      WHERE id = 1
        AND review_in_progress = 0
    `).run();

    return result.changes > 0;
  }

  completeReviewRun({ lastReviewAt = nowIso(), mutationsSinceLastReview = 0 } = {}) {
    return this.updateReviewState({
      lastReviewAt,
      mutationsSinceLastReview,
      reviewInProgress: false,
    });
  }

  getPendingGraphProposalStats(statuses = ["pending", "proposed"]) {
    const normalizedStatuses = Array.isArray(statuses)
      ? statuses.map((status) => String(status ?? "").trim()).filter(Boolean)
      : [];
    const effectiveStatuses = normalizedStatuses.length ? normalizedStatuses : ["pending", "proposed"];
    const placeholders = effectiveStatuses.map(() => "?").join(", ");

    const row = this.prepare(`
      SELECT
        COUNT(*) AS total,
        MIN(created_at) AS oldestCreatedAt,
        MAX(created_at) AS newestCreatedAt
      FROM graph_proposals
      WHERE status IN (${placeholders})
    `).get(...effectiveStatuses);

    return {
      total: Number(row?.total ?? 0),
      oldestCreatedAt: row?.oldestCreatedAt ?? null,
      newestCreatedAt: row?.newestCreatedAt ?? null,
    };
  }

  prepare(sql) {
    if (this.statements.has(sql)) {
      // Move to end for LRU ordering (Map iteration order = insertion order)
      const stmt = this.statements.get(sql);
      this.statements.delete(sql);
      this.statements.set(sql, stmt);
      return stmt;
    }

    const stmt = this.sqlite.prepare(sql);
    this.statements.set(sql, stmt);

    // Evict oldest entry if cache exceeds max size
    if (this.statements.size > this._stmtMaxSize) {
      const oldest = this.statements.keys().next().value;
      this.statements.delete(oldest);
    }

    return stmt;
  }

  withTransaction(fn) {
    if (this._inTransaction) {
      return fn();
    }
    this.sqlite.exec("BEGIN IMMEDIATE");
    this._inTransaction = true;
    try {
      const result = fn();
      this.sqlite.exec("COMMIT");
      return result;
    } catch (error) {
      this.sqlite.exec("ROLLBACK");
      throw error;
    } finally {
      this._inTransaction = false;
    }
  }

  createConversation(title = "Untitled Conversation") {
    const id = createId("conv");
    const timestamp = nowIso();

    this.prepare(`
      INSERT INTO conversations (id, title, created_at, updated_at)
      VALUES (?, ?, ?, ?)
    `).run(id, title, timestamp, timestamp);

    return { id, title, createdAt: timestamp, updatedAt: timestamp };
  }

  getConversation(conversationId) {
    return this.prepare(`
      SELECT id, title, created_at AS createdAt, updated_at AS updatedAt
      FROM conversations
      WHERE id = ?
      LIMIT 1
    `).get(conversationId);
  }

  listConversations(optionsOrLimit = null) {
    const request = resolveListMode(optionsOrLimit);

    if (request.mode === "pagination") {
      const { sql, params } = paginationClause({
        cursor: request.cursor,
        limit: request.limit,
        orderBy: "updated_at",
        direction: "DESC",
      });
      const rows = this.prepare(`
        SELECT
          id,
          title,
          created_at AS createdAt,
          updated_at AS updatedAt,
          updated_at AS _cursor_key
        FROM conversations
        WHERE 1 = 1${sql}
      `).all(...params);
      return paginateResults(rows, request.limit);
    }

    const params = request.mode === "legacy" ? [request.limit] : [];
    const limitClause = request.mode === "legacy" ? "\n      LIMIT ?\n    " : "";

    return this.prepare(`
      SELECT id, title, created_at AS createdAt, updated_at AS updatedAt
      FROM conversations
      ORDER BY updated_at DESC${limitClause}
    `).all(...params);
  }

  touchConversation(conversationId) {
    this.prepare(`
      UPDATE conversations
      SET updated_at = ?
      WHERE id = ?
    `).run(nowIso(), conversationId);
  }

  insertMessage({
    conversationId,
    role,
    direction,
    content,
    tokenCount,
    raw,
    ingestId = null,
    actorId = "system",
    originKind = "user",
    sourceMessageId = null,
    scopeKind = "private",
    scopeId = null,
  }) {
    return this.withTransaction(() => {
      const id = createId("msg");
      const capturedAt = nowIso();
      const resolvedIngestId = ingestId ?? createId("ing");

      const result = this.prepare(`
        INSERT OR IGNORE INTO messages (
          id, conversation_id, role, direction, actor_id, origin_kind, source_message_id, scope_kind,
          scope_id, content, token_count, captured_at, raw_json, ingest_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        id,
        conversationId,
        role,
        direction,
        actorId,
        originKind,
        sourceMessageId,
        scopeKind,
        scopeId,
        content,
        tokenCount,
        capturedAt,
        stableJson(raw),
        resolvedIngestId,
      );

      if (!result.changes) {
        return {
          ...this.getMessageByIngestId(resolvedIngestId),
          deduped: true,
        };
      }

      this.touchConversation(conversationId);
      return {
        id,
        conversationId,
        role,
        direction,
        actorId,
        originKind,
        sourceMessageId,
        scopeKind,
        scopeId,
        content,
        tokenCount,
        capturedAt,
        ingestId: resolvedIngestId,
        deduped: false,
      };
    });
  }

  getMessageByIngestId(ingestId) {
    return this.prepare(`
      SELECT
        id,
        conversation_id AS conversationId,
        role,
        direction,
        actor_id AS actorId,
        origin_kind AS originKind,
        source_message_id AS sourceMessageId,
        scope_kind AS scopeKind,
        scope_id AS scopeId,
        content,
        token_count AS tokenCount,
        captured_at AS capturedAt,
        ingest_id AS ingestId
      FROM messages
      WHERE ingest_id = ?
      LIMIT 1
    `).get(ingestId);
  }

  listMessages(conversationId, optionsOrLimit = null) {
    const request = resolveListMode(optionsOrLimit);

    if (request.mode === "pagination") {
      const { sql, params } = paginationClause({
        cursor: request.cursor,
        limit: request.limit,
        orderBy: "captured_at",
        direction: "ASC",
      });
      const rows = this.prepare(`
        SELECT
          id,
          conversation_id AS conversationId,
          role,
          direction,
          actor_id AS actorId,
          origin_kind AS originKind,
          source_message_id AS sourceMessageId,
          scope_kind AS scopeKind,
          scope_id AS scopeId,
          content,
          token_count AS tokenCount,
          captured_at AS capturedAt,
          ingest_id AS ingestId,
          captured_at AS _cursor_key
        FROM messages
        WHERE conversation_id = ?${sql}
      `).all(conversationId, ...params);
      return paginateResults(rows, request.limit);
    }

    const params = request.mode === "legacy" ? [conversationId, request.limit] : [conversationId];
    const limitClause = request.mode === "legacy" ? "\n      LIMIT ?\n    " : "";

    return this.prepare(`
      SELECT
        id,
        conversation_id AS conversationId,
        role,
        direction,
        actor_id AS actorId,
        origin_kind AS originKind,
        source_message_id AS sourceMessageId,
        scope_kind AS scopeKind,
        scope_id AS scopeId,
        content,
        token_count AS tokenCount,
        captured_at AS capturedAt,
        ingest_id AS ingestId
      FROM messages
      WHERE conversation_id = ?
      ORDER BY captured_at ASC${limitClause}
    `).all(...params);
  }

  getMessage(messageId) {
    return this.prepare(`
      SELECT
        id,
        conversation_id AS conversationId,
        role,
        direction,
        actor_id AS actorId,
        origin_kind AS originKind,
        source_message_id AS sourceMessageId,
        scope_kind AS scopeKind,
        scope_id AS scopeId,
        content,
        token_count AS tokenCount,
        captured_at AS capturedAt,
        ingest_id AS ingestId
      FROM messages
      WHERE id = ?
      LIMIT 1
    `).get(messageId);
  }

  getMessageContextWindowByIngestId(ingestId, { before = 6, after = 6 } = {}) {
    const center = this.getMessageByIngestId(ingestId);
    if (!center) {
      return null;
    }

    const beforeRows = this.prepare(`
      SELECT
        id,
        conversation_id AS conversationId,
        role,
        direction,
        actor_id AS actorId,
        origin_kind AS originKind,
        source_message_id AS sourceMessageId,
        scope_kind AS scopeKind,
        scope_id AS scopeId,
        content,
        token_count AS tokenCount,
        captured_at AS capturedAt,
        ingest_id AS ingestId
      FROM messages
      WHERE conversation_id = ?
        AND (captured_at < ? OR (captured_at = ? AND id < ?))
      ORDER BY captured_at DESC, id DESC
      LIMIT ?
    `).all(
      center.conversationId,
      center.capturedAt,
      center.capturedAt,
      center.id,
      Math.max(0, Math.trunc(before)),
    ).reverse();

    const afterRows = this.prepare(`
      SELECT
        id,
        conversation_id AS conversationId,
        role,
        direction,
        actor_id AS actorId,
        origin_kind AS originKind,
        source_message_id AS sourceMessageId,
        scope_kind AS scopeKind,
        scope_id AS scopeId,
        content,
        token_count AS tokenCount,
        captured_at AS capturedAt,
        ingest_id AS ingestId
      FROM messages
      WHERE conversation_id = ?
        AND (captured_at > ? OR (captured_at = ? AND id > ?))
      ORDER BY captured_at ASC, id ASC
      LIMIT ?
    `).all(
      center.conversationId,
      center.capturedAt,
      center.capturedAt,
      center.id,
      Math.max(0, Math.trunc(after)),
    );

    return {
      center,
      before: beforeRows,
      after: afterRows,
    };
  }

  upsertMessageEmbedding({ messageId, embedding, model = DEFAULT_EMBEDDING_MODEL }) {
    const serialized = serializeEmbedding(embedding);
    if (!serialized) {
      return null;
    }

    const createdAt = nowIso();

    this.prepare(`
      INSERT INTO message_embeddings (message_id, embedding, model, created_at)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(message_id) DO UPDATE SET
        embedding = excluded.embedding,
        model = excluded.model,
        created_at = excluded.created_at
    `).run(messageId, serialized, model, createdAt);

    this.prepare(`
      UPDATE messages
      SET embedding = ?
      WHERE id = ?
    `).run(serialized, messageId);

    return {
      messageId,
      model,
      createdAt,
    };
  }

  upsertObservationEmbedding({ observationId, embedding, model = DEFAULT_EMBEDDING_MODEL }) {
    const serialized = serializeEmbedding(embedding);
    if (!serialized) {
      return null;
    }

    const createdAt = nowIso();

    this.prepare(`
      INSERT INTO observation_embeddings (observation_id, embedding, model, created_at)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(observation_id) DO UPDATE SET
        embedding = excluded.embedding,
        model = excluded.model,
        created_at = excluded.created_at
    `).run(observationId, serialized, model, createdAt);

    return {
      observationId,
      model,
      createdAt,
    };
  }

  getMessageEmbedding(messageId) {
    const row = this.prepare(`
      SELECT
        message_id AS messageId,
        embedding,
        model,
        created_at AS createdAt
      FROM message_embeddings
      WHERE message_id = ?
      LIMIT 1
    `).get(messageId);

    if (!row) {
      return null;
    }

    return {
      ...row,
      embedding: deserializeEmbedding(row.embedding),
    };
  }

  getObservationEmbedding(observationId) {
    const row = this.prepare(`
      SELECT
        observation_id AS observationId,
        embedding,
        model,
        created_at AS createdAt
      FROM observation_embeddings
      WHERE observation_id = ?
      LIMIT 1
    `).get(observationId);

    if (!row) {
      return null;
    }

    return {
      ...row,
      embedding: deserializeEmbedding(row.embedding),
    };
  }

  /**
   * Delete observation embeddings whose observation no longer exists.
   * Returns the number of orphaned rows removed.
   */
  pruneOrphanedObservationEmbeddings() {
    const result = this.prepare(`
      DELETE FROM observation_embeddings
      WHERE observation_id NOT IN (SELECT id FROM observations)
    `).run();
    return result.changes;
  }

  listMessagesMissingEmbeddings(limit = null) {
    const resolvedLimit = clampLimit(limit);
    const params = resolvedLimit === null ? [] : [resolvedLimit];
    const limitClause = resolvedLimit === null ? "" : "\n      LIMIT ?\n    ";

    return this.prepare(`
      SELECT
        m.id,
        m.conversation_id AS conversationId,
        m.role,
        m.direction,
        m.actor_id AS actorId,
        m.origin_kind AS originKind,
        m.source_message_id AS sourceMessageId,
        m.scope_kind AS scopeKind,
        m.scope_id AS scopeId,
        m.content,
        m.token_count AS tokenCount,
        m.captured_at AS capturedAt,
        m.ingest_id AS ingestId
      FROM messages m
      LEFT JOIN message_embeddings me ON me.message_id = m.id
      WHERE me.message_id IS NULL
      ORDER BY m.captured_at ASC, m.id ASC${limitClause}
    `).all(...params);
  }

  listObservationsMissingEmbeddings(limit = null) {
    const resolvedLimit = clampLimit(limit);
    const params = resolvedLimit === null ? [] : [resolvedLimit];
    const limitClause = resolvedLimit === null ? "" : "\n      LIMIT ?\n    ";

    return this.prepare(`
      SELECT
        o.id,
        o.conversation_id AS conversationId,
        o.message_id AS message_id,
        o.category,
        o.predicate,
        o.subject_entity_id AS subject_entity_id,
        o.object_entity_id AS object_entity_id,
        o.scope_kind AS scope_kind,
        o.scope_id AS scope_id,
        o.detail,
        o.confidence,
        o.source_span AS source_span,
        o.created_at AS created_at,
        subject.label AS subjectLabel,
        object.label AS objectLabel,
        m.role AS message_role,
        m.content AS message_content,
        m.captured_at AS message_captured_at,
        m.ingest_id AS message_ingest_id,
        m.origin_kind AS origin_kind
      FROM observations o
      JOIN messages m ON m.id = o.message_id
      LEFT JOIN observation_embeddings oe ON oe.observation_id = o.id
      LEFT JOIN entities subject ON subject.id = o.subject_entity_id
      LEFT JOIN entities object ON object.id = o.object_entity_id
      WHERE oe.observation_id IS NULL
      ORDER BY o.created_at ASC, o.id ASC${limitClause}
    `).all(...params);
  }

  listEmbeddedMessages(scopeFilter = null) {
    return this.prepare(`
      SELECT
        m.id,
        m.conversation_id AS conversationId,
        m.role,
        m.direction,
        m.actor_id AS actorId,
        m.origin_kind AS originKind,
        m.source_message_id AS sourceMessageId,
        m.scope_kind AS scope_kind,
        m.scope_id AS scope_id,
        m.content,
        m.token_count AS tokenCount,
        m.captured_at AS capturedAt,
        m.ingest_id AS ingestId,
        me.embedding,
        me.model,
        me.created_at AS embeddingCreatedAt
      FROM message_embeddings me
      JOIN messages m ON m.id = me.message_id
      ORDER BY m.captured_at DESC, m.id DESC
    `)
      .all()
      .filter((row) => scopeMatches(row, scopeFilter, "private"))
      .map((row) => ({
        id: row.id,
        conversationId: row.conversationId,
        role: row.role,
        direction: row.direction,
        actorId: row.actorId,
        originKind: row.originKind,
        sourceMessageId: row.sourceMessageId,
        scopeKind: row.scope_kind,
        scopeId: row.scope_id,
        content: row.content,
        tokenCount: row.tokenCount,
        capturedAt: row.capturedAt,
        ingestId: row.ingestId,
        model: row.model,
        embeddingCreatedAt: row.embeddingCreatedAt,
        embedding: deserializeEmbedding(row.embedding),
      }))
      .filter((row) => row.embedding);
  }

  listEmbeddedObservations(scopeFilter = null) {
    return this.prepare(`
      SELECT
        o.id,
        o.conversation_id AS conversationId,
        o.message_id AS message_id,
        o.category,
        o.predicate,
        o.subject_entity_id AS subject_entity_id,
        o.object_entity_id AS object_entity_id,
        o.scope_kind AS scope_kind,
        o.scope_id AS scope_id,
        o.detail,
        o.confidence,
        o.source_span AS source_span,
        o.created_at AS created_at,
        subject.label AS subjectLabel,
        object.label AS objectLabel,
        m.role AS message_role,
        m.content AS message_content,
        m.captured_at AS message_captured_at,
        m.ingest_id AS message_ingest_id,
        m.origin_kind AS origin_kind,
        oe.embedding,
        oe.model,
        oe.created_at AS embeddingCreatedAt
      FROM observation_embeddings oe
      JOIN observations o ON o.id = oe.observation_id
      JOIN messages m ON m.id = o.message_id
      LEFT JOIN entities subject ON subject.id = o.subject_entity_id
      LEFT JOIN entities object ON object.id = o.object_entity_id
      WHERE o.compressed_into IS NULL
      ORDER BY o.created_at DESC, o.id DESC
    `)
      .all()
      .filter((row) => scopeMatches(row, scopeFilter, "private"))
      .map((row) => ({
        id: row.id,
        conversationId: row.conversationId,
        message_id: row.message_id,
        category: row.category,
        predicate: row.predicate,
        subject_entity_id: row.subject_entity_id,
        object_entity_id: row.object_entity_id,
        scope_kind: row.scope_kind,
        scope_id: row.scope_id,
        detail: row.detail,
        confidence: Number(row.confidence ?? 0),
        source_span: row.source_span,
        created_at: row.created_at,
        subjectLabel: row.subjectLabel,
        objectLabel: row.objectLabel,
        message_role: row.message_role,
        message_content: row.message_content,
        message_captured_at: row.message_captured_at,
        message_ingest_id: row.message_ingest_id,
        origin_kind: row.origin_kind,
        model: row.model,
        embeddingCreatedAt: row.embeddingCreatedAt,
        embedding: deserializeEmbedding(row.embedding),
      }))
      .filter((row) => row.embedding);
  }

  /**
   * Check WAL file integrity: verify the DB file, WAL, and SHM are still
   * linked on disk and the inode hasn't changed since we opened the connection.
   *
   * Returns { ok: true } when healthy, or { ok: false, warnings: [...] }
   * when files are missing/replaced — indicating potential data loss risk.
   */
  checkWalIntegrity() {
    const warnings = [];
    const dbExists = fs.existsSync(this.dbPath);
    const walExists = fs.existsSync(this.dbPath + "-wal");
    const shmExists = fs.existsSync(this.dbPath + "-shm");

    if (!dbExists) warnings.push("DB file missing from disk — process may be using deleted inode");
    if (!walExists) warnings.push("WAL file missing — writes may not be durable");
    if (!shmExists) warnings.push("SHM file missing — shared memory index unavailable");

    // Check if the DB file's inode changed (replaced by another file)
    if (dbExists && this._openIno != null) {
      try {
        const stat = fs.statSync(this.dbPath);
        if (stat.ino !== this._openIno || stat.dev !== this._openDev) {
          warnings.push(
            `DB file inode changed (opened: ${this._openIno}, current: ${stat.ino}) — file was replaced while process was running`
          );
        }
      } catch (e) {
        warnings.push(`Cannot stat DB file: ${e.message}`);
      }
    }

    return warnings.length === 0
      ? { ok: true }
      : { ok: false, warnings };
  }

  getEmbeddingCoverage() {
    const row = this.prepare(`
      SELECT
        (SELECT COUNT(*) FROM messages) AS total,
        (SELECT COUNT(*)
         FROM message_embeddings me
         JOIN messages m ON m.id = me.message_id) AS embedded
    `).get();

    const total = Number(row?.total ?? 0);
    const embedded = Number(row?.embedded ?? 0);

    const claimRow = this.hasTable("claim_embeddings")
      ? this.prepare(`
          SELECT
            (SELECT COUNT(*) FROM claims WHERE value_text IS NOT NULL AND length(value_text) > 10
              AND lifecycle_state IN ('active', 'candidate', 'disputed')) AS claimsTotal,
            (SELECT COUNT(*) FROM claim_embeddings) AS claimsEmbedded
        `).get()
      : { claimsTotal: 0, claimsEmbedded: 0 };

    const claimsTotal = Number(claimRow?.claimsTotal ?? 0);
    const claimsEmbedded = Number(claimRow?.claimsEmbedded ?? 0);

    return {
      embedded,
      total,
      coverage: total > 0 ? Number(((embedded / total) * 100).toFixed(2)) : 0,
      claimsEmbedded,
      claimsTotal,
      claimsCoverage: claimsTotal > 0 ? Number(((claimsEmbedded / claimsTotal) * 100).toFixed(2)) : 0,
    };
  }

  listOpenTasks() {
    return this.prepare(`
      SELECT
        t.id,
        t.observation_id AS observationId,
        t.entity_id AS entityId,
        t.title,
        t.status,
        t.priority,
        t.created_at AS createdAt,
        e.label AS entityLabel,
        o.message_id AS messageId,
        m.ingest_id AS eventId
      FROM tasks t
      JOIN observations o ON o.id = t.observation_id
      JOIN messages m ON m.id = o.message_id
      LEFT JOIN entities e ON e.id = t.entity_id
      WHERE t.status NOT IN ('closed', 'done')
      ORDER BY t.created_at DESC
    `).all();
  }

  listOpenDecisions() {
    return this.prepare(`
      SELECT
        d.id,
        d.observation_id AS observationId,
        d.entity_id AS entityId,
        d.title,
        d.rationale,
        d.created_at AS createdAt,
        e.label AS entityLabel,
        o.message_id AS messageId,
        m.ingest_id AS eventId
      FROM decisions d
      JOIN observations o ON o.id = d.observation_id
      JOIN messages m ON m.id = o.message_id
      LEFT JOIN entities e ON e.id = d.entity_id
      ORDER BY d.created_at DESC
    `).all();
  }

  listOpenConstraints() {
    return this.prepare(`
      SELECT
        c.id,
        c.observation_id AS observationId,
        c.entity_id AS entityId,
        c.detail,
        c.severity,
        c.created_at AS createdAt,
        e.label AS entityLabel,
        o.message_id AS messageId,
        m.ingest_id AS eventId
      FROM constraints c
      JOIN observations o ON o.id = c.observation_id
      JOIN messages m ON m.id = o.message_id
      LEFT JOIN entities e ON e.id = c.entity_id
      ORDER BY c.created_at DESC
    `).all();
  }

  queryRegistry(name, { query = "", filters = {} } = {}) {
    const registry = String(name ?? "").trim().toLowerCase();
    const clauses = [];
    const params = [];

    if (registry === "tasks") {
      const requestedStatus = String(filters.status ?? "").trim().toLowerCase();
      if (requestedStatus === "active") {
        clauses.push(`t.status NOT IN ('closed', 'done')`);
      } else if (requestedStatus) {
        clauses.push(`LOWER(t.status) = ?`);
        params.push(requestedStatus);
      }

      applyTextSearch(clauses, params, query, ["t.title", "e.label", "o.detail"]);
      applyMetadataTagFilters(clauses, params, "o.metadata_json", filters);
      applyCreatedAtFilters(clauses, params, "t.created_at", filters);

      const sql = `
        SELECT
          t.id,
          'task' AS type,
          t.title,
          t.status,
          t.priority,
          t.created_at AS createdAt,
          t.entity_id AS entityId,
          e.label AS entityLabel,
          o.id AS observationId,
          o.detail,
          o.metadata_json AS metadataJson,
          o.message_id AS messageId,
          m.ingest_id AS eventId
        FROM tasks t
        JOIN observations o ON o.id = t.observation_id
        JOIN messages m ON m.id = o.message_id
        LEFT JOIN entities e ON e.id = t.entity_id
        ${clauses.length ? `WHERE ${clauses.join(" AND ")}` : ""}
        ORDER BY t.created_at DESC
        LIMIT 200
      `;

      return this.prepare(sql).all(...params);
    }

    if (registry === "decisions") {
      const requestedStatus = String(filters.status ?? "").trim().toLowerCase();
      if (requestedStatus && requestedStatus !== "active") {
        return [];
      }

      applyTextSearch(clauses, params, query, ["d.title", "d.rationale", "e.label", "o.detail"]);
      applyMetadataTagFilters(clauses, params, "o.metadata_json", filters);
      applyCreatedAtFilters(clauses, params, "d.created_at", filters);

      const sql = `
        SELECT
          d.id,
          'decision' AS type,
          d.title,
          'active' AS status,
          d.rationale,
          d.created_at AS createdAt,
          d.entity_id AS entityId,
          e.label AS entityLabel,
          o.id AS observationId,
          o.detail,
          o.metadata_json AS metadataJson,
          o.message_id AS messageId,
          m.ingest_id AS eventId
        FROM decisions d
        JOIN observations o ON o.id = d.observation_id
        JOIN messages m ON m.id = o.message_id
        LEFT JOIN entities e ON e.id = d.entity_id
        ${clauses.length ? `WHERE ${clauses.join(" AND ")}` : ""}
        ORDER BY d.created_at DESC
        LIMIT 200
      `;

      return this.prepare(sql).all(...params);
    }

    if (registry === "constraints") {
      const requestedStatus = String(filters.status ?? "").trim().toLowerCase();
      if (requestedStatus && requestedStatus !== "active") {
        return [];
      }

      applyTextSearch(clauses, params, query, ["c.detail", "e.label", "o.detail"]);
      applyMetadataTagFilters(clauses, params, "o.metadata_json", filters);
      applyCreatedAtFilters(clauses, params, "c.created_at", filters);

      const sql = `
        SELECT
          c.id,
          'constraint' AS type,
          c.detail AS title,
          c.detail,
          'active' AS status,
          c.severity,
          c.created_at AS createdAt,
          c.entity_id AS entityId,
          e.label AS entityLabel,
          o.id AS observationId,
          o.metadata_json AS metadataJson,
          o.message_id AS messageId,
          m.ingest_id AS eventId
        FROM constraints c
        JOIN observations o ON o.id = c.observation_id
        JOIN messages m ON m.id = o.message_id
        LEFT JOIN entities e ON e.id = c.entity_id
        ${clauses.length ? `WHERE ${clauses.join(" AND ")}` : ""}
        ORDER BY c.created_at DESC
        LIMIT 200
      `;

      return this.prepare(sql).all(...params);
    }

    if (registry === "entities" || registry === "projects") {
      if (registry === "projects") {
        clauses.push(`e.kind IN ('project', 'component', 'system')`);
      }

      applyTextSearch(clauses, params, query, ["e.label", "e.summary", "e.kind"]);
      applyMetadataTagFilters(clauses, params, "e.metadata_json", filters);
      applyCreatedAtFilters(clauses, params, "e.updated_at", filters);

      const sql = `
        SELECT
          e.id,
          '${registry === "projects" ? "project" : "entity"}' AS type,
          e.slug,
          e.label,
          e.kind,
          e.summary,
          e.complexity_score AS complexityScore,
          e.mention_count AS mentionCount,
          e.updated_at AS updatedAt,
          e.created_at AS createdAt
        FROM entities e
        ${clauses.length ? `WHERE ${clauses.join(" AND ")}` : ""}
        ORDER BY e.mention_count DESC, e.complexity_score DESC, e.label ASC
        LIMIT 200
      `;

      return this.prepare(sql).all(...params);
    }

    if (registry === "breakthroughs" || registry === "profile") {
      applyTextSearch(clauses, params, query, ["f.detail", "e.label", "o.detail"]);
      applyMetadataTagFilters(clauses, params, "o.metadata_json", filters);
      applyCreatedAtFilters(clauses, params, "f.created_at", filters);

      const sql = `
        SELECT
          f.id,
          '${registry === "breakthroughs" ? "breakthrough" : "profile"}' AS type,
          f.detail AS title,
          f.detail,
          f.created_at AS createdAt,
          f.entity_id AS entityId,
          e.label AS entityLabel,
          o.id AS observationId,
          o.metadata_json AS metadataJson,
          o.message_id AS messageId,
          m.ingest_id AS eventId
        FROM facts f
        JOIN observations o ON o.id = f.observation_id
        JOIN messages m ON m.id = o.message_id
        LEFT JOIN entities e ON e.id = f.entity_id
        ${clauses.length ? `WHERE ${clauses.join(" AND ")}` : ""}
        ORDER BY f.created_at DESC
        LIMIT 200
      `;

      return this.prepare(sql).all(...params);
    }

    return [];
  }

  findEntityBySlugOrAlias(label) {
    const slug = slugify(label);

    return this.prepare(`
      SELECT e.*
      FROM entities e
      LEFT JOIN entity_aliases a ON a.entity_id = e.id
      WHERE e.slug = ? OR a.alias_slug = ?
      LIMIT 1
    `).get(slug, slug);
  }

  findEntityByName(name) {
    const slug = slugify(name);
    const label = String(name ?? "").trim().toLowerCase();

    return this.prepare(`
      SELECT e.*
      FROM entities e
      LEFT JOIN entity_aliases a ON a.entity_id = e.id
      WHERE e.id = ?
         OR e.slug = ?
         OR LOWER(e.label) = ?
         OR a.alias_slug = ?
         OR LOWER(a.alias) = ?
      ORDER BY CASE
        WHEN e.id = ? THEN 0
        WHEN e.slug = ? THEN 1
        WHEN LOWER(e.label) = ? THEN 2
        ELSE 3
      END
      LIMIT 1
    `).get(name, slug, label, slug, label, name, slug, label);
  }

  insertEntity({
    label,
    kind = "concept",
    summary = null,
    aliases = [],
    metadata = null,
    scopeKind = "shared",
    scopeId = null,
    ownerId = "system",
  }) {
    return this.withTransaction(() => {
      const id = createId("ent");
      const timestamp = nowIso();
      const slug = slugify(label);

      this.prepare(`
        INSERT INTO entities (
          id, slug, label, kind, summary, complexity_score, mention_count, miss_count,
          scope_kind, scope_id, owner_id, metadata_json, created_at, updated_at, last_seen_at
        )
        VALUES (?, ?, ?, ?, ?, 1, 1, 0, ?, ?, ?, ?, ?, ?, ?)
      `).run(id, slug, label, kind, summary, scopeKind, scopeId, ownerId, stableJson(metadata), timestamp, timestamp, timestamp);

      for (const alias of aliases) {
        this.insertEntityAlias(id, alias);
      }

      const graphVersion = this.bumpGraphVersion();
      return {
        ...this.getEntity(id),
        graphVersion,
      };
    });
  }

  updateEntity(
    id,
    {
      label,
      kind,
      summary,
      aliases = [],
      metadata = null,
      scopeKind,
      scopeId,
      ownerId,
      mentionIncrement = 0,
      missIncrement = 0,
      complexityDelta = 0,
    },
  ) {
    return this.withTransaction(() => {
      const current = this.getEntity(id);
      const timestamp = nowIso();
      const nextLabel = label ?? current.label;
      const nextKind = kind ?? current.kind;
      const nextSummary = summary ?? current.summary;
      const nextScopeKind = scopeKind ?? current.scope_kind ?? "shared";
      const nextScopeId = scopeId === undefined ? current.scope_id ?? null : scopeId;
      const nextOwnerId = ownerId ?? current.owner_id ?? "system";
      const nextMetadata = metadata ?? parseJson(current.metadata_json, null);
      const mentionCount = current.mention_count + mentionIncrement;
      const missCount = current.miss_count + missIncrement;
      const complexityScore = Math.max(1, Number(current.complexity_score) + complexityDelta);

      this.prepare(`
        UPDATE entities
        SET label = ?, kind = ?, summary = ?, complexity_score = ?, mention_count = ?, miss_count = ?,
            scope_kind = ?, scope_id = ?, owner_id = ?, metadata_json = ?, updated_at = ?, last_seen_at = ?
        WHERE id = ?
      `).run(
        nextLabel,
        nextKind,
        nextSummary,
        complexityScore,
        mentionCount,
        missCount,
        nextScopeKind,
        nextScopeId,
        nextOwnerId,
        stableJson(nextMetadata),
        timestamp,
        timestamp,
        id,
      );

      if (nextLabel !== current.label) {
        this.prepare(`UPDATE entities SET slug = ? WHERE id = ?`).run(slugify(nextLabel), id);
      }

      for (const alias of aliases) {
        this.insertEntityAlias(id, alias);
      }

      const graphVersion = this.bumpGraphVersion();
      return {
        ...this.getEntity(id),
        graphVersion,
      };
    });
  }

  insertEntityAlias(entityId, alias) {
    if (!alias) {
      return;
    }

    this.prepare(`
      INSERT OR IGNORE INTO entity_aliases (entity_id, alias, alias_slug)
      VALUES (?, ?, ?)
    `).run(entityId, alias, slugify(alias));
  }

  getEntity(id) {
    return this.prepare(`SELECT * FROM entities WHERE id = ?`).get(id);
  }

  listEntities(optionsOrLimit = null) {
    const request = resolveListMode(optionsOrLimit);

    if (request.mode === "pagination") {
      const { sql, params } = paginationClause({
        cursor: request.cursor,
        limit: request.limit,
        orderBy: "complexity_score",
        direction: "DESC",
      });
      const rows = this.prepare(`
        SELECT
          *,
          complexity_score AS _cursor_key
        FROM entities
        WHERE 1 = 1${sql}
      `).all(...params);
      return paginateResults(rows, request.limit);
    }

    const params = request.mode === "legacy" ? [request.limit] : [];
    const limitClause = request.mode === "legacy" ? "\n      LIMIT ?\n    " : "";

    return this.prepare(`
      SELECT *
      FROM entities
      ORDER BY complexity_score DESC, mention_count DESC, label ASC${limitClause}
    `).all(...params);
  }

  insertRelationship({ subjectEntityId, predicate, objectEntityId, weight = 1, provenanceMessageId = null, metadata = null }) {
    const id = createId("rel");
    const createdAt = nowIso();

    const existing = this.prepare(`
      SELECT id, weight
      FROM relationships
      WHERE subject_entity_id = ? AND predicate = ? AND object_entity_id = ?
      LIMIT 1
    `).get(subjectEntityId, predicate, objectEntityId);

    if (existing) {
      this.prepare(`
        UPDATE relationships
        SET weight = ?, metadata_json = ?
        WHERE id = ?
      `).run(Math.max(existing.weight, weight), stableJson(metadata), existing.id);
      const graphVersion = this.bumpGraphVersion();
      return {
        ...this.getRelationship(existing.id),
        graphVersion,
      };
    }

    this.prepare(`
      INSERT INTO relationships (id, subject_entity_id, predicate, object_entity_id, weight, provenance_message_id, metadata_json, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(id, subjectEntityId, predicate, objectEntityId, weight, provenanceMessageId, stableJson(metadata), createdAt);

    const graphVersion = this.bumpGraphVersion();
    return {
      ...this.getRelationship(id),
      graphVersion,
    };
  }

  getRelationship(id) {
    return this.prepare(`SELECT * FROM relationships WHERE id = ?`).get(id);
  }

  listRelationships() {
    return this.prepare(`SELECT * FROM relationships ORDER BY created_at ASC`).all();
  }

  listRelationshipsForEntity(entityId) {
    return this.prepare(`
      SELECT
        r.id,
        r.subject_entity_id AS subjectEntityId,
        source.slug AS subjectSlug,
        source.label AS subjectLabel,
        r.predicate,
        r.object_entity_id AS objectEntityId,
        target.slug AS objectSlug,
        target.label AS objectLabel,
        r.weight,
        r.provenance_message_id AS provenanceMessageId,
        r.created_at AS createdAt
      FROM relationships r
      JOIN entities source ON source.id = r.subject_entity_id
      JOIN entities target ON target.id = r.object_entity_id
      WHERE r.subject_entity_id = ? OR r.object_entity_id = ?
      ORDER BY r.weight DESC, r.created_at DESC
    `).all(entityId, entityId);
  }

  listRecentMessagesForEntity(entityId, limit = 10) {
    return this.prepare(`
      SELECT DISTINCT
        m.id,
        m.conversation_id AS conversationId,
        m.role,
        m.direction,
        m.actor_id AS actorId,
        m.origin_kind AS originKind,
        m.source_message_id AS sourceMessageId,
        m.scope_kind AS scopeKind,
        m.scope_id AS scopeId,
        m.content,
        m.token_count AS tokenCount,
        m.captured_at AS capturedAt,
        m.ingest_id AS ingestId
      FROM messages m
      JOIN observations o ON o.message_id = m.id
      WHERE o.subject_entity_id = ? OR o.object_entity_id = ?
      ORDER BY m.captured_at DESC, m.id DESC
      LIMIT ?
    `).all(entityId, entityId, Math.max(1, Math.trunc(limit)));
  }

  insertObservation({
    conversationId,
    messageId,
    actorId = "system",
    category,
    predicate = null,
    subjectEntityId = null,
    objectEntityId = null,
    detail,
    confidence = 0.5,
    sourceSpan = null,
    metadata = null,
    scopeKind = "private",
    scopeId = null,
  }) {
    const id = createId("obs");
    const createdAt = nowIso();

    this.prepare(`
      INSERT INTO observations (
        id, conversation_id, message_id, actor_id, category, predicate, subject_entity_id,
        object_entity_id, detail, confidence, source_span, metadata_json, scope_kind, scope_id, created_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      id,
      conversationId,
      messageId,
      actorId,
      category,
      predicate,
      subjectEntityId,
      objectEntityId,
      detail,
      confidence,
      sourceSpan,
      stableJson(metadata),
      scopeKind,
      scopeId,
      createdAt,
    );

    this.prepare(`
      INSERT INTO observation_fts (observation_id, category, content)
      VALUES (?, ?, ?)
    `).run(id, category, detail);

    const graphVersion = this.bumpGraphVersion();
    return { id, createdAt, scopeKind, scopeId, graphVersion };
  }

  getObservation(observationId) {
    return this.prepare(`
      SELECT
        o.id,
        o.conversation_id AS conversationId,
        o.message_id AS messageId,
        o.actor_id AS actorId,
        o.category,
        o.predicate,
        o.subject_entity_id AS subject_entity_id,
        o.object_entity_id AS object_entity_id,
        o.detail,
        o.confidence,
        o.source_span AS sourceSpan,
        o.metadata_json AS metadataJson,
        o.scope_kind AS scope_kind,
        o.scope_id AS scope_id,
        o.created_at AS created_at,
        o.compressed_into AS compressedInto,
        subject.label AS subjectLabel,
        object.label AS objectLabel,
        m.role AS message_role,
        m.content AS message_content,
        m.captured_at AS message_captured_at,
        m.ingest_id AS message_ingest_id,
        m.origin_kind AS origin_kind
      FROM observations o
      JOIN messages m ON m.id = o.message_id
      LEFT JOIN entities subject ON subject.id = o.subject_entity_id
      LEFT JOIN entities object ON object.id = o.object_entity_id
      WHERE o.id = ?
      LIMIT 1
    `).get(observationId) ?? null;
  }

  insertTask({ observationId, entityId, title, status = "open", priority = "medium" }) {
    const id = createId("task");
    this.prepare(`
      INSERT INTO tasks (id, observation_id, entity_id, title, status, priority, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(id, observationId, entityId, title, status, priority, nowIso());
    return id;
  }

  insertDecision({ observationId, entityId, title, rationale = null }) {
    const id = createId("decision");
    this.prepare(`
      INSERT INTO decisions (id, observation_id, entity_id, title, rationale, created_at)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(id, observationId, entityId, title, rationale, nowIso());
    return id;
  }

  insertConstraint({ observationId, entityId, detail, severity = "high" }) {
    const id = createId("constraint");
    this.prepare(`
      INSERT INTO constraints (id, observation_id, entity_id, detail, severity, created_at)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(id, observationId, entityId, detail, severity, nowIso());
    return id;
  }

  insertFact({ observationId, entityId, detail }) {
    const id = createId("fact");
    this.prepare(`
      INSERT INTO facts (id, observation_id, entity_id, detail, created_at)
      VALUES (?, ?, ?, ?, ?)
    `).run(id, observationId, entityId, detail, nowIso());
    return id;
  }

  insertClaim(claim) {
    const id = claim.id ?? createId("claim");
    const timestamp = nowIso();
    const metadataValue = claim.metadata_json ?? claim.metadataJson ?? claim.metadata ?? null;
    const metadataJson = typeof metadataValue === "string" ? metadataValue : stableJson(metadataValue);
    const createdAt = claim.created_at ?? claim.createdAt ?? timestamp;
    const updatedAt = claim.updated_at ?? claim.updatedAt ?? createdAt;
    const supersededByClaimId = claim.superseded_by_claim_id ?? claim.supersededByClaimId ?? null;
    const lifecycleState = normalizeLifecycleStateWrite({
      currentClaim: null,
      nextState: claim.lifecycle_state ?? claim.lifecycleState ?? "candidate",
      supersededByClaimId,
    });
    const validTo = claim.valid_to ?? claim.validTo ?? (lifecycleState === "superseded" ? updatedAt : null);

    this.prepare(`
      INSERT INTO claims (
        id, observation_id, conversation_id, message_id, actor_id, claim_type,
        subject_entity_id, predicate, object_entity_id, value_text, confidence,
        source_type, lifecycle_state, valid_from, valid_to, resolution_key, facet_key,
        supersedes_claim_id, superseded_by_claim_id, scope_kind, scope_id,
        metadata_json, created_at, updated_at, importance_score
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      id,
      claim.observation_id ?? claim.observationId ?? null,
      claim.conversation_id ?? claim.conversationId ?? null,
      claim.message_id ?? claim.messageId ?? null,
      claim.actor_id ?? claim.actorId ?? null,
      claim.claim_type ?? claim.claimType,
      claim.subject_entity_id ?? claim.subjectEntityId ?? null,
      claim.predicate ?? null,
      claim.object_entity_id ?? claim.objectEntityId ?? null,
      claim.value_text ?? claim.valueText ?? null,
      claim.confidence ?? 0.5,
      claim.source_type ?? claim.sourceType ?? "implicit",
      lifecycleState,
      claim.valid_from ?? claim.validFrom ?? timestamp,
      validTo,
      claim.resolution_key ?? claim.resolutionKey ?? null,
      claim.facet_key ?? claim.facetKey ?? null,
      claim.supersedes_claim_id ?? claim.supersedesClaimId ?? null,
      supersededByClaimId,
      claim.scope_kind ?? claim.scopeKind ?? "private",
      claim.scope_id ?? claim.scopeId ?? null,
      metadataJson,
      createdAt,
      updatedAt,
      normalizeImportanceValue(claim.importance_score ?? claim.importanceScore),
    );

    const valueText = claim.value_text ?? claim.valueText ?? null;
    if (valueText && this.hasTable("claims_fts")) {
      this.prepare(`
        INSERT OR IGNORE INTO claims_fts (claim_id, claim_type, content)
        VALUES (?, ?, ?)
      `).run(id, claim.claim_type ?? claim.claimType, valueText);
    }

    return this.getClaim(id);
  }

  getClaim(id) {
    return this.prepare(`
      SELECT *
      FROM claims
      WHERE id = ?
      LIMIT 1
    `).get(id) ?? null;
  }

  getClaimsByIds(ids) {
    if (!Array.isArray(ids) || !ids.length) {
      return [];
    }

    const placeholders = ids.map(() => "?").join(", ");
    const rows = this.prepare(`
      SELECT *
      FROM claims
      WHERE id IN (${placeholders})
    `).all(...ids);
    const positions = new Map(ids.map((claimId, index) => [claimId, index]));

    return rows.sort((left, right) => (positions.get(left.id) ?? 0) - (positions.get(right.id) ?? 0));
  }

  getClaimByObservationId(observationId) {
    return this.prepare(`
      SELECT *
      FROM claims
      WHERE observation_id = ?
      LIMIT 1
    `).get(observationId) ?? null;
  }

  getObservationImportanceScore(observationId) {
    const normalizedObservationId = String(observationId ?? "").trim();
    if (!normalizedObservationId) {
      return 1.0;
    }

    const directClaim = this.getClaimByObservationId(normalizedObservationId);
    if (directClaim) {
      return normalizeImportanceValue(directClaim.importance_score);
    }

    const observation = this.prepare(`
      SELECT
        subject_entity_id AS subjectEntityId,
        object_entity_id AS objectEntityId
      FROM observations
      WHERE id = ?
      LIMIT 1
    `).get(normalizedObservationId);

    const entityIds = [...new Set([
      observation?.subjectEntityId ?? null,
      observation?.objectEntityId ?? null,
    ].map((value) => String(value ?? "").trim()).filter(Boolean))];

    if (!entityIds.length) {
      return 1.0;
    }

    const placeholders = entityIds.map(() => "?").join(", ");
    const row = this.prepare(`
      SELECT importance_score
      FROM claims
      WHERE lifecycle_state = 'active'
        AND (
          subject_entity_id IN (${placeholders})
          OR object_entity_id IN (${placeholders})
        )
      ORDER BY importance_score DESC, updated_at DESC
      LIMIT 1
    `).get(...entityIds, ...entityIds);

    return normalizeImportanceValue(row?.importance_score);
  }

  listClaimsByObservationIds(observationIds, scopeFilter = null) {
    const normalizedObservationIds = Array.isArray(observationIds)
      ? observationIds.map((value) => String(value ?? "").trim()).filter(Boolean)
      : [];
    if (!normalizedObservationIds.length) {
      return [];
    }

    const placeholders = normalizedObservationIds.map(() => "?").join(", ");

    return this.prepare(`
      SELECT *
      FROM claims
      WHERE observation_id IN (${placeholders})
      ORDER BY created_at DESC
    `)
      .all(...normalizedObservationIds)
      .filter((row) => scopeMatches(row, scopeFilter, "private"));
  }

  listCurrentClaims({ types = null, entityIds = null, scopeFilter = null, limit = 100 } = {}) {
    const normalizedTypes = Array.isArray(types)
      ? types.map((value) => String(value ?? "").trim()).filter(Boolean)
      : [];
    const normalizedEntityIds = Array.isArray(entityIds)
      ? entityIds.map((value) => String(value ?? "").trim()).filter(Boolean)
      : [];
    const clauses = [
      "lifecycle_state IN (?, ?)",
      "superseded_by_claim_id IS NULL",
      "valid_from <= ?",
      "(valid_to IS NULL OR valid_to > ?)",
    ];
    const params = ["active", "disputed"];
    const timestamp = nowIso();

    params.push(timestamp, timestamp);

    if (normalizedTypes.length) {
      clauses.push(`claim_type IN (${normalizedTypes.map(() => "?").join(", ")})`);
      params.push(...normalizedTypes);
    }

    if (normalizedEntityIds.length) {
      const entityPlaceholders = normalizedEntityIds.map(() => "?").join(", ");
      clauses.push(`(subject_entity_id IN (${entityPlaceholders}) OR object_entity_id IN (${entityPlaceholders}))`);
      params.push(...normalizedEntityIds, ...normalizedEntityIds);
    }

    params.push(clampLimit(limit, 100));

    return this.prepare(`
      SELECT *
      FROM claims
      WHERE ${clauses.join(" AND ")}
      ORDER BY created_at DESC
      LIMIT ?
    `)
      .all(...params)
      .filter((row) => scopeMatches(row, scopeFilter, "private"));
  }

  listRecentClaims({
    scopeFilter = null,
    entityIds = null,
    claimTypes = null,
    lifecycleStates = null,
    limit = 100,
  } = {}) {
    const normalizedEntityIds = Array.isArray(entityIds)
      ? entityIds.map((value) => String(value ?? "").trim()).filter(Boolean)
      : [];
    const normalizedClaimTypes = Array.isArray(claimTypes)
      ? claimTypes.map((value) => String(value ?? "").trim()).filter(Boolean)
      : [];
    const normalizedLifecycleStates = Array.isArray(lifecycleStates)
      ? lifecycleStates.map((value) => String(value ?? "").trim()).filter(Boolean)
      : [];
    const clauses = [];
    const params = [];

    if (normalizedClaimTypes.length) {
      clauses.push(`claim_type IN (${normalizedClaimTypes.map(() => "?").join(", ")})`);
      params.push(...normalizedClaimTypes);
    }

    if (normalizedLifecycleStates.length) {
      clauses.push(`lifecycle_state IN (${normalizedLifecycleStates.map(() => "?").join(", ")})`);
      params.push(...normalizedLifecycleStates);
    }

    if (normalizedEntityIds.length) {
      const placeholders = normalizedEntityIds.map(() => "?").join(", ");
      clauses.push(`(subject_entity_id IN (${placeholders}) OR object_entity_id IN (${placeholders}))`);
      params.push(...normalizedEntityIds, ...normalizedEntityIds);
    }

    params.push(clampLimit(limit, 100));
    const whereClause = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";

    return this.prepare(`
      SELECT *
      FROM claims
      ${whereClause}
      ORDER BY created_at DESC
      LIMIT ?
    `)
      .all(...params)
      .filter((row) => scopeMatches(row, scopeFilter, "private"));
  }

  listDisputedClaims({ limit = 100 } = {}) {
    return this.prepare(`
      SELECT *
      FROM claims
      WHERE lifecycle_state = 'disputed'
      ORDER BY created_at DESC
      LIMIT ?
    `).all(clampLimit(limit, 100));
  }

  listClaimsByResolutionKey(resolutionKey) {
    if (resolutionKey === null || resolutionKey === undefined) {
      return [];
    }

    return this.prepare(`
      SELECT *
      FROM claims
      WHERE resolution_key = ?
      ORDER BY created_at DESC, id DESC
    `).all(resolutionKey);
  }

  listClaims({
    types = null,
    state = null,
    entityId = null,
    resolutionKey = null,
    scopeFilter = null,
    limit = 100,
    offset = 0,
  } = {}) {
    const normalizedTypes = Array.isArray(types)
      ? types.map((value) => String(value ?? "").trim()).filter(Boolean)
      : [];
    const normalizedState = typeof state === "string" ? state.trim() : "";
    const normalizedEntityId = typeof entityId === "string" ? entityId.trim() : "";
    const normalizedResolutionKey = typeof resolutionKey === "string" ? resolutionKey.trim() : "";
    const clauses = [];
    const params = [];

    if (normalizedTypes.length) {
      clauses.push(`claim_type IN (${normalizedTypes.map(() => "?").join(", ")})`);
      params.push(...normalizedTypes);
    }

    if (normalizedState) {
      clauses.push("lifecycle_state = ?");
      params.push(normalizedState);
    }

    if (normalizedEntityId) {
      clauses.push("(subject_entity_id = ? OR object_entity_id = ?)");
      params.push(normalizedEntityId, normalizedEntityId);
    }

    if (normalizedResolutionKey) {
      clauses.push("resolution_key = ?");
      params.push(normalizedResolutionKey);
    }

    const whereClause = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
    const normalizedLimit = clampLimit(limit, 100);
    const numericOffset = Number(offset);
    const normalizedOffset = Number.isFinite(numericOffset) ? Math.max(0, Math.trunc(numericOffset)) : 0;
    const claims = this.prepare(`
      SELECT *
      FROM claims
      ${whereClause}
      ORDER BY created_at DESC
      LIMIT ?
      OFFSET ?
    `)
      .all(...params, normalizedLimit, normalizedOffset)
      .filter((row) => scopeMatches(row, scopeFilter, "private"));
    const countRow = this.prepare(`
      SELECT COUNT(*) AS count
      FROM claims
      ${whereClause}
    `).get(...params);

    return {
      claims,
      count: Number(countRow?.count ?? 0),
    };
  }

  updateClaim(id, fields) {
    return this.withTransaction(() => {
      const existing = this.getClaim(id);
      if (!existing) {
        return null;
      }

      const allowedColumns = new Map([
        ["value_text", "value_text"],
        ["valueText", "value_text"],
        ["lifecycle_state", "lifecycle_state"],
        ["lifecycleState", "lifecycle_state"],
        ["valid_to", "valid_to"],
        ["validTo", "valid_to"],
        ["supersedes_claim_id", "supersedes_claim_id"],
        ["supersedesClaimId", "supersedes_claim_id"],
        ["superseded_by_claim_id", "superseded_by_claim_id"],
        ["supersededByClaimId", "superseded_by_claim_id"],
        ["metadata_json", "metadata_json"],
        ["metadataJson", "metadata_json"],
        ["importance_score", "importance_score"],
        ["importanceScore", "importance_score"],
      ]);
      const pendingValues = new Map();

      for (const [key, value] of Object.entries(fields ?? {})) {
        const column = allowedColumns.get(key);
        if (!column) {
          throw new Error(`Cannot update claim field: ${key}`);
        }

        const resolvedValue = column === "metadata_json" && typeof value !== "string"
          ? stableJson(value)
          : column === "importance_score"
            ? normalizeImportanceValue(value)
            : value;
        pendingValues.set(column, resolvedValue);
      }

      if (pendingValues.has("lifecycle_state") || pendingValues.has("superseded_by_claim_id")) {
        const lifecycleState = normalizeLifecycleStateWrite({
          currentClaim: existing,
          nextState: pendingValues.has("lifecycle_state")
            ? pendingValues.get("lifecycle_state")
            : existing.lifecycle_state,
          supersededByClaimId: pendingValues.has("superseded_by_claim_id")
            ? pendingValues.get("superseded_by_claim_id")
            : existing.superseded_by_claim_id,
        });
        pendingValues.set("lifecycle_state", lifecycleState);

        if (lifecycleState === "superseded") {
          const existingValidTo = pendingValues.has("valid_to") ? pendingValues.get("valid_to") : existing.valid_to;
          pendingValues.set("valid_to", existingValidTo ?? nowIso());
        }

        if (lifecycleState === "active" && pendingValues.has("valid_to")) {
          pendingValues.set("valid_to", null);
        }
      }

      const updates = [];
      const params = [];
      for (const [column, value] of pendingValues.entries()) {
        updates.push(`${column} = ?`);
        params.push(value);
      }

      updates.push("updated_at = ?");
      params.push(nowIso(), id);

      this.prepare(`
        UPDATE claims
        SET ${updates.join(", ")}
        WHERE id = ?
      `).run(...params);

      return this.getClaim(id);
    });
  }

  getClaimStateStats() {
    const rows = this.prepare(`
      SELECT
        claim_type,
        lifecycle_state,
        COUNT(*) AS count
      FROM claims
      GROUP BY claim_type, lifecycle_state
    `).all();

    return rows.reduce((totals, row) => {
      if (!totals[row.claim_type]) {
        totals[row.claim_type] = {};
      }

      totals[row.claim_type][row.lifecycle_state] = Number(row.count ?? 0);
      return totals;
    }, {});
  }

  getClaimCoverageRatio() {
    const row = this.prepare(`
      SELECT
        (SELECT COUNT(*) FROM observations) AS total_observations,
        (SELECT COUNT(DISTINCT observation_id)
         FROM claims
         WHERE observation_id IS NOT NULL) AS observations_with_claims
    `).get();

    const totalObservations = Number(row?.total_observations ?? 0);
    const observationsWithClaims = Number(row?.observations_with_claims ?? 0);

    return {
      observations_with_claims: observationsWithClaims,
      total_observations: totalObservations,
      ratio: totalObservations > 0
        ? Number((observationsWithClaims / totalObservations).toFixed(4))
        : 0,
    };
  }

  getClaimBackfillCoverage() {
    const row = this.prepare(`
      SELECT
        (SELECT COUNT(*) FROM observations) AS total_observations,
        (SELECT COUNT(DISTINCT observation_id)
         FROM claims
         WHERE observation_id IS NOT NULL) AS observations_with_claims,
        (SELECT COUNT(*)
         FROM claim_backfill_status
         WHERE status = 'no_claim') AS observations_with_no_claim,
        (SELECT COUNT(*)
         FROM claim_backfill_status
         WHERE status = 'failed') AS failed_observations
    `).get();

    const totalObservations = Number(row?.total_observations ?? 0);
    const withClaims = Number(row?.observations_with_claims ?? 0);
    const noClaim = Number(row?.observations_with_no_claim ?? 0);
    const failed = Number(row?.failed_observations ?? 0);
    const processed = withClaims + noClaim + failed;
    const notYetProcessed = Math.max(0, totalObservations - processed);

    return {
      total_observations: totalObservations,
      not_yet_processed: notYetProcessed,
      processed_with_claims: withClaims,
      processed_with_no_claim: noClaim,
      failed,
      processed,
      remaining: notYetProcessed,
      completion_ratio: totalObservations > 0
        ? Number((processed / totalObservations).toFixed(4))
        : 0,
    };
  }

  listObservationsForClaimBackfill(limit = null) {
    const numericLimit = Number(limit);
    const normalizedLimit = Number.isFinite(numericLimit) && numericLimit > 0
      ? Math.trunc(numericLimit)
      : null;
    const params = normalizedLimit === null ? [] : [normalizedLimit];
    const limitClause = normalizedLimit === null ? "" : "\n      LIMIT ?";

    return this.prepare(`
      SELECT
        o.id,
        o.conversation_id AS conversation_id,
        o.message_id AS message_id,
        o.actor_id AS actor_id,
        o.category,
        o.predicate,
        o.subject_entity_id AS subject_entity_id,
        o.object_entity_id AS object_entity_id,
        o.detail,
        o.confidence,
        o.scope_kind AS scope_kind,
        o.scope_id AS scope_id,
        o.metadata_json AS metadata_json,
        o.created_at AS created_at,
        m.origin_kind AS origin_kind,
        c.id AS claim_id,
        c.lifecycle_state AS claim_lifecycle_state,
        bs.status AS backfill_status
      FROM observations o
      JOIN messages m ON m.id = o.message_id
      LEFT JOIN claims c ON c.observation_id = o.id
      LEFT JOIN claim_backfill_status bs ON bs.observation_id = o.id
      WHERE c.id IS NULL
        AND (bs.status IS NULL OR bs.status = 'failed')
      ORDER BY o.created_at ASC, o.id ASC${limitClause}
    `).all(...params);
  }

  getClaimBackfillStatus(observationId) {
    return this.prepare(`
      SELECT *
      FROM claim_backfill_status
      WHERE observation_id = ?
      LIMIT 1
    `).get(observationId) ?? null;
  }

  upsertClaimBackfillStatus({ observationId, status, claimId = null, errorMessage = null }) {
    const normalizedStatus = normalizeBackfillStatus(status);
    const timestamp = nowIso();

    this.prepare(`
      INSERT INTO claim_backfill_status (
        observation_id,
        status,
        claim_id,
        error_message,
        attempts,
        first_attempted_at,
        last_attempted_at,
        processed_at,
        updated_at
      )
      VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?)
      ON CONFLICT(observation_id) DO UPDATE SET
        status = excluded.status,
        claim_id = excluded.claim_id,
        error_message = excluded.error_message,
        attempts = claim_backfill_status.attempts + 1,
        first_attempted_at = COALESCE(claim_backfill_status.first_attempted_at, excluded.first_attempted_at),
        last_attempted_at = excluded.last_attempted_at,
        processed_at = excluded.processed_at,
        updated_at = excluded.updated_at
    `).run(
      observationId,
      normalizedStatus,
      claimId,
      errorMessage,
      timestamp,
      timestamp,
      normalizedStatus === "failed" ? null : timestamp,
      timestamp,
    );

    return this.getClaimBackfillStatus(observationId);
  }

  /**
   * Archive claims that are in the given lifecycle states and were last updated
   * before the specified date.
   *
   * @param {object} options
   * @param {string[]} options.lifecycleStates - States to archive (e.g., ['superseded', 'disputed'])
   * @param {string} options.beforeDate - ISO timestamp; claims updated before this are archived
   * @returns {number} Count of rows archived
   */
  archiveClaimsBefore({ lifecycleStates, beforeDate }) {
    const normalizedStates = Array.isArray(lifecycleStates)
      ? lifecycleStates.map((s) => String(s ?? "").trim()).filter(Boolean)
      : [];

    if (!normalizedStates.length || !beforeDate) {
      return 0;
    }

    const placeholders = normalizedStates.map(() => "?").join(", ");
    const result = this.sqlite.prepare(`
      UPDATE claims
      SET lifecycle_state = 'archived', updated_at = ?
      WHERE lifecycle_state IN (${placeholders})
        AND updated_at < ?
    `).run(nowIso(), ...normalizedStates, beforeDate);

    return result.changes;
  }

  /**
   * List observations grouped by (subject_entity_id, category) within a time window.
   * Used by observation compression.
   *
   * @param {string} cutoffDate - ISO timestamp — only observations created after this
   * @returns {object[]} Observation rows with embedding data if available
   */
  listObservationsForCompression(cutoffDate) {
    return this.prepare(`
      SELECT
        o.id,
        o.conversation_id AS conversationId,
        o.message_id AS messageId,
        o.actor_id AS actorId,
        o.category,
        o.predicate,
        o.subject_entity_id AS subject_entity_id,
        o.object_entity_id AS object_entity_id,
        o.detail,
        o.confidence,
        o.source_span AS sourceSpan,
        o.metadata_json AS metadataJson,
        o.scope_kind AS scopeKind,
        o.scope_id AS scopeId,
        o.created_at AS createdAt,
        oe.embedding
      FROM observations o
      LEFT JOIN observation_embeddings oe ON oe.observation_id = o.id
      WHERE o.compressed_into IS NULL
        AND o.created_at >= ?
      ORDER BY o.subject_entity_id ASC, o.category ASC, o.created_at ASC
    `).all(cutoffDate).map((row) => ({
      ...row,
      embedding: row.embedding ? deserializeEmbedding(row.embedding) : null,
    }));
  }

  /**
   * Mark a list of observations as compressed into a summary observation.
   *
   * @param {string[]} observationIds - IDs of original observations to mark
   * @param {string} summaryObservationId - ID of the new summary observation
   * @returns {number} Count of rows updated
   */
  markObservationsCompressed(observationIds, summaryObservationId) {
    if (!observationIds.length || !summaryObservationId) {
      return 0;
    }

    const placeholders = observationIds.map(() => "?").join(", ");
    const result = this.sqlite.prepare(`
      UPDATE observations
      SET compressed_into = ?
      WHERE id IN (${placeholders})
        AND compressed_into IS NULL
    `).run(summaryObservationId, ...observationIds);

    return result.changes;
  }

  /**
   * Re-link claims that point to compressed observations to the summary observation.
   *
   * @param {string[]} compressedObservationIds - Original observation IDs
   * @param {string} summaryObservationId - The new summary observation ID
   * @returns {number} Count of rows updated
   */
  relinkClaimsToObservation(compressedObservationIds, summaryObservationId) {
    if (!compressedObservationIds.length || !summaryObservationId) {
      return 0;
    }

    const placeholders = compressedObservationIds.map(() => "?").join(", ");
    const result = this.sqlite.prepare(`
      UPDATE claims
      SET observation_id = ?
      WHERE observation_id IN (${placeholders})
    `).run(summaryObservationId, ...compressedObservationIds);

    return result.changes;
  }

  /**
   * List claims created since a given timestamp.
   *
   * @param {string} sinceTimestamp - ISO timestamp
   * @param {number} limit - Max rows to return
   * @returns {object[]} Claim rows
   */
  listClaimsCreatedSince(sinceTimestamp, limit = 200) {
    return this.prepare(`
      SELECT *
      FROM claims
      WHERE created_at > ?
      ORDER BY created_at ASC
      LIMIT ?
    `).all(sinceTimestamp, clampLimit(limit, 200));
  }

  /**
   * List claims that were transitioned (updated but not just created) since a given timestamp.
   *
   * @param {string} sinceTimestamp - ISO timestamp
   * @param {number} limit - Max rows to return
   * @returns {object[]} Claim rows
   */
  listClaimsTransitionedSince(sinceTimestamp, limit = 200) {
    return this.prepare(`
      SELECT *
      FROM claims
      WHERE updated_at > ?
        AND created_at <= ?
      ORDER BY updated_at ASC
      LIMIT ?
    `).all(sinceTimestamp, sinceTimestamp, clampLimit(limit, 200));
  }

  /**
   * Save a session checkpoint. Keeps at most 10 checkpoints (deletes oldest on overflow).
   *
   * @param {object} options
   * @param {number} options.graphVersion - Current graph version
   * @param {string[]} options.activeTaskIds - Active task claim IDs
   * @param {string[]} options.activeDecisionIds - Active decision claim IDs
   * @param {string[]} options.activeGoalIds - Active goal claim IDs
   * @returns {object} The inserted checkpoint row
   */
  saveSessionCheckpoint({ graphVersion, activeTaskIds = [], activeDecisionIds = [], activeGoalIds = [] }) {
    return this.withTransaction(() => {
      const savedAt = nowIso();

      this.sqlite.prepare(`
        INSERT INTO session_checkpoints (graph_version, saved_at, active_task_ids, active_decision_ids, active_goal_ids)
        VALUES (?, ?, ?, ?, ?)
      `).run(
        graphVersion,
        savedAt,
        JSON.stringify(activeTaskIds),
        JSON.stringify(activeDecisionIds),
        JSON.stringify(activeGoalIds),
      );

      // Keep max 10 checkpoints: delete all but the 10 most recent
      this.sqlite.prepare(`
        DELETE FROM session_checkpoints
        WHERE id NOT IN (
          SELECT id FROM session_checkpoints
          ORDER BY id DESC
          LIMIT 10
        )
      `).run();

      return this.loadLatestCheckpoint();
    });
  }

  /**
   * Load the most recently saved session checkpoint.
   *
   * @returns {object|null} The latest checkpoint row, or null if none exists
   */
  loadLatestCheckpoint() {
    const row = this.sqlite.prepare(`
      SELECT *
      FROM session_checkpoints
      ORDER BY id DESC
      LIMIT 1
    `).get();

    if (!row) {
      return null;
    }

    return {
      id: row.id,
      graphVersion: row.graph_version,
      savedAt: row.saved_at,
      activeTaskIds: parseJson(row.active_task_ids, []),
      activeDecisionIds: parseJson(row.active_decision_ids, []),
      activeGoalIds: parseJson(row.active_goal_ids, []),
    };
  }

  listObservationsForEntities(entityIds, scopeFilterOrOptions = null, maybeOptions = null) {
    if (!entityIds.length) {
      return [];
    }

    const { scopeFilter, options } = resolveScopeAndPagination(scopeFilterOrOptions, maybeOptions);
    const request = resolveListMode(options);
    const placeholders = entityIds.map(() => "?").join(", ");

    if (request.mode === "pagination") {
      const { sql, params } = paginationClause({
        cursor: request.cursor,
        limit: request.limit,
        orderBy: "o.created_at",
        direction: "DESC",
      });
      const rows = this.prepare(`
        SELECT
          o.*,
          e.label AS subjectLabel,
          ee.label AS objectLabel,
          m.role AS message_role,
          m.content AS message_content,
          m.captured_at AS message_captured_at,
          m.ingest_id AS message_ingest_id,
          m.origin_kind AS origin_kind,
          o.created_at AS _cursor_key
        FROM observations o
        JOIN messages m ON m.id = o.message_id
        LEFT JOIN entities e ON e.id = o.subject_entity_id
        LEFT JOIN entities ee ON ee.id = o.object_entity_id
        WHERE (o.subject_entity_id IN (${placeholders}) OR o.object_entity_id IN (${placeholders}))
          AND o.compressed_into IS NULL${sql}
      `)
        .all(...entityIds, ...entityIds, ...params)
        .filter((row) => scopeMatches(row, scopeFilter, "private"));
      return paginateResults(rows, request.limit);
    }

    const params = request.mode === "legacy"
      ? [...entityIds, ...entityIds, request.limit]
      : [...entityIds, ...entityIds];
    const limitClause = request.mode === "legacy" ? "\n      LIMIT ?\n    " : "";

    return this.prepare(`
      SELECT
        o.*,
        e.label AS subjectLabel,
        ee.label AS objectLabel,
        m.role AS message_role,
        m.content AS message_content,
        m.captured_at AS message_captured_at,
        m.ingest_id AS message_ingest_id,
        m.origin_kind AS origin_kind
      FROM observations o
      JOIN messages m ON m.id = o.message_id
      LEFT JOIN entities e ON e.id = o.subject_entity_id
      LEFT JOIN entities ee ON ee.id = o.object_entity_id
      WHERE (o.subject_entity_id IN (${placeholders}) OR o.object_entity_id IN (${placeholders}))
        AND o.compressed_into IS NULL
      ORDER BY o.created_at DESC${limitClause}
    `)
      .all(...params)
      .filter((row) => scopeMatches(row, scopeFilter, "private"));
  }

  listTasksForEntities(entityIds, scopeFilterOrOptions = null, maybeOptions = null) {
    if (!entityIds.length) {
      return [];
    }

    const { scopeFilter, options } = resolveScopeAndPagination(scopeFilterOrOptions, maybeOptions);
    const request = resolveListMode(options);
    const placeholders = entityIds.map(() => "?").join(", ");

    if (request.mode === "pagination") {
      const { sql, params } = paginationClause({
        cursor: request.cursor,
        limit: request.limit,
        orderBy: "t.created_at",
        direction: "DESC",
      });
      const rows = this.prepare(`
        SELECT
          t.*,
          e.label AS entityLabel,
          o.message_id AS message_id,
          m.role AS message_role,
          m.content AS message_content,
          m.captured_at AS message_captured_at,
          m.ingest_id AS message_ingest_id,
          m.origin_kind AS origin_kind,
          o.scope_kind AS scope_kind,
          o.scope_id AS scope_id,
          t.created_at AS _cursor_key
        FROM tasks t
        JOIN observations o ON o.id = t.observation_id
        JOIN messages m ON m.id = o.message_id
        LEFT JOIN entities e ON e.id = t.entity_id
        WHERE t.entity_id IN (${placeholders})${sql}
      `)
        .all(...entityIds, ...params)
        .filter((row) => scopeMatches(row, scopeFilter, "private"));
      return paginateResults(rows, request.limit);
    }

    const params = request.mode === "legacy" ? [...entityIds, request.limit] : [...entityIds];
    const limitClause = request.mode === "legacy" ? "\n      LIMIT ?\n    " : "";

    return this.prepare(`
      SELECT
        t.*,
        e.label AS entityLabel,
        o.message_id AS message_id,
        m.role AS message_role,
        m.content AS message_content,
        m.captured_at AS message_captured_at,
        m.ingest_id AS message_ingest_id,
        m.origin_kind AS origin_kind,
        o.scope_kind AS scope_kind,
        o.scope_id AS scope_id
      FROM tasks t
      JOIN observations o ON o.id = t.observation_id
      JOIN messages m ON m.id = o.message_id
      LEFT JOIN entities e ON e.id = t.entity_id
      WHERE t.entity_id IN (${placeholders})
      ORDER BY t.created_at DESC${limitClause}
    `)
      .all(...params)
      .filter((row) => scopeMatches(row, scopeFilter, "private"));
  }

  listDecisionsForEntities(entityIds, scopeFilterOrOptions = null, maybeOptions = null) {
    if (!entityIds.length) {
      return [];
    }

    const { scopeFilter, options } = resolveScopeAndPagination(scopeFilterOrOptions, maybeOptions);
    const request = resolveListMode(options);
    const placeholders = entityIds.map(() => "?").join(", ");

    if (request.mode === "pagination") {
      const { sql, params } = paginationClause({
        cursor: request.cursor,
        limit: request.limit,
        orderBy: "d.created_at",
        direction: "DESC",
      });
      const rows = this.prepare(`
        SELECT
          d.*,
          e.label AS entityLabel,
          o.message_id AS message_id,
          m.role AS message_role,
          m.content AS message_content,
          m.captured_at AS message_captured_at,
          m.ingest_id AS message_ingest_id,
          m.origin_kind AS origin_kind,
          o.scope_kind AS scope_kind,
          o.scope_id AS scope_id,
          d.created_at AS _cursor_key
        FROM decisions d
        JOIN observations o ON o.id = d.observation_id
        JOIN messages m ON m.id = o.message_id
        LEFT JOIN entities e ON e.id = d.entity_id
        WHERE d.entity_id IN (${placeholders})${sql}
      `)
        .all(...entityIds, ...params)
        .filter((row) => scopeMatches(row, scopeFilter, "private"));
      return paginateResults(rows, request.limit);
    }

    const params = request.mode === "legacy" ? [...entityIds, request.limit] : [...entityIds];
    const limitClause = request.mode === "legacy" ? "\n      LIMIT ?\n    " : "";

    return this.prepare(`
      SELECT
        d.*,
        e.label AS entityLabel,
        o.message_id AS message_id,
        m.role AS message_role,
        m.content AS message_content,
        m.captured_at AS message_captured_at,
        m.ingest_id AS message_ingest_id,
        m.origin_kind AS origin_kind,
        o.scope_kind AS scope_kind,
        o.scope_id AS scope_id
      FROM decisions d
      JOIN observations o ON o.id = d.observation_id
      JOIN messages m ON m.id = o.message_id
      LEFT JOIN entities e ON e.id = d.entity_id
      WHERE d.entity_id IN (${placeholders})
      ORDER BY d.created_at DESC${limitClause}
    `)
      .all(...params)
      .filter((row) => scopeMatches(row, scopeFilter, "private"));
  }

  listConstraintsForEntities(entityIds, scopeFilterOrOptions = null, maybeOptions = null) {
    if (!entityIds.length) {
      return [];
    }

    const { scopeFilter, options } = resolveScopeAndPagination(scopeFilterOrOptions, maybeOptions);
    const request = resolveListMode(options);
    const placeholders = entityIds.map(() => "?").join(", ");

    if (request.mode === "pagination") {
      const { sql, params } = paginationClause({
        cursor: request.cursor,
        limit: request.limit,
        orderBy: "c.created_at",
        direction: "DESC",
      });
      const rows = this.prepare(`
        SELECT
          c.*,
          e.label AS entityLabel,
          o.message_id AS message_id,
          m.role AS message_role,
          m.content AS message_content,
          m.captured_at AS message_captured_at,
          m.ingest_id AS message_ingest_id,
          m.origin_kind AS origin_kind,
          o.scope_kind AS scope_kind,
          o.scope_id AS scope_id,
          c.created_at AS _cursor_key
        FROM constraints c
        JOIN observations o ON o.id = c.observation_id
        JOIN messages m ON m.id = o.message_id
        LEFT JOIN entities e ON e.id = c.entity_id
        WHERE c.entity_id IN (${placeholders})${sql}
      `)
        .all(...entityIds, ...params)
        .filter((row) => scopeMatches(row, scopeFilter, "private"));
      return paginateResults(rows, request.limit);
    }

    const params = request.mode === "legacy" ? [...entityIds, request.limit] : [...entityIds];
    const limitClause = request.mode === "legacy" ? "\n      LIMIT ?\n    " : "";

    return this.prepare(`
      SELECT
        c.*,
        e.label AS entityLabel,
        o.message_id AS message_id,
        m.role AS message_role,
        m.content AS message_content,
        m.captured_at AS message_captured_at,
        m.ingest_id AS message_ingest_id,
        m.origin_kind AS origin_kind,
        o.scope_kind AS scope_kind,
        o.scope_id AS scope_id
      FROM constraints c
      JOIN observations o ON o.id = c.observation_id
      JOIN messages m ON m.id = o.message_id
      LEFT JOIN entities e ON e.id = c.entity_id
      WHERE c.entity_id IN (${placeholders})
      ORDER BY c.created_at DESC${limitClause}
    `)
      .all(...params)
      .filter((row) => scopeMatches(row, scopeFilter, "private"));
  }

  listFactsForEntities(entityIds, scopeFilterOrOptions = null, maybeOptions = null) {
    if (!entityIds.length) {
      return [];
    }

    const { scopeFilter, options } = resolveScopeAndPagination(scopeFilterOrOptions, maybeOptions);
    const request = resolveListMode(options);
    const placeholders = entityIds.map(() => "?").join(", ");

    if (request.mode === "pagination") {
      const { sql, params } = paginationClause({
        cursor: request.cursor,
        limit: request.limit,
        orderBy: "f.created_at",
        direction: "DESC",
      });
      const rows = this.prepare(`
        SELECT
          f.*,
          e.label AS entityLabel,
          o.message_id AS message_id,
          m.role AS message_role,
          m.content AS message_content,
          m.captured_at AS message_captured_at,
          m.ingest_id AS message_ingest_id,
          m.origin_kind AS origin_kind,
          o.scope_kind AS scope_kind,
          o.scope_id AS scope_id,
          f.created_at AS _cursor_key
        FROM facts f
        JOIN observations o ON o.id = f.observation_id
        JOIN messages m ON m.id = o.message_id
        LEFT JOIN entities e ON e.id = f.entity_id
        WHERE f.entity_id IN (${placeholders})${sql}
      `)
        .all(...entityIds, ...params)
        .filter((row) => scopeMatches(row, scopeFilter, "private"));
      return paginateResults(rows, request.limit);
    }

    const params = request.mode === "legacy" ? [...entityIds, request.limit] : [...entityIds];
    const limitClause = request.mode === "legacy" ? "\n      LIMIT ?\n    " : "";

    return this.prepare(`
      SELECT
        f.*,
        e.label AS entityLabel,
        o.message_id AS message_id,
        m.role AS message_role,
        m.content AS message_content,
        m.captured_at AS message_captured_at,
        m.ingest_id AS message_ingest_id,
        m.origin_kind AS origin_kind,
        o.scope_kind AS scope_kind,
        o.scope_id AS scope_id
      FROM facts f
      JOIN observations o ON o.id = f.observation_id
      JOIN messages m ON m.id = o.message_id
      LEFT JOIN entities e ON e.id = f.entity_id
      WHERE f.entity_id IN (${placeholders})
      ORDER BY f.created_at DESC${limitClause}
    `)
      .all(...params)
      .filter((row) => scopeMatches(row, scopeFilter, "private"));
  }

  listClaimsForEntities(entityIds, scopeFilter = null, { states = ['active', 'candidate'], types = null, limit = 100 } = {}) {
    if (!entityIds.length) {
      return [];
    }

    const normalizedStates = Array.isArray(states)
      ? states.map((value) => String(value ?? "").trim()).filter(Boolean)
      : ['active', 'candidate'];
    const normalizedTypes = Array.isArray(types)
      ? types.map((value) => String(value ?? "").trim()).filter(Boolean)
      : [];
    const normalizedLimit = clampLimit(limit, 100);
    const placeholders = entityIds.map(() => "?").join(", ");

    const clauses = [
      `(c.subject_entity_id IN (${placeholders}) OR c.object_entity_id IN (${placeholders}))`,
      `c.lifecycle_state IN (${normalizedStates.map(() => "?").join(", ")})`,
    ];
    const params = [...entityIds, ...entityIds, ...normalizedStates];

    // For candidate claims, require confidence >= 0.7
    clauses.push(`(c.lifecycle_state = 'active' OR c.confidence >= 0.7)`);

    if (normalizedTypes.length) {
      clauses.push(`c.claim_type IN (${normalizedTypes.map(() => "?").join(", ")})`);
      params.push(...normalizedTypes);
    }

    const whereClause = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";

    const rows = this.prepare(`
      SELECT
        c.*,
        se.label AS subjectLabel,
        oe.label AS objectLabel,
        c.importance_score,
        c.confidence,
        c.lifecycle_state,
        c.created_at
      FROM claims c
      LEFT JOIN entities se ON se.id = c.subject_entity_id
      LEFT JOIN entities oe ON oe.id = c.object_entity_id
      ${whereClause}
      ORDER BY c.importance_score DESC, c.created_at DESC
      LIMIT ?
    `)
      .all(...params, normalizedLimit)
      .filter((row) => scopeMatches(row, scopeFilter, "private"));

    return rows;
  }

  upsertDocument({ filePath, checksum, metadata = null }) {
    const existing = this.prepare(`
      SELECT *
      FROM documents
      WHERE path = ?
      LIMIT 1
    `).get(filePath);

    if (existing) {
      this.prepare(`
        UPDATE documents
        SET checksum = ?, indexed_at = ?, metadata_json = ?
        WHERE id = ?
      `).run(checksum, nowIso(), stableJson(metadata), existing.id);

      this.prepare(`DELETE FROM chunk_entities WHERE chunk_id IN (SELECT id FROM document_chunks WHERE document_id = ?)`).run(existing.id);
      this.prepare(`DELETE FROM chunk_fts WHERE chunk_id IN (SELECT id FROM document_chunks WHERE document_id = ?)`).run(existing.id);
      this.prepare(`DELETE FROM document_chunks WHERE document_id = ?`).run(existing.id);

      return existing.id;
    }

    const id = createId("doc");
    this.prepare(`
      INSERT INTO documents (id, path, checksum, indexed_at, metadata_json)
      VALUES (?, ?, ?, ?, ?)
    `).run(id, filePath, checksum, nowIso(), stableJson(metadata));

    return id;
  }

  insertDocumentChunk({
    documentId,
    ordinal,
    heading = null,
    content,
    tokenCount,
    scopeKind = "shared",
    scopeId = null,
  }) {
    const id = createId("chunk");

    this.prepare(`
      INSERT INTO document_chunks (id, document_id, ordinal, heading, content, token_count, scope_kind, scope_id, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(id, documentId, ordinal, heading, content, tokenCount, scopeKind, scopeId, nowIso());

    this.prepare(`
      INSERT INTO chunk_fts (chunk_id, content)
      VALUES (?, ?)
    `).run(id, content);

    return id;
  }

  linkChunkEntity({ chunkId, entityId, score = 1 }) {
    this.prepare(`
      INSERT OR REPLACE INTO chunk_entities (chunk_id, entity_id, score)
      VALUES (?, ?, ?)
    `).run(chunkId, entityId, score);
  }

  searchChunks(queryText, scopeFilter = null) {
    if (!queryText.trim()) {
      return [];
    }

    return this.prepare(`
      SELECT
        dc.id,
        dc.document_id AS documentId,
        dc.ordinal,
        dc.heading,
        dc.content,
        dc.token_count AS tokenCount,
        dc.scope_kind AS scope_kind,
        dc.scope_id AS scope_id,
        d.path,
        bm25(chunk_fts) AS rank
      FROM chunk_fts
      JOIN document_chunks dc ON dc.id = chunk_fts.chunk_id
      JOIN documents d ON d.id = dc.document_id
      WHERE chunk_fts MATCH ?
      ORDER BY rank
      LIMIT 50
    `)
      .all(queryText)
      .filter((row) => scopeMatches(row, scopeFilter, "shared"));
  }

  searchObservations(queryText, scopeFilter = null) {
    if (!queryText.trim()) {
      return [];
    }

    return this.prepare(`
      SELECT
        o.id,
        o.category,
        o.detail,
        o.confidence,
        o.message_id AS message_id,
        o.subject_entity_id AS subject_entity_id,
        o.object_entity_id AS object_entity_id,
        o.scope_kind AS scope_kind,
        o.scope_id AS scope_id,
        subject.label AS subjectLabel,
        object.label AS objectLabel,
        m.role AS message_role,
        m.content AS message_content,
        m.captured_at AS message_captured_at,
        m.ingest_id AS message_ingest_id,
        m.origin_kind AS origin_kind,
        bm25(observation_fts) AS rank
      FROM observation_fts
      JOIN observations o ON o.id = observation_fts.observation_id
      JOIN messages m ON m.id = o.message_id
      LEFT JOIN entities subject ON subject.id = o.subject_entity_id
      LEFT JOIN entities object ON object.id = o.object_entity_id
      WHERE observation_fts MATCH ?
        AND o.compressed_into IS NULL
      ORDER BY rank
      LIMIT 50
    `)
      .all(queryText)
      .filter((row) => scopeMatches(row, scopeFilter, "private"));
  }

  searchClaimsFts(queryText, scopeFilter = null) {
    if (!queryText?.trim() || !this.hasTable("claims_fts")) {
      return [];
    }

    return this.prepare(`
      SELECT
        c.id,
        c.claim_type,
        c.value_text,
        c.lifecycle_state,
        c.confidence,
        c.importance_score,
        c.subject_entity_id,
        c.object_entity_id,
        c.predicate,
        c.scope_kind,
        c.scope_id,
        c.created_at,
        c.valid_from,
        c.valid_to,
        bm25(claims_fts) AS rank
      FROM claims_fts
      JOIN claims c ON c.id = claims_fts.claim_id
      WHERE claims_fts MATCH ?
        AND c.lifecycle_state IN ('active', 'candidate', 'disputed')
        AND c.superseded_by_claim_id IS NULL
      ORDER BY rank
      LIMIT 50
    `)
      .all(queryText)
      .filter((row) => scopeMatches(row, scopeFilter, "private"));
  }

  listChunksForEntities(entityIds, scopeFilterOrOptions = null, maybeOptions = null) {
    if (!entityIds.length) {
      return [];
    }

    const { scopeFilter, options } = resolveScopeAndPagination(scopeFilterOrOptions, maybeOptions);
    const request = resolveListMode(options);
    const placeholders = entityIds.map(() => "?").join(", ");

    if (request.mode === "pagination") {
      const { sql, params } = paginationClause({
        cursor: request.cursor,
        limit: request.limit,
        orderBy: "ce.score",
        direction: "DESC",
      });
      const rows = this.prepare(`
        SELECT
          dc.id,
          dc.document_id AS documentId,
          dc.ordinal,
          dc.heading,
          dc.content,
          dc.token_count AS tokenCount,
          dc.scope_kind AS scope_kind,
          dc.scope_id AS scope_id,
          d.path,
          ce.entity_id AS entityId,
          ce.score,
          ce.score AS _cursor_key
        FROM chunk_entities ce
        JOIN document_chunks dc ON dc.id = ce.chunk_id
        JOIN documents d ON d.id = dc.document_id
        WHERE ce.entity_id IN (${placeholders})${sql}
      `)
        .all(...entityIds, ...params)
        .filter((row) => scopeMatches(row, scopeFilter, "shared"));
      return paginateResults(rows, request.limit);
    }

    const params = request.mode === "legacy" ? [...entityIds, request.limit] : [...entityIds];
    const limitClause = request.mode === "legacy" ? "\n      LIMIT ?\n    " : "";

    return this.prepare(`
      SELECT
        dc.id,
        dc.document_id AS documentId,
        dc.ordinal,
        dc.heading,
        dc.content,
        dc.token_count AS tokenCount,
        dc.scope_kind AS scope_kind,
        dc.scope_id AS scope_id,
        d.path,
        ce.entity_id AS entityId,
        ce.score
      FROM chunk_entities ce
      JOIN document_chunks dc ON dc.id = ce.chunk_id
      JOIN documents d ON d.id = dc.document_id
      WHERE ce.entity_id IN (${placeholders})
      ORDER BY ce.score DESC, dc.token_count DESC${limitClause}
    `)
      .all(...params)
      .filter((row) => scopeMatches(row, scopeFilter, "shared"));
  }

  _normalizeGraphProposalDedupFields({ subjectLabel = null, predicate = null, objectLabel = null, detail = null }) {
    return {
      subjectLabel: normalizeGraphProposalComparisonValue(subjectLabel),
      predicate: normalizeGraphProposalComparisonValue(predicate),
      objectLabel: normalizeGraphProposalComparisonValue(objectLabel),
      detail: normalizeGraphProposalComparisonValue(detail),
    };
  }

  _findGraphProposalDuplicate({ proposalType, subjectLabel = null, predicate = null, objectLabel = null, detail = null }) {
    const normalizedFields = this._normalizeGraphProposalDedupFields({
      subjectLabel,
      predicate,
      objectLabel,
      detail,
    });

    const candidateClauses = [
      `proposal_type = ?`,
      `status IN ('pending', 'proposed')`,
    ];
    const candidateParams = [proposalType];

    for (const [column, value] of Object.entries({
      subject_label: normalizedFields.subjectLabel,
      predicate: normalizedFields.predicate,
      object_label: normalizedFields.objectLabel,
      detail: normalizedFields.detail,
    })) {
      candidateClauses.push(value === null ? `${column} IS NULL` : `${column} IS NOT NULL`);
    }

    const candidates = this.prepare(`
      SELECT
        id,
        confidence,
        scope_kind AS scopeKind,
        scope_id AS scopeId,
        subject_label AS subjectLabel,
        predicate,
        object_label AS objectLabel,
        detail
      FROM graph_proposals INDEXED BY idx_graph_proposals_dedup
      WHERE ${candidateClauses.join(" AND ")}
      ORDER BY confidence DESC, created_at ASC
    `).all(...candidateParams);

    return candidates.find((candidate) => {
      const normalizedCandidate = this._normalizeGraphProposalDedupFields(candidate);
      return normalizedCandidate.subjectLabel === normalizedFields.subjectLabel
        && normalizedCandidate.predicate === normalizedFields.predicate
        && normalizedCandidate.objectLabel === normalizedFields.objectLabel
        && normalizedCandidate.detail === normalizedFields.detail;
    }) ?? null;
  }

  insertGraphProposal({
    conversationId = null,
    messageId = null,
    sourceRunId = null,
    actorId = "system",
    scopeKind = "private",
    scopeId = null,
    proposalType,
    subjectLabel = null,
    predicate = null,
    objectLabel = null,
    detail = null,
    confidence = 0.5,
    status = "pending",
    reason = null,
    payload,
    writeClass = "ai_proposed",
  }) {
    const cleanedProposalType = cleanGraphProposalTextValue(proposalType);
    const cleanedSubjectLabel = cleanGraphProposalTextValue(subjectLabel);
    const cleanedPredicate = cleanGraphProposalTextValue(predicate);
    const cleanedObjectLabel = cleanGraphProposalTextValue(objectLabel);
    const cleanedDetail = cleanGraphProposalTextValue(detail);
    const cleanedReason = cleanGraphProposalTextValue(reason);

    return this.withTransaction(() => {
      const existingProposal = this._findGraphProposalDuplicate({
        proposalType: cleanedProposalType,
        subjectLabel: cleanedSubjectLabel,
        predicate: cleanedPredicate,
        objectLabel: cleanedObjectLabel,
        detail: cleanedDetail,
      });

      if (existingProposal) {
        const existingConfidence = Number(existingProposal.confidence ?? 0);
        const graphVersion = this.getGraphVersion();

        if (Number(confidence) > existingConfidence) {
          this.prepare(`
            UPDATE graph_proposals
            SET confidence = ?
            WHERE id = ?
          `).run(confidence, existingProposal.id);

          return {
            id: existingProposal.id,
            scopeKind: existingProposal.scopeKind,
            scopeId: existingProposal.scopeId,
            graphVersion,
            deduplicated: true,
          };
        }

        return {
          id: existingProposal.id,
          scopeKind: existingProposal.scopeKind,
          scopeId: existingProposal.scopeId,
          graphVersion,
          deduplicated: true,
          skipped: true,
        };
      }

      const id = createId("gp");
      const createdAt = nowIso();

      this.prepare(`
        INSERT INTO graph_proposals (
          id, conversation_id, message_id, source_run_id, actor_id, scope_kind, scope_id, proposal_type,
          subject_label, predicate, object_label, detail, confidence, status, reason, payload_json,
          created_at, reviewed_by_actor, reviewed_at, accepted_at, write_class
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        id,
        conversationId,
        messageId,
        sourceRunId,
        actorId,
        scopeKind,
        scopeId,
        cleanedProposalType,
        cleanedSubjectLabel,
        cleanedPredicate,
        cleanedObjectLabel,
        cleanedDetail,
        confidence,
        status,
        cleanedReason,
        stableJson(payload),
        createdAt,
        null,
        null,
        null,
        writeClass,
      );

      const graphVersion = this.bumpGraphVersion();
      return { id, scopeKind, scopeId, graphVersion, deduplicated: false };
    });
  }

  updateGraphProposalStatus(id, { status, reason = null, actorId = "system" }) {
    const reviewedAt = nowIso();

    this.prepare(`
      UPDATE graph_proposals
      SET status = ?,
          reason = ?,
          reviewed_by_actor = ?,
          reviewed_at = ?,
          accepted_at = CASE WHEN ? = 'accepted' THEN ? ELSE NULL END
      WHERE id = ?
    `).run(status, reason, actorId, reviewedAt, status, reviewedAt, id);
    const graphVersion = this.bumpGraphVersion();
    return { id, status, reason, actorId, reviewedAt, graphVersion };
  }

  getGraphProposal(id) {
    return this.prepare(`
      SELECT *
      FROM graph_proposals
      WHERE id = ?
      LIMIT 1
    `).get(id);
  }

  getGraphProposalsByIds(ids = []) {
    if (!Array.isArray(ids) || !ids.length) {
      return [];
    }

    const normalizedIds = ids
      .map((id) => String(id ?? "").trim())
      .filter(Boolean);

    if (!normalizedIds.length) {
      return [];
    }

    const placeholders = normalizedIds.map(() => "?").join(", ");
    const rows = this.prepare(`
      SELECT gp.*, m.ingest_id AS source_event_id
      FROM graph_proposals gp
      LEFT JOIN messages m ON m.id = gp.message_id
      WHERE gp.id IN (${placeholders})
    `).all(...normalizedIds);
    const rowsById = new Map(rows.map((row) => [row.id, row]));

    return normalizedIds
      .map((id) => rowsById.get(id))
      .filter(Boolean);
  }

  _buildGraphProposalFilters({
    status = null,
    statuses = null,
    writeClass = null,
    writeClasses = null,
    proposalType = null,
    proposalTypes = null,
    sourceEventId = null,
    minConfidence = null,
    maxConfidence = null,
    queueBucket = null,
    createdBefore = null,
  } = {}) {
    const normalizeValues = (value) => Array.isArray(value)
      ? value.map((entry) => String(entry ?? "").trim()).filter(Boolean)
      : [];

    const normalizedStatuses = normalizeValues(statuses);
    if (status && !normalizedStatuses.length) {
      normalizedStatuses.push(String(status).trim());
    }

    const normalizedWriteClasses = normalizeValues(writeClasses);
    if (writeClass && !normalizedWriteClasses.length) {
      normalizedWriteClasses.push(String(writeClass).trim());
    }

    const normalizedProposalTypes = normalizeValues(proposalTypes);
    if (proposalType && !normalizedProposalTypes.length) {
      normalizedProposalTypes.push(String(proposalType).trim());
    }

    const clauses = [];
    const params = [];
    let needsMessageJoin = false;

    if (normalizedStatuses.length) {
      clauses.push(`gp.status IN (${normalizedStatuses.map(() => "?").join(", ")})`);
      params.push(...normalizedStatuses);
    }

    if (normalizedWriteClasses.length) {
      clauses.push(`gp.write_class IN (${normalizedWriteClasses.map(() => "?").join(", ")})`);
      params.push(...normalizedWriteClasses);
    }

    if (normalizedProposalTypes.length) {
      clauses.push(`gp.proposal_type IN (${normalizedProposalTypes.map(() => "?").join(", ")})`);
      params.push(...normalizedProposalTypes);
    }

    const normalizedSourceEventId = String(sourceEventId ?? "").trim();
    if (normalizedSourceEventId) {
      clauses.push(`m.ingest_id = ?`);
      params.push(normalizedSourceEventId);
      needsMessageJoin = true;
    }

    const hasMinConfidence = minConfidence !== null && minConfidence !== undefined && String(minConfidence).trim() !== "";
    const normalizedMinConfidence = Number(minConfidence);
    if (hasMinConfidence && Number.isFinite(normalizedMinConfidence)) {
      clauses.push(`gp.confidence >= ?`);
      params.push(normalizedMinConfidence);
    }

    const hasMaxConfidence = maxConfidence !== null && maxConfidence !== undefined && String(maxConfidence).trim() !== "";
    const normalizedMaxConfidence = Number(maxConfidence);
    if (hasMaxConfidence && Number.isFinite(normalizedMaxConfidence)) {
      clauses.push(`gp.confidence <= ?`);
      params.push(normalizedMaxConfidence);
    }

    const normalizedQueueBucket = normalizeQueueBucket(queueBucket);
    if (normalizedQueueBucket === "actionable") {
      clauses.push(`gp.status IN ('pending', 'proposed')`);
      clauses.push(`(
        gp.write_class IN ('auto', 'canonical')
        OR (gp.write_class = 'ai_proposed' AND gp.confidence >= ?)
      )`);
      params.push(AI_PROPOSED_PARKING_THRESHOLD);
    } else if (normalizedQueueBucket === "parked") {
      clauses.push(`gp.status IN ('pending', 'proposed')`);
      clauses.push(`gp.write_class = 'ai_proposed'`);
      clauses.push(`gp.confidence < ?`);
      params.push(AI_PROPOSED_PARKING_THRESHOLD);
    } else if (normalizedQueueBucket === "not_queued") {
      clauses.push(`gp.status NOT IN ('pending', 'proposed')`);
    }

    const normalizedCreatedBefore = String(createdBefore ?? "").trim();
    if (normalizedCreatedBefore) {
      clauses.push(`gp.created_at < ?`);
      params.push(normalizedCreatedBefore);
    }

    const whereClause = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
    return { whereClause, params, needsMessageJoin };
  }

  countGraphProposals(options = {}) {
    const { whereClause, params, needsMessageJoin } = this._buildGraphProposalFilters(options);
    const joinClause = needsMessageJoin ? "\n      LEFT JOIN messages m ON m.id = gp.message_id" : "";
    return Number(this.prepare(`
      SELECT COUNT(*) AS count
      FROM graph_proposals gp${joinClause}
      ${whereClause}
    `).get(...params)?.count ?? 0);
  }

  listGraphProposals({
    sort = "newest",
    limit = 100,
    ...filterOptions
  } = {}) {
    const { whereClause, params, needsMessageJoin } = this._buildGraphProposalFilters(filterOptions);
    const sortDirection = String(sort ?? "newest").trim().toLowerCase() === "oldest" ? "ASC" : "DESC";
    const hasExplicitLimit = limit !== null && limit !== undefined && String(limit).trim() !== "";
    const resolvedLimit = hasExplicitLimit && Number.isFinite(Number(limit))
      ? Math.max(1, Math.trunc(Number(limit)))
      : null;
    const limitClause = resolvedLimit === null ? "" : "\n      LIMIT ?";

    if (resolvedLimit !== null) {
      params.push(resolvedLimit);
    }

    const selectClause = needsMessageJoin ? "gp.*, m.ingest_id AS source_event_id" : "gp.*, NULL AS source_event_id";
    const joinClause = needsMessageJoin ? "\n      LEFT JOIN messages m ON m.id = gp.message_id" : "";

    return this.prepare(`
      SELECT ${selectClause}
      FROM graph_proposals gp${joinClause}
      ${whereClause}
      ORDER BY gp.created_at ${sortDirection}, gp.id ${sortDirection}${limitClause}
    `).all(...params);
  }

  listRecentGraphProposals(optionsOrLimit = null) {
    const request = resolveListMode(optionsOrLimit, 25);

    if (request.mode === "pagination") {
      const { sql, params } = paginationClause({
        cursor: request.cursor,
        limit: request.limit,
        orderBy: "created_at",
        direction: "DESC",
      });
      const rows = this.prepare(`
        SELECT
          *,
          created_at AS _cursor_key
        FROM graph_proposals
        WHERE 1 = 1${sql}
      `).all(...params);
      return paginateResults(rows, request.limit);
    }

    return this.prepare(`
      SELECT *
      FROM graph_proposals
      ORDER BY created_at DESC
      LIMIT ?
    `).all(request.limit);
  }

  getDashboardStats() {
    const counts = {
      conversations: this.prepare(`SELECT COUNT(*) AS count FROM conversations`).get().count,
      messages: this.prepare(`SELECT COUNT(*) AS count FROM messages`).get().count,
      entities: this.prepare(`SELECT COUNT(*) AS count FROM entities`).get().count,
      relationships: this.prepare(`SELECT COUNT(*) AS count FROM relationships`).get().count,
      chunks: this.prepare(`SELECT COUNT(*) AS count FROM document_chunks`).get().count,
      graphProposals: this.prepare(`SELECT COUNT(*) AS count FROM graph_proposals WHERE status = 'pending'`).get().count,
    };

    const hotEntities = this.prepare(`
      SELECT id, label, kind, complexity_score AS complexityScore, mention_count AS mentionCount, miss_count AS missCount
      FROM entities
      ORDER BY complexity_score DESC, miss_count DESC, mention_count DESC
      LIMIT 12
    `).all();

    const projects = this.prepare(`
      SELECT
        e.id,
        e.label,
        e.kind,
        e.complexity_score AS complexityScore,
        e.mention_count AS mentionCount,
        COALESCE(open_tasks.count, 0) AS openTaskCount,
        COALESCE(decision_count.count, 0) AS decisionCount,
        COALESCE(warning_count.count, 0) AS warningCount
      FROM entities e
      LEFT JOIN (
        SELECT entity_id, COUNT(*) AS count
        FROM tasks
        WHERE status != 'done'
        GROUP BY entity_id
      ) AS open_tasks ON open_tasks.entity_id = e.id
      LEFT JOIN (
        SELECT entity_id, COUNT(*) AS count
        FROM decisions
        GROUP BY entity_id
      ) AS decision_count ON decision_count.entity_id = e.id
      LEFT JOIN (
        SELECT entity_id, SUM(CASE WHEN severity IN ('high', 'critical') THEN 1 ELSE 0 END) AS count
        FROM constraints
        GROUP BY entity_id
      ) AS warning_count ON warning_count.entity_id = e.id
      WHERE e.kind IN ('project', 'component', 'system')
      ORDER BY warningCount DESC, openTaskCount DESC, e.complexity_score DESC
      LIMIT 12
    `).all();

    return { counts, hotEntities, projects };
  }

  countObservationCategories() {
    const rows = this.prepare(`
      SELECT category, COUNT(*) AS count
      FROM observations
      GROUP BY category
    `).all();

    return rows.reduce((totals, row) => {
      totals[row.category] = Number(row.count ?? 0);
      return totals;
    }, {});
  }

  getRegistryCounts() {
    const openTasks = Number(this.prepare(`
      SELECT COUNT(*) AS count
      FROM tasks
      WHERE status NOT IN ('closed', 'done')
    `).get().count ?? 0);
    const totalTasks = Number(this.prepare(`SELECT COUNT(*) AS count FROM tasks`).get().count ?? 0);
    const totalDecisions = Number(this.prepare(`SELECT COUNT(*) AS count FROM decisions`).get().count ?? 0);
    const totalConstraints = Number(this.prepare(`SELECT COUNT(*) AS count FROM constraints`).get().count ?? 0);
    const pendingMutations = Number(this.prepare(`
      SELECT COUNT(*) AS count
      FROM graph_proposals
      WHERE status IN ('pending', 'proposed')
    `).get().count ?? 0);

    return {
      tasks: {
        active: openTasks,
        total: totalTasks,
      },
      decisions: {
        active: totalDecisions,
        total: totalDecisions,
      },
      constraints: {
        active: totalConstraints,
        total: totalConstraints,
      },
      pendingMutations,
    };
  }

  // ── Cluster LOD Level Embeddings (v2.4) ───────────────────────────────────

  upsertClusterLevelEmbedding({ clusterId, level, embedding, model = DEFAULT_EMBEDDING_MODEL }) {
    const serialized = serializeEmbedding(embedding);
    if (!serialized) {
      return null;
    }

    const createdAt = nowIso();

    this.prepare(`
      INSERT INTO cluster_level_embeddings (cluster_id, level, embedding, model, created_at)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(cluster_id, level) DO UPDATE SET
        embedding = excluded.embedding,
        model = excluded.model,
        created_at = excluded.created_at
    `).run(clusterId, level, serialized, model, createdAt);

    return {
      clusterId,
      level,
      model,
      createdAt,
    };
  }

  getClusterLevelEmbedding(clusterId, level) {
    const row = this.prepare(`
      SELECT
        cluster_id AS clusterId,
        level,
        embedding,
        model,
        created_at AS createdAt
      FROM cluster_level_embeddings
      WHERE cluster_id = ? AND level = ?
      LIMIT 1
    `).get(clusterId, level);

    if (!row) {
      return null;
    }

    return {
      ...row,
      embedding: deserializeEmbedding(row.embedding),
    };
  }

  listClusterLevelsMissingEmbeddings(limit = null) {
    const resolvedLimit = clampLimit(limit);
    const params = resolvedLimit === null ? [] : [resolvedLimit];
    const limitClause = resolvedLimit === null ? "" : "\n      LIMIT ?\n    ";

    return this.prepare(`
      SELECT
        cl.cluster_id AS clusterId,
        cl.level,
        cl.text,
        oc.topic_label AS topicLabel,
        oc.entities,
        oc.created_at AS createdAt
      FROM cluster_levels cl
      JOIN observation_clusters oc ON oc.id = cl.cluster_id
      LEFT JOIN cluster_level_embeddings cle ON cle.cluster_id = cl.cluster_id AND cle.level = cl.level
      WHERE cle.cluster_id IS NULL
      ORDER BY cl.generated_at ASC, cl.cluster_id ASC, cl.level ASC${limitClause}
    `).all(...params);
  }

  listClusterLevels(clusterId) {
    return this.prepare(`
      SELECT
        cluster_id AS clusterId,
        level,
        text,
        source_observation_ids AS sourceObservationIds,
        char_count AS charCount,
        generated_at AS generatedAt
      FROM cluster_levels
      WHERE cluster_id = ?
      ORDER BY level ASC
    `).all(clusterId);
  }

  getClusterLevel(clusterId, level) {
    const row = this.prepare(`
      SELECT
        cluster_id AS clusterId,
        level,
        text,
        source_observation_ids AS sourceObservationIds,
        char_count AS charCount,
        generated_at AS generatedAt
      FROM cluster_levels
      WHERE cluster_id = ? AND level = ?
      LIMIT 1
    `).get(clusterId, level);

    return row || null;
  }

  searchClusterLevelsFts(query, limit = 20) {
    return this.prepare(`
      SELECT
        cluster_id AS clusterId,
        level,
        text,
        rank
      FROM cluster_level_fts
      WHERE cluster_level_fts MATCH ?
      ORDER BY rank
      LIMIT ?
    `).all(query, limit);
  }

  insertClusterLevelFts(clusterId, level, text) {
    this.prepare(`
      INSERT INTO cluster_level_fts (text, cluster_id, level)
      VALUES (?, ?, ?)
    `).run(text, clusterId, level);
  }

  close() {
    this.closed = true;
    this.sqlite.close();
  }
}
