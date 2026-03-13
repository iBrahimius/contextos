import fs from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

import { TELEMETRY_SCHEMA } from "./telemetry-schema.js";
import { paginateResults, paginationClause } from "../core/pagination.js";
import { createId, nowIso, stableJson } from "../core/utils.js";

const TELEMETRY_SCHEMA_MIGRATIONS = [
  {
    table: "proxy_events",
    column: "actor_id",
    definition: "TEXT NOT NULL DEFAULT 'system'",
  },
  {
    table: "retrieval_hints",
    column: "actor_id",
    definition: "TEXT NOT NULL DEFAULT 'system'",
  },
  {
    table: "model_runs",
    column: "actor_id",
    definition: "TEXT NOT NULL DEFAULT 'system'",
  },
  // v2.4 — Advanced Retrieval telemetry extensions
  {
    table: "retrieval_queries",
    column: "intent",
    definition: "TEXT",
  },
  {
    table: "retrieval_queries",
    column: "cache_hit",
    definition: "INTEGER DEFAULT 0",
  },
  {
    table: "retrieval_queries",
    column: "lod_levels_used",
    definition: "TEXT",
  },
  {
    table: "retrieval_queries",
    column: "signal_contributions",
    definition: "TEXT",
  },
  {
    table: "retrieval_queries",
    column: "ann_candidates",
    definition: "INTEGER",
  },
];

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

export class TelemetryDatabase {
  constructor(dbPath) {
    this.dbPath = dbPath;
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    this.sqlite = new DatabaseSync(dbPath);
    this.applyTableMigrations();
    this.sqlite.exec(TELEMETRY_SCHEMA);
    this.statements = new Map();
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
    for (const migration of TELEMETRY_SCHEMA_MIGRATIONS) {
      this.ensureColumn(migration.table, migration.column, migration.definition);
    }
  }

  prepare(sql) {
    if (!this.statements.has(sql)) {
      this.statements.set(sql, this.sqlite.prepare(sql));
    }

    return this.statements.get(sql);
  }

  logRetrieval({
    conversationId = null,
    queryText,
    latencyMs,
    seedEntityIds,
    expandedEntityIds,
    expansionPath,
    itemsReturned,
    tokensConsumed,
    missEntityIds = [],
    results,
  }) {
    const id = createId("rq");

    this.prepare(`
      INSERT INTO retrieval_queries (
        id, conversation_id, query_text, latency_ms, seed_entities_json, expanded_entities_json,
        expansion_path_json, items_returned, tokens_consumed, miss_entities_json, created_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      id,
      conversationId,
      queryText,
      latencyMs,
      stableJson(seedEntityIds),
      stableJson(expandedEntityIds),
      stableJson(expansionPath),
      itemsReturned,
      tokensConsumed,
      stableJson(missEntityIds),
      nowIso(),
    );

    results.forEach((result, index) => {
      this.prepare(`
        INSERT INTO retrieval_results (id, query_id, item_type, item_id, rank, score, entity_id, summary)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        createId("rr"),
        id,
        result.type,
        result.id,
        index + 1,
        result.score,
        result.entityId ?? null,
        result.summary ?? null,
      );
    });

    return id;
  }

  listRetrievalResults(queryId, optionsOrLimit = null) {
    const request = resolveListMode(optionsOrLimit);

    if (request.mode === "pagination") {
      const { sql, params } = paginationClause({
        cursor: request.cursor,
        limit: request.limit,
        orderBy: "rank",
        direction: "ASC",
      });
      const rows = this.prepare(`
        SELECT
          *,
          rank AS _cursor_key
        FROM retrieval_results
        WHERE query_id = ?${sql}
      `).all(queryId, ...params);
      return paginateResults(rows, request.limit);
    }

    const params = request.mode === "legacy" ? [queryId, request.limit] : [queryId];
    const limitClause = request.mode === "legacy" ? "\n      LIMIT ?\n    " : "";

    return this.prepare(`
      SELECT *
      FROM retrieval_results
      WHERE query_id = ?
      ORDER BY rank ASC${limitClause}
    `).all(...params);
  }

  listRecentRetrievals(optionsOrLimit = null) {
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
        FROM retrieval_queries
        WHERE 1 = 1${sql}
      `).all(...params);
      return paginateResults(rows, request.limit);
    }

    return this.prepare(`
      SELECT *
      FROM retrieval_queries
      ORDER BY created_at DESC
      LIMIT ?
    `).all(request.limit);
  }

  logModelRun({
    conversationId = null,
    messageId = null,
    actorId = "system",
    stage,
    provider,
    modelName,
    transport = "in_process",
    status = "ok",
    latencyMs = 0,
    inputTokens = 0,
    outputTokens = 0,
    inputPayload,
    outputPayload = null,
    errorText = null,
  }) {
    const id = createId("mr");

    this.prepare(`
      INSERT INTO model_runs (
        id, conversation_id, message_id, actor_id, stage, provider, model_name, transport,
        status, latency_ms, input_tokens, output_tokens, input_json, output_json, error_text, created_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      id,
      conversationId,
      messageId,
      actorId,
      stage,
      provider,
      modelName,
      transport,
      status,
      latencyMs,
      inputTokens,
      outputTokens,
      stableJson(inputPayload),
      stableJson(outputPayload),
      errorText,
      nowIso(),
    );

    return id;
  }

  listRecentModelRuns(optionsOrLimit = null) {
    const request = resolveListMode(optionsOrLimit, 20);

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
        FROM model_runs
        WHERE 1 = 1${sql}
      `).all(...params);
      return paginateResults(rows, request.limit);
    }

    return this.prepare(`
      SELECT *
      FROM model_runs
      ORDER BY created_at DESC
      LIMIT ?
    `).all(request.limit);
  }

  insertRetrievalHint({
    conversationId = null,
    messageId = null,
    sourceRunId = null,
    actorId = "system",
    seedEntityId = null,
    seedLabel,
    expandEntityId = null,
    expandLabel,
    reason,
    weight = 1,
    ttlTurns = 6,
    status = "active",
  }) {
    const id = createId("hint");
    this.prepare(`
      INSERT INTO retrieval_hints (
        id, conversation_id, message_id, source_run_id, actor_id, seed_entity_id, seed_label,
        expand_entity_id, expand_label, reason, weight, ttl_turns, status, created_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      id,
      conversationId,
      messageId,
      sourceRunId,
      actorId,
      seedEntityId,
      seedLabel,
      expandEntityId,
      expandLabel,
      reason,
      weight,
      ttlTurns,
      status,
      nowIso(),
    );

    this.ensureRetrievalHintStats(id);
    return { id };
  }

  ensureRetrievalHintStats(hintId) {
    this.prepare(`
      INSERT OR IGNORE INTO retrieval_hint_stats (
        hint_id, times_considered, times_applied, times_rewarded, times_unused,
        avg_reward, last_reward, last_applied_at, updated_at
      )
      VALUES (?, 0, 0, 0, 0, 0, 0, NULL, ?)
    `).run(hintId, nowIso());
  }

  getRetrievalHint(hintId) {
    return this.prepare(`
      SELECT *
      FROM retrieval_hints
      WHERE id = ?
      LIMIT 1
    `).get(hintId);
  }

  getRetrievalHintStats(hintId) {
    this.ensureRetrievalHintStats(hintId);
    return this.prepare(`
      SELECT *
      FROM retrieval_hint_stats
      WHERE hint_id = ?
      LIMIT 1
    `).get(hintId);
  }

  updateRetrievalHintPolicy({ hintId, weight, ttlTurns, status = "active" }) {
    this.prepare(`
      UPDATE retrieval_hints
      SET weight = ?, ttl_turns = ?, status = ?
      WHERE id = ?
    `).run(weight, ttlTurns, status, hintId);
  }

  logRetrievalHintEvent({
    hintId,
    queryId = null,
    conversationId = null,
    eventType,
    reward = 0,
    penalty = 0,
    detail = {},
  }) {
    const id = createId("he");
    this.prepare(`
      INSERT INTO retrieval_hint_events (
        id, hint_id, query_id, conversation_id, event_type, reward, penalty, detail_json, created_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      id,
      hintId,
      queryId,
      conversationId,
      eventType,
      reward,
      penalty,
      stableJson(detail),
      nowIso(),
    );

    return id;
  }

  recordRetrievalHintOutcome({
    hintId,
    queryId = null,
    conversationId = null,
    applied = false,
    rewarded = false,
    decayed = false,
    reward = 0,
    penalty = 0,
    nextWeight,
    nextTtlTurns,
    detail = {},
  }) {
    const stats = this.getRetrievalHintStats(hintId);
    const timestamp = nowIso();
    const consideredCount = Number(stats.times_considered ?? 0) + 1;
    const netReward = Number((reward - penalty).toFixed(3));
    const avgReward = Number((
      ((Number(stats.avg_reward ?? 0) * Number(stats.times_considered ?? 0)) + netReward) /
      Math.max(1, consideredCount)
    ).toFixed(3));

    this.prepare(`
      UPDATE retrieval_hint_stats
      SET times_considered = ?,
          times_applied = ?,
          times_rewarded = ?,
          times_unused = ?,
          avg_reward = ?,
          last_reward = ?,
          last_applied_at = ?,
          updated_at = ?
      WHERE hint_id = ?
    `).run(
      consideredCount,
      Number(stats.times_applied ?? 0) + (applied ? 1 : 0),
      Number(stats.times_rewarded ?? 0) + (rewarded ? 1 : 0),
      Number(stats.times_unused ?? 0) + (applied ? 0 : 1),
      avgReward,
      netReward,
      applied ? timestamp : stats.last_applied_at ?? null,
      timestamp,
      hintId,
    );

    this.updateRetrievalHintPolicy({
      hintId,
      weight: nextWeight,
      ttlTurns: nextTtlTurns,
      status: "active",
    });

    this.logRetrievalHintEvent({
      hintId,
      queryId,
      conversationId,
      eventType: "considered",
      reward,
      penalty,
      detail: {
        ...detail,
        nextWeight,
        nextTtlTurns,
      },
    });

    if (applied) {
      this.logRetrievalHintEvent({
        hintId,
        queryId,
        conversationId,
        eventType: "applied",
        reward,
        penalty,
        detail,
      });
    }

    if (rewarded) {
      this.logRetrievalHintEvent({
        hintId,
        queryId,
        conversationId,
        eventType: "rewarded",
        reward,
        penalty,
        detail,
      });
    }

    if (decayed) {
      this.logRetrievalHintEvent({
        hintId,
        queryId,
        conversationId,
        eventType: "decayed",
        reward,
        penalty,
        detail,
      });
    }
  }

  expireStaleRetrievalHints() {
    const expiring = this.prepare(`
      SELECT *
      FROM retrieval_hints
      WHERE status = 'active'
        AND (
          SELECT COUNT(*)
          FROM retrieval_queries rq
          WHERE rq.created_at >= retrieval_hints.created_at
            AND (
              retrieval_hints.conversation_id IS NULL
              OR rq.conversation_id = retrieval_hints.conversation_id
            )
        ) >= retrieval_hints.ttl_turns
    `).all();

    this.prepare(`
      UPDATE retrieval_hints
      SET status = 'expired'
      WHERE status = 'active'
        AND (
          SELECT COUNT(*)
          FROM retrieval_queries rq
          WHERE rq.created_at >= retrieval_hints.created_at
            AND (
              retrieval_hints.conversation_id IS NULL
              OR rq.conversation_id = retrieval_hints.conversation_id
            )
        ) >= retrieval_hints.ttl_turns
    `).run();

    for (const hint of expiring) {
      this.logRetrievalHintEvent({
        hintId: hint.id,
        conversationId: hint.conversation_id ?? null,
        eventType: "expired",
        detail: {
          seedLabel: hint.seed_label,
          expandLabel: hint.expand_label,
          weight: hint.weight,
          ttlTurns: hint.ttl_turns,
        },
      });
    }
  }

  listActiveRetrievalHints(optionsOrLimit = null) {
    this.expireStaleRetrievalHints();

    const request = resolveListMode(optionsOrLimit, 25);

    if (request.mode === "pagination") {
      const { sql, params } = paginationClause({
        cursor: request.cursor,
        limit: request.limit,
        orderBy: "retrieval_hints.weight",
        direction: "DESC",
      });
      const rows = this.prepare(`
        SELECT
          retrieval_hints.*,
          rhs.times_considered,
          rhs.times_applied,
          rhs.times_rewarded,
          rhs.times_unused,
          rhs.avg_reward,
          rhs.last_reward,
          rhs.last_applied_at,
          (
            SELECT COUNT(*)
            FROM retrieval_queries rq
            WHERE rq.created_at >= retrieval_hints.created_at
              AND (
                retrieval_hints.conversation_id IS NULL
                OR rq.conversation_id = retrieval_hints.conversation_id
              )
          ) AS turns_elapsed,
          MAX(
            0,
            retrieval_hints.ttl_turns - (
              SELECT COUNT(*)
              FROM retrieval_queries rq
              WHERE rq.created_at >= retrieval_hints.created_at
                AND (
                  retrieval_hints.conversation_id IS NULL
                  OR rq.conversation_id = retrieval_hints.conversation_id
                )
            )
          ) AS turns_remaining,
          retrieval_hints.weight AS _cursor_key
        FROM retrieval_hints
        LEFT JOIN retrieval_hint_stats rhs ON rhs.hint_id = retrieval_hints.id
        WHERE status = 'active'${sql}
      `).all(...params);
      return paginateResults(rows, request.limit);
    }

    return this.prepare(`
      SELECT
        retrieval_hints.*,
        rhs.times_considered,
        rhs.times_applied,
        rhs.times_rewarded,
        rhs.times_unused,
        rhs.avg_reward,
        rhs.last_reward,
        rhs.last_applied_at,
        (
          SELECT COUNT(*)
          FROM retrieval_queries rq
          WHERE rq.created_at >= retrieval_hints.created_at
            AND (
              retrieval_hints.conversation_id IS NULL
              OR rq.conversation_id = retrieval_hints.conversation_id
            )
        ) AS turns_elapsed,
        MAX(
          0,
          retrieval_hints.ttl_turns - (
            SELECT COUNT(*)
            FROM retrieval_queries rq
            WHERE rq.created_at >= retrieval_hints.created_at
              AND (
                retrieval_hints.conversation_id IS NULL
                OR rq.conversation_id = retrieval_hints.conversation_id
              )
          )
        ) AS turns_remaining
      FROM retrieval_hints
      LEFT JOIN retrieval_hint_stats rhs ON rhs.hint_id = retrieval_hints.id
      WHERE status = 'active'
      ORDER BY weight DESC, created_at DESC
      LIMIT ?
    `).all(request.limit);
  }

  listRecentRetrievalHintEvents(optionsOrLimit = null) {
    const request = resolveListMode(optionsOrLimit, 25);

    if (request.mode === "pagination") {
      const { sql, params } = paginationClause({
        cursor: request.cursor,
        limit: request.limit,
        orderBy: "rhe.created_at",
        direction: "DESC",
      });
      const rows = this.prepare(`
        SELECT
          rhe.*,
          rh.seed_label AS seedLabel,
          rh.expand_label AS expandLabel,
          rhe.created_at AS _cursor_key
        FROM retrieval_hint_events rhe
        JOIN retrieval_hints rh ON rh.id = rhe.hint_id
        WHERE 1 = 1${sql}
      `).all(...params);
      return paginateResults(rows, request.limit);
    }

    return this.prepare(`
      SELECT
        rhe.*,
        rh.seed_label AS seedLabel,
        rh.expand_label AS expandLabel
      FROM retrieval_hint_events rhe
      JOIN retrieval_hints rh ON rh.id = rhe.hint_id
      ORDER BY rhe.created_at DESC
      LIMIT ?
    `).all(request.limit);
  }

  listRetrievalHintStats(optionsOrLimit = null) {
    const request = resolveListMode(optionsOrLimit, 25);

    if (request.mode === "pagination") {
      const { sql, params } = paginationClause({
        cursor: request.cursor,
        limit: request.limit,
        orderBy: "rhs.avg_reward",
        direction: "DESC",
      });
      const rows = this.prepare(`
        SELECT
          rh.*,
          rhs.times_considered,
          rhs.times_applied,
          rhs.times_rewarded,
          rhs.times_unused,
          rhs.avg_reward,
          rhs.last_reward,
          rhs.last_applied_at,
          rhs.updated_at,
          rhs.avg_reward AS _cursor_key
        FROM retrieval_hint_stats rhs
        JOIN retrieval_hints rh ON rh.id = rhs.hint_id
        WHERE 1 = 1${sql}
      `).all(...params);
      return paginateResults(rows, request.limit);
    }

    return this.prepare(`
      SELECT
        rh.*,
        rhs.times_considered,
        rhs.times_applied,
        rhs.times_rewarded,
        rhs.times_unused,
        rhs.avg_reward,
        rhs.last_reward,
        rhs.last_applied_at,
        rhs.updated_at
      FROM retrieval_hint_stats rhs
      JOIN retrieval_hints rh ON rh.id = rhs.hint_id
      ORDER BY rhs.avg_reward DESC, rhs.times_rewarded DESC, rh.weight DESC
      LIMIT ?
    `).all(request.limit);
  }

  logProxyEvent({ conversationId = null, actorId = "system", direction, stage, verdict, reasons, payload }) {
    const id = createId("proxy");
    this.prepare(`
      INSERT INTO proxy_events (
        id, conversation_id, actor_id, direction, stage, verdict, reasons_json, payload_json, created_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(id, conversationId, actorId, direction, stage, verdict, stableJson(reasons), stableJson(payload), nowIso());

    return id;
  }

  listRecentProxyEvents(optionsOrLimit = null) {
    const request = resolveListMode(optionsOrLimit, 20);

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
        FROM proxy_events
        WHERE 1 = 1${sql}
      `).all(...params);
      return paginateResults(rows, request.limit);
    }

    return this.prepare(`
      SELECT *
      FROM proxy_events
      ORDER BY created_at DESC
      LIMIT ?
    `).all(request.limit);
  }

  getDashboardStats() {
    return {
      counts: {
        modelRuns: this.prepare(`SELECT COUNT(*) AS count FROM model_runs`).get().count,
        retrievalHints: this.prepare(`SELECT COUNT(*) AS count FROM retrieval_hints WHERE status = 'active'`).get().count,
        hintEvents: this.prepare(`SELECT COUNT(*) AS count FROM retrieval_hint_events`).get().count,
        warnings: this.prepare(`
          SELECT COUNT(*) AS count
          FROM proxy_events
          WHERE verdict IN ('warn', 'block')
        `).get().count,
      },
    };
  }

  close() {
    this.sqlite.close();
  }
}
