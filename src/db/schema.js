export const SCHEMA = `
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS conversations (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  role TEXT NOT NULL,
  direction TEXT NOT NULL,
  actor_id TEXT NOT NULL DEFAULT 'system',
  origin_kind TEXT NOT NULL DEFAULT 'user' CHECK (origin_kind IN ('user', 'agent', 'system', 'import')),
  source_message_id TEXT REFERENCES messages(id) ON DELETE SET NULL,
  scope_kind TEXT NOT NULL DEFAULT 'private' CHECK (scope_kind IN ('private', 'project', 'shared', 'public')),
  scope_id TEXT,
  content TEXT NOT NULL,
  token_count INTEGER NOT NULL,
  captured_at TEXT NOT NULL,
  embedding BLOB,
  raw_json TEXT,
  ingest_id TEXT UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_messages_conversation ON messages(conversation_id, captured_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_ingest_id ON messages(ingest_id) WHERE ingest_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS message_embeddings (
  message_id TEXT PRIMARY KEY REFERENCES messages(id),
  embedding BLOB NOT NULL,
  model TEXT NOT NULL DEFAULT 'embeddinggemma-300m',
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS entities (
  id TEXT PRIMARY KEY,
  slug TEXT NOT NULL UNIQUE,
  label TEXT NOT NULL,
  kind TEXT NOT NULL,
  summary TEXT,
  complexity_score REAL NOT NULL DEFAULT 1,
  mention_count INTEGER NOT NULL DEFAULT 0,
  miss_count INTEGER NOT NULL DEFAULT 0,
  scope_kind TEXT NOT NULL DEFAULT 'shared' CHECK (scope_kind IN ('private', 'project', 'shared', 'public')),
  scope_id TEXT,
  owner_id TEXT NOT NULL DEFAULT 'system',
  metadata_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS entity_aliases (
  entity_id TEXT NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
  alias TEXT NOT NULL,
  alias_slug TEXT NOT NULL,
  PRIMARY KEY (entity_id, alias_slug)
);

CREATE INDEX IF NOT EXISTS idx_entity_aliases_slug ON entity_aliases(alias_slug);

CREATE TABLE IF NOT EXISTS relationships (
  id TEXT PRIMARY KEY,
  subject_entity_id TEXT NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
  predicate TEXT NOT NULL,
  object_entity_id TEXT NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
  weight REAL NOT NULL DEFAULT 1,
  provenance_message_id TEXT REFERENCES messages(id) ON DELETE SET NULL,
  metadata_json TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_relationships_subject ON relationships(subject_entity_id, predicate);
CREATE INDEX IF NOT EXISTS idx_relationships_object ON relationships(object_entity_id, predicate);

CREATE TABLE IF NOT EXISTS observations (
  id TEXT PRIMARY KEY,
  conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  actor_id TEXT NOT NULL DEFAULT 'system',
  category TEXT NOT NULL,
  predicate TEXT,
  subject_entity_id TEXT REFERENCES entities(id) ON DELETE SET NULL,
  object_entity_id TEXT REFERENCES entities(id) ON DELETE SET NULL,
  scope_kind TEXT NOT NULL DEFAULT 'private' CHECK (scope_kind IN ('private', 'project', 'shared', 'public')),
  scope_id TEXT,
  detail TEXT NOT NULL,
  confidence REAL NOT NULL,
  source_span TEXT,
  metadata_json TEXT,
  created_at TEXT NOT NULL,
  compressed_into TEXT DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_observations_message ON observations(message_id, category);
CREATE INDEX IF NOT EXISTS idx_observations_entities ON observations(subject_entity_id, object_entity_id);

CREATE TABLE IF NOT EXISTS observation_embeddings (
  observation_id TEXT PRIMARY KEY REFERENCES observations(id),
  embedding BLOB NOT NULL,
  model TEXT NOT NULL DEFAULT 'embeddinggemma-300m',
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE VIRTUAL TABLE IF NOT EXISTS observation_fts USING fts5(
  observation_id UNINDEXED,
  category UNINDEXED,
  content
);

-- ── Episodes & Topic Clustering (v2.3) ────────────────────────────

CREATE TABLE IF NOT EXISTS episodes (
  id TEXT PRIMARY KEY,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  session_gap_minutes INTEGER DEFAULT 30,
  cluster_count INTEGER DEFAULT 0,
  metadata JSON,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_episodes_time ON episodes(started_at, ended_at);

CREATE TABLE IF NOT EXISTS observation_clusters (
  id TEXT PRIMARY KEY,
  episode_id TEXT NOT NULL REFERENCES episodes(id) ON DELETE CASCADE,
  topic_label TEXT,
  entities JSON,
  topics JSON,
  time_span_start TEXT NOT NULL,
  time_span_end TEXT NOT NULL,
  observation_count INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  metadata JSON
);

CREATE INDEX IF NOT EXISTS idx_observation_clusters_episode ON observation_clusters(episode_id);
CREATE INDEX IF NOT EXISTS idx_observation_clusters_time ON observation_clusters(time_span_start, time_span_end);

-- ── Cluster LOD Levels (v2.4) ──────────────────────────────────────

CREATE TABLE IF NOT EXISTS cluster_levels (
  cluster_id TEXT NOT NULL REFERENCES observation_clusters(id) ON DELETE CASCADE,
  level INTEGER NOT NULL,
  text TEXT NOT NULL,
  source_observation_ids TEXT,
  char_count INTEGER,
  generated_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (cluster_id, level)
);

CREATE TABLE IF NOT EXISTS cluster_level_embeddings (
  cluster_id TEXT NOT NULL,
  level INTEGER NOT NULL,
  embedding BLOB NOT NULL,
  model TEXT NOT NULL DEFAULT 'embeddinggemma-300m',
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (cluster_id, level),
  FOREIGN KEY (cluster_id) REFERENCES observation_clusters(id) ON DELETE CASCADE
);

CREATE VIRTUAL TABLE IF NOT EXISTS cluster_level_fts USING fts5(
  text,
  cluster_id UNINDEXED,
  level UNINDEXED
);

CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY,
  observation_id TEXT NOT NULL REFERENCES observations(id) ON DELETE CASCADE,
  entity_id TEXT REFERENCES entities(id) ON DELETE SET NULL,
  title TEXT NOT NULL,
  status TEXT NOT NULL,
  priority TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS decisions (
  id TEXT PRIMARY KEY,
  observation_id TEXT NOT NULL REFERENCES observations(id) ON DELETE CASCADE,
  entity_id TEXT REFERENCES entities(id) ON DELETE SET NULL,
  title TEXT NOT NULL,
  rationale TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS constraints (
  id TEXT PRIMARY KEY,
  observation_id TEXT NOT NULL REFERENCES observations(id) ON DELETE CASCADE,
  entity_id TEXT REFERENCES entities(id) ON DELETE SET NULL,
  detail TEXT NOT NULL,
  severity TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS facts (
  id TEXT PRIMARY KEY,
  observation_id TEXT NOT NULL REFERENCES observations(id) ON DELETE CASCADE,
  entity_id TEXT REFERENCES entities(id) ON DELETE SET NULL,
  detail TEXT NOT NULL,
  created_at TEXT NOT NULL
);

-- ── Claims (v2.1) ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS claims (
  id TEXT PRIMARY KEY,
  observation_id TEXT UNIQUE,
  conversation_id TEXT,
  message_id TEXT,
  actor_id TEXT,

  claim_type TEXT NOT NULL
    CHECK (claim_type IN (
      'fact', 'decision', 'task', 'constraint', 'preference',
      'goal', 'habit', 'rule', 'event', 'state_change', 'relationship'
    )),
  subject_entity_id TEXT,
  predicate TEXT,
  object_entity_id TEXT,
  value_text TEXT,

  confidence REAL DEFAULT 0.5
    CHECK (confidence >= 0.0 AND confidence <= 1.0),
  source_type TEXT DEFAULT 'implicit'
    CHECK (source_type IN ('explicit', 'implicit', 'inference', 'derived')),

  lifecycle_state TEXT NOT NULL DEFAULT 'candidate'
    CHECK (lifecycle_state IN (
      'candidate', 'active', 'superseded', 'disputed', 'archived'
    )),

  valid_from TEXT DEFAULT (datetime('now')),
  valid_to TEXT,

  resolution_key TEXT,
  facet_key TEXT,
  supersedes_claim_id TEXT REFERENCES claims(id),
  superseded_by_claim_id TEXT REFERENCES claims(id),

  scope_kind TEXT DEFAULT 'private'
    CHECK (scope_kind IN ('private', 'project', 'shared', 'public')),
  scope_id TEXT,

  metadata_json TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now')),

  importance_score REAL NOT NULL DEFAULT 1.0,

  FOREIGN KEY (observation_id) REFERENCES observations(id)
);

CREATE INDEX IF NOT EXISTS idx_claims_type_state ON claims(claim_type, lifecycle_state);
CREATE INDEX IF NOT EXISTS idx_claims_subject ON claims(subject_entity_id);
CREATE INDEX IF NOT EXISTS idx_claims_object ON claims(object_entity_id);
CREATE INDEX IF NOT EXISTS idx_claims_resolution ON claims(resolution_key, facet_key);
CREATE INDEX IF NOT EXISTS idx_claims_lifecycle ON claims(lifecycle_state);
CREATE INDEX IF NOT EXISTS idx_claims_conversation ON claims(conversation_id);
CREATE INDEX IF NOT EXISTS idx_claims_valid_time ON claims(valid_from, valid_to);

CREATE TABLE IF NOT EXISTS claim_backfill_status (
  observation_id TEXT PRIMARY KEY REFERENCES observations(id) ON DELETE CASCADE,
  status TEXT NOT NULL CHECK (status IN ('claim_created', 'no_claim', 'failed')),
  claim_id TEXT REFERENCES claims(id) ON DELETE SET NULL,
  error_message TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  first_attempted_at TEXT,
  last_attempted_at TEXT,
  processed_at TEXT,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_claim_backfill_status ON claim_backfill_status(status, updated_at);

-- ── Documents ──────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS documents (
  id TEXT PRIMARY KEY,
  path TEXT NOT NULL UNIQUE,
  checksum TEXT NOT NULL,
  indexed_at TEXT NOT NULL,
  metadata_json TEXT
);

CREATE TABLE IF NOT EXISTS document_chunks (
  id TEXT PRIMARY KEY,
  document_id TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  ordinal INTEGER NOT NULL,
  heading TEXT,
  content TEXT NOT NULL,
  token_count INTEGER NOT NULL,
  scope_kind TEXT NOT NULL DEFAULT 'shared' CHECK (scope_kind IN ('private', 'project', 'shared', 'public')),
  scope_id TEXT,
  created_at TEXT NOT NULL,
  UNIQUE (document_id, ordinal)
);

CREATE TABLE IF NOT EXISTS chunk_entities (
  chunk_id TEXT NOT NULL REFERENCES document_chunks(id) ON DELETE CASCADE,
  entity_id TEXT NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
  score REAL NOT NULL DEFAULT 1,
  PRIMARY KEY (chunk_id, entity_id)
);

CREATE VIRTUAL TABLE IF NOT EXISTS chunk_fts USING fts5(
  chunk_id UNINDEXED,
  content,
  tokenize = 'porter unicode61'
);

CREATE TABLE IF NOT EXISTS graph_proposals (
  id TEXT PRIMARY KEY,
  conversation_id TEXT REFERENCES conversations(id) ON DELETE SET NULL,
  message_id TEXT REFERENCES messages(id) ON DELETE SET NULL,
  source_run_id TEXT,
  actor_id TEXT NOT NULL DEFAULT 'system',
  proposal_type TEXT NOT NULL,
  subject_label TEXT,
  predicate TEXT,
  object_label TEXT,
  detail TEXT,
  confidence REAL NOT NULL,
  status TEXT NOT NULL,
  reason TEXT,
  scope_kind TEXT NOT NULL DEFAULT 'private' CHECK (scope_kind IN ('private', 'project', 'shared', 'public')),
  scope_id TEXT,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  reviewed_by_actor TEXT,
  reviewed_at TEXT,
  accepted_at TEXT,
  write_class TEXT NOT NULL DEFAULT 'ai_proposed'
);

CREATE INDEX IF NOT EXISTS idx_graph_proposals_status ON graph_proposals(status, created_at DESC);

CREATE TABLE IF NOT EXISTS system_state (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

INSERT OR IGNORE INTO system_state VALUES ('graph_version', '0', datetime('now'));

-- ── Session Checkpoints (v2.3) ──────────────────────────────────────

CREATE TABLE IF NOT EXISTS session_checkpoints (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  graph_version INTEGER,
  saved_at TEXT,
  active_task_ids TEXT,
  active_decision_ids TEXT,
  active_goal_ids TEXT
);
`;
