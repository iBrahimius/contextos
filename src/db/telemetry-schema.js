export const TELEMETRY_SCHEMA = `
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = OFF;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS retrieval_queries (
  id TEXT PRIMARY KEY,
  conversation_id TEXT,
  query_text TEXT NOT NULL,
  latency_ms INTEGER NOT NULL,
  seed_entities_json TEXT NOT NULL,
  expanded_entities_json TEXT NOT NULL,
  expansion_path_json TEXT NOT NULL,
  items_returned INTEGER NOT NULL,
  tokens_consumed INTEGER NOT NULL,
  miss_entities_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS retrieval_results (
  id TEXT PRIMARY KEY,
  query_id TEXT NOT NULL REFERENCES retrieval_queries(id) ON DELETE CASCADE,
  item_type TEXT NOT NULL,
  item_id TEXT NOT NULL,
  rank INTEGER NOT NULL,
  score REAL NOT NULL,
  entity_id TEXT,
  summary TEXT
);

CREATE INDEX IF NOT EXISTS idx_retrieval_results_query ON retrieval_results(query_id, rank);

CREATE TABLE IF NOT EXISTS retrieval_hints (
  id TEXT PRIMARY KEY,
  conversation_id TEXT,
  message_id TEXT,
  source_run_id TEXT REFERENCES model_runs(id) ON DELETE SET NULL,
  actor_id TEXT NOT NULL DEFAULT 'system',
  seed_entity_id TEXT,
  seed_label TEXT NOT NULL,
  expand_entity_id TEXT,
  expand_label TEXT NOT NULL,
  reason TEXT NOT NULL,
  weight REAL NOT NULL,
  ttl_turns INTEGER NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_retrieval_hints_status ON retrieval_hints(status, created_at DESC);

CREATE TABLE IF NOT EXISTS retrieval_hint_stats (
  hint_id TEXT PRIMARY KEY REFERENCES retrieval_hints(id) ON DELETE CASCADE,
  times_considered INTEGER NOT NULL DEFAULT 0,
  times_applied INTEGER NOT NULL DEFAULT 0,
  times_rewarded INTEGER NOT NULL DEFAULT 0,
  times_unused INTEGER NOT NULL DEFAULT 0,
  avg_reward REAL NOT NULL DEFAULT 0,
  last_reward REAL NOT NULL DEFAULT 0,
  last_applied_at TEXT,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS retrieval_hint_events (
  id TEXT PRIMARY KEY,
  hint_id TEXT NOT NULL REFERENCES retrieval_hints(id) ON DELETE CASCADE,
  query_id TEXT REFERENCES retrieval_queries(id) ON DELETE SET NULL,
  conversation_id TEXT,
  event_type TEXT NOT NULL,
  reward REAL NOT NULL DEFAULT 0,
  penalty REAL NOT NULL DEFAULT 0,
  detail_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_retrieval_hint_events_hint ON retrieval_hint_events(hint_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_retrieval_hint_events_type ON retrieval_hint_events(event_type, created_at DESC);

CREATE TABLE IF NOT EXISTS model_runs (
  id TEXT PRIMARY KEY,
  conversation_id TEXT,
  message_id TEXT,
  actor_id TEXT NOT NULL DEFAULT 'system',
  stage TEXT NOT NULL,
  provider TEXT NOT NULL,
  model_name TEXT NOT NULL,
  transport TEXT NOT NULL,
  status TEXT NOT NULL,
  latency_ms INTEGER NOT NULL,
  input_tokens INTEGER NOT NULL,
  output_tokens INTEGER NOT NULL,
  input_json TEXT NOT NULL,
  output_json TEXT,
  error_text TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_model_runs_created ON model_runs(created_at DESC, stage);

CREATE TABLE IF NOT EXISTS proxy_events (
  id TEXT PRIMARY KEY,
  conversation_id TEXT,
  actor_id TEXT NOT NULL DEFAULT 'system',
  direction TEXT NOT NULL,
  stage TEXT NOT NULL,
  verdict TEXT NOT NULL,
  reasons_json TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_proxy_events_created ON proxy_events(created_at, verdict);
`;
