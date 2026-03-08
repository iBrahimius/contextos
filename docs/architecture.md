# ContextOS Architecture

## First principles

ContextOS is built as an append-only memory runtime, not as a post-hoc summarizer. Every human and agent message becomes durable local state immediately. During ingestion, the runtime emits a stream of typed observations. Retrieval is greedy and graph-aware: gather everything relevant first, meter token cost second, and let assembly decide what to use.

This prototype deliberately avoids backend frameworks. The runtime uses:

- Node `http` for the local API and proxy surface
- Node `sqlite` for local persistence
- In-memory maps for hot graph traversal
- Static HTML/CSS/JS for the human dashboard

## Memory model

The elegant alternative to batch classification is an observation stream:

- `message`: raw captured content
- `entity`: stable node discovered or reinforced during ingest
- `relationship`: durable edge between entities
- `observation`: typed claim linked back to a source span

Typed observations project into:

- `task`
- `decision`
- `constraint`
- `fact`

That gives one ingestion path and multiple retrieval views.

## Data flow

1. A message enters through the proxy or direct ingest API.
2. The prompt-injection guard scans inbound text and logs its verdict.
3. The raw message is persisted in SQLite.
4. The live classifier extracts entities and typed observations sentence by sentence.
5. Entities are upserted into SQLite and mirrored into the RAM graph.
6. Relationships are persisted and inserted into adjacency lists immediately.
7. A retrieval request seeds graph traversal from matching entities, active retrieval hints, plus FTS hits.
8. Graph expansion walks both incoming and outgoing edges, then blends in decaying hint edges learned from prior misses.
9. Expansion aggressiveness increases for entities with elevated complexity scores.
10. Retrieval telemetry records latency, expansion path, items returned, token consumption, and whether each hop came from the stored graph or a live hint.

## Why the graph lives in RAM

Correctness depends on deterministic traversal, not remote vector calls. By loading entities and relationships into memory at startup:

- expansion is cheap and explainable
- query latency is stable
- relationship-aware retrieval is not gated on SQL joins
- missed entities can directly update future traversal strategy

SQLite remains the system of record. On startup the graph is reconstructed from durable tables.

## Retrieval strategy

Retrieval follows the rule: retrieve greedily, assemble selectively.

- Seed entities are found by lexical matching plus live entity extraction on the query itself.
- The engine traverses related entities bidirectionally and can inject retrieval hints as virtual edges.
- Relation type and entity complexity modify edge weight.
- Hints decay by retrieval turns using `ttl_turns`, so recall gets more aggressive temporarily instead of permanently distorting the graph.
- No hard item cap is imposed inside the retrieval engine; everything above threshold is returned.
- Tokens are estimated and logged instead of being used as a pruning budget.

## Living complexity scores

Each entity has a `complexity_score` and `miss_count`.

- Base complexity starts at `1.0`
- If a later query references an entity that should have been reachable from a recent query but was not expanded, that entity is marked as missed
- Misses increase `miss_count` and push `complexity_score` upward
- Higher complexity increases expansion depth and lowers inclusion thresholds on later queries

This is a simple self-correcting feedback loop that can later be upgraded with explicit human feedback.

## Hint policy learning

`retrieval_hints` are policy, not truth.

- Hints are proposed by the live model/compiler and stored separately from graph facts.
- During retrieval they act as decaying virtual edges and each returned item carries hint provenance when applicable.
- After every retrieval the engine logs per-hint events such as `considered`, `applied`, `rewarded`, `decayed`, and `expired`.
- A deterministic scorer updates hint `weight` and `ttl_turns` from observed contribution, so useful hint hops get stronger while unused hints fade quickly.
- Aggregate hint stats live in SQLite for fast dashboard reads, while the append-only event log preserves auditability.

## Document indexing

Markdown indexing is local and deterministic:

- recursively walk `.md` files
- split by headings and chunk size
- store chunks in SQLite
- index chunk text in FTS5
- classify each chunk to link it to entities and relationships

That lets retrieval blend conversation memory with document memory.

## Proxy layer

The proxy surface is OpenClaw-like in the sense that it owns conversation capture, memory retrieval, and prompt safety inside one process instead of depending on an external testing harness.

In this prototype it provides:

- inbound and outbound prompt scanning
- durable event logging
- retrieval before response synthesis
- a synthetic local response path so the system can be demonstrated standalone

## Frontend

The dashboard is intentionally visual-first:

- system counters
- tracked components
- proxy warnings
- graph heat map
- complexity pressure
- retrieval telemetry
- recent conversations

The styling follows shadcn conventions locally instead of depending on a component package or cloud build pipeline.

## Sub-Agent Memory Architecture

Sub-agent work (Codex, Claude Code sessions) is NOT ingested through the normal Scribe → classify → observe pipeline. This is a deliberate architectural decision to prevent noise explosion in the knowledge graph.

Instead, sub-agent output follows a **commit + signpost** pattern:
- Scribe skips sub-agent session messages entirely
- The orchestrating agent writes a single L0 signpost observation (commit SHA, decisions, gaps)
- Detail retrieval via deterministic hops: `git show` for diff, `sessions_history` for reasoning

See [design note: Sub-Agent Memory Contract](../../context-os-v2/design-notes/2026-03-08-subagent-memory-contract.md) for full rationale.
