# ContextOS

ContextOS is a local-first memory operating system for AI agents. It captures every conversation message, classifies knowledge live into typed observations, persists everything in SQLite, keeps an entity graph in RAM for fast traversal, indexes markdown with FTS5, logs full retrieval telemetry, and exposes a proxy layer that can intercept prompt injections before a response leaves the system.

## What the prototype includes

- Framework-free Node runtime using built-in HTTP and SQLite support
- Live observation stream for decisions, tasks, constraints, facts, and entity relationships
- RAM-resident entity graph with SQLite-backed persistence
- Greedy graph-aware retrieval with self-correcting complexity scores
- Markdown chunking plus FTS5 search linked back to entities
- Prompt-injection scanning on inbound and outbound proxy traffic
- `contextos-modeld` contract for local extraction, alias resolution, expansion hints, and answer composition
- Patch-based memory compilation with `model_runs`, `retrieval_hints`, and `graph_proposals`
- Retrieval hints decay by query turns and are applied as virtual edges during graph expansion
- Hint policy learning reinforces useful hint hops and decays unused ones with a full local event log
- Local dashboard with shadcn-style visual language

## Run it

```bash
npm run demo
npm start
npm run modeld
```

The server listens on `http://127.0.0.1:4181`.
The standalone model daemon listens on `http://127.0.0.1:4182` when started, and the app will use it if `CONTEXTOS_MODELD_URL` is set.

## Useful endpoints

- `GET /api/dashboard`
- `POST /api/messages`
- `POST /api/retrieve`
- `POST /api/index`
- `POST /api/proxy/chat`
- `GET /api/entities/:slug`
- `GET /api/model-runs`
- `GET /api/retrieval-hints`
- `GET /api/retrieval-hint-stats`
- `GET /api/retrieval-hint-events`
- `GET /api/graph-proposals`

## Structure

- `src/core`: graph, patch validator, retrieval, indexer, proxy guard, orchestration
- `src/db`: SQLite schema and data access layer
- `src/http`: HTTP router
- `src/modeld`: local model daemon contract, engine, and server
- `src/ui`: dashboard frontend
- `scripts/demo.js`: seeds a realistic architecture conversation and document corpus
- `docs/architecture.md`: first-principles design
