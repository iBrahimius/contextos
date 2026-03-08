# ContextOS Overview

ContextOS is a local-first memory system for AI agents.

## Core topology

The memory system depends on the retrieval pipeline, storage layer, proxy layer, and entity graph.
The storage layer stores all messages and observations in SQLite.
The entity graph lives in RAM and depends on SQLite for persistence.
The retrieval pipeline retrieves entities, chunks, tasks, constraints, and decisions.
The proxy layer captures inbound prompts and outbound responses.
The dashboard frontend integrates with telemetry and warnings.

## Retrieval rule

Retrieve greedily and assemble selectively.
No token budget should truncate retrieval. The system meters token consumption instead.

## Safety rule

Prompt injection defenses must intercept ignore-previous-instructions attacks before they reach the agent.
