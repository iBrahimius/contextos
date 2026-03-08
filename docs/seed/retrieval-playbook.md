# Retrieval Playbook

## Expansion behavior

Querying the memory system should also surface the embedding engine, retrieval pipeline, and storage layer because they are related components.
If the embedding engine is repeatedly missed, its complexity score should rise and future expansion should be more aggressive.

## Telemetry

Every retrieval query logs latency, items returned, tokens consumed, and the exact expansion path.
Telemetry integrates with the dashboard frontend.
