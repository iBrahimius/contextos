# Tests

## Isolation

Every test creates its own temporary database and ContextOS instance. No test reads from or writes to the live production database (`data/contextos.db`).

This means:
- Test fixtures use generic names (Alice, ace, bot, etc.) — not real user data
- Tests can run safely in CI or locally without affecting running instances
- The golden retrieval set seeds its own data via `seedAuditDataset()`

**If you add integration tests against live data in the future**, those tests must handle real entity names that exist in the production database — not the generic test fixture names.
