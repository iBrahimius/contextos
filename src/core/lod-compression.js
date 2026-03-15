/**
 * LOD Compression — Generate Level-of-Detail abstraction hierarchy.
 *
 * From atoms and observations, generates three nested levels:
 * - L2: ~4K chars — Full narrative arc with quotes, emotional texture, all details
 * - L1: ~2K chars — Structured synopsis, organized by type (decision/fact/tension/etc)
 * - L0: ~300 chars — Signpost: entities + topic + headline
 *
 * Contradictions preserved at all levels. Emotional context = information.
 * Failure (bad JSON, LLM error) → return empty levels object (graceful degradation).
 */

/**
 * Build the Haiku prompt for LOD level generation.
 *
 * Instructs LLM to generate L2 → L1 → L0, with specific constraints for each level.
 * Emphasizes contradiction preservation and emotional context as information.
 *
 * @param {Array<object>} atoms — Extracted atoms from the cluster
 * @param {Array<object>} observations — Original observations
 * @returns {string} — Ready-to-send prompt
 */
export function buildLevelGenerationPrompt(atoms, observations) {
  const atomsText = atoms
    .map(
      (a) =>
        `[${a.type.toUpperCase()}] ${a.text} (confidence: ${a.confidence})`
    )
    .join("\n");

  const obsText = observations
    .map((o) => `[${o.id}] ${o.text}`)
    .join("\n");

  return `You are a knowledge compression specialist. Generate a 3-level abstraction hierarchy from the following atoms and observations.

ATOMS (semantic units extracted from observations):
${atomsText}

OBSERVATIONS (raw source):
${obsText}

Generate three levels of abstraction:

**L2 (Detailed Narrative, ~4000 chars)**
- Full narrative arc and story flow
- Include relevant quotes and direct observations
- Preserve emotional context and texture
- Keep contradictions visible; don't resolve them
- Timestamp significant events
- This is the comprehensive, human-readable summary

**L1 (Structured Synopsis, ~2000 chars)**
- Organize by atom type: decisions, facts, tensions, emotions, quotes
- Hierarchical: most important first
- Preserve contradictions explicitly (e.g., "Wants A but also values B")
- Fewer details than L2, more than L0
- Machine-scannable structure

**L0 (Signpost/Headline, ~300 chars)**
- List key entities mentioned
- State the primary topic/theme
- One-line headline that captures the essence
- No narrative flow; pure information density

Return a JSON object:
{
  "l2": "...",
  "l1": "...",
  "l0": "..."
}

CRITICAL RULES:
- Do not resolve contradictions — they are information
- Emotional context is information (frustration, joy, concern, skepticism)
- Do not editorialize or synthesize false consensus
- Each level should be self-contained and readable independently
- Respect the character limits as soft targets (within ~10–20%)

Generate the three levels now.`;
}

/**
 * Generate LOD levels from atoms and observations using an LLM client.
 *
 * Returns empty object on any failure (graceful degradation).
 *
 * @param {Array<object>} atoms — Extracted atoms
 * @param {Array<object>} observations — Original observations
 * @param {import('./llm-client.js').LLMClient} llmClient
 * @returns {Promise<{ l0?: string, l1?: string, l2?: string }>}
 */
export async function generateLevels(atoms, observations, llmClient) {
  if (!atoms || !observations) {
    return {};
  }

  try {
    const prompt = buildLevelGenerationPrompt(atoms, observations);

    const result = await llmClient.completeJSON({
      prompt,
      model: "anthropic/claude-haiku-4-5",
      maxTokens: 4096,
      timeoutMs: 300_000, // 5 min — gateway proxy + large prompts
    });

    const levels = result.data ?? {};

    // Validate structure: ensure we have at least one level
    if (!levels.l0 && !levels.l1 && !levels.l2) {
      console.warn("[lod-compression] No valid levels in LLM response");
      return {};
    }

    return {
      l0: levels.l0 ?? "",
      l1: levels.l1 ?? "",
      l2: levels.l2 ?? "",
    };
  } catch (err) {
    console.error("[lod-compression] LLM call failed:", err.message);
    return {};
  }
}

/**
 * Persist LOD levels to the database.
 *
 * Each level is stored with its character count and source observation IDs.
 * L0 and L1 levels are also embedded (via embedText) and indexed for vector + FTS search.
 *
 * @param {object} db — SQLite database instance
 * @param {number} clusterId — Cluster ID to associate levels with
 * @param {object} levels — { l0, l1, l2 } level texts
 * @param {Array<number>} [sourceObservationIds] — Optional observation IDs (stored as JSON)
 * @param {Function} [embedText] — Optional embedding function; if provided, embeds L0 + L1
 * @returns {Promise<void>}
 */
export async function persistLevels(db, clusterId, levels, sourceObservationIds = [], embedText = null) {
  const stmt = db.prepare(`
    INSERT INTO cluster_levels (cluster_id, level, text, source_observation_ids, char_count, generated_at)
    VALUES (?, ?, ?, ?, ?, datetime('now'))
    ON CONFLICT(cluster_id, level) DO UPDATE SET
      text = excluded.text,
      source_observation_ids = excluded.source_observation_ids,
      char_count = excluded.char_count,
      generated_at = datetime('now')
  `);

  for (const [levelKey, levelText] of Object.entries(levels)) {
    if (!levelText) continue;

    const levelNum = parseInt(levelKey.charAt(1), 10); // "l0" → 0, "l1" → 1, etc.
    if (isNaN(levelNum)) continue;

    stmt.run(
      clusterId,
      levelNum,
      levelText,
      JSON.stringify(sourceObservationIds),
      levelText.length
    );

    // Embed L0 and L1 for vector search; L2 is only loaded on-demand
    if ((levelNum === 0 || levelNum === 1) && embedText) {
      try {
        const embedding = await embedText(levelText);
        if (embedding) {
          db.upsertClusterLevelEmbedding({
            clusterId,
            level: levelNum,
            embedding,
          });
        }
      } catch (err) {
        console.warn(`[lod-compression] Failed to embed cluster L${levelNum}:`, err.message);
      }
    }

    // Index L0 and L1 in FTS for full-text search
    if (levelNum === 0 || levelNum === 1) {
      try {
        db.insertClusterLevelFts(clusterId, levelNum, levelText);
      } catch (err) {
        console.warn(`[lod-compression] Failed to index cluster L${levelNum} in FTS:`, err.message);
      }
    }
  }
}
