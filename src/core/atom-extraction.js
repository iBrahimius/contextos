/**
 * Atom Extraction — Extract semantic atoms from observations via LLM.
 *
 * Atoms are discrete, semantically meaningful units extracted from a cluster of
 * observations: facts, decisions, rationales, tensions, emotions, contradictions,
 * quotes, and open loops.
 *
 * The LLM is prompted with ALL observations in a cluster, then returns a structured
 * list of atoms. Source observation IDs are validated against the input set.
 *
 * Failure (bad JSON, LLM error, validation failure) → return empty array (graceful degradation).
 */

/**
 * Build the Haiku prompt for atom extraction.
 *
 * Includes all observations with their IDs, timestamps, confidence, and text.
 * Instructs the LLM to extract discrete atoms and cite their source observations.
 *
 * @param {Array<object>} observations — Array of observations
 *   Each should have: { id, timestamp, text, confidence }
 * @returns {string} — Ready-to-send prompt
 */
export function buildAtomExtractionPrompt(observations) {
  const obsText = observations
    .map(
      (obs) =>
        `[ID: ${obs.id}] (${obs.timestamp}, confidence: ${obs.confidence}) ${obs.text}`
    )
    .join("\n\n");

  return `You are an atom extractor. Analyze the following observations and extract discrete, semantically meaningful atoms.

Observations:
${obsText}

Extract atoms in the following categories:
- fact: A verifiable or stated claim (e.g., "Ibrahim prefers async patterns")
- decision: A choice or commitment made (e.g., "We decided to use SQLite")
- rationale: The reasoning behind a decision or fact (e.g., "SQLite chosen for zero dependencies")
- tension: A conflict or opposing forces (e.g., "Speed vs. correctness tradeoff")
- emotion: An emotional state or reaction (e.g., "Frustration with slow iteration")
- contradiction: Conflicting statements or values (e.g., "Wants fast iteration but also high quality")
- quote: A direct or paraphrased quote from the observations (e.g., "Build it right, not what's quickest")
- open_loop: An unresolved question or pending item (e.g., "How to handle missing embeddings?")

Return a JSON array of atoms. Each atom must have:
- type: one of the categories above
- text: the atom content (concise, actionable)
- source_observation_ids: array of observation IDs that support this atom (must exist in the input)
- confidence: 0.0–1.0, your confidence in this extraction

Example output format:
{
  "atoms": [
    {
      "type": "decision",
      "text": "Use SQLite for persistence",
      "source_observation_ids": [1, 3, 5],
      "confidence": 0.95
    }
  ]
}

Extract as many meaningful atoms as you can find. Be thorough.`;
}

/**
 * Extract atoms from observations using an LLM client.
 *
 * Validates that all source_observation_ids reference real observations.
 * Returns empty array on any failure (graceful degradation).
 *
 * @param {Array<object>} observations
 * @param {import('./llm-client.js').LLMClient} llmClient
 * @returns {Promise<Array<object>>} — Array of validated atoms
 */
export async function extractAtoms(observations, llmClient) {
  if (!observations || observations.length === 0) {
    return [];
  }

  // Build valid ID set for validation
  const validIds = new Set(observations.map((o) => o.id));

  try {
    const prompt = buildAtomExtractionPrompt(observations);

    const result = await llmClient.completeJSON({
      prompt,
      model: "anthropic/claude-haiku-4-5",
      maxTokens: 2048,
    });

    const atoms = result.data?.atoms ?? [];

    // Validate source IDs
    const validatedAtoms = atoms.filter((atom) => {
      const sourceIds = atom.source_observation_ids ?? [];
      return (
        Array.isArray(sourceIds) &&
        sourceIds.every((id) => validIds.has(id))
      );
    });

    return validatedAtoms;
  } catch (err) {
    console.error("[atom-extraction] LLM call failed:", err.message);
    return [];
  }
}

/**
 * Persist extracted atoms to the database.
 *
 * @param {object} db — SQLite database instance
 * @param {number} clusterId — Cluster ID to associate atoms with
 * @param {Array<object>} atoms — Atoms to persist
 * @returns {Promise<void>}
 */
export async function persistAtoms(db, clusterId, atoms) {
  const stmt = db.prepare(`
    INSERT INTO cluster_atoms (cluster_id, atom_type, text, source_observation_ids, confidence, created_at)
    VALUES (?, ?, ?, ?, ?, datetime('now'))
  `);

  for (const atom of atoms) {
    stmt.run(
      clusterId,
      atom.type,
      atom.text,
      JSON.stringify(atom.source_observation_ids ?? []),
      atom.confidence ?? 0.5
    );
  }
}
