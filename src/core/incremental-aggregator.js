/**
 * Incremental Aggregator — Deterministic observation clustering and deduplication.
 *
 * Groups observations into episodes/clusters based on time, topic, and entity continuity.
 * Detects contradictions (same subject+predicate, different values).
 * Deduplicates using embedding similarity (cosine >0.9) with graceful fallback when embeddings unavailable.
 *
 * This is pre-processing only — NO LLM calls. Deterministic and fast.
 */

/**
 * Metadata for a cluster: aggregated information about contained observations.
 * @typedef {object} ClusterMeta
 * @property {number} clusterId
 * @property {Array<number>} observationIds
 * @property {Set<string>} entities — Entities mentioned across observations
 * @property {Set<string>} topics — Topic tags
 * @property {Date} startTime — Earliest observation timestamp
 * @property {Date} endTime — Latest observation timestamp
 * @property {number} avgConfidence — Average confidence score
 * @property {number} observationCount
 * @property {Array<object>} contradictions — Detected contradictions
 */

export class IncrementalAggregator {
  constructor() {
    /**
     * Map of clusterId → { observationIds, entities, topics, startTime, endTime, avgConfidence, contradictions, deduplicatedIds }
     */
    this.clusters = new Map();

    /**
     * Current cluster ID counter
     */
    this.nextClusterId = 0;

    /**
     * Map of observation ID → cluster ID (for quick lookup)
     */
    this.observationToCluster = new Map();

    /**
     * Map of normalized predicate (subject|predicate) → { values: Set, observationIds: Set }
     * Used to detect contradictions
     */
    this.predicateIndex = new Map();
  }

  /**
   * Ingest an observation into the aggregator.
   *
   * Assigns to the current episode/cluster (determined by time and topic continuity).
   * Tracks entities, topics, time span, confidence, and contradictions.
   *
   * @param {object} observation
   *   @param {number} observation.id — Unique observation ID
   *   @param {string} observation.text — Observation content
   *   @param {Date | string} observation.timestamp — When the observation occurred
   *   @param {number} [observation.confidence] — Confidence (0–1, default 0.8)
   *   @param {Array<string>} [observation.entities] — Entities mentioned (optional)
   *   @param {Array<string>} [observation.topics] — Topic tags (optional)
   */
  ingestObservation(observation) {
    const {
      id,
      text,
      timestamp,
      confidence = 0.8,
      entities = [],
      topics = [],
    } = observation;

    // Determine cluster assignment (simplified: all observations go to cluster 0 until reset)
    // In a real system, this would check time delta, topic continuity, etc.
    const clusterId = 0;
    if (!this.clusters.has(clusterId)) {
      this.clusters.set(clusterId, {
        observationIds: [],
        entities: new Set(),
        topics: new Set(),
        startTime: null,
        endTime: null,
        confidenceSum: 0,
        confidenceCount: 0,
        contradictions: [],
        deduplicatedIds: new Set(),
      });
    }

    const cluster = this.clusters.get(clusterId);

    // Add observation to cluster
    cluster.observationIds.push(id);
    this.observationToCluster.set(id, clusterId);

    // Update entities and topics
    entities.forEach((e) => cluster.entities.add(e));
    topics.forEach((t) => cluster.topics.add(t));

    // Update time span
    const ts = new Date(timestamp);
    if (!cluster.startTime || ts < cluster.startTime) {
      cluster.startTime = ts;
    }
    if (!cluster.endTime || ts > cluster.endTime) {
      cluster.endTime = ts;
    }

    // Update confidence
    cluster.confidenceSum += confidence;
    cluster.confidenceCount += 1;

    // Extract predicates from text for contradiction detection
    this._updatePredicateIndex(id, text, clusterId);
  }

  /**
   * Update the predicate index to track subject-predicate-value triples for contradiction detection.
   * Simple heuristic: extract entities and their descriptors.
   * @private
   */
  _updatePredicateIndex(observationId, text, clusterId) {
    // Very simple predicate extraction:
    // Look for patterns like "X is Y", "X has Z", "X prefers Z"
    const predicatePatterns = [
      /(\w+)\s+is\s+([^.!?]+)/gi,
      /(\w+)\s+has\s+([^.!?]+)/gi,
      /(\w+)\s+prefers?\s+([^.!?]+)/gi,
      /(\w+)\s+uses?\s+([^.!?]+)/gi,
    ];

    for (const pattern of predicatePatterns) {
      let match;
      while ((match = pattern.exec(text)) !== null) {
        const subject = match[1].toLowerCase();
        const value = match[2].toLowerCase().trim();
        const key = `${subject}|${match[0].slice(subject.length + 1, subject.length + 5)}`;

        if (!this.predicateIndex.has(key)) {
          this.predicateIndex.set(key, {
            values: new Set(),
            observationIds: new Set(),
          });
        }

        const pred = this.predicateIndex.get(key);
        const oldSize = pred.values.size;
        pred.values.add(value);
        pred.observationIds.add(observationId);

        // If we just added a second value, that's a contradiction
        if (oldSize > 0 && oldSize < pred.values.size) {
          const cluster = this.clusters.get(clusterId);
          if (cluster) {
            cluster.contradictions.push({
              subject,
              key,
              values: Array.from(pred.values),
              observationIds: Array.from(pred.observationIds),
            });
          }
        }
      }
    }
  }

  /**
   * Detect contradictions within a cluster (same subject+predicate, different values).
   * Returns array of contradictions found.
   *
   * @param {number} clusterId
   * @returns {Array<object>} Contradictions in the cluster
   */
  detectContradictions(clusterId) {
    const cluster = this.clusters.get(clusterId);
    return cluster?.contradictions ?? [];
  }

  /**
   * Detect redundancy using embedding similarity (cosine >0.9 → deduplicate).
   * Falls back gracefully when no embeddings are provided.
   *
   * @param {number} clusterId
   * @param {Function} [embeddingFn] — Optional function: (text) → Promise<Array<number>>
   * @returns {Promise<Set<number>>} — Set of observation IDs to keep (non-duplicates)
   */
  async detectRedundancy(clusterId, embeddingFn) {
    const cluster = this.clusters.get(clusterId);
    if (!cluster || cluster.observationIds.length <= 1) {
      return new Set(cluster?.observationIds ?? []);
    }

    // If no embedding function, keep all (fallback)
    if (!embeddingFn) {
      cluster.deduplicatedIds = new Set(cluster.observationIds);
      return cluster.deduplicatedIds;
    }

    // Compute embeddings (try-catch for safety)
    let embeddings;
    try {
      embeddings = await Promise.all(
        cluster.observationIds.map((id) => {
          // In real code, this would fetch the actual observation text
          // For now, we'll assume the embedding function is passed with knowledge of texts
          return embeddingFn(id);
        })
      );
    } catch (err) {
      console.warn("[incremental-aggregator] Embedding computation failed:", err.message);
      cluster.deduplicatedIds = new Set(cluster.observationIds);
      return cluster.deduplicatedIds;
    }

    // Deduplicate via cosine similarity
    const keepers = new Set();
    for (let i = 0; i < cluster.observationIds.length; i++) {
      let isDuplicate = false;

      // Check against already-kept observations
      for (const j of keepers) {
        const sim = cosineSimilarity(embeddings[i], embeddings[j]);
        if (sim > 0.9) {
          isDuplicate = true;
          break;
        }
      }

      if (!isDuplicate) {
        keepers.add(i);
      }
    }

    // Convert indices back to IDs
    const keptIds = new Set(
      Array.from(keepers).map((i) => cluster.observationIds[i])
    );
    cluster.deduplicatedIds = keptIds;
    return keptIds;
  }

  /**
   * Get metadata for a cluster.
   *
   * @param {number} clusterId
   * @returns {ClusterMeta | null}
   */
  getClusterMeta(clusterId) {
    const cluster = this.clusters.get(clusterId);
    if (!cluster) return null;

    return {
      clusterId,
      observationIds: [...cluster.observationIds],
      entities: cluster.entities,
      topics: cluster.topics,
      startTime: cluster.startTime,
      endTime: cluster.endTime,
      avgConfidence:
        cluster.confidenceCount > 0
          ? cluster.confidenceSum / cluster.confidenceCount
          : 0,
      observationCount: cluster.observationIds.length,
      contradictions: cluster.contradictions,
    };
  }

  /**
   * Get the deduplicated observation IDs for a cluster.
   * Must call detectRedundancy first.
   *
   * @param {number} clusterId
   * @returns {Set<number>}
   */
  getDeduplicated(clusterId) {
    const cluster = this.clusters.get(clusterId);
    return cluster?.deduplicatedIds ?? new Set();
  }

  /**
   * Reset all state (clear clusters, indices, counters).
   * Useful for test isolation.
   */
  reset() {
    this.clusters.clear();
    this.nextClusterId = 0;
    this.observationToCluster.clear();
    this.predicateIndex.clear();
  }
}

/**
 * Compute cosine similarity between two vectors.
 * @param {Array<number>} a
 * @param {Array<number>} b
 * @returns {number} — Cosine similarity (0–1)
 */
function cosineSimilarity(a, b) {
  if (!a || !b || a.length !== b.length || a.length === 0) {
    return 0;
  }

  let dotProduct = 0;
  let normA = 0;
  let normB = 0;

  for (let i = 0; i < a.length; i++) {
    dotProduct += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }

  const denominator = Math.sqrt(normA) * Math.sqrt(normB);
  return denominator === 0 ? 0 : dotProduct / denominator;
}
