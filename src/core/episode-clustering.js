import { createId, nowIso } from "./utils.js";

/**
 * Detects episodes by grouping observations with time gaps.
 * An episode ends when there's a gap of > gapMinutes between consecutive observations.
 *
 * @param {Array} observations - Array of observation records, each with created_at
 * @param {number} gapMinutes - Minimum gap (in minutes) to split episodes. Default: 30
 * @returns {Array} Array of episodes: { started_at, ended_at, observations[] }
 */
export function detectEpisodes(observations, gapMinutes = 30) {
  if (!observations || observations.length === 0) {
    return [];
  }

  const gapMs = gapMinutes * 60 * 1000;
  const sorted = [...observations].sort(
    (a, b) => new Date(a.created_at) - new Date(b.created_at)
  );

  const episodes = [];
  let currentEpisode = {
    started_at: sorted[0].created_at,
    observations: [sorted[0]],
  };

  for (let i = 1; i < sorted.length; i++) {
    const prevTime = new Date(sorted[i - 1].created_at).getTime();
    const currTime = new Date(sorted[i].created_at).getTime();
    const gap = currTime - prevTime;

    if (gap > gapMs) {
      // End current episode and start a new one
      currentEpisode.ended_at = sorted[i - 1].created_at;
      episodes.push(currentEpisode);
      currentEpisode = {
        started_at: sorted[i].created_at,
        observations: [sorted[i]],
      };
    } else {
      currentEpisode.observations.push(sorted[i]);
    }
  }

  // Close the final episode
  currentEpisode.ended_at = sorted[sorted.length - 1].created_at;
  episodes.push(currentEpisode);

  return episodes;
}

/**
 * Extracts unique entity labels from an observation's metadata.
 *
 * @param {object} observation - The observation record
 * @returns {Set} Set of entity label strings
 */
function extractEntitiesFromObservation(observation) {
  const entities = new Set();

  // Direct entity references
  if (observation.subject_entity_id) {
    entities.add(observation.subject_entity_id);
  }
  if (observation.object_entity_id) {
    entities.add(observation.object_entity_id);
  }

  // Entities in metadata
  if (observation.metadata_json) {
    try {
      const metadata = typeof observation.metadata_json === "string"
        ? JSON.parse(observation.metadata_json)
        : observation.metadata_json;

      if (metadata.entities && Array.isArray(metadata.entities)) {
        for (const entity of metadata.entities) {
          if (entity.label) {
            entities.add(entity.label.toLowerCase());
          }
        }
      }
    } catch (e) {
      // Ignore JSON parse errors
    }
  }

  return entities;
}

/**
 * Computes the Jaccard similarity between two sets.
 *
 * @param {Set} setA
 * @param {Set} setB
 * @returns {number} Similarity score [0, 1]
 */
function jaccardSimilarity(setA, setB) {
  if (setA.size === 0 && setB.size === 0) {
    return 1.0;
  }

  const intersection = new Set([...setA].filter((x) => setB.has(x)));
  const union = new Set([...setA, ...setB]);

  return intersection.size / (union.size || 1);
}

/**
 * Computes cosine similarity between two Float32Arrays.
 *
 * @param {Float32Array} vecA
 * @param {Float32Array} vecB
 * @returns {number} Cosine similarity [-1, 1]
 */
function cosineSimilarity(vecA, vecB) {
  if (!vecA || !vecB || vecA.length !== vecB.length) {
    return 0;
  }

  let dotProduct = 0;
  let magnitudeA = 0;
  let magnitudeB = 0;

  for (let i = 0; i < vecA.length; i++) {
    dotProduct += vecA[i] * vecB[i];
    magnitudeA += vecA[i] * vecA[i];
    magnitudeB += vecB[i] * vecB[i];
  }

  magnitudeA = Math.sqrt(magnitudeA);
  magnitudeB = Math.sqrt(magnitudeB);

  if (magnitudeA === 0 || magnitudeB === 0) {
    return 0;
  }

  return dotProduct / (magnitudeA * magnitudeB);
}

/**
 * Detects topic shifts within an episode's observations.
 * Uses entity overlap (50% threshold) and/or embedding similarity (0.5 threshold).
 * Creates clusters with max 200 observations each.
 *
 * @param {Array} episodeObservations - Observations within a single episode
 * @param {object} options - Configuration
 * @param {object} options.embeddings - Map of observation_id -> embedding (Float32Array)
 * @param {number} options.entityOverlapThreshold - Entity overlap required to stay in cluster (default: 0.5)
 * @param {number} options.embeddingSimilarityThreshold - Centroid similarity required to stay in cluster (default: 0.5)
 * @param {number} options.maxClusterSize - Max observations per cluster (default: 200)
 * @returns {Array} Array of clusters: { topic_label, entities, topics, time_span_start, time_span_end, observations[], metadata }
 */
export function detectTopicClusters(episodeObservations, options = {}) {
  const {
    embeddings = new Map(),
    entityOverlapThreshold = 0.5,
    embeddingSimilarityThreshold = 0.5,
    maxClusterSize = 200,
  } = options;

  if (!episodeObservations || episodeObservations.length === 0) {
    return [];
  }

  const sorted = [...episodeObservations].sort(
    (a, b) => new Date(a.created_at) - new Date(b.created_at)
  );

  const clusters = [];
  let currentCluster = {
    observations: [sorted[0]],
    entitySet: extractEntitiesFromObservation(sorted[0]),
    centroid: embeddings.get(sorted[0].id),
    observationCount: 1,
  };

  for (let i = 1; i < sorted.length; i++) {
    const obs = sorted[i];
    const obsEntities = extractEntitiesFromObservation(obs);
    const obsEmbedding = embeddings.get(obs.id);

    // Check for topic shift based on entity overlap
    let shouldShift = false;
    if (currentCluster.entitySet.size > 0) {
      const overlap = jaccardSimilarity(currentCluster.entitySet, obsEntities);
      if (overlap < entityOverlapThreshold) {
        shouldShift = true;
      }
    }

    // Check for topic shift based on embedding similarity (if available)
    if (!shouldShift && currentCluster.centroid && obsEmbedding) {
      const similarity = cosineSimilarity(currentCluster.centroid, obsEmbedding);
      if (similarity < embeddingSimilarityThreshold) {
        shouldShift = true;
      }
    }

    // Also shift if cluster size exceeds max
    if (!shouldShift && currentCluster.observations.length >= maxClusterSize) {
      shouldShift = true;
    }

    if (shouldShift) {
      // Finalize and save current cluster
      clusters.push(currentCluster);
      currentCluster = {
        observations: [obs],
        entitySet: obsEntities,
        centroid: obsEmbedding,
        observationCount: 1,
      };
    } else {
      // Add to current cluster
      currentCluster.observations.push(obs);
      currentCluster.entitySet = new Set([...currentCluster.entitySet, ...obsEntities]);
      currentCluster.observationCount += 1;

      // Update centroid if we have embeddings
      if (obsEmbedding && currentCluster.centroid) {
        const count = currentCluster.observations.length;
        const newCentroid = new Float32Array(currentCluster.centroid.length);
        for (let j = 0; j < newCentroid.length; j++) {
          newCentroid[j] =
            (currentCluster.centroid[j] * (count - 1) + obsEmbedding[j]) / count;
        }
        currentCluster.centroid = newCentroid;
      } else if (obsEmbedding && !currentCluster.centroid) {
        currentCluster.centroid = obsEmbedding;
      }
    }
  }

  // Finalize the last cluster
  clusters.push(currentCluster);

  // Convert internal cluster format to output format
  return clusters.map((cluster) => {
    const timeSpanStart = cluster.observations[0].created_at;
    const timeSpanEnd = cluster.observations[cluster.observations.length - 1].created_at;

    return {
      topic_label: null, // To be assigned by caller if needed
      entities: [...cluster.entitySet],
      topics: [], // To be computed by caller if needed
      time_span_start: timeSpanStart,
      time_span_end: timeSpanEnd,
      observations: cluster.observations,
      metadata: {
        observation_count: cluster.observations.length,
        has_embeddings: Boolean(cluster.centroid),
      },
    };
  });
}

/**
 * Top-level orchestrator: clusters observations into episodes and topics.
 * Persists episodes and clusters to DB, marks observations with compressed_into.
 *
 * @param {object} db - ContextDatabase instance
 * @param {object} options - Options
 * @param {string} options.since - ISO timestamp (start of range)
 * @param {string} options.until - ISO timestamp (end of range)
 * @param {number} options.sessionGapMinutes - Episode detection gap threshold (default: 30)
 * @returns {object} { episodes_detected, clusters_detected, observations_clustered }
 */
export function clusterObservations(db, options = {}) {
  const { since, until, sessionGapMinutes = 30 } = options;

  // 1. Fetch observations in time range (using raw query since listObservations may not exist)
  const observations = db.prepare(`
    SELECT
      id,
      conversation_id,
      message_id,
      category,
      predicate,
      subject_entity_id,
      object_entity_id,
      detail,
      confidence,
      source_span,
      metadata_json,
      created_at,
      compressed_into
    FROM observations
    WHERE 1 = 1
      ${since ? 'AND created_at >= ?' : ''}
      ${until ? 'AND created_at <= ?' : ''}
    ORDER BY created_at ASC
    LIMIT 10000
  `).all(
    ...(since && until ? [since, until] : since ? [since] : until ? [until] : [])
  );

  if (observations.length === 0) {
    return {
      episodes_detected: 0,
      clusters_detected: 0,
      observations_clustered: 0,
    };
  }

  // 2. Load embeddings for observations
  const embeddings = new Map();
  for (const obs of observations) {
    const embRecord = db.getObservationEmbedding(obs.id);
    if (embRecord && embRecord.embedding) {
      embeddings.set(obs.id, embRecord.embedding);
    }
  }

  // 3. Detect episodes
  const episodes = detectEpisodes(observations, sessionGapMinutes);

  let totalClusters = 0;
  let totalClustered = 0;

  // 4. Within each episode, detect topic clusters
  for (const episode of episodes) {
    const episodeId = createId();

    // Persist episode record
    db.prepare(`
      INSERT INTO episodes (id, started_at, ended_at, session_gap_minutes, cluster_count, metadata, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(
      episodeId,
      episode.started_at,
      episode.ended_at,
      sessionGapMinutes,
      0, // Will be updated after clustering
      JSON.stringify({ manual: false }),
      nowIso()
    );

    const clusters = detectTopicClusters(episode.observations, { embeddings });
    totalClusters += clusters.length;

    // Update cluster count for episode
    db.prepare(`UPDATE episodes SET cluster_count = ? WHERE id = ?`).run(
      clusters.length,
      episodeId
    );

    // 5. Persist clusters and mark observations
    for (const cluster of clusters) {
      const clusterId = createId();

      db.prepare(`
        INSERT INTO observation_clusters (
          id, episode_id, topic_label, entities, topics,
          time_span_start, time_span_end, observation_count, created_at, metadata
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        clusterId,
        episodeId,
        cluster.topic_label,
        JSON.stringify(cluster.entities),
        JSON.stringify(cluster.topics),
        cluster.time_span_start,
        cluster.time_span_end,
        cluster.observations.length,
        nowIso(),
        JSON.stringify(cluster.metadata)
      );

      // Mark observations with cluster_id
      for (const obs of cluster.observations) {
        db.prepare(`
          UPDATE observations SET compressed_into = ? WHERE id = ?
        `).run(clusterId, obs.id);
        totalClustered += 1;
      }
    }
  }

  return {
    episodes_detected: episodes.length,
    clusters_detected: totalClusters,
    observations_clustered: totalClustered,
  };
}
