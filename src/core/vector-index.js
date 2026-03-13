function clamp(value, minimum, maximum) {
  return Math.min(maximum, Math.max(minimum, value));
}

function swap(entries, leftIndex, rightIndex) {
  const value = entries[leftIndex];
  entries[leftIndex] = entries[rightIndex];
  entries[rightIndex] = value;
}

function nthElementByDistance(entries, targetIndex) {
  let left = 0;
  let right = entries.length - 1;

  while (left < right) {
    const pivot = entries[left + ((right - left) >> 1)].distance;
    let lower = left;
    let cursor = left;
    let upper = right;

    while (cursor <= upper) {
      if (entries[cursor].distance < pivot) {
        swap(entries, lower, cursor);
        lower += 1;
        cursor += 1;
      } else if (entries[cursor].distance > pivot) {
        swap(entries, cursor, upper);
        upper -= 1;
      } else {
        cursor += 1;
      }
    }

    if (targetIndex < lower) {
      right = lower - 1;
      continue;
    }

    if (targetIndex > upper) {
      left = upper + 1;
      continue;
    }

    return;
  }
}

function sampleIndices(length, count) {
  const resolvedCount = Math.max(0, Math.min(length, Math.trunc(count) || 0));
  const indices = [];
  const seen = new Set();

  while (indices.length < resolvedCount) {
    const candidate = Math.floor(Math.random() * length);
    if (seen.has(candidate)) {
      continue;
    }

    seen.add(candidate);
    indices.push(candidate);
  }

  return indices;
}

function compareResults(left, right) {
  const distanceDelta = Number(left.distance ?? 0) - Number(right.distance ?? 0);
  if (distanceDelta !== 0) {
    return distanceDelta;
  }

  const scoreDelta = Number(right.score ?? 0) - Number(left.score ?? 0);
  if (scoreDelta !== 0) {
    return scoreDelta;
  }

  return String(left.id ?? "").localeCompare(String(right.id ?? ""));
}

function createLeaf(item) {
  return {
    item,
    threshold: 0,
    left: null,
    right: null,
    size: 1,
    height: 1,
  };
}

function refreshNode(node) {
  if (!node) {
    return null;
  }

  node.size = 1 + (node.left?.size ?? 0) + (node.right?.size ?? 0);
  node.height = 1 + Math.max(node.left?.height ?? 0, node.right?.height ?? 0);
  return node;
}

function normalizeVector(embedding, dimensions) {
  if (!embedding?.length || embedding.length !== dimensions) {
    return null;
  }

  const vector = embedding instanceof Float32Array
    ? embedding
    : Float32Array.from(embedding);
  let norm = 0;

  for (let index = 0; index < vector.length; index += 1) {
    norm += vector[index] * vector[index];
  }

  const magnitude = Math.sqrt(norm);
  const unit = new Float32Array(vector.length);

  if (magnitude) {
    for (let index = 0; index < vector.length; index += 1) {
      unit[index] = vector[index] / magnitude;
    }
  }

  return {
    unit,
    hasMagnitude: magnitude > 0,
  };
}

function createStoredItem(rawItem, dimensions) {
  const id = String(rawItem?.id ?? "").trim();
  const type = String(rawItem?.type ?? "").trim();
  if (!id || !type) {
    return null;
  }

  const vector = normalizeVector(rawItem.embedding, dimensions);
  if (!vector) {
    return null;
  }

  return {
    ...vector,
    id,
    type,
    scopeKind: rawItem.scopeKind ?? rawItem.scope_kind ?? null,
    scopeId: rawItem.scopeId ?? rawItem.scope_id ?? null,
  };
}

function prepareQueryVector(embedding, dimensions) {
  return normalizeVector(embedding, dimensions);
}

function dotProduct(left, right) {
  let total = 0;
  for (let index = 0; index < left.length; index += 1) {
    total += left[index] * right[index];
  }
  return total;
}

function unitDistance(left, right) {
  let total = 0;
  for (let index = 0; index < left.length; index += 1) {
    const delta = left[index] - right[index];
    total += delta * delta;
  }
  return Math.sqrt(total);
}

export class VectorIndex {
  constructor(dimensions = 768) {
    this.dimensions = Math.max(1, Math.trunc(dimensions) || 768);
    this.root = null;
    this.itemsById = new Map();
    this.needsRebuild = false;
  }

  get size() {
    return this.itemsById.size;
  }

  rebuild(items) {
    const storedItems = [];
    const deduped = new Map();

    for (const item of items ?? []) {
      const storedItem = createStoredItem(item, this.dimensions);
      if (!storedItem) {
        continue;
      }

      deduped.set(storedItem.id, storedItem);
    }

    for (const item of deduped.values()) {
      storedItems.push(item);
    }

    this._rebuildFromStoredItems(storedItems);
    return this.size;
  }

  insert(id, type, embedding, metadata = null) {
    const storedItem = createStoredItem({
      id,
      type,
      embedding,
      ...(metadata ?? {}),
    }, this.dimensions);
    if (!storedItem) {
      return null;
    }

    this.itemsById.set(storedItem.id, storedItem);
    this._rebuildFromStoredItems([...this.itemsById.values()]);
    return storedItem;
  }

  remove(id) {
    const key = String(id ?? "").trim();
    if (!key || !this.itemsById.has(key)) {
      return false;
    }

    this.itemsById.delete(key);
    this._rebuildFromStoredItems([...this.itemsById.values()]);
    return true;
  }

  query(embedding, k = 50, threshold = 0.3, filter = null) {
    if (this.needsRebuild) {
      this._rebuildFromStoredItems([...this.itemsById.values()]);
    }

    const limit = Math.max(0, Math.trunc(k) || 0);
    const preparedQuery = prepareQueryVector(embedding, this.dimensions);
    if (!this.root || !preparedQuery || limit === 0) {
      return [];
    }

    const minimumScore = clamp(Number.isFinite(threshold) ? threshold : 0.3, -1, 1);
    const predicate = typeof filter === "function" ? filter : null;
    const maxDistance = Math.sqrt(Math.max(0, 2 - 2 * minimumScore));
    const best = [];

    this._search(this.root, preparedQuery, {
      best,
      limit,
      maxDistance,
      minimumScore,
      predicate,
    });

    return best
      .sort((left, right) =>
        Number(right.score ?? 0) - Number(left.score ?? 0)
        || Number(left.distance ?? 0) - Number(right.distance ?? 0)
        || String(left.id ?? "").localeCompare(String(right.id ?? "")))
      .map(({ id, type, score }) => ({
        id,
        type,
        score,
      }));
  }

  _rebuildFromStoredItems(storedItems) {
    this.itemsById = new Map(storedItems.map((item) => [item.id, item]));
    this.root = this._buildTree(storedItems);
    this.needsRebuild = false;
  }

  _buildTree(items) {
    if (!items.length) {
      return null;
    }

    if (items.length === 1) {
      return createLeaf(items[0]);
    }

    const { index: vantageIndex, distances } = this._chooseVantagePoint(items);
    const vantagePoint = items[vantageIndex];
    const entries = [];

    for (let index = 0; index < items.length; index += 1) {
      if (index === vantageIndex) {
        continue;
      }

      entries.push({
        item: items[index],
        distance: Number(distances[index] ?? 0),
      });
    }

    if (!entries.length) {
      return createLeaf(vantagePoint);
    }

    const medianIndex = Math.floor(entries.length / 2);
    nthElementByDistance(entries, medianIndex);
    const threshold = Number(entries[medianIndex]?.distance ?? 0);
    const left = [];
    const equal = [];
    const right = [];

    for (const entry of entries) {
      if (entry.distance < threshold) {
        left.push(entry.item);
      } else if (entry.distance > threshold) {
        right.push(entry.item);
      } else {
        equal.push(entry.item);
      }
    }

    while (left.length < medianIndex && equal.length) {
      left.push(equal.shift());
    }

    const node = {
      item: vantagePoint,
      threshold,
      left: this._buildTree(left),
      right: this._buildTree([...equal, ...right]),
      size: 1,
      height: 1,
    };

    return refreshNode(node);
  }

  _chooseVantagePoint(items) {
    const candidateIndices = sampleIndices(items.length, Math.min(5, items.length));
    let bestIndex = candidateIndices[0] ?? 0;
    let bestSpread = Number.NEGATIVE_INFINITY;
    let bestDistances = [];

    for (const candidateIndex of candidateIndices) {
      const candidate = items[candidateIndex];
      const distances = new Array(items.length).fill(0);
      let count = 0;
      let mean = 0;
      let sumSquares = 0;

      for (let index = 0; index < items.length; index += 1) {
        if (index === candidateIndex) {
          continue;
        }

        const distance = this._distanceBetweenItems(candidate, items[index]);
        distances[index] = distance;
        count += 1;
        mean += distance;
        sumSquares += distance * distance;
      }

      const resolvedMean = count ? mean / count : 0;
      const variance = count ? (sumSquares / count) - (resolvedMean * resolvedMean) : 0;

      if (variance > bestSpread) {
        bestIndex = candidateIndex;
        bestSpread = variance;
        bestDistances = distances;
      }
    }

    return {
      index: bestIndex,
      distances: bestDistances,
    };
  }

  _search(node, query, options) {
    if (!node) {
      return;
    }

    const distance = this._distanceToQuery(query, node.item);
    const score = this._scoreQuery(query, node.item);

    if (
      score >= options.minimumScore
      && distance <= options.maxDistance
      && (!options.predicate || options.predicate(node.item))
    ) {
      this._pushResult(options.best, {
        id: node.item.id,
        type: node.item.type,
        score,
        distance,
      }, options.limit);
    }

    const radius = this._searchRadius(options.best, options.limit, options.maxDistance);
    if (distance < node.threshold) {
      if (node.left && distance - radius <= node.threshold) {
        this._search(node.left, query, options);
      }

      if (node.right && distance + radius >= node.threshold) {
        this._search(node.right, query, options);
      }

      return;
    }

    if (node.right && distance + radius >= node.threshold) {
      this._search(node.right, query, options);
    }

    if (node.left && distance - radius <= node.threshold) {
      this._search(node.left, query, options);
    }
  }

  _pushResult(best, entry, limit) {
    let low = 0;
    let high = best.length;

    while (low < high) {
      const middle = low + ((high - low) >> 1);
      if (compareResults(entry, best[middle]) < 0) {
        high = middle;
      } else {
        low = middle + 1;
      }
    }

    best.splice(low, 0, entry);
    if (best.length > limit) {
      best.pop();
    }
  }

  _searchRadius(best, limit, maxDistance) {
    const worstDistance = best.length < limit
      ? Number.POSITIVE_INFINITY
      : Number(best.at(-1)?.distance ?? Number.POSITIVE_INFINITY);
    return Math.min(maxDistance, worstDistance);
  }

  _distanceBetweenItems(left, right) {
    return unitDistance(left.unit, right.unit);
  }

  _distanceToQuery(query, item) {
    return unitDistance(query.unit, item.unit);
  }

  _scoreQuery(query, item) {
    if (!query.hasMagnitude || !item.hasMagnitude) {
      return 0;
    }

    return clamp(dotProduct(query.unit, item.unit), -1, 1);
  }
}
