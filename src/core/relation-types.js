export const RELATION_REGISTRY = {
  part_of: {
    inverse: "has_part",
    symmetric: false,
    directed: true,
    transitive: true,
    defaultWeight: 1.0,
    description: "Subject is a component/member of object",
  },
  has_part: {
    inverse: "part_of",
    symmetric: false,
    directed: true,
    transitive: true,
    defaultWeight: 1.0,
    description: "Subject contains/owns object as component",
  },
  depends_on: {
    inverse: "depended_on_by",
    symmetric: false,
    directed: true,
    transitive: false,
    defaultWeight: 0.95,
    description: "Subject requires object to function",
  },
  depended_on_by: {
    inverse: "depends_on",
    symmetric: false,
    directed: true,
    transitive: false,
    defaultWeight: 0.95,
    description: "Subject is required by object",
  },
  integrates_with: {
    inverse: "integrates_with",
    symmetric: true,
    directed: false,
    transitive: false,
    defaultWeight: 0.9,
    description: "Bidirectional integration between subject and object",
  },
  owns: {
    inverse: "owned_by",
    symmetric: false,
    directed: true,
    transitive: false,
    defaultWeight: 0.92,
    description: "Subject owns/controls object",
  },
  owned_by: {
    inverse: "owns",
    symmetric: false,
    directed: true,
    transitive: false,
    defaultWeight: 0.92,
    description: "Subject is owned/controlled by object",
  },
  stores_in: {
    inverse: "stores",
    symmetric: false,
    directed: true,
    transitive: false,
    defaultWeight: 0.88,
    description: "Subject persists data in object",
  },
  stores: {
    inverse: "stores_in",
    symmetric: false,
    directed: true,
    transitive: false,
    defaultWeight: 0.88,
    description: "Subject is a storage target for object",
  },
  captures: {
    inverse: "captured_by",
    symmetric: false,
    directed: true,
    transitive: false,
    defaultWeight: 0.84,
    description: "Subject captures/records object",
  },
  captured_by: {
    inverse: "captures",
    symmetric: false,
    directed: true,
    transitive: false,
    defaultWeight: 0.84,
    description: "Subject is captured/recorded by object",
  },
  indexes: {
    inverse: "indexed_by",
    symmetric: false,
    directed: true,
    transitive: false,
    defaultWeight: 0.82,
    description: "Subject indexes/catalogs object",
  },
  indexed_by: {
    inverse: "indexes",
    symmetric: false,
    directed: true,
    transitive: false,
    defaultWeight: 0.82,
    description: "Subject is indexed/cataloged by object",
  },
  retrieves: {
    inverse: "retrieved_by",
    symmetric: false,
    directed: true,
    transitive: false,
    defaultWeight: 0.84,
    description: "Subject retrieves data from object",
  },
  retrieved_by: {
    inverse: "retrieves",
    symmetric: false,
    directed: true,
    transitive: false,
    defaultWeight: 0.84,
    description: "Subject provides data to object",
  },
  related_to: {
    inverse: "related_to",
    symmetric: true,
    directed: false,
    transitive: false,
    defaultWeight: 0.68,
    description: "General undirected relationship",
  },
};

export function getRelationType(predicate) {
  return RELATION_REGISTRY[predicate] ?? RELATION_REGISTRY.related_to;
}

export function getInverse(predicate) {
  return RELATION_REGISTRY[predicate]?.inverse ?? "related_to";
}

export function getWeight(predicate) {
  return RELATION_REGISTRY[predicate]?.defaultWeight ?? 0.5;
}

export function isSymmetric(predicate) {
  return RELATION_REGISTRY[predicate]?.symmetric ?? false;
}

export function isTransitive(predicate) {
  return RELATION_REGISTRY[predicate]?.transitive ?? false;
}
