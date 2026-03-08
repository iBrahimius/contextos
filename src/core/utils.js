import crypto from "node:crypto";

export function createId(prefix = "id") {
  return `${prefix}_${Date.now().toString(36)}_${crypto.randomBytes(4).toString("hex")}`;
}

export function slugify(value) {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 96);
}

export function nowIso() {
  return new Date().toISOString();
}

export function estimateTokens(text = "") {
  return Math.max(1, Math.ceil(text.trim().length / 4));
}

export function sentenceSplit(text = "") {
  return text
    .replace(/\r/g, "")
    .split(/[\n]+|(?<=[.!?])\s+/)
    .map((line) => line.trim())
    .filter(Boolean);
}

export function unique(values) {
  return [...new Set(values.filter(Boolean))];
}

export function stableJson(value) {
  return JSON.stringify(value ?? null);
}

export function parseJson(value, fallback = null) {
  if (!value) {
    return fallback;
  }

  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

export function checksum(content) {
  return crypto.createHash("sha256").update(content).digest("hex");
}

export function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

export function average(values) {
  if (!values.length) {
    return 0;
  }

  return values.reduce((sum, value) => sum + value, 0) / values.length;
}
