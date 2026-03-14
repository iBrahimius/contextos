export const ALERT_LEVELS = Object.freeze({
  INFO: "info",
  WARNING: "warning",
  ERROR: "error",
  CRITICAL: "critical",
});

export const ALERT_TYPES = Object.freeze({
  PIPELINE_ERROR: "pipeline_error",
  MUTATION_FAILED: "mutation_failed",
  CLASSIFICATION_FAILED: "classification_failed",
  STARTUP_OK: "startup_ok",
  STARTUP_FAILED: "startup_failed",
  EMBEDDING_ERROR: "embedding_error",
});

const DEFAULT_MIN_INTERVAL_MS = 30_000;

const VALID_LEVELS = new Set(Object.values(ALERT_LEVELS));

function normalizeNowValue(value) {
  if (value instanceof Date) {
    return value.getTime();
  }

  if (typeof value === "string") {
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  if (Number.isFinite(value)) {
    return Number(value);
  }

  return Date.now();
}

function toTimestamp(value) {
  return new Date(value).toISOString();
}

function normalizeErrorMessage(error) {
  if (error instanceof Error) {
    return error.message;
  }

  if (typeof error === "string") {
    return error;
  }

  if (error && typeof error === "object" && typeof error.message === "string") {
    return error.message;
  }

  return String(error ?? "Unknown error");
}

function normalizeWindowMs(value, fallback) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }

  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) {
    return fallback;
  }

  return Math.max(0, numericValue);
}

function defaultSendAlert(payload) {
  const logLine = `${payload.level.toUpperCase()} ${payload.type} — ${payload.message}`;
  const metadata = payload.metadata && Object.keys(payload.metadata).length
    ? payload.metadata
    : null;
  const logger = {
    [ALERT_LEVELS.INFO]: console.info,
    [ALERT_LEVELS.WARNING]: console.warn,
    [ALERT_LEVELS.ERROR]: console.error,
    [ALERT_LEVELS.CRITICAL]: console.error,
  }[payload.level] ?? console.log;

  if (metadata) {
    logger(`[push-alert] ${logLine}`, metadata);
    return;
  }

  logger(`[push-alert] ${logLine}`);
}

export class PushAlerter {
  constructor({
    sendAlert = defaultSendAlert,
    now = () => Date.now(),
    minIntervalMs = DEFAULT_MIN_INTERVAL_MS,
    dedupWindowMs = minIntervalMs,
  } = {}) {
    this.sendAlert = typeof sendAlert === "function" ? sendAlert : defaultSendAlert;
    this.now = typeof now === "function" ? now : () => Date.now();
    this.minIntervalMs = normalizeWindowMs(minIntervalMs, DEFAULT_MIN_INTERVAL_MS);
    this.dedupWindowMs = normalizeWindowMs(dedupWindowMs, this.minIntervalMs);
    this.lastAlertAtMs = 0;
    this.lastAlert = null;
    this.lastSuppressed = null;
    this.lastDispatchError = null;
    this.sentCount = 0;
    this.suppressedCount = 0;
    this.suppressedByReason = {
      rate_limited: 0,
      deduplicated: 0,
      dispatch_failed: 0,
    };
    this.dedupCache = new Map();
  }

  nowMs() {
    return normalizeNowValue(this.now());
  }

  buildPayload(level, type, message, opts = {}) {
    const normalizedLevel = String(level ?? "").trim().toLowerCase();
    if (!VALID_LEVELS.has(normalizedLevel)) {
      throw new Error(`Unsupported alert level: ${level}`);
    }

    const normalizedType = String(type ?? "").trim();
    if (!normalizedType) {
      throw new Error("Alert type is required");
    }

    const nowMs = this.nowMs();
    const metadata = opts.metadata && typeof opts.metadata === "object" && !Array.isArray(opts.metadata)
      ? { ...opts.metadata }
      : {};
    const key = String(opts.key ?? `${normalizedType}:${message}`).trim();

    return {
      level: normalizedLevel,
      type: normalizedType,
      message: String(message ?? "").trim(),
      key,
      timestamp: toTimestamp(nowMs),
      metadata,
    };
  }

  shouldSuppress(payload, nowMs) {
    const lastForKey = payload.key ? this.dedupCache.get(payload.key) ?? 0 : 0;
    if (payload.key && this.dedupWindowMs > 0 && lastForKey && nowMs - lastForKey < this.dedupWindowMs) {
      return "deduplicated";
    }

    if (this.minIntervalMs > 0 && this.lastAlertAtMs && nowMs - this.lastAlertAtMs < this.minIntervalMs) {
      return "rate_limited";
    }

    return null;
  }

  recordSuppressed(payload, reason, nowMs) {
    this.suppressedCount += 1;
    this.suppressedByReason[reason] = (this.suppressedByReason[reason] ?? 0) + 1;
    this.lastSuppressed = {
      reason,
      level: payload.level,
      type: payload.type,
      key: payload.key,
      timestamp: toTimestamp(nowMs),
    };
  }

  async alert(level, type, message, opts = {}) {
    const payload = this.buildPayload(level, type, message, opts);
    const nowMs = Date.parse(payload.timestamp);
    const suppressionReason = this.shouldSuppress(payload, nowMs);

    if (suppressionReason) {
      this.recordSuppressed(payload, suppressionReason, nowMs);
      return {
        dispatched: false,
        reason: suppressionReason,
        payload,
      };
    }

    this.lastAlertAtMs = nowMs;
    this.lastAlert = payload;
    this.sentCount += 1;
    if (payload.key) {
      this.dedupCache.set(payload.key, nowMs);
    }

    try {
      await Promise.resolve(this.sendAlert(payload));
      this.lastDispatchError = null;
      return {
        dispatched: true,
        payload,
      };
    } catch (error) {
      const messageText = normalizeErrorMessage(error);
      this.suppressedByReason.dispatch_failed += 1;
      this.lastDispatchError = {
        message: messageText,
        timestamp: toTimestamp(this.nowMs()),
      };
      console.error(`[push-alert] Failed to dispatch ${payload.type}: ${messageText}`);
      return {
        dispatched: false,
        reason: "dispatch_failed",
        error,
        payload,
      };
    }
  }

  getStatus() {
    return {
      minIntervalMs: this.minIntervalMs,
      dedupWindowMs: this.dedupWindowMs,
      sentCount: this.sentCount,
      suppressedCount: this.suppressedCount,
      suppressedByReason: { ...this.suppressedByReason },
      lastAlertAt: this.lastAlert?.timestamp ?? null,
      lastAlert: this.lastAlert,
      lastSuppressed: this.lastSuppressed,
      dedupKeyCount: this.dedupCache.size,
      dedupKeys: [...this.dedupCache.keys()],
      lastDispatchError: this.lastDispatchError,
    };
  }

  resetDedup() {
    this.dedupCache.clear();
    return this.getStatus();
  }

  pipelineError(stage, error, opts = {}) {
    const stageName = String(stage ?? "unknown_stage").trim() || "unknown_stage";
    const errorMessage = normalizeErrorMessage(error);
    return this.alert(
      ALERT_LEVELS.ERROR,
      ALERT_TYPES.PIPELINE_ERROR,
      `Pipeline error in stage \"${stageName}\": ${errorMessage}`,
      {
        key: opts.key ?? `pipeline:${stageName}`,
        metadata: {
          stage: stageName,
          error: errorMessage,
          ...(opts.metadata ?? {}),
        },
      },
    );
  }

  mutationFailed(mutationId, error, opts = {}) {
    const mutationKey = String(mutationId ?? "unknown_mutation").trim() || "unknown_mutation";
    const errorMessage = normalizeErrorMessage(error);
    return this.alert(
      ALERT_LEVELS.WARNING,
      ALERT_TYPES.MUTATION_FAILED,
      `Mutation ${mutationKey} failed: ${errorMessage}`,
      {
        key: opts.key ?? `mutation:${mutationKey}`,
        metadata: {
          mutationId: mutationKey,
          error: errorMessage,
          ...(opts.metadata ?? {}),
        },
      },
    );
  }

  classificationFailed(stage, error, opts = {}) {
    const stageName = String(stage ?? "classification").trim() || "classification";
    const errorMessage = normalizeErrorMessage(error);
    return this.alert(
      ALERT_LEVELS.ERROR,
      ALERT_TYPES.CLASSIFICATION_FAILED,
      `Classification failed in stage \"${stageName}\": ${errorMessage}`,
      {
        key: opts.key ?? `classification:${stageName}`,
        metadata: {
          stage: stageName,
          error: errorMessage,
          ...(opts.metadata ?? {}),
        },
      },
    );
  }

  startupOk(eventCount, pendingMutations, opts = {}) {
    return this.alert(
      ALERT_LEVELS.INFO,
      ALERT_TYPES.STARTUP_OK,
      "ContextOS startup complete",
      {
        key: opts.key ?? ALERT_TYPES.STARTUP_OK,
        metadata: {
          eventCount: Number(eventCount ?? 0),
          pendingMutations: Number(pendingMutations ?? 0),
          ...(opts.metadata ?? {}),
        },
      },
    );
  }

  startupFailed(error, opts = {}) {
    const errorMessage = normalizeErrorMessage(error);
    return this.alert(
      ALERT_LEVELS.CRITICAL,
      ALERT_TYPES.STARTUP_FAILED,
      `ContextOS startup failed: ${errorMessage}`,
      {
        key: opts.key ?? ALERT_TYPES.STARTUP_FAILED,
        metadata: {
          error: errorMessage,
          ...(opts.metadata ?? {}),
        },
      },
    );
  }

  embeddingError(target, error, opts = {}) {
    const targetName = String(target ?? "embedding").trim() || "embedding";
    const errorMessage = normalizeErrorMessage(error);
    return this.alert(
      ALERT_LEVELS.ERROR,
      ALERT_TYPES.EMBEDDING_ERROR,
      `Embedding error for \"${targetName}\": ${errorMessage}`,
      {
        key: opts.key ?? `embedding:${targetName}`,
        metadata: {
          target: targetName,
          error: errorMessage,
          ...(opts.metadata ?? {}),
        },
      },
    );
  }
}
