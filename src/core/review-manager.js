/**
 * Automated mutation review scheduler for ContextOS.
 *
 * The initial implementation focuses on trigger evaluation, quiet-hours
 * deferral, persisted runtime state, and deterministic queue summaries.
 *
 * @module review-manager
 */

const DEFAULT_COUNT_THRESHOLD = 50;
const DEFAULT_REVIEW_INTERVAL_MS = 2 * 60 * 60 * 1000;
const DEFAULT_MORNING_HOUR = 8;
const DEFAULT_END_OF_DAY_HOUR = 22;
const DEFAULT_END_OF_DAY_MINUTE = 30;
const DEFAULT_AUTO_APPLY_MIN_CONFIDENCE = 0.85;
const DEFAULT_AUTO_APPLY_TYPES = ["fact", "relationship"];
const DEFAULT_AUTO_EXPIRE_DAYS = 30;
const QUIET_HOURS_START = 23;
const QUIET_HOURS_END = 8;
const DAY_IN_MS = 24 * 60 * 60 * 1000;
const AUTO_APPLY_REASON = "auto_applied: high_confidence_observation";

function defaultNow() {
  return Date.now();
}

function defaultSetTimeout(callback, delay) {
  return setTimeout(callback, delay);
}

function defaultClearTimeout(handle) {
  clearTimeout(handle);
}

function toTimestamp(value) {
  if (value instanceof Date) {
    return value.getTime();
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  const parsed = Date.parse(String(value ?? ""));
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeMutationType(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/^add_/, "");
}

function normalizeConfidence(value, fallback = DEFAULT_AUTO_APPLY_MIN_CONFIDENCE) {
  if (value === null || value === undefined || String(value).trim() === "") {
    return fallback;
  }

  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }

  return Math.min(1, Math.max(0, numeric));
}

function normalizeNonNegativeInteger(value, fallback = DEFAULT_AUTO_EXPIRE_DAYS) {
  if (value === null || value === undefined || String(value).trim() === "") {
    return fallback;
  }

  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }

  return Math.max(0, Math.trunc(numeric));
}

function normalizeTypeList(value, fallback = DEFAULT_AUTO_APPLY_TYPES) {
  if (value === null || value === undefined) {
    return [...fallback];
  }

  const values = Array.isArray(value) ? value : String(value).split(",");
  return Array.from(new Set(values
    .map((entry) => normalizeMutationType(entry))
    .filter(Boolean)));
}

function expandProposalTypes(types = []) {
  return Array.from(new Set(types.flatMap((type) => {
    const normalized = normalizeMutationType(type);
    if (!normalized) {
      return [];
    }

    return [normalized, `add_${normalized}`];
  })));
}

function nextLocalTime(nowMs, hour, minute = 0) {
  const target = new Date(nowMs);
  target.setHours(hour, minute, 0, 0);

  if (target.getTime() <= nowMs) {
    target.setDate(target.getDate() + 1);
  }

  return target;
}

/**
 * Review manager with persisted state and timer-driven triggers.
 */
export class ReviewManager {
  /**
   * @param {object} options
   * @param {import("./context-os.js").ContextOS} options.contextOS
   * @param {import("../db/database.js").ContextDatabase} options.database
   * @param {() => number} [options.now]
   * @param {(callback: Function, delay: number) => any} [options.setTimeout]
   * @param {(handle: any) => void} [options.clearTimeout]
   * @param {(task: Promise<unknown>) => Promise<unknown>} [options.registerTask]
   * @param {Console} [options.logger]
   * @param {number} [options.countThreshold]
   * @param {number} [options.reviewIntervalMs]
   * @param {number} [options.morningHour]
   * @param {number} [options.endOfDayHour]
   * @param {number} [options.endOfDayMinute]
   * @param {(input: object) => Promise<object>|object} [options.processReview]
   * @param {string} [options.actorId]
   * @param {number} [options.autoApplyMinConfidence]
   * @param {string[]|string} [options.autoApplyTypes]
   * @param {number} [options.autoExpireDays]
   */
  constructor({
    contextOS,
    database,
    now = defaultNow,
    setTimeout = defaultSetTimeout,
    clearTimeout = defaultClearTimeout,
    registerTask = null,
    logger = console,
    countThreshold = DEFAULT_COUNT_THRESHOLD,
    reviewIntervalMs = DEFAULT_REVIEW_INTERVAL_MS,
    morningHour = DEFAULT_MORNING_HOUR,
    endOfDayHour = DEFAULT_END_OF_DAY_HOUR,
    endOfDayMinute = DEFAULT_END_OF_DAY_MINUTE,
    processReview = null,
    actorId = "review-manager",
    autoApplyMinConfidence = DEFAULT_AUTO_APPLY_MIN_CONFIDENCE,
    autoApplyTypes = DEFAULT_AUTO_APPLY_TYPES,
    autoExpireDays = DEFAULT_AUTO_EXPIRE_DAYS,
  }) {
    this.contextOS = contextOS;
    this.database = database;
    this.now = now;
    this._setTimeout = setTimeout;
    this._clearTimeout = clearTimeout;
    this.registerTask = registerTask;
    this.logger = logger;
    this.countThreshold = countThreshold;
    this.reviewIntervalMs = reviewIntervalMs;
    this.morningHour = morningHour;
    this.endOfDayHour = endOfDayHour;
    this.endOfDayMinute = endOfDayMinute;
    this.processReview = processReview;
    this.actorId = actorId;
    this.autoApplyMinConfidence = normalizeConfidence(autoApplyMinConfidence, DEFAULT_AUTO_APPLY_MIN_CONFIDENCE);
    this.autoApplyTypes = normalizeTypeList(autoApplyTypes, DEFAULT_AUTO_APPLY_TYPES);
    this.autoExpireDays = normalizeNonNegativeInteger(autoExpireDays, DEFAULT_AUTO_EXPIRE_DAYS);
    this.started = false;
    this._activeReviewPromise = null;
    this._pendingAutomaticTriggerPromise = null;
    this._timers = {
      morning: null,
      endOfDay: null,
      timeThreshold: null,
    };
    this._scheduledAt = {
      morning: null,
      endOfDay: null,
      timeThreshold: null,
    };
  }

  /**
   * Reset crash state and arm review timers.
   *
   * @returns {object}
   */
  start() {
    if (this.started || this.database.closed) {
      return this.getStatus();
    }

    this.database.resetReviewInProgress();
    this.started = true;
    this._scheduleDailyTrigger("morning");
    this._scheduleDailyTrigger("endOfDay");
    this._scheduleTimeThreshold();
    return this.getStatus();
  }

  /**
   * Stop timers and wait for any in-flight review to settle.
   *
   * @returns {Promise<void>}
   */
  async stop() {
    this.started = false;
    this._clearTimer("morning");
    this._clearTimer("endOfDay");
    this._clearTimer("timeThreshold");
    await this.waitForIdle();
  }

  /**
   * Wait for the current review run to finish.
   *
   * @returns {Promise<void>}
   */
  async waitForIdle() {
    if (!this._activeReviewPromise) {
      return;
    }

    await this._activeReviewPromise.catch(() => {});
  }

  /**
   * Record newly queued mutations and evaluate automatic triggers.
   *
   * @param {object} [options]
   * @param {number} [options.count=1]
   * @param {string} [options.source="mutation_queue"]
   * @returns {object}
   */
  noteQueuedMutations({ count = 1, source = "mutation_queue" } = {}) {
    const state = this.database.incrementReviewMutationCount(count);
    const queueStats = this.database.getPendingGraphProposalStats();

    if (this.started && !this.database.closed) {
      this._scheduleTimeThreshold(state, queueStats);
      const evaluation = this._evaluateTriggers({ state, queueStats });
      if (evaluation.countThresholdReached || evaluation.timeThresholdReached) {
        this._enqueueAutomaticTrigger({
          source,
          reason: evaluation.countThresholdReached ? "mutation_count_threshold" : "time_threshold",
          respectQuietHours: true,
        });
      }
    }

    return this.getStatus();
  }

  /**
   * Return current persisted state plus computed schedule metadata.
   *
   * @returns {object}
   */
  getStatus() {
    if (this.database.closed) {
      return {
        started: false,
        database_closed: true,
      };
    }

    const state = this.database.getReviewState();
    const queueStats = this.database.getPendingGraphProposalStats();
    const evaluation = this._evaluateTriggers({ state, queueStats });

    return {
      started: this.started,
      last_review_at: state.lastReviewAt,
      mutations_since_last_review: state.mutationsSinceLastReview,
      review_in_progress: state.reviewInProgress,
      pending_queue_total: queueStats.total,
      oldest_pending_created_at: queueStats.oldestCreatedAt,
      newest_pending_created_at: queueStats.newestCreatedAt,
      due_reasons: evaluation.dueReasons,
      quiet_hours: {
        active: evaluation.quietHoursActive,
        queued_for_morning: evaluation.queuedForMorning,
        next_morning_at: evaluation.nextMorningAt,
      },
      triggers: {
        count_threshold_reached: evaluation.countThresholdReached,
        time_threshold_reached: evaluation.timeThresholdReached,
        count_threshold: this.countThreshold,
        review_interval_ms: this.reviewIntervalMs,
      },
      scheduled: {
        morning_at: this._scheduledAt.morning,
        end_of_day_at: this._scheduledAt.endOfDay,
        time_threshold_at: this._scheduledAt.timeThreshold,
        next_at: this._nextScheduledAt(),
      },
    };
  }

  /**
   * Trigger a review run.
   *
   * Manual triggers bypass quiet hours; automated triggers do not.
   *
   * @param {object} [options]
   * @param {string} [options.source="manual"]
   * @param {string|null} [options.reason=null]
   * @param {boolean} [options.force]
   * @param {boolean} [options.respectQuietHours]
   * @returns {Promise<object>}
   */
  async trigger({
    source = "manual",
    reason = null,
    force = source === "manual",
    respectQuietHours = source !== "manual",
  } = {}) {
    if (this.database.closed) {
      return {
        ok: false,
        status: "stopped",
        reason: "database_closed",
      };
    }

    const state = this.database.getReviewState();
    const queueStats = this.database.getPendingGraphProposalStats();
    const evaluation = this._evaluateTriggers({ state, queueStats });
    const triggerReason = reason ?? evaluation.dueReasons[0] ?? null;
    const shouldRun = force || Boolean(triggerReason);

    if (!shouldRun) {
      return {
        ok: true,
        status: "idle",
        due_reasons: evaluation.dueReasons,
        review_state: this.getStatus(),
      };
    }

    if (respectQuietHours && evaluation.quietHoursActive) {
      return {
        ok: true,
        status: "queued_for_morning",
        trigger: {
          source,
          reason: triggerReason,
        },
        run_at: evaluation.nextMorningAt,
        review_state: this.getStatus(),
      };
    }

    if (!this.database.tryStartReviewRun()) {
      return {
        ok: true,
        status: "skipped",
        reason: "review_in_progress",
        trigger: {
          source,
          reason: triggerReason,
        },
        review_state: this.getStatus(),
      };
    }

    const reviewStartedAt = new Date(this.now()).toISOString();
    const triggerDetails = {
      source,
      reason: triggerReason,
      forced: Boolean(force),
      due_reasons: evaluation.dueReasons,
    };

    const task = Promise.resolve()
      .then(() => this._runReviewProcessor({
        trigger: triggerDetails,
        reviewStartedAt,
        state,
        queueStats,
      }))
      .then((review) => {
        const reviewedAt = new Date(this.now()).toISOString();
        const nextState = this.database.completeReviewRun({
          lastReviewAt: reviewedAt,
          mutationsSinceLastReview: 0,
        });
        this._scheduleTimeThreshold(nextState);
        this._logInfo(`[review-manager] completed ${triggerReason} review with ${queueStats.total} queued mutations`);

        return {
          ok: true,
          status: "completed",
          trigger: triggerDetails,
          review_started_at: reviewStartedAt,
          reviewed_at: reviewedAt,
          review_state: {
            ...this.getStatus(),
            last_review_at: nextState.lastReviewAt,
            mutations_since_last_review: nextState.mutationsSinceLastReview,
            review_in_progress: nextState.reviewInProgress,
          },
          review,
        };
      })
      .catch((error) => {
        this.database.resetReviewInProgress();
        this._scheduleTimeThreshold();
        this._logError(`[review-manager] review failed: ${error.message}`);
        throw error;
      })
      .finally(() => {
        if (this._activeReviewPromise === task) {
          this._activeReviewPromise = null;
        }
      });

    this._activeReviewPromise = task;
    return task;
  }

  _runReviewProcessor(input) {
    if (typeof this.processReview === "function") {
      return this.processReview(input);
    }

    const parkedCount = Number(this.database.countGraphProposals({
      statuses: ["pending", "proposed"],
      queueBucket: "parked",
    }) ?? 0);
    const reviewStartedAtMs = toTimestamp(input.reviewStartedAt) ?? this.now();
    const autoApplyProposalTypes = expandProposalTypes(this.autoApplyTypes);
    const autoApplyMutationIds = autoApplyProposalTypes.length
      ? this.database.listGraphProposals({
        statuses: ["proposed"],
        writeClasses: ["ai_proposed"],
        minConfidence: this.autoApplyMinConfidence,
        proposalTypes: autoApplyProposalTypes,
        sort: "oldest",
        limit: null,
      }).map((row) => row.id)
      : [];
    const expiredCreatedBefore = new Date(reviewStartedAtMs - (this.autoExpireDays * DAY_IN_MS)).toISOString();
    const expiredParkedMutationIds = this.database.listGraphProposals({
      statuses: ["proposed"],
      queueBucket: "parked",
      createdBefore: expiredCreatedBefore,
      sort: "oldest",
      limit: null,
    }).map((row) => row.id);
    const autoApplied = autoApplyMutationIds.length
      ? this.contextOS.reviewMutations({
        action: "apply_batch",
        mutationIds: autoApplyMutationIds,
        reason: AUTO_APPLY_REASON,
        actorId: this.actorId,
      })
      : this._emptyBatchReview("apply_batch", "accepted", AUTO_APPLY_REASON);
    const autoExpireReason = this._getAutoExpireReason();
    const autoExpired = expiredParkedMutationIds.length
      ? this.contextOS.reviewMutations({
        action: "reject_batch",
        mutationIds: expiredParkedMutationIds,
        reason: autoExpireReason,
        actorId: this.actorId,
      })
      : this._emptyBatchReview("reject_batch", "rejected", autoExpireReason);
    const parkedTotal = parkedCount;
    const reviewedTotal = Math.max(0, Number(input.queueStats?.total ?? this.database.getPendingGraphProposalStats().total));
    const actionableTotal = Math.max(0, reviewedTotal - parkedTotal);
    const remainingTotal = Math.max(0, reviewedTotal - autoApplied.count - autoExpired.count);
    const autoApplyLabel = this.autoApplyTypes.length ? this.autoApplyTypes.join("/") : "configured";

    this._logInfo(`[review-manager] auto-applied ${autoApplied.count} ${autoApplyLabel} mutations, auto-expired ${autoExpired.count} parked mutations`);

    return {
      action: "auto_review_policy",
      reviewed_total: reviewedTotal,
      actionable_total: actionableTotal,
      parked_total: parkedTotal,
      remaining_total: remainingTotal,
      auto_apply_min_confidence: this.autoApplyMinConfidence,
      auto_apply_types: [...this.autoApplyTypes],
      auto_expire_days: this.autoExpireDays,
      auto_applied: {
        count: autoApplied.count,
        reason: AUTO_APPLY_REASON,
        mutation_ids: autoApplyMutationIds,
        mutations: autoApplied.mutations,
        results: autoApplied.results,
      },
      auto_expired: {
        count: autoExpired.count,
        reason: autoExpireReason,
        mutation_ids: expiredParkedMutationIds,
        mutations: autoExpired.mutations,
        results: autoExpired.results,
      },
      summary: {
        auto_applied: autoApplied.count,
        auto_expired: autoExpired.count,
        remaining: remainingTotal,
      },
    };
  }

  _getAutoExpireReason() {
    return `auto_expired: parked_over_${this.autoExpireDays}_days`;
  }

  _emptyBatchReview(action, status, reason) {
    return {
      ok: true,
      action,
      status,
      reason,
      mutation_ids: [],
      count: 0,
      mutations: [],
      results: [],
    };
  }

  _evaluateTriggers({ state = null, queueStats = null } = {}) {
    const resolvedState = state ?? this.database.getReviewState();
    const resolvedQueueStats = queueStats ?? this.database.getPendingGraphProposalStats();
    const nowMs = this.now();
    const dueReasons = [];
    const countThresholdReached = resolvedState.mutationsSinceLastReview >= this.countThreshold;
    const timeThresholdReached = this._isTimeThresholdDue(nowMs, resolvedState, resolvedQueueStats);
    const quietHoursActive = this._isQuietHours(nowMs);

    if (countThresholdReached) {
      dueReasons.push("mutation_count_threshold");
    }

    if (timeThresholdReached) {
      dueReasons.push("time_threshold");
    }

    return {
      dueReasons,
      countThresholdReached,
      timeThresholdReached,
      quietHoursActive,
      queuedForMorning: quietHoursActive && dueReasons.length > 0,
      nextMorningAt: nextLocalTime(nowMs, this.morningHour, 0).toISOString(),
    };
  }

  _isTimeThresholdDue(nowMs, state, queueStats) {
    const targetMs = this._resolveTimeThresholdAtMs(state, queueStats);
    return targetMs !== null && targetMs <= nowMs;
  }

  _resolveTimeThresholdAtMs(state = null, queueStats = null) {
    const resolvedState = state ?? this.database.getReviewState();
    const resolvedQueueStats = queueStats ?? this.database.getPendingGraphProposalStats();

    if (resolvedState.mutationsSinceLastReview <= 0 && resolvedQueueStats.total <= 0) {
      return null;
    }

    const lastReviewAtMs = toTimestamp(resolvedState.lastReviewAt);
    if (lastReviewAtMs !== null) {
      return lastReviewAtMs + this.reviewIntervalMs;
    }

    const oldestPendingAtMs = toTimestamp(resolvedQueueStats.oldestCreatedAt);
    if (oldestPendingAtMs !== null) {
      return oldestPendingAtMs + this.reviewIntervalMs;
    }

    return null;
  }

  _isQuietHours(nowMs) {
    const hour = new Date(nowMs).getHours();
    return hour >= QUIET_HOURS_START || hour < QUIET_HOURS_END;
  }

  _scheduleDailyTrigger(kind) {
    if (!this.started || this.database.closed) {
      return;
    }

    const schedule = kind === "morning"
      ? { hour: this.morningHour, minute: 0, reason: "morning_catch_up" }
      : { hour: this.endOfDayHour, minute: this.endOfDayMinute, reason: "end_of_day_sweep" };
    const nowMs = this.now();
    const runAt = nextLocalTime(nowMs, schedule.hour, schedule.minute);

    this._setTimer(kind, runAt.getTime() - nowMs, runAt.toISOString(), () => {
      this._scheduleDailyTrigger(kind);
      this._enqueueAutomaticTrigger({
        source: "scheduled",
        reason: schedule.reason,
        force: true,
        respectQuietHours: false,
      });
    });
  }

  _scheduleTimeThreshold(state = null, queueStats = null) {
    this._clearTimer("timeThreshold");

    if (!this.started || this.database.closed) {
      return;
    }

    const runAtMs = this._resolveTimeThresholdAtMs(state, queueStats);
    if (runAtMs === null) {
      return;
    }

    const delay = Math.max(0, runAtMs - this.now());
    this._setTimer("timeThreshold", delay, new Date(runAtMs).toISOString(), () => {
      this._enqueueAutomaticTrigger({
        source: "scheduled",
        reason: "time_threshold",
        respectQuietHours: true,
      });
    });
  }

  _setTimer(kind, delay, scheduledAt, callback) {
    this._clearTimer(kind);

    const handle = this._setTimeout(() => {
      this._timers[kind] = null;
      this._scheduledAt[kind] = null;

      if (!this.started || this.database.closed) {
        return;
      }

      callback();
    }, Math.max(0, delay));
    handle?.unref?.();

    this._timers[kind] = handle;
    this._scheduledAt[kind] = scheduledAt;
  }

  _clearTimer(kind) {
    if (this._timers[kind]) {
      this._clearTimeout(this._timers[kind]);
    }

    this._timers[kind] = null;
    this._scheduledAt[kind] = null;
  }

  _enqueueAutomaticTrigger(options) {
    if (this._pendingAutomaticTriggerPromise) {
      return this._pendingAutomaticTriggerPromise;
    }

    const task = this.trigger(options)
      .catch((error) => ({
        ok: false,
        status: "error",
        error: error.message,
      }))
      .finally(() => {
        if (this._pendingAutomaticTriggerPromise === task) {
          this._pendingAutomaticTriggerPromise = null;
        }
      });

    this._pendingAutomaticTriggerPromise = task;
    if (typeof this.registerTask === "function") {
      this.registerTask(task);
    }

    return task;
  }

  _nextScheduledAt() {
    return Object.values(this._scheduledAt)
      .filter(Boolean)
      .sort()[0] ?? null;
  }

  _logInfo(message) {
    this.logger?.info?.(message);
  }

  _logError(message) {
    this.logger?.error?.(message);
  }
}
