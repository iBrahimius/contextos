/**
 * Preconscious buffer for ContextOS v2.3.
 *
 * An in-memory circular buffer for high-salience claim changes that should
 * be surfaced mid-session without waiting for an explicit recall query.
 *
 * @module preconscious
 */

/**
 * In-memory circular buffer for mid-session alerts.
 *
 * When the buffer exceeds maxSize, the oldest entry is evicted (shift).
 * poll() returns undelivered alerts and marks them delivered.
 * peek() returns the count of undelivered alerts without marking them.
 * clear() empties the buffer entirely.
 */
export class PreconsciousBuffer {
  /**
   * @param {number} [maxSize=50] - Maximum number of alerts to hold
   */
  constructor(maxSize = 50) {
    this.buffer = [];
    this.maxSize = maxSize;
    this.lastPollTimestamp = null;
  }

  /**
   * Push a new alert into the buffer.
   *
   * Adds buffered_at timestamp and delivered: false. If the buffer exceeds
   * maxSize, the oldest alert is dropped.
   *
   * @param {object} alert - Alert payload (type, detail, entity_label, etc.)
   */
  push(alert) {
    this.buffer.push({
      ...alert,
      buffered_at: new Date().toISOString(),
      delivered: false,
    });

    if (this.buffer.length > this.maxSize) {
      this.buffer.shift(); // evict oldest
    }
  }

  /**
   * Return all undelivered alerts and mark them as delivered.
   *
   * Subsequent calls return only newly-pushed alerts.
   *
   * @returns {object[]} Array of undelivered alert objects
   */
  poll() {
    const undelivered = this.buffer.filter((alert) => !alert.delivered);

    for (const alert of undelivered) {
      alert.delivered = true;
    }

    this.lastPollTimestamp = new Date().toISOString();
    return undelivered;
  }

  /**
   * Return the count of undelivered alerts without marking them delivered.
   *
   * @returns {number} Number of pending undelivered alerts
   */
  peek() {
    return this.buffer.filter((alert) => !alert.delivered).length;
  }

  /**
   * Clear all alerts from the buffer.
   */
  clear() {
    this.buffer = [];
  }
}
