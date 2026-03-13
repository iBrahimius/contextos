/**
 * Cursor-based pagination helper.
 *
 * Cursor is an opaque base64-encoded string containing the sort key(s)
 * of the last item in the previous page.
 *
 * Usage:
 *   const { sql, params } = paginationClause({ cursor, limit, orderBy: 'created_at', direction: 'DESC' });
 *   // Append sql to your query, spread params into your bind
 */

export function encodeCursor(values) {
  return Buffer.from(JSON.stringify(values)).toString("base64url");
}

export function decodeCursor(cursor) {
  if (!cursor) {
    return null;
  }

  try {
    return JSON.parse(Buffer.from(cursor, "base64url").toString());
  } catch {
    return null;
  }
}

export function paginationClause({ cursor, limit = 50, orderBy = "created_at", direction = "DESC" }) {
  const decoded = decodeCursor(cursor);
  const numericLimit = Number(limit);
  const maxLimit = Math.min(Math.max(1, Number.isFinite(numericLimit) ? Math.trunc(numericLimit) : 50), 200);

  if (!decoded) {
    return {
      sql: ` ORDER BY ${orderBy} ${direction} LIMIT ?`,
      params: [maxLimit + 1],
    };
  }

  const op = direction === "DESC" ? "<" : ">";
  return {
    sql: ` AND ${orderBy} ${op} ? ORDER BY ${orderBy} ${direction} LIMIT ?`,
    params: [decoded.value, maxLimit + 1],
  };
}

export function paginateResults(rows, limit = 50) {
  const numericLimit = Number(limit);
  const maxLimit = Math.min(Math.max(1, Number.isFinite(numericLimit) ? Math.trunc(numericLimit) : 50), 200);
  const hasMore = rows.length > maxLimit;
  const visibleRows = hasMore ? rows.slice(0, maxLimit) : rows;
  const lastRow = visibleRows.at(-1) ?? null;
  const items = visibleRows.map((row) => {
    if (!row || typeof row !== "object" || !Object.hasOwn(row, "_cursor_key")) {
      return row;
    }

    const { _cursor_key: _, ...item } = row;
    return item;
  });
  const nextCursor = hasMore && lastRow ? encodeCursor({ value: lastRow._cursor_key }) : null;

  return { items, hasMore, nextCursor };
}
