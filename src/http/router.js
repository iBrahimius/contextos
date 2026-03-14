import fs from "node:fs/promises";
import path from "node:path";
import { createCachedRegistry } from "../core/claim-registries.js";
import { analyzeClaimsTruthSet } from "../core/claim-resolution.js";
import { normalizeEnrichment } from "../core/normalize-enrichment.js";
import { getValidTransitions, validateTransition } from "../core/claim-types.js";
import { lintRefTags, lintRefTagsPath } from "../core/ref-linter.js";

const CONTENT_TYPES = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
};
const CORS_ALLOW_HEADERS = "content-type, authorization";
const CORS_ORIGIN = process.env.CONTEXTOS_CORS_ORIGIN || "*";
const MAX_BODY_BYTES = 1_048_576; // 1 MB
const MAX_PAGE_SIZE = 100;

const cachedRegistry = createCachedRegistry(5000);

function withGraphVersion(payload, graphVersion) {
  return {
    ...payload,
    graph_version: graphVersion,
  };
}

function resolveGraphVersion(contextOS) {
  return Number(
    contextOS?.graph?.getGraphVersion?.() ??
    contextOS?.database?.getGraphVersion?.() ??
    0,
  );
}

function enrichClaimsTruth(contextOS, claims) {
  const normalizedClaims = Array.isArray(claims) ? claims.filter(Boolean) : [];
  if (!normalizedClaims.length) {
    return [];
  }

  const groupedClaims = new Map();
  for (const claim of normalizedClaims) {
    const resolutionKey = String(claim?.resolution_key ?? "").trim();
    if (!resolutionKey) {
      groupedClaims.set(claim.id, [claim]);
      continue;
    }

    if (!groupedClaims.has(resolutionKey)) {
      groupedClaims.set(resolutionKey, contextOS.database.listClaimsByResolutionKey(resolutionKey));
    }
  }

  const truthAnalysis = analyzeClaimsTruthSet([...groupedClaims.values()].flat());
  return normalizedClaims.map((claim) => ({
    ...claim,
    truth: truthAnalysis.byClaimId.get(claim.id) ?? null,
  }));
}

function enrichClaimTruth(contextOS, claim) {
  return enrichClaimsTruth(contextOS, claim ? [claim] : [])[0] ?? claim;
}

function sendJson(response, statusCode, payload) {
  response.writeHead(statusCode, {
    "Access-Control-Allow-Origin": CORS_ORIGIN,
    "Access-Control-Allow-Headers": CORS_ALLOW_HEADERS,
    "Access-Control-Allow-Methods": "GET,POST,PATCH,OPTIONS",
    "Content-Type": "application/json; charset=utf-8",
  });
  response.end(JSON.stringify(payload, null, 2));
}

function sendFile(response, statusCode, body, contentType) {
  response.writeHead(statusCode, {
    "Access-Control-Allow-Origin": CORS_ORIGIN,
    "Content-Type": contentType,
  });
  response.end(body);
}

async function parseJsonBody(request) {
  const chunks = [];
  let totalBytes = 0;
  for await (const chunk of request) {
    totalBytes += chunk.length;
    if (totalBytes > MAX_BODY_BYTES) {
      const error = new Error("Request body too large");
      error.statusCode = 413;
      throw error;
    }
    chunks.push(chunk);
  }

  const body = Buffer.concat(chunks).toString("utf8");
  if (!body) return {};

  try {
    return JSON.parse(body);
  } catch {
    const error = new Error("Invalid JSON in request body");
    error.statusCode = 400;
    throw error;
  }
}

function parsePaginationParams(searchParams) {
  const hasCursor = searchParams.has("cursor");
  const hasLimit = searchParams.has("limit");

  if (!hasCursor && !hasLimit) {
    return null;
  }

  const pagination = {};
  if (hasCursor) {
    pagination.cursor = searchParams.get("cursor") || null;
  }
  if (hasLimit) {
    const limit = Number(searchParams.get("limit"));
    if (Number.isFinite(limit)) {
      pagination.limit = Math.min(Math.max(1, Math.trunc(limit)), MAX_PAGE_SIZE);
    }
  }

  return pagination;
}

function parseIntegerParam(value, fallback, minimum = 0) {
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) {
    return fallback;
  }

  return Math.max(minimum, Math.trunc(numericValue));
}

function parseRegistryFilter(searchParams) {
  const entityId = searchParams.get("entity_id") ?? searchParams.get("entityId") ?? null;
  const scope = searchParams.get("scope") ?? null;
  const filter = {};

  if (entityId) {
    filter.entityIds = [entityId];
  }

  if (scope) {
    filter.scopeFilter = scope;
  }

  return filter;
}

function isPaginatedPayload(payload) {
  return Boolean(payload)
    && typeof payload === "object"
    && !Array.isArray(payload)
    && Array.isArray(payload.items)
    && typeof payload.hasMore === "boolean";
}

async function serveUiAsset(rootDir, pathname, response, graphVersion) {
  const assetPath = pathname === "/" ? "/index.html" : pathname;
  const uiDir = path.resolve(rootDir, "src", "ui");
  const absolutePath = path.resolve(uiDir, assetPath.replace(/^\/+/, ""));

  // Guard against path traversal (e.g. ../../etc/passwd)
  if (!absolutePath.startsWith(uiDir + path.sep) && absolutePath !== uiDir) {
    sendJson(response, 403, withGraphVersion({ error: "Forbidden" }, graphVersion));
    return;
  }

  try {
    const body = await fs.readFile(absolutePath);
    const extension = path.extname(absolutePath);
    sendFile(response, 200, body, CONTENT_TYPES[extension] ?? "application/octet-stream");
  } catch {
    sendJson(response, 404, withGraphVersion({ error: "Not found" }, graphVersion));
  }
}

export async function handleRequest(contextOS, rootDir, request, response) {
  if (request.method === "OPTIONS") {
    response.writeHead(204, {
      "Access-Control-Allow-Origin": CORS_ORIGIN,
      "Access-Control-Allow-Headers": CORS_ALLOW_HEADERS,
      "Access-Control-Allow-Methods": "GET,POST,PATCH,OPTIONS",
    });
    response.end();
    return;
  }

  const url = new URL(request.url, "http://localhost");
  const { pathname } = url;
  const authToken = process.env.CONTEXTOS_AUTH_TOKEN ?? "";

  if (
    authToken
    && pathname.startsWith("/api/")
    && !(request.method === "GET" && pathname === "/api/health")
  ) {
    const authorizationHeader = request.headers.authorization ?? "";
    const matchedToken = authorizationHeader.match(/^Bearer\s+(.+)$/i)?.[1] ?? null;

    if (!authorizationHeader) {
      sendJson(response, 401, { error: "Authentication required" });
      return;
    }

    if (matchedToken !== authToken) {
      sendJson(response, 403, { error: "Invalid authentication token" });
      return;
    }
  }

  const pagination = parsePaginationParams(url.searchParams);

  try {
    if (request.method === "GET" && pathname === "/api/health") {
      sendJson(response, 200, withGraphVersion(contextOS.getHealthData(), resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "GET" && pathname === "/api/status") {
      const statusData = contextOS.ready
        ? contextOS.getStatusData()
        : { ready: false };
      sendJson(response, 200, withGraphVersion(statusData, resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "GET" && pathname === "/api/alerts/status") {
      sendJson(response, 200, withGraphVersion(contextOS.getAlertStatus(), resolveGraphVersion(contextOS)));
      return;
    }

    if (!contextOS.ready && pathname.startsWith("/api/")) {
      sendJson(response, 503, { error: "initializing", ready: false });
      return;
    }

    if (request.method === "POST" && pathname === "/api/backfill-embeddings") {
      // Trigger embedding backfill — runs in background, returns immediately
      contextOS.backfillEmbeddings({ logProgress: true }).catch(() => {});
      sendJson(response, 202, { ok: true, message: "Backfill started in background" });
      return;
    }

    if (request.method === "POST" && pathname === "/api/lint/refs") {
      const body = await parseJsonBody(request);

      if (typeof body.content === "string") {
        const filePath = typeof body.path === "string" && body.path.trim() ? body.path.trim() : "<content>";
        const result = lintRefTags({
          content: body.content,
          filePath,
          database: contextOS.database,
        });
        sendJson(response, 200, withGraphVersion(result, resolveGraphVersion(contextOS)));
        return;
      }

      if (typeof body.path === "string" && body.path.trim()) {
        const requestedPath = body.path.trim();
        const filePath = path.isAbsolute(requestedPath)
          ? requestedPath
          : path.resolve(rootDir, requestedPath);

        // Restrict file reads to the repo root — prevent arbitrary file access
        const resolvedRoot = path.resolve(rootDir);
        if (!filePath.startsWith(resolvedRoot + path.sep) && filePath !== resolvedRoot) {
          sendJson(response, 403, withGraphVersion({ error: "Path must be within the project root" }, resolveGraphVersion(contextOS)));
          return;
        }

        const result = await lintRefTagsPath({
          path: filePath,
          database: contextOS.database,
        });
        sendJson(response, 200, withGraphVersion(result, resolveGraphVersion(contextOS)));
        return;
      }

      sendJson(response, 400, withGraphVersion({ error: "Request body must include content or path" }, resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "GET" && pathname === "/api/dashboard") {
      sendJson(response, 200, withGraphVersion(contextOS.getDashboardData(), resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "GET" && pathname === "/api/aggregator") {
      sendJson(response, 200, withGraphVersion(contextOS.getIncrementalAggregationData(), resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "GET" && pathname === "/api/conversations") {
      const conversations = contextOS.database.listConversations(pagination);
      sendJson(
        response,
        200,
        isPaginatedPayload(conversations) ? withGraphVersion(conversations, resolveGraphVersion(contextOS)) : conversations,
      );
      return;
    }

    if (request.method === "POST" && pathname === "/api/conversations") {
      const body = await parseJsonBody(request);
      sendJson(
        response,
        201,
        withGraphVersion(contextOS.database.createConversation(body.title ?? "ContextOS Session"), resolveGraphVersion(contextOS)),
      );
      return;
    }

    if (request.method === "GET" && pathname.startsWith("/api/conversations/") && pathname.endsWith("/messages")) {
      const conversationId = pathname.split("/")[3];
      const messages = contextOS.database.listMessages(conversationId, pagination);
      sendJson(
        response,
        200,
        isPaginatedPayload(messages) ? withGraphVersion(messages, resolveGraphVersion(contextOS)) : messages,
      );
      return;
    }

    if (request.method === "POST" && pathname === "/api/messages") {
      const body = await parseJsonBody(request);
      const actorId = body.actorId ?? "api";
      const record = await contextOS.ingestMessage({
        conversationId: body.conversationId ?? null,
        conversationTitle: body.conversationTitle ?? "ContextOS Session",
        role: body.role ?? "user",
        direction: body.direction ?? (body.role === "assistant" ? "outbound" : "inbound"),
        actorId,
        content: body.content ?? "",
        raw: body.raw ?? body,
        ingestId: body.ingestId ?? null,
        originKind: body.originKind ?? null,
        sourceMessageId: body.sourceMessageId ?? null,
        scopeKind: body.scopeKind ?? "private",
        scopeId: body.scopeId ?? null,
      });
      sendJson(response, 201, withGraphVersion(record, resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "POST" && pathname === "/api/ingest/enrich") {
      const body = await parseJsonBody(request);
      const message = contextOS.database.getMessageByIngestId(body.ingestId);

      if (!message) {
        sendJson(response, 404, withGraphVersion({ error: "Message not found" }, resolveGraphVersion(contextOS)));
        return;
      }

      // Normalize Haiku output before persisting (deterministic cleanup)
      const normalized = normalizeEnrichment({
        entities: body.entities ?? [],
        observations: body.observations ?? [],
        graphProposals: body.graphProposals ?? [],
      });

      const result = contextOS.persistKnowledgePatch({
        conversationId: message.conversationId,
        messageId: message.id,
        patch: {
          entities: normalized.entities,
          observations: normalized.observations,
          graphProposals: normalized.graphProposals,
          retrieveHints: [],
          complexityAdjustments: [],
        },
        actorId: body.source || "haiku-classifier",
        scopeKind: message.scopeKind,
        scopeId: message.scopeId,
      });

      sendJson(response, 200, withGraphVersion({ ...result, normalization: normalized.stats }, resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "POST" && pathname === "/api/retrieve") {
      const body = await parseJsonBody(request);
      sendJson(
        response,
        200,
        withGraphVersion(
          await contextOS.retrieve({
            conversationId: body.conversationId ?? null,
            queryText: body.queryText ?? "",
            scopeFilter: body.scopeFilter ?? (body.scopeKind ? { scopeKind: body.scopeKind, scopeId: body.scopeId ?? null } : null),
          }),
          resolveGraphVersion(contextOS),
        ),
      );
      return;
    }

    if (request.method === "POST" && pathname === "/api/recall") {
      const body = await parseJsonBody(request);
      sendJson(
        response,
        200,
        withGraphVersion(
          await contextOS.recall({
            conversationId: body.conversationId ?? null,
            query: body.query ?? body.queryText ?? "",
            mode: body.mode ?? "hybrid",
            scope: body.scope ?? "all",
            tokenBudget: body.token_budget ?? body.tokenBudget ?? 2000,
            scopeFilter: body.scopeFilter ?? (body.scopeKind ? { scopeKind: body.scopeKind, scopeId: body.scopeId ?? null } : null),
          }),
          resolveGraphVersion(contextOS),
        ),
      );
      return;
    }

    if (request.method === "POST" && pathname === "/api/recall/context-window") {
      const body = await parseJsonBody(request);
      const window = contextOS.getContextWindow({
        eventId: body.event_id ?? body.eventId ?? null,
        before: body.before ?? 6,
        after: body.after ?? 6,
      });

      if (!window) {
        sendJson(response, 404, withGraphVersion({ error: "Event not found" }, resolveGraphVersion(contextOS)));
        return;
      }

      sendJson(response, 200, withGraphVersion(window, resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "GET" && pathname === "/api/claims") {
      const types = (url.searchParams.get("types") ?? "")
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean);
      const claims = contextOS.database.listClaims({
        types: types.length ? types : null,
        state: url.searchParams.get("state") ?? null,
        entityId: url.searchParams.get("entity_id") ?? url.searchParams.get("entityId") ?? null,
        resolutionKey: url.searchParams.get("resolution_key") ?? url.searchParams.get("resolutionKey") ?? null,
        limit: parseIntegerParam(url.searchParams.get("limit"), 100, 1),
        offset: parseIntegerParam(url.searchParams.get("offset"), 0, 0),
      });
      sendJson(response, 200, withGraphVersion({
        ...claims,
        claims: enrichClaimsTruth(contextOS, claims.claims),
      }, resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "GET" && pathname === "/api/claims/stats") {
      const stats = contextOS.database.getClaimStateStats();
      const totals = Object.values(stats).reduce((accumulator, byState) => {
        for (const [state, count] of Object.entries(byState)) {
          accumulator[state] = (accumulator[state] ?? 0) + Number(count ?? 0);
        }

        return accumulator;
      }, {});
      sendJson(response, 200, withGraphVersion({ stats, totals }, resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "PATCH" && pathname.startsWith("/api/claims/") && pathname.endsWith("/transition")) {
      const claimId = pathname.split("/")[3];
      const claim = contextOS.database.getClaim(claimId);

      if (!claim) {
        sendJson(response, 404, withGraphVersion({ error: "Claim not found" }, resolveGraphVersion(contextOS)));
        return;
      }

      const body = await parseJsonBody(request);
      const toState = body.to_state ?? body.toState ?? null;
      const currentState = claim.value_text ?? null;
      const validTransitions = getValidTransitions(claim.claim_type, currentState);

      if (!toState || !validateTransition(claim.claim_type, currentState, toState)) {
        sendJson(response, 400, withGraphVersion({
          error: "Invalid transition",
          valid_transitions: validTransitions,
        }, resolveGraphVersion(contextOS)));
        return;
      }

      const updatedClaim = contextOS.database.updateClaim(claim.id, {
        value_text: toState,
      });
      sendJson(response, 200, withGraphVersion({ claim: enrichClaimTruth(contextOS, updatedClaim) }, resolveGraphVersion(contextOS)));
      return;
    }

    if (
      request.method === "GET"
      && pathname.startsWith("/api/claims/")
      && pathname !== "/api/claims/disputed"
      && pathname !== "/api/claims/backfill/status"
    ) {
      const claimId = pathname.split("/")[3];
      const claim = contextOS.database.getClaim(claimId);

      if (!claim) {
        sendJson(response, 404, withGraphVersion({ error: "Claim not found" }, resolveGraphVersion(contextOS)));
        return;
      }

      sendJson(response, 200, withGraphVersion({ claim: enrichClaimTruth(contextOS, claim) }, resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "GET" && pathname === "/api/claims/disputed") {
      const claims = contextOS.database.listDisputedClaims({
        limit: parseIntegerParam(url.searchParams.get("limit"), 100, 1),
      });
      sendJson(response, 200, withGraphVersion({ claims: enrichClaimsTruth(contextOS, claims) }, resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "GET" && pathname === "/api/claims/backfill/status") {
      sendJson(response, 200, withGraphVersion({
        backfill: contextOS.getClaimBackfillStatus(),
      }, resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "POST" && pathname === "/api/claims/backfill") {
      const body = await parseJsonBody(request);
      const result = contextOS.backfillClaims({
        limit: parseIntegerParam(body.limit ?? body.batch_size ?? body.batchSize, 100, 1),
      });
      sendJson(response, 200, withGraphVersion(result, resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "GET" && pathname === "/api/registries/tasks") {
      sendJson(
        response,
        200,
        withGraphVersion({
          tasks: cachedRegistry.getTaskRegistry(contextOS.database, parseRegistryFilter(url.searchParams)),
        }, resolveGraphVersion(contextOS)),
      );
      return;
    }

    if (request.method === "GET" && pathname === "/api/registries/decisions") {
      sendJson(
        response,
        200,
        withGraphVersion({
          decisions: cachedRegistry.getDecisionRegistry(contextOS.database, parseRegistryFilter(url.searchParams)),
        }, resolveGraphVersion(contextOS)),
      );
      return;
    }

    if (request.method === "GET" && pathname === "/api/registries/rules") {
      sendJson(
        response,
        200,
        withGraphVersion({
          rules: cachedRegistry.getRuleRegistry(contextOS.database, parseRegistryFilter(url.searchParams)),
        }, resolveGraphVersion(contextOS)),
      );
      return;
    }

    if (request.method === "GET" && pathname === "/api/registries/goals") {
      sendJson(
        response,
        200,
        withGraphVersion({
          goals: cachedRegistry.getGoalRegistry(contextOS.database, parseRegistryFilter(url.searchParams)),
        }, resolveGraphVersion(contextOS)),
      );
      return;
    }

    if (request.method === "POST" && pathname === "/api/dream-cycle") {
      const body = await parseJsonBody(request);
      const report = await contextOS.dreamCycle(body);
      sendJson(response, 200, withGraphVersion(report, resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "POST" && pathname === "/api/session-recovery") {
      const body = await parseJsonBody(request);
      const packet = await contextOS.sessionRecovery(body);
      sendJson(response, 200, withGraphVersion(packet, resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "GET" && pathname === "/api/preconscious") {
      const alerts = contextOS.preconsciousBuffer.buffer.filter((a) => !a.delivered);
      sendJson(response, 200, withGraphVersion({ alerts, size: contextOS.preconsciousBuffer.buffer.length }, resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "POST" && pathname === "/api/preconscious/poll") {
      const alerts = contextOS.preconsciousBuffer.poll();
      sendJson(response, 200, withGraphVersion({ alerts }, resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "GET" && pathname === "/api/preconscious/peek") {
      const count = contextOS.preconsciousBuffer.peek();
      sendJson(response, 200, withGraphVersion({ count }, resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "POST" && pathname === "/api/memory/consolidate") {
      await parseJsonBody(request);
      sendJson(response, 200, withGraphVersion({
        status: "not_implemented",
        message: "Consolidation available in v2.3",
      }, resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "POST" && pathname === "/api/context-packet") {
      const body = await parseJsonBody(request);
      sendJson(
        response,
        200,
        withGraphVersion(await contextOS.contextPacket(body), resolveGraphVersion(contextOS)),
      );
      return;
    }

    if (request.method === "POST" && pathname === "/api/memory-brief") {
      const body = await parseJsonBody(request);
      sendJson(
        response,
        200,
        withGraphVersion(await contextOS.memoryBrief(body), resolveGraphVersion(contextOS)),
      );
      return;
    }

    if (request.method === "GET" && pathname === "/api/registries/open-items") {
      const kind = url.searchParams.get("kind") ?? "all";
      sendJson(response, 200, withGraphVersion(contextOS.listOpenItems(kind), resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "POST" && pathname === "/api/registries/query") {
      const body = await parseJsonBody(request);
      sendJson(
        response,
        200,
        withGraphVersion(
          contextOS.queryRegistry({
            name: body.name ?? "",
            query: body.query ?? "",
            filters: body.filters ?? {},
          }),
          resolveGraphVersion(contextOS),
        ),
      );
      return;
    }

    if (request.method === "POST" && pathname === "/api/mutations/propose") {
      const body = await parseJsonBody(request);
      sendJson(
        response,
        200,
        withGraphVersion(
          contextOS.proposeMutation({
            type: body.type ?? "",
            payload: body.payload ?? {},
            confidence: body.confidence ?? 0.5,
            sourceEventId: body.source_event_id ?? body.sourceEventId ?? null,
            actorId: body.actorId ?? "api",
          }),
          resolveGraphVersion(contextOS),
        ),
      );
      return;
    }

    if (request.method === "POST" && pathname === "/api/mutations/review") {
      const body = await parseJsonBody(request);
      sendJson(
        response,
        200,
        withGraphVersion(
          contextOS.reviewMutations({
            action: body.action ?? "list",
            mutationId: body.mutation_id ?? body.mutationId ?? null,
            mutationIds: body.mutation_ids ?? body.mutationIds ?? null,
            reason: body.reason ?? null,
            actorId: body.actorId ?? "api",
            filters: {
              status: body.status ?? null,
              statuses: body.statuses ?? null,
              writeClass: body.write_class ?? body.writeClass ?? null,
              writeClasses: body.write_classes ?? body.writeClasses ?? null,
              proposalType: body.proposal_type ?? body.proposalType ?? null,
              proposalTypes: body.proposal_types ?? body.proposalTypes ?? null,
              triage: body.triage ?? null,
              includeParked: body.include_parked ?? body.includeParked ?? null,
              minConfidence: body.min_confidence ?? body.minConfidence ?? null,
              maxConfidence: body.max_confidence ?? body.maxConfidence ?? null,
              sourceEventId: body.source_event_id ?? body.sourceEventId ?? null,
              sort: body.sort ?? null,
              limit: body.limit ?? null,
            },
          }),
          resolveGraphVersion(contextOS),
        ),
      );
      return;
    }

    if (request.method === "GET" && pathname === "/api/review/status") {
      sendJson(
        response,
        200,
        withGraphVersion(contextOS.getReviewStatus(), resolveGraphVersion(contextOS)),
      );
      return;
    }

    if (request.method === "POST" && pathname === "/api/review/trigger") {
      const body = await parseJsonBody(request);
      sendJson(
        response,
        200,
        withGraphVersion(
          await contextOS.triggerReview({
            source: "manual",
            reason: body.reason ?? "manual_trigger",
          }),
          resolveGraphVersion(contextOS),
        ),
      );
      return;
    }

    if (request.method === "POST" && pathname === "/api/index") {
      const body = await parseJsonBody(request);
      const result = await contextOS.indexMarkdownDirectory(body.path ?? "docs", {
        scopeKind: body.scopeKind ?? "shared",
        scopeId: body.scopeId ?? null,
      });
      sendJson(response, 200, withGraphVersion({ indexed: result }, resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "POST" && pathname === "/api/proxy/chat") {
      const body = await parseJsonBody(request);
      const actorId = body.actorId ?? "api";
      const result = await contextOS.proxyChat({
        conversationId: body.conversationId ?? null,
        title: body.title ?? "Proxy Session",
        messages: body.messages ?? [],
        mockResponse: body.mockResponse ?? null,
        actorId,
        scopeKind: body.scopeKind ?? "private",
        scopeId: body.scopeId ?? null,
      });
      sendJson(response, 200, withGraphVersion(result, resolveGraphVersion(contextOS)));
      return;
    }

    if (request.method === "GET" && pathname === "/api/retrievals") {
      const retrievals = contextOS.telemetry.listRecentRetrievals(pagination);
      sendJson(
        response,
        200,
        isPaginatedPayload(retrievals) ? withGraphVersion(retrievals, resolveGraphVersion(contextOS)) : retrievals,
      );
      return;
    }

    if (request.method === "GET" && pathname === "/api/model-runs") {
      const modelRuns = contextOS.telemetry.listRecentModelRuns(pagination);
      sendJson(
        response,
        200,
        isPaginatedPayload(modelRuns) ? withGraphVersion(modelRuns, resolveGraphVersion(contextOS)) : modelRuns,
      );
      return;
    }

    if (request.method === "GET" && pathname === "/api/retrieval-hints") {
      const retrievalHints = contextOS.telemetry.listActiveRetrievalHints(pagination);
      sendJson(
        response,
        200,
        isPaginatedPayload(retrievalHints) ? withGraphVersion(retrievalHints, resolveGraphVersion(contextOS)) : retrievalHints,
      );
      return;
    }

    if (request.method === "GET" && pathname === "/api/retrieval-hint-stats") {
      const retrievalHintStats = contextOS.telemetry.listRetrievalHintStats(pagination);
      sendJson(
        response,
        200,
        isPaginatedPayload(retrievalHintStats) ? withGraphVersion(retrievalHintStats, resolveGraphVersion(contextOS)) : retrievalHintStats,
      );
      return;
    }

    if (request.method === "GET" && pathname === "/api/retrieval-hint-events") {
      const retrievalHintEvents = contextOS.telemetry.listRecentRetrievalHintEvents(pagination);
      sendJson(
        response,
        200,
        isPaginatedPayload(retrievalHintEvents) ? withGraphVersion(retrievalHintEvents, resolveGraphVersion(contextOS)) : retrievalHintEvents,
      );
      return;
    }

    if (request.method === "GET" && pathname === "/api/graph-proposals") {
      const graphProposals = contextOS.database.listRecentGraphProposals(pagination);
      sendJson(
        response,
        200,
        isPaginatedPayload(graphProposals) ? withGraphVersion(graphProposals, resolveGraphVersion(contextOS)) : graphProposals,
      );
      return;
    }

    if (request.method === "GET" && pathname.startsWith("/api/entities/")) {
      const name = decodeURIComponent(pathname.replace("/api/entities/", ""));
      const detail = contextOS.getEntityDetail(name, {
        includeRecentEvents: url.searchParams.get("include_recent_events") === "true",
      });
      if (!detail) {
        sendJson(response, 404, withGraphVersion({ error: "Entity not found" }, resolveGraphVersion(contextOS)));
        return;
      }
      sendJson(response, 200, withGraphVersion(detail, resolveGraphVersion(contextOS)));
      return;
    }

    if (pathname.startsWith("/api/")) {
      sendJson(response, 404, withGraphVersion({ error: "Not found" }, resolveGraphVersion(contextOS)));
      return;
    }

    await serveUiAsset(rootDir, pathname, response, resolveGraphVersion(contextOS));
  } catch (error) {
    const statusCode = Number(error?.statusCode);
    sendJson(response, Number.isInteger(statusCode) ? statusCode : 500, withGraphVersion({
      error: error.message ?? "Internal server error",
    }, resolveGraphVersion(contextOS)));
  }
}
