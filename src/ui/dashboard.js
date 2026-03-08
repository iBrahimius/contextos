function formatNumber(value) {
  return new Intl.NumberFormat().format(value ?? 0);
}

function formatRelative(timestamp) {
  if (!timestamp) {
    return "unknown";
  }

  const deltaSeconds = Math.round((Date.now() - new Date(timestamp).getTime()) / 1000);
  if (deltaSeconds < 60) {
    return `${deltaSeconds}s ago`;
  }
  if (deltaSeconds < 3600) {
    return `${Math.round(deltaSeconds / 60)}m ago`;
  }
  if (deltaSeconds < 86400) {
    return `${Math.round(deltaSeconds / 3600)}h ago`;
  }
  return `${Math.round(deltaSeconds / 86400)}d ago`;
}

function asItems(payload) {
  if (Array.isArray(payload)) {
    return payload;
  }

  return payload?.items ?? [];
}

function renderStats(counts) {
  const container = document.getElementById("stats-grid");
  const stats = [
    ["Conversations", counts.conversations],
    ["Messages", counts.messages],
    ["Entities", counts.entities],
    ["Relationships", counts.relationships],
    ["Chunks", counts.chunks],
    ["Model Runs", counts.modelRuns],
    ["Live Hints", counts.retrievalHints],
    ["Hint Events", counts.hintEvents],
    ["Proposals", counts.graphProposals],
    ["Warnings", counts.warnings],
  ];

  container.innerHTML = stats
    .map(
      ([label, value]) => `
        <article class="card stat-card">
          <div class="eyebrow">${label}</div>
          <div class="stat-value">${formatNumber(value)}</div>
          <div class="stat-label">Persisted locally in SQLite</div>
        </article>
      `,
    )
    .join("");
}

function renderProjects(projects) {
  const container = document.getElementById("projects");
  if (!projects.length) {
    container.innerHTML = `<div class="empty">No tracked components yet. Seed the system with messages or documents.</div>`;
    return;
  }

  container.innerHTML = projects
    .map(
      (project) => `
        <article class="project">
          <div class="project-head">
            <div>
              <h3>${project.label}</h3>
              <div class="muted">${project.kind}</div>
            </div>
            <span class="badge ${project.warningCount ? "badge-warn" : "badge-calm"}">
              complexity ${project.complexityScore.toFixed(1)}
            </span>
          </div>
          <div class="project-meta">
            <span>${project.openTaskCount} open tasks</span>
            <span>${project.decisionCount} decisions</span>
            <span>${project.warningCount} critical constraints</span>
          </div>
          <div class="bar"><span style="width:${Math.min(100, project.complexityScore * 18)}%"></span></div>
        </article>
      `,
    )
    .join("");
}

function renderWarnings(warnings) {
  const container = document.getElementById("warnings");
  if (!warnings.length) {
    container.innerHTML = `<div class="empty">No recent proxy alerts.</div>`;
    return;
  }

  container.innerHTML = warnings
    .map(
      (warning) => `
        <article class="warning">
          <div class="warning-head">
            <strong>${warning.verdict.toUpperCase()}</strong>
            <span class="badge ${warning.verdict === "block" ? "badge-warn" : ""}">${warning.direction}</span>
          </div>
          <div class="warning-meta">
            <span>${warning.stage}</span>
            <span>${formatRelative(warning.createdAt)}</span>
          </div>
          <p class="muted">${warning.reasons.join(", ") || "No reasons recorded"}</p>
        </article>
      `,
    )
    .join("");
}

function renderHotEntities(entities) {
  const container = document.getElementById("hot-entities");
  if (!entities.length) {
    container.innerHTML = `<div class="empty">Complexity scores will appear after retrieval activity.</div>`;
    return;
  }

  container.innerHTML = entities
    .map(
      (entity) => `
        <article class="entity-row">
          <div class="entity-head">
            <strong>${entity.label}</strong>
            <span class="badge">${entity.complexityScore.toFixed(1)}</span>
          </div>
          <div class="entity-meta">
            <span>${entity.kind}</span>
            <span>${entity.mentionCount} mentions</span>
            <span>${entity.missCount} misses</span>
          </div>
          <div class="bar"><span style="width:${Math.min(100, entity.complexityScore * 14)}%"></span></div>
        </article>
      `,
    )
    .join("");
}

function renderRetrievals(rows) {
  const tbody = document.getElementById("retrieval-table");
  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="5" class="muted">No retrieval telemetry yet.</td></tr>`;
    return;
  }

  tbody.innerHTML = rows
    .map(
      (row) => `
        <tr>
          <td>${row.queryText}</td>
          <td>${row.latencyMs} ms</td>
          <td>${row.itemsReturned}</td>
          <td>${row.tokensConsumed}</td>
          <td>${row.expandedEntities.length} entities / ${row.hintHops} hint hops</td>
        </tr>
      `,
    )
    .join("");
}

function renderHintPolicy(rows) {
  const container = document.getElementById("hint-policy");
  if (!rows.length) {
    container.innerHTML = `<div class="empty">Hint policy will appear after adaptive retrieval runs.</div>`;
    return;
  }

  container.innerHTML = rows
    .slice(0, 8)
    .map(
      (row) => `
        <article class="project">
          <div class="project-head">
            <div>
              <h3>${row.seedLabel} -> ${row.expandLabel}</h3>
              <div class="muted">${row.status}, avg reward ${Number(row.avgReward ?? 0).toFixed(2)}</div>
            </div>
            <span class="badge ${Number(row.lastReward ?? 0) >= 0 ? "badge-calm" : "badge-warn"}">
              weight ${Number(row.weight).toFixed(2)}
            </span>
          </div>
          <div class="project-meta">
            <span>TTL ${row.ttlTurns}</span>
            <span>${row.timesApplied} applied</span>
            <span>${row.timesRewarded} rewarded</span>
            <span>${row.timesUnused} unused</span>
          </div>
          <div class="bar"><span style="width:${Math.min(100, Math.max(8, Number(row.weight) * 28))}%"></span></div>
        </article>
      `,
    )
    .join("");
}

function renderHintEvents(rows) {
  const container = document.getElementById("hint-events");
  if (!rows.length) {
    container.innerHTML = `<div class="empty">No hint learning events yet.</div>`;
    return;
  }

  container.innerHTML = rows
    .slice(0, 10)
    .map(
      (row) => `
        <article class="warning">
          <div class="warning-head">
            <strong>${row.eventType.toUpperCase()}</strong>
            <span class="badge ${Number(row.reward) >= Number(row.penalty) ? "badge-calm" : "badge-warn"}">
              ${row.seedLabel} -> ${row.expandLabel}
            </span>
          </div>
          <div class="warning-meta">
            <span>reward ${Number(row.reward).toFixed(2)}</span>
            <span>penalty ${Number(row.penalty).toFixed(2)}</span>
            <span>${formatRelative(row.createdAt)}</span>
          </div>
          <p class="muted">${row.detail?.metrics ? `${row.detail.metrics.attributedItemCount} items, ${row.detail.metrics.uniqueEntityCount} unique entities` : "No metrics recorded"}</p>
        </article>
      `,
    )
    .join("");
}

function renderConversations(conversations) {
  const container = document.getElementById("conversations");
  if (!conversations.length) {
    container.innerHTML = `<div class="empty">No conversations captured yet.</div>`;
    return;
  }

  container.innerHTML = conversations
    .map(
      (conversation) => `
        <article class="conversation">
          <h3>${conversation.title}</h3>
          <div class="conversation-meta">
            <span>created ${formatRelative(conversation.createdAt)}</span>
            <span>updated ${formatRelative(conversation.updatedAt)}</span>
          </div>
        </article>
      `,
    )
    .join("");
}

function renderGraph(graph) {
  const svg = document.getElementById("graph");
  const width = 860;
  const height = 420;
  const centerX = width / 2;
  const centerY = height / 2;
  const radius = 150;
  const nodes = graph.nodes ?? [];
  const edges = graph.edges ?? [];

  if (!nodes.length) {
    svg.innerHTML = "";
    return;
  }

  const positions = new Map();
  nodes.forEach((node, index) => {
    const angle = (Math.PI * 2 * index) / nodes.length - Math.PI / 2;
    const x = centerX + Math.cos(angle) * radius * (1 + (index % 3) * 0.08);
    const y = centerY + Math.sin(angle) * radius * (1 + ((index + 1) % 3) * 0.08);
    positions.set(node.id, { x, y });
  });

  const edgeMarkup = edges
    .map((edge) => {
      const source = positions.get(edge.source);
      const target = positions.get(edge.target);
      if (!source || !target) {
        return "";
      }
      return `
        <line
          x1="${source.x}"
          y1="${source.y}"
          x2="${target.x}"
          y2="${target.y}"
          stroke="hsla(162 72% 28% / 0.28)"
          stroke-width="${1 + edge.weight}"
        />
      `;
    })
    .join("");

  const nodeMarkup = nodes
    .map((node) => {
      const { x, y } = positions.get(node.id);
      const fill = node.kind === "technology" ? "hsl(28 86% 54%)" : "hsl(162 72% 28%)";
      return `
        <g>
          <circle cx="${x}" cy="${y}" r="${14 + Math.min(16, node.complexityScore * 2.4)}" fill="${fill}" fill-opacity="0.88" />
          <circle cx="${x}" cy="${y}" r="${18 + Math.min(16, node.complexityScore * 2.4)}" fill="${fill}" fill-opacity="0.12" />
          <text x="${x}" y="${y + 36}" text-anchor="middle" font-size="12" fill="hsl(222 47% 11%)">${node.label}</text>
        </g>
      `;
    })
    .join("");

  svg.innerHTML = `
    <rect x="0" y="0" width="${width}" height="${height}" rx="24" fill="transparent"></rect>
    ${edgeMarkup}
    ${nodeMarkup}
  `;
}

async function refresh() {
  const response = await fetch("/api/dashboard");
  const data = await response.json();

  renderStats(data.counts);
  renderProjects(asItems(data.projects));
  renderWarnings(asItems(data.warnings));
  renderHotEntities(asItems(data.hotEntities));
  renderRetrievals(asItems(data.recentRetrievals));
  renderHintPolicy(asItems(data.topHintStats));
  renderHintEvents(asItems(data.recentHintEvents));
  renderConversations(asItems(data.conversations));
  renderGraph(data.graph);

  const posture = document.getElementById("retrieval-posture");
  const avgTokens =
    data.recentRetrievals.reduce((sum, row) => sum + row.tokensConsumed, 0) / Math.max(1, data.recentRetrievals.length);
  posture.textContent = avgTokens > 100 ? "Greedy" : "Measured";
}

refresh();
setInterval(refresh, 5000);
