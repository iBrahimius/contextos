/**
 * LLM Client — thin HTTP wrapper for OpenClaw gateway's /v1/chat/completions endpoint.
 *
 * Uses the gateway as a model-agnostic proxy. Auth, model routing, and token
 * management are handled by OpenClaw — we just POST OpenAI-format requests.
 *
 * Requires:
 *   - OPENCLAW_GATEWAY_URL (default: http://127.0.0.1:41833)
 *   - OPENCLAW_GATEWAY_TOKEN (required, no default)
 *
 * Gateway config prerequisite:
 *   openclaw config set gateway.http.endpoints.chatCompletions.enabled true
 */

const DEFAULT_GATEWAY_URL = "http://127.0.0.1:41833";
const DEFAULT_MODEL = "anthropic/claude-haiku-4-5";
const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_TOKENS = 2048;

export class LLMClient {
  /**
   * @param {object} [options]
   * @param {string} [options.gatewayUrl] — Override gateway URL
   * @param {string} [options.gatewayToken] — Override gateway token
   * @param {string} [options.model] — Default model for all calls
   * @param {number} [options.timeoutMs] — Request timeout
   */
  constructor(options = {}) {
    this.gatewayUrl = options.gatewayUrl
      ?? process.env.OPENCLAW_GATEWAY_URL
      ?? DEFAULT_GATEWAY_URL;
    this.gatewayToken = options.gatewayToken
      ?? process.env.OPENCLAW_GATEWAY_TOKEN
      ?? null;
    this.defaultModel = options.model ?? DEFAULT_MODEL;
    this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;

    if (!this.gatewayToken) {
      throw new Error(
        "LLMClient: OPENCLAW_GATEWAY_TOKEN is required. " +
        "Set it in env or pass gatewayToken in options."
      );
    }
  }

  /**
   * Send a prompt to an LLM via the gateway.
   *
   * @param {object} params
   * @param {string} params.prompt — User message content
   * @param {string} [params.system] — System message
   * @param {string} [params.model] — Override default model
   * @param {number} [params.maxTokens] — Max response tokens
   * @param {number} [params.timeoutMs] — Override timeout
   * @returns {Promise<{ text: string, model: string, raw: object }>}
   */
  async complete({ prompt, system, model, maxTokens, timeoutMs }) {
    const messages = [];
    if (system) {
      messages.push({ role: "system", content: system });
    }
    messages.push({ role: "user", content: prompt });

    const body = {
      model: model ?? this.defaultModel,
      messages,
      max_tokens: maxTokens ?? DEFAULT_MAX_TOKENS,
    };

    const url = `${this.gatewayUrl}/v1/chat/completions`;
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${this.gatewayToken}`,
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(timeoutMs ?? this.timeoutMs),
    });

    if (!response.ok) {
      const errorText = await response.text().catch(() => "");
      throw new LLMError(
        `Gateway returned ${response.status}: ${errorText.slice(0, 200)}`,
        { status: response.status, model: body.model }
      );
    }

    const result = await response.json();
    const choice = result.choices?.[0];
    if (!choice?.message?.content) {
      throw new LLMError("Empty response from gateway", {
        model: body.model,
        raw: result,
      });
    }

    return {
      text: choice.message.content,
      model: result.model ?? body.model,
      raw: result,
    };
  }

  /**
   * Send a prompt and parse the response as JSON.
   * Strips markdown code fences if present.
   *
   * @param {object} params — Same as complete(), response parsed as JSON
   * @returns {Promise<{ data: any, text: string, model: string, raw: object }>}
   */
  async completeJSON(params) {
    const systemPrefix =
      "You are a JSON-only function. Return ONLY valid JSON. " +
      "Do not wrap in markdown fences. Do not include commentary.";

    const result = await this.complete({
      ...params,
      system: params.system
        ? `${systemPrefix}\n\n${params.system}`
        : systemPrefix,
    });

    const cleaned = stripCodeFences(result.text);
    try {
      const data = JSON.parse(cleaned);
      return { data, ...result };
    } catch (err) {
      throw new LLMError(
        `Failed to parse LLM response as JSON: ${err.message}`,
        { model: result.model, text: result.text }
      );
    }
  }

  /**
   * Compatibility shim: call(input, options) for modules using the older API.
   */
  async call(input, options = {}) {
    const prompt = Array.isArray(input)
      ? input.map((m) => m.content).join("\n")
      : input;
    const result = await this.complete({
      prompt,
      system: options.system,
      model: options.model,
      maxTokens: options.maxTokens,
    });
    return {
      content: result.text,
      stopReason: "stop",
      usage: result.raw?.usage ?? {},
      raw: result.raw,
    };
  }
}

export class LLMError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = "LLMError";
    this.details = details;
  }
}

/**
 * Strip markdown code fences from LLM output.
 * @param {string} s
 * @returns {string}
 */
function stripCodeFences(s) {
  const trimmed = s.trim();
  const m = trimmed.match(/^```(?:json|JSON)?\s*\n?([\s\S]*?)\n?\s*```\s*$/);
  if (m) return (m[1] ?? "").trim();
  const jsonMatch = trimmed.match(/(\{[\s\S]*\}|\[[\s\S]*\])/);
  return jsonMatch ? jsonMatch[1].trim() : trimmed;
}

/**
 * Create an LLMClient from environment, with graceful fallback.
 * Returns null if gateway token is not configured.
 *
 * @param {object} [options]
 * @returns {LLMClient | null}
 */
export function createLLMClient(options = {}) {
  const token = options.gatewayToken ?? process.env.OPENCLAW_GATEWAY_TOKEN;
  if (!token) return null;
  return new LLMClient({ ...options, gatewayToken: token });
}
