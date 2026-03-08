import { existsSync } from "node:fs";
import os from "node:os";
import path from "node:path";

export const EMBEDDING_MODEL_NAME = "embeddinggemma-300m";
export const EMBEDDING_DIMENSIONS = 768;
export const EMBEDDING_MODEL_PATH = path.join(
  os.homedir(),
  ".node-llama-cpp",
  "models",
  "hf_ggml-org_embeddinggemma-300m-qat-Q8_0.gguf",
);

const state = {
  loadPromise: null,
  workQueue: Promise.resolve(),
  warnings: new Set(),
};

function warnOnce(key, message) {
  if (state.warnings.has(key)) {
    return;
  }

  state.warnings.add(key);
  console.warn(`[embeddings] ${message}`);
}

async function loadRuntime() {
  if (state.loadPromise) {
    return state.loadPromise;
  }

  state.loadPromise = (async () => {
    let llamaModule;

    try {
      llamaModule = await import("node-llama-cpp");
    } catch (error) {
      if (error?.code === "ERR_MODULE_NOT_FOUND" || /Cannot find package ['"]node-llama-cpp['"]/.test(error?.message ?? "")) {
        warnOnce("missing-package", "node-llama-cpp is unavailable; semantic retrieval is disabled.");
        return null;
      }

      warnOnce("module-load-failed", `Failed to import node-llama-cpp: ${error.message}`);
      return null;
    }

    if (!existsSync(EMBEDDING_MODEL_PATH)) {
      warnOnce("missing-model", `Embedding model not found at ${EMBEDDING_MODEL_PATH}; semantic retrieval is disabled.`);
      return null;
    }

    try {
      const { getLlama, LlamaLogLevel } = llamaModule;
      const llama = await getLlama({ logLevel: LlamaLogLevel.error });
      const model = await llama.loadModel({ modelPath: EMBEDDING_MODEL_PATH });
      const context = await model.createEmbeddingContext();

      return {
        context,
        model,
      };
    } catch (error) {
      warnOnce("runtime-load-failed", `Failed to initialize embeddings: ${error.message}`);
      return null;
    }
  })();

  const runtime = await state.loadPromise;
  if (!runtime) {
    state.loadPromise = null;
  }

  return runtime;
}

function toFloat32Array(vector) {
  if (!Array.isArray(vector) || !vector.length) {
    return new Float32Array(0);
  }

  return new Float32Array(vector);
}

// embeddinggemma-300m has a ~2048 token context window.
// Truncate long texts to stay within limits (~4 chars per token conservatively).
const MAX_EMBED_CHARS = 6000;

function truncateForEmbedding(text) {
  const s = String(text ?? "");
  if (s.length <= MAX_EMBED_CHARS) return s;
  return s.slice(0, MAX_EMBED_CHARS);
}

function runQueued(work) {
  const job = state.workQueue
    .catch(() => undefined)
    .then(work);

  state.workQueue = job.catch(() => undefined);
  return job;
}

export async function embedText(text) {
  return runQueued(async () => {
    const runtime = await loadRuntime();
    if (!runtime) {
      return null;
    }

    const embedding = await runtime.context.getEmbeddingFor(truncateForEmbedding(text));
    return toFloat32Array(embedding.vector);
  });
}

export async function embedBatch(texts) {
  if (!Array.isArray(texts) || !texts.length) {
    return [];
  }

  return runQueued(async () => {
    const runtime = await loadRuntime();
    if (!runtime) {
      return null;
    }

    const embeddings = [];
    for (const text of texts) {
      const embedding = await runtime.context.getEmbeddingFor(truncateForEmbedding(text));
      embeddings.push(toFloat32Array(embedding.vector));
    }

    return embeddings;
  });
}
