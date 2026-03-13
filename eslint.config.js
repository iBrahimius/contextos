import js from "@eslint/js";

export default [
  js.configs.recommended,
  {
    languageOptions: {
      ecmaVersion: 2025,
      sourceType: "module",
      globals: {
        // Node.js globals
        console: "readonly",
        process: "readonly",
        Buffer: "readonly",
        URL: "readonly",
        URLSearchParams: "readonly",
        setTimeout: "readonly",
        setInterval: "readonly",
        clearTimeout: "readonly",
        clearInterval: "readonly",
        setImmediate: "readonly",
        clearImmediate: "readonly",
        queueMicrotask: "readonly",
        structuredClone: "readonly",
        fetch: "readonly",
        Response: "readonly",
        TextEncoder: "readonly",
        TextDecoder: "readonly",
        AbortController: "readonly",
        AbortSignal: "readonly",
        EventTarget: "readonly",
        Event: "readonly",
        performance: "readonly",
        crypto: "readonly",
        __dirname: "readonly",
        __filename: "readonly",
        global: "readonly",
        globalThis: "readonly",
      },
    },
    rules: {
      // False positives on variables initialized with defaults before conditional reassignment
      "no-useless-assignment": "off",
      // Allow unused vars prefixed with _
      "no-unused-vars": ["error", { argsIgnorePattern: "^_", varsIgnorePattern: "^_" }],
    },
  },
  {
    files: ["src/ui/**/*.js"],
    languageOptions: {
      globals: {
        document: "readonly",
        window: "readonly",
        localStorage: "readonly",
        sessionStorage: "readonly",
        HTMLElement: "readonly",
      },
    },
  },
  {
    files: ["test/**/*.js"],
    languageOptions: {
      globals: {
        // node:test globals
        describe: "readonly",
        it: "readonly",
        test: "readonly",
        before: "readonly",
        after: "readonly",
        beforeEach: "readonly",
        afterEach: "readonly",
      },
    },
  },
];
