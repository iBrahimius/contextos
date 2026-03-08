const RULES = {
  inbound: [
    {
      verdict: "block",
      reason: "Prompt override attempt",
      pattern: /\b(ignore|disregard|forget)\b.{0,32}\b(previous|prior|system)\b.{0,32}\binstruction/i,
    },
    {
      verdict: "block",
      reason: "Prompt exfiltration attempt",
      pattern: /\b(reveal|print|dump|show)\b.{0,24}\b(system prompt|hidden prompt|secret instructions)\b/i,
    },
    {
      verdict: "warn",
      reason: "Role confusion attempt",
      pattern: /\byou are now\b|\bpretend to be\b|\bact as\b/i,
    },
    {
      verdict: "warn",
      reason: "Tool misuse attempt",
      pattern: /\b(run shell|execute command|delete file|disable guardrails)\b/i,
    },
  ],
  outbound: [
    {
      verdict: "block",
      reason: "Potential secret leakage",
      pattern: /\b(api[_ -]?key|secret|token|password)\b.{0,16}[:=]/i,
    },
    {
      verdict: "warn",
      reason: "System prompt leakage",
      pattern: /\b(system prompt|hidden instructions|internal rules)\b/i,
    },
    {
      verdict: "warn",
      reason: "Unscoped filesystem disclosure",
      pattern: /\/Users\/|\/etc\/|\.ssh\//i,
    },
  ],
};

export class InjectionGuard {
  scan({ direction, text }) {
    const rules = RULES[direction] ?? [];
    const reasons = [];
    let verdict = "allow";

    for (const rule of rules) {
      if (!rule.pattern.test(text)) {
        continue;
      }

      reasons.push(rule.reason);
      if (rule.verdict === "block") {
        verdict = "block";
      } else if (verdict !== "block") {
        verdict = "warn";
      }
    }

    return { verdict, reasons };
  }
}
