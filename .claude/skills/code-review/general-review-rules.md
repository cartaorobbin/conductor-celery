# General Review Rules

Use this file for all PR reviews, regardless of stack.

## Review philosophy

- Prioritize business rules and correctness.
- Prefer simple, readable code over clever code.
- Avoid premature abstractions.
- Flag architecture problems only when they are real and material.

## Language and tone

- Always respond in Portuguese.
- Be direct: identify the problem, explain why it matters, propose a fix.
- Do not hedge and do not add praise as noise.
- Do not include final summaries inside inline comments.

## Severity model

### 🔴 Critical — blocks merge

- Logic bugs: unhandled edge cases, broken conditions, wrong behavior in critical flows.
- Bug fix without regression test.
- New business logic without tests.
- Commented-out tests.
- Cross-module database access.
- Critical anti-patterns in critical flows (god object, hidden side effects, harmful coupling).
- Security flaws (injection, secrets in code, unsafe deserialization, path traversal, SSRF).
- Explicit violations of project rules marked as `MUST`, `MUST NOT`, `NEVER`, `FORBIDDEN`, or equivalent hard language.

### 🟡 Important — should be fixed

- Premature abstractions.
- Scattered business rules.
- Pattern divergence without justification.
- Missing tests for critical changes.
- Low-quality tests (happy-path only, weak assertions, excessive mocking, implementation-coupled tests).
- Flaky test risks (time/order/shared-state/external dependency instability).
- Complex control flow inside tests (`if/else`, loops, `try/except` in test body).
- Excessive complexity and low readability.
- Error handling that hides root causes.
- Project-convention mismatches expressed as expected/default behavior (`should`, `prefer`, conventions, or documented architectural decisions).

### 🔵 Suggestion — optional but useful

- Naming or structure improvements with real readability/maintainability value.
- Non-critical duplication or simplification opportunities.
- Optional convention improvements documented by the project.

## What to look for

Review each modified snippet inline. Skip snippets without material issues.

- Bugs and logic errors.
- Readability and maintainability risks.
- Missing or weak tests for non-trivial behavior.
- Violations of project-local conventions loaded in Phase 2.0.

Only comment when there is a concrete problem, real risk, or clear simplification with material value.

## Project pattern detection

Before criticizing architecture, read existing patterns in the repository.

Flag only when:
1. The change clearly diverges from established patterns without justification, or
2. The pattern is objectively harmful.

Do not impose patterns that do not exist.

## Readability bar

A mid-level developer should understand a function in under 30 seconds.
If not, flag complexity and suggest simplification.

## Test evaluation checklist

- Do tests cover edge cases, not only happy path?
- Do assertions verify behavior (not only execution)?
- Are tests stable and deterministic?
- Is mocking minimal and justified?

## Review guidelines

- Be specific and explain why.
- Suggest fixes when possible.
- Avoid lint/format-only comments.
- If a file is good, do not force comments.
