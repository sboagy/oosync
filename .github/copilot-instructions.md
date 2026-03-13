# GitHub Copilot: Global Execution Rules for oosync

This file contains repo-wide execution guardrails for the oosync repository.

Repository architecture and layer ownership live in `ARCHITECTURE.md` and the root/scoped `AGENTS.md` files. Treat those files as the canonical source for how oosync is structured.

## 0. Analyze Before Editing

- Clarify ambiguous requirements before writing code.
- Identify the owning layer before changing anything:
  - `src/codegen-schema.ts` for schema introspection and output generation
  - `src/**` for shared contracts, runtime abstractions, and sync engine logic
  - `worker/**` for the generic worker runtime
- Prefer existing utilities, types, and helpers over introducing parallel patterns.
- Plan validation up front: `npm run typecheck`, `npm run lint`, `npm run check`, and targeted tests when behavior changes.

## 1. Reliability Rules

- Read the current file state before editing it.
- Verify symbols and surrounding context before applying a patch.
- If an edit introduces a type error, syntax error, or failing test, stop and correct it deliberately.
- Do not retry near-identical patches without new information.
- Before finishing, check for duplicate imports, exports, helper functions, config fields, and instruction text.

## 2. Repository Standards

- Keep oosync schema-agnostic. Do not hard-code consumer table names, column names, collections, or domain concepts in runtime logic.
- Generated outputs are write-only. If generated content is wrong, fix the generator or its inputs; do not patch generated files by hand.
- Preserve the separation between the library runtime and the worker runtime:
  - `src/**` must not depend on `worker/**`
  - generic worker code must not depend on consumer application code
- Keep public package entrypoints stable unless the task explicitly requires an API change.
- Prefer small, reviewable diffs, but not at the expense of clarity or correctness.

## 3. Type Safety

- TypeScript is strict in this repository. Avoid `any`.
- If a boundary is genuinely dynamic, use `unknown`, structural types, and explicit narrowing.
- Keep type assertions narrow and justified.
- Do not suppress lint/type issues when the underlying code can be typed correctly.

## 4. Validation Expectations

- Run the smallest relevant checks for the layer you changed.
- When touching architecture, codegen boundaries, runtime wiring, or public exports, read `ARCHITECTURE.md` first and update it if the description is no longer accurate.
- When changing codegen behavior, validate both generation behavior and drift-check behavior.

## 5. Process Guardrails

- **No Commits / Pushes:** Never commit, push, or open PRs unless explicitly requested.
- **Validate Changes:** When making code changes, run the smallest relevant checks (typecheck/lint/tests) when practical and report results.
- **Keep Architecture Docs Current:** If a task changes architecture, sync boundaries, codegen outputs, runtime wiring, or repository responsibilities, read `ARCHITECTURE.md` before editing and update it in the same change unless the architecture document remains accurate without modification.
- Do not revert unrelated user changes.
- If an existing boundary violation is outside the current task, leave it in place and call it out in the summary instead of broadening scope.
