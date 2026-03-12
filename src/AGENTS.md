# src/AGENTS Instructions

Scope: `src/**` in the oosync repository.

Inherits global execution guardrails from `.github/copilot-instructions.md`, repository architecture from `ARCHITECTURE.md`, and repo-wide invariants from the root `AGENTS.md`.

## Mission

Maintain the schema-agnostic library surface of oosync:

- code generation from Postgres introspection
- shared protocol and metadata helpers
- runtime abstractions for platform injection
- the sync engine, outbox, adapters, and service lifecycle

This layer should be portable and publishable without any consumer-application assumptions.

## Layer Map

- `src/codegen-schema.ts`
  - Introspects Postgres and writes generated artifacts into consumer-owned paths.
- `src/shared/**`
  - Shared protocol, constants, and metadata helpers.
- `src/runtime/**`
  - Platform contracts and runtime-level abstractions.
- `src/sync/**`
  - Core synchronization runtime and adjacent tests.
- `src/index.ts`
  - Public package surface.

## Critical Rules

1. No worker imports. `src/**` must not depend on `worker/**`.
2. No consumer imports. Do not pull in UI libraries, framework-specific packages, or consumer schema modules.
3. No hard-coded schema assumptions. Table names, column names, and rule semantics must come from generated metadata, config, or injected runtime state.
4. Keep public exports deliberate. Changes to `src/index.ts` or exported types are API changes and should be treated carefully.
5. Keep shared protocol files minimal and portable. Avoid unnecessary runtime dependencies in `src/shared/**`.
6. Adjacent tests under `src/sync/**` should remain deterministic and narrowly scoped.

## Codegen Guidance

- Prefer inference from catalogs, constraints, indexes, and Postgres comments before adding override-only behavior.
- If the generated output is wrong, fix the generator logic or input interpretation rather than adjusting the generated content.
- Preserve stable output ordering where possible to keep diffs reviewable.
- Treat drift-check behavior as part of the contract, not as optional tooling.

## Sync Runtime Guidance

- `runtime-context.ts` is the injection boundary for platform-specific behavior.
- `adapters.ts` is the single place for table-level casing and normalization helpers.
- `engine.ts` and `service.ts` should orchestrate sync using injected runtime state, not direct consumer dependencies.
- `apply-remote-changes.ts` and `outbox.ts` should remain generic over schema objects and conflict targets.

## Validation

- Run `npm run typecheck` for type-boundary changes.
- Run `npm run lint` or `npm run check` for style and static-rule changes.
- Run `npm run test` when changing sync behavior or adjacent tests.