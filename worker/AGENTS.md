# worker/AGENTS Instructions

Scope: `worker/**` in the oosync repository.

Inherits global execution guardrails from `.github/copilot-instructions.md`, repository architecture from `ARCHITECTURE.md`, and repo-wide invariants from the root `AGENTS.md`.

## Mission

Maintain the generic worker runtime that consumers embed in their own worker package.

This layer is responsible for:

- request authentication and sync endpoint handling
- push application against Postgres
- pull evaluation using generated metadata and worker config
- schema-driven filtering, sanitization, and collection loading

This layer must remain generic and artifact-injected.

## Layer Map

- `worker/src/index.ts`
  - Worker factory, fetch entrypoint, auth verification, DB setup, push/pull orchestration.
- `worker/src/sync-schema.ts`
  - Table metadata helpers, pull-rule evaluation, push sanitization, and config interpretation.
- `worker/src/debug.ts`
  - Diagnostics utilities.

## Critical Rules

1. No consumer imports. Consumer worker packages should inject artifacts into this layer; this layer must not import consumer code.
2. No hard-coded schema knowledge. Table names, columns, ownership collections, and rule variants must come from injected artifacts or config.
3. Keep the worker entrypoint thin. `worker/src/index.ts` should orchestrate; schema/rule details belong in `worker/src/sync-schema.ts` or shared helpers.
4. Use shared protocol types for request and response shapes.
5. Preserve auth, DB, and error handling as generic infrastructure rather than consumer-specific policy.

## Artifact Injection Rules

- The worker must receive schema tables, syncable table names, table metadata, and worker config from the consumer.
- If a requested change seems to require editing generated consumer worker files, stop and move the change to generator logic or config inputs.
- Rule evaluation should remain declarative: config-driven collections, pull rules, and push rules are preferred over special-case code.

## Performance and Safety

- Keep query-building reviewable and typed.
- Prefer structured database errors over raw SQL-heavy messages.
- Be conservative with diagnostics so debug output can be enabled without changing behavior.
- Avoid leaking low-level implementation details into the public worker factory surface.

## Validation

- Run `npm run typecheck` for worker type changes.
- Run `npm run lint` or `npm run check` after changing worker logic.
- When behavior changes affect push/pull semantics, validate the relevant targeted tests in the repo.