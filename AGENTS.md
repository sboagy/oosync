# oosync AGENTS Instructions

Scope: repository-wide instructions for the oosync codebase.

Global execution guardrails live in `.github/copilot-instructions.md`. `ARCHITECTURE.md` is the top-level cross-cutting architecture guide for this repository and should be read before making changes that affect codegen, runtime boundaries, public exports, or worker behavior.

## Instruction Hierarchy

Root (this file) -> `src/AGENTS.md` (generator, shared contract, runtime, sync engine) -> `worker/AGENTS.md` (generic worker runtime).

## What oosync Is

oosync is an opinionated offline sync toolkit for Postgres-backed applications that run against a local SQLite database and synchronize through a worker.

It owns:

- schema introspection and generated contract production
- shared protocol and metadata primitives
- a portable sync runtime for local SQLite workflows
- a generic worker runtime for push/pull against Postgres

It does not own consumer UI behavior, consumer schema semantics, or consumer deployment wiring.

## Core Invariants

1. Schema-agnostic by default. Runtime code must not encode consumer-specific table names, column names, collections, or domain terminology.
2. Generation over hand-maintenance. If schema-derived output is wrong, fix generator logic or inputs rather than patching generated files.
3. Consumer-owned artifacts. Generated files belong to the consuming project, not to hand-authored code in this repository.
4. Layer separation. `src/**` must not depend on `worker/**`; generic worker code must not depend on consumer application code.
5. Local-database-first runtime. oosync assumes the consumer's live runtime data is local and sync is the transport/reconciliation layer around it.
6. Strict typing. Avoid `any`; use structural types and narrowing at dynamic boundaries.
7. Portability. Code should remain suitable for packaging as a standalone npm library.

## Repository Stack

- TypeScript (strict mode)
- Drizzle ORM
- postgres-js
- Cloudflare Worker runtime types
- jose for JWT verification
- Biome for lint/format/check
- Vitest for targeted tests

## Generated Artifact Rules

- Generated outputs are write-only.
- Do not hand-edit generated artifacts to fix behavior.
- If a task requires changing generated output shape, update `src/codegen-schema.ts` and validate regeneration/drift behavior.
- Prefer Postgres catalogs and table comments over manual overrides when new facts can be inferred safely.

## Boundary Rules

- Do not import consumer application code anywhere in this repository.
- Do not make `src/**` depend on worker runtime details.
- Do not hard-code consumer-specific SQL or sync rules in worker logic.
- Keep shared protocol and metadata layers data-oriented and portable.

## Validation Commands

- `npm run typecheck`
- `npm run lint`
- `npm run check`
- `npm run test`

Use the smallest relevant set for the change you made, but validate public-surface and architecture changes more broadly.

## What To Read First

- Codegen or output-shape changes: `ARCHITECTURE.md`, then `src/AGENTS.md`, then `src/codegen-schema.ts`.
- Shared protocol/runtime changes: `ARCHITECTURE.md`, then `src/AGENTS.md`, then the relevant files under `src/shared/**`, `src/runtime/**`, or `src/sync/**`.
- Worker behavior changes: `ARCHITECTURE.md`, then `worker/AGENTS.md`, then `worker/src/index.ts` and `worker/src/sync-schema.ts`.

## Stop Signs

Pause and ask if:

- the requested fix requires hand-editing generated consumer artifacts
- the change would introduce consumer-specific logic into oosync runtime code
- the change would cross the `src/**` -> `worker/**` boundary in the wrong direction
- the proposed solution adds manual workflow steps where inference or config should be the durable approach
