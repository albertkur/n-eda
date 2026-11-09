# n-eda — contributor/agent guide

TypeScript framework for event-driven architecture (`@nivinjoseph/n-eda`). ESM only, Node >= 24.10.
Redis-backed partitioned event log with DI-resolved handlers.

## Where to look first

- `llms.txt` — condensed API guide and, critically, the **rules the type system does not enforce**
  (serialization decorators, `.subscribe()` on topics, the silent publish-time drop, retry semantics).
  Read it before writing any consumer-style code.
- `test/utils/eda-test-utils.ts` + `test/eda.test.ts` — the canonical reference implementation
  (events, handlers, installer, a configured manager, and an end-to-end ordering assertion).
  **When README and code disagree, trust these files.**
- `test/utils/observer-test-utils.ts` + `test/observer.test.ts` — the reference implementation for the
  distributed observer: the three-decorator handler, `subscribeToObservables`, and the fan-out path.
- `README.md` — concepts and the full per-export API reference.
- `ARCHITECTURE.md` — the runtime: Redis keys, consume loop, delivery guarantees, tuning constants.
- `docs/known-issues.md` — split into **Open** (current behavior, documented deliberately) and **Fixed**.
  Check the Open list before "fixing" something; several entries are intentional.

## Layout

- `src/` — `src/index.ts` is the barrel (27 exports + the `Symbol.metadata` polyfill).
  Root-level files are the public contracts; `src/redis-implementation/` is the runtime;
  `src/discovery/` is convention-based handler discovery.
- `test/` — `node:test` suites, one process per file. `eda.test.ts`, `observer.test.ts`, and
  `event-age.test.ts` need a live Redis; `guards.test.ts` (bootstrap validation) and `otel-metrics.test.ts`
  (OpenTelemetry instruments, against an in-memory metrics SDK) do not.
- `dist/` — published build output, **committed to git**.

Fully commented-out and therefore inactive: `src/in-memory-implementation/*`,
`src/event-handler-tracer.ts`, `src/redis-implementation/consumer-profiler.ts`,
`src/redis-implementation/profiling-consumer.ts`. `src/redis-implementation/default-scheduler.ts` is
live but `@deprecated`; `Broker` always constructs `OptimizedScheduler`.

## Build & test (Yarn 4)

- `yarn ts-build` — typecheck + lint + compile in place (`tsconfig.json`). Compiled `.js` lands beside
  the sources in `src/`/`test/` and is gitignored.
- `yarn setup-redis-server` — Docker `redis:7.0` on localhost:6379. Required before tests. Recreates the
  container each time: `test/eda.test.ts` publishes deterministic event ids, so a reused Redis suppresses
  them as duplicates via the persisted dedupe window and the suite fails with `0 !== 20000`.
- `yarn test` — sets up Redis, builds, runs `node --test ./test/**/*.test.js`, tears Redis down, and
  **propagates the test exit code**. Tests exercise the compiled `.js`, so always build first.
- Running a single suite directly (`node --test ./test/observer.test.js`) skips the container recreation —
  `docker exec test-redis redis-cli flushall` first if you have run the suite before.
- `yarn ts-build-dist` — build the publishable `dist/`. Part of a release, not of ordinary work.

## Conventions & maintenance rules

- Runtime contracts are expressed with `given(...)` from `@nivinjoseph/n-defensive`; follow that style.
  Every user-visible error message in this library is a `given(...)` message.
- Standard ES2023+ decorators only (no `experimentalDecorators`, no `reflect-metadata`). Decorator
  metadata lives under `Symbol.metadata`, polyfilled at the top of `src/index.ts` — which is why
  importing the barrel is a prerequisite for the decorators to work at all.
- Decorator registration symbols use `Symbol.for("@nivinjoseph/n-eda/<name>")` so they stay identical
  across duplicate copies of the package. `src/discovery/event-handler-discovery.ts` re-derives them
  deliberately; keep the two in sync.
- House code style: Allman braces, 120-column lines (see `.vscode/settings.json`).
- Event class names and event *handler* class names are load-bearing identities — the former is the
  persisted wire discriminator, the latter is a DI registration key. Never rename either casually.
- **Docs must move with the API**: any public-signature change requires updating the README API
  reference, `llms.txt`, and the TSDoc on the changed member.
- **TSDoc reaches consumers only through `dist/`.** `package.json` points `types` at `./dist/index.d.ts`,
  `tsconfig` sets `removeComments: false`, and `dist/` is committed. Doc comments added to `src/` do
  not appear in a consumer's IDE until the next `yarn ts-build-dist` and commit.
- ESLint enforces no JSDoc rules — documentation coverage is a review responsibility, not a lint gate.
