---
name: data-layer
description: How to change persistence in multi-agent-search safely — SQLAlchemy models, hand-written Alembic migrations (revision naming, CONCURRENTLY indexes, batched data migrations), the TaskStore protocol and its two implementations (in-memory and SQLAlchemy) kept in parity by conformance tests, row locking and compare-and-set writes, and the search/finalize job queue (claims with SKIP LOCKED, leases, retries, broker re-dispatch, stale recovery). Use it whenever you add a column, table, index or store method, write a migration, touch src/repositories/, src/db/, alembic/, src/workers/ or job_queue_mixin.py, debug a race or a stuck research, or a conformance/migration test fails.
---

# Data layer: schema, migrations, stores, job queue

## Architecture in one screen

- **`src/repositories/protocols.py` `TaskStore`** is the contract: about 100 methods. The ~30 with non-obvious guarantees (locking, CAS, ordering) carry a contract comment above them; read it before calling or changing one.
- **Two implementations must stay in parity:**
  - `InMemoryTaskStore` holds dicts and lists. Three RLocks mirror the SQL locking only where a guard matters:
    - `_state_lock` is taken by the guarded writes: status transition, reset-for-retry, `try_begin_finalization`, graph_state merge/append, `append_research_graph_event`, `requeue_failed_research_finalize_job`, `requeue_search_task_job_of_active_research`, `delete_research_tasks`. Plain job writes (claims, complete, update, record-failure, stale recovery) take no lock.
    - `_user_lock` covers users and auth tokens.
    - `_admission_lock` covers admission.
  - `SQLAlchemyTaskStore` runs each method in one `session_scope()`: one transaction, committed on exit. It calls `_emit_change(research_id)`, which sends the SSE notify, **after** the `with` block, so it fires post-commit.
- **Domain records** are Pydantic models in `src/domain/models.py`, re-exported from `src/domain/__init__.py` (guarded by `tests/test_domain_exports.py`).
- **Mappers** from ORM to domain live in `src/repositories/mappers.py`; the user and token mappers are private to the SQL store.
- **Factory** (`src/repositories/factory.py`): `TASK_STORE_BACKEND` defaults to `postgres`. `memory` is refused unless `ALLOW_MEMORY_TASK_STORE=true` or `DEBUG=true`.
- **Engine** (`src/db/session.py`): `pool_pre_ping`, `hide_parameters=True`, and `prepare_threshold: None`, because the app and workers connect through PgBouncer in transaction mode. Never rely on session-level state. The admission lock is `pg_advisory_xact_lock`, which is transaction-scoped.
- **Parity guards** in `tests/test_task_store_conformance.py` (and `test_admin_store_conformance.py`):
  - The `store` fixture runs every test on `memory` and on `postgres`. The postgres leg is marked `postgres` and skips without a database.
  - `test_every_protocol_method_runs_in_the_conformance_suite` parses the modules with AST and requires a call `store.<method>(…)` for every public protocol method. The receiver must be a variable literally named `store`.
  - `test_both_stores_implement_the_protocol_signatures` requires both classes' parameters (name, kind, default) to equal the protocol's exactly.

## Recipe: a new column or table and a store method

1. **`src/db/models.py`.**
   - Timestamps: `DateTime(timezone=True)`, `default=utcnow`. JSONB: `default=dict` or `default=list`.
   - FKs: `ondelete="CASCADE"` plus `passive_deletes=True` on the relationship, or `SET NULL`.
   - Declare every index and CHECK constraint in the model. Partial indexes use `postgresql_where=text(…)`.
   - **Server defaults must match the migration.** `tests/test_migration_contract.py` compares types and server defaults between the ORM and the migrated schema at head, and the diff must be empty. Either keep the same `server_default` in the model, or add the column with a default and drop it in the migration (`op.alter_column(…, server_default=None)`, as `000023` does for `language`).
2. **Migration** `alembic/versions/YYYYMMDD_NNNNNN_<snake_description>.py`.
   - Hand-written. The revision id is the date plus a global six-digit counter, and the chain is linear with one head. The head is the `.py` file with the largest counter (`ls alembic/versions/2*.py | sort | tail -1`; a bare `ls` also lists `__pycache__`). Set `down_revision` to the value of that file's `revision =` line, not its docstring (see `000026`).
   - Docstring format: one-line summary, `Revision ID:`, `Revises:`, `Create Date:`, then *why*, including the lock and backfill reasoning.
   - Then `revision`, `down_revision`, `branch_labels = None`, `depends_on = None`, `upgrade()`, `downgrade()`.
   - Copy the closest example:
     - add a column: `20260925_000029_add_llm_usage_cache_hit_tokens.py`;
     - a table plus backfill: `20260925_000033_add_email_verification_and_auth_action_tokens.py`;
     - an index on a live table: `20260925_000031_add_researches_created_index.py`;
     - a batched data migration: `20260925_000032_delete_orphan_prompt_copies.py`.
3. **Domain field** in `src/domain/models.py`, plus the mapper (or `_user_record` / `_auth_action_token_record`). A new record class also goes into both the import block and `__all__` of `src/domain/__init__.py` (`tests/test_domain_exports.py`).
4. **Protocol method** with a contract comment. Use the same keyword-only `*` and defaults in all three places.
5. **Both implementations.** SQL in one `session_scope`. In-memory under the matching RLock, returning copies where SQL returns detached objects (see pitfalls).
6. **Conformance test** calling `store.<method>`, covering the success case *and* the lost-guard/conflict case.
7. **New table only:**
   - Add it to the TRUNCATE list in `tests/postgres_helpers.py`. Nothing enforces this, and a missing entry leaks rows between tests.
   - Add a `_SQL_ROWS` entry and an in-memory branch to `_backdate` in the conformance suite if tests need to age its rows.
   - **The in-memory store copies FK cascades by hand.** For `ON DELETE CASCADE` or `SET NULL`, do the same in the in-memory `delete_user` / `delete_research`, and test the delete in the conformance suite (`delete_research` already leaves finalize and search jobs behind that SQL would cascade).
8. **Migration step test** in `tests/test_migration_steps.py`, for any backfill, data migration or non-trivial index. The pattern: `database_at("<previous rev>")` → seed rows with raw SQL → `migrate_throwaway_database(_DATABASE, "<new rev>")` → assert → downgrade → upgrade again. Alembic does not compare CHECK constraints, so test them explicitly (`pg_constraint`, a failing insert), as the `000033` test does.
9. **README.** Add operationally significant revisions (long builds, irreversible deletes) to "Database Migrations".

Then run `ci_local.sh pytest tests/test_task_store_conformance.py`, bring up a database and run `ci_local.sh postgres`, and run `ci_local.sh smoke` before pushing (see the `run-checks` skill).

## Migration safety rules

- **Index on a live table:**
  - Inside `with op.get_context().autocommit_block():` run `CREATE [UNIQUE] INDEX CONCURRENTLY IF NOT EXISTS`. Downgrade with `DROP INDEX CONCURRENTLY IF EXISTS`.
  - Before building, check `pg_index.indisvalid` and drop an INVALID leftover from a failed build, so a re-run repairs it (see `000031`).
  - A brand-new empty table can use plain `op.create_index`.
- **Constraint or FK on a live table:** `ADD CONSTRAINT … NOT VALID` inside the autocommit block, then `VALIDATE CONSTRAINT` (see `000026`).
- **Columns:** nullable with no default, or a constant `server_default`, is a metadata-only change. A NOT NULL column comes with a server default.
- **Data migrations:**
  - A small table: one UPDATE.
  - A large table: a keyset loop of 1,000 rows per transaction inside `autocommit_block`, with a single-statement fallback in offline (`--sql`) mode (see `000032`).
  - An irreversible migration has `downgrade(): pass` and says "take a backup first" in the README (`scripts/backup_postgres.sh`).
- **Connections:** the compose `migrate` service connects straight to `db`, not PgBouncer. `alembic/env.py` takes the URL from `settings.resolved_database_url`.
- **Timeouts:** no migration sets `lock_timeout` or `statement_timeout`. Think about locks yourself.

## Concurrency rules for any writer

- **Read-modify-write** happens in one `session_scope`: `select(M).where(M.id == …).with_for_update()`, then write. Models:
  - `merge_research_graph_state`
  - `append_research_graph_event`
  - `transition_research_status`
  - `reset_research_for_retry`
- **graph_state.** Service code never computes a whole `graph_state` from an earlier read. Pass only the keys it owns to `merge_research_graph_state(research_id, patch, remove_keys=[…])`, and use `append_research_graph_state_item` for lists (`tests/test_locked_graph_state_writes.py`).
- **Counters** are atomic in SQL: `values(col=M.col + 1)` (`token_version`, `lease_epoch`), or a SQL `case(…)`.
- **Compare-and-set.** Put the guard in the WHERE clause and check the outcome:
  - `.returning(M)` with `scalar_one_or_none()`, where None means another writer won;
  - or `rowcount == 1`.
  - The service turns a lost guard into `ConflictError` or a no-op.
- **Claims:** `status='pending' ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED`, then set RUNNING and `attempt_count + 1`. The by-id variants also use SKIP LOCKED. `tests/test_job_claim_concurrency.py` runs 8 threads on Postgres.
- **Lock order:**
  - finalize job row, then its research row (`complete_research_finalize_job`, `requeue_failed_research_finalize_job`);
  - research row, then its search tasks and search jobs (`requeue_search_task_job_of_active_research`, `delete_research_tasks`);
  - users row, then `auth_action_tokens`.
- **`FOR NO KEY UPDATE`** (`with_for_update(key_share=True)`) avoids blocking FK inserts, as in `try_begin_finalization`.
- **Upserts:** `pg_insert(…).on_conflict_do_update(…)`, never check-then-insert. Branch on SQLSTATE 23505 / 23503 where needed.
- **Retention deletes:** model new ones on `cleanup_old_researches`: batches of 1,000, one transaction each, with the predicate re-checked in the DELETE itself. The job and cache cleanups select ids and then delete by id with no re-check, so do not copy them.
- **The unique index `uq_running_finalize_job_per_research`** (partial) is the database backstop against two RUNNING finalize jobs for one research.

**Not models to copy.** A few existing SQL methods break these rules:
- `update_search_task_job` and `record_search_task_job_failure` read without FOR UPDATE, then write;
- `recover_stale_search_task_jobs` selects RUNNING rows unlocked;
- `put_cached_search` and `upsert_worker_heartbeat` check, then insert.

Copy the guarded methods named above instead.

**Checklist for a new writer:**
- one `session_scope`;
- `FOR UPDATE` on any row read before writing;
- the guard in the WHERE clause;
- return None/False on a lost guard;
- `_emit_change` after commit;
- the in-memory twin under the same RLock;
- conformance tests for both outcomes.

## Job queue

- **States** (`FinalizeJobStatus`, `SearchJobStatus`):
  - `pending` → `running` on claim (attempts + 1).
  - From `running`: `completed`; back to `pending` on failure while attempts < max; `dead_letter` at the max; `failed` for a search job whose task is gone.
  - Requeue: the store allows it from `dead_letter` or `failed`, the admin service only from `dead_letter`.
- **Finalize leases.** `lease_epoch` fences stale runners (`ensure_finalize_job_lease`, which raises `FinalizeLeaseLost`). Complete, update and record-failure take the epoch: a runner must always pass it, because `lease_epoch=None` skips the fence. Requeue and stale recovery bump it. Search jobs have **no** lease.
- **Enqueue:**
  - Search: `add_search_task_job`, then `broker.push_search_job`.
  - Finalize: `enqueue_research_finalization`, which runs the `try_begin_finalization` CAS into ANALYZING, then `_dispatch_finalize_job` (reuses, requeues or adds a job, then pushes).
- **Workers.** `scripts/run_finalize_worker.py [--once]` runs `JobWorker`: maintenance if due, then `SearchWorker`, then `FinalizeWorker`, then a heartbeat.
  - Broker mode: BLPOP an id, then `claim_*_by_id`. If that is not claimable, fall back to `claim_next_*`, which recovers lost pushes.
  - Polling mode: drain `claim_next_*`.
- **Broker** (`src/brokers/redis_broker.py`): the lists `mas:search_jobs` and `mas:finalize_jobs` carry ids only. Push failures are logged, and Postgres is the source of truth.
- **Maintenance** (`JobQueueMixin.run_queue_maintenance`) runs isolated steps: pending decompositions, stale search recovery, stale finalize recovery, the stalled-research sweep, cleanups. The stale cutoff is never below the job timeout.

**Twins that must change together:**
1. The memory store and the SQL store, for every method.
2. **Every path that leaves a job PENDING must push to the broker:** create, retry after failure, stale recovery, admin requeue, `_dispatch_finalize_job`, for both search and finalize. The improvement plan's `JOB-RETRY` finding was a retry path without this push, which hung researches forever.
3. `claim_next_*` and `claim_*_by_id`; the broker and polling branches of both workers.
4. The "latest job" ordering (created_at, updated_at, id descending). The in-memory `_latest_job` breaks ties by insertion order.
5. A service pre-check and the store's guarded single-transaction method (`requeue_failed_research_finalize_job`, `requeue_search_task_job_of_active_research`). The store re-checks, and None becomes 409.

## Pitfalls

- **The in-memory store hands out its live objects**, while SQL returns detached copies. Mutating a returned record passes on memory and fails on Postgres; the conformance suite's postgres leg is what catches it.
- **In-memory claims take no row lock.** Real claim concurrency is only tested on Postgres.
- **No FK from `user_events` to `researches`.** Prompt copies are deleted explicitly in the same transaction as the research. Any new JSON reference to a research needs the same explicit cleanup.
- **Not atomic:** the finalize CAS and the job insert are separate transactions. A crash between them leaves ANALYZING with no job, and the stalled sweep is the safety net. Do not build new logic that assumes they are atomic.
- **Trust the code over docstrings:** `000026`'s docstring names the wrong `Revises:`.
- **`scripts/smoke_postgres_runtime.py` migrates and writes to the configured database.** It is not a throwaway.
- **Restoring:** stop api, workers and pgbouncer, run `scripts/restore_postgres.sh <dump> --confirm`, then migrate. Back up before any irreversible migration.
