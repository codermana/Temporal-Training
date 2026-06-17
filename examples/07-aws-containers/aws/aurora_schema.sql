-- Aurora (PostgreSQL-compatible) as the TRANSACTIONAL SINK of the import
-- pipeline: the `load` step writes rows here. Temporal Activities are
-- at-least-once — a retried, timed-out, or replayed-after-Worker-crash `load`
-- Activity can run TWICE. Plain INSERTs would double the data. The fix is the
-- relational analog of the DynamoDB conditional-write example: a UNIQUE
-- idempotency key plus INSERT ... ON CONFLICT DO NOTHING, all inside ONE
-- transaction so a partial batch never leaks. at-least-once Activity +
-- idempotent SQL = effectively-once writes.

-- ---------------------------------------------------------------------------
-- The destination table for imported rows.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS loaded_rows (
    id              BIGSERIAL PRIMARY KEY,
    -- The idempotency key. STABLE ACROSS ATTEMPTS: it must be derived from
    -- something deterministic (workflowId + a batch index / a business key),
    -- NOT from wall-clock time or random() — otherwise a retry produces a new
    -- key and the dedupe does nothing. Format used by the load Activity:
    --   "<workflowId>:<batchIndex>"  e.g. "import-2026-06-16:0"
    idempotency_key TEXT        NOT NULL,
    order_id        TEXT        NOT NULL,
    amount_cents    BIGINT      NOT NULL,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- The dedupe lives HERE: the unique constraint is what makes ON CONFLICT
    -- DO NOTHING a no-op on the second attempt. Without it, ON CONFLICT has no
    -- target and the second insert succeeds — duplicating the row.
    CONSTRAINT loaded_rows_idempotency_key_uniq UNIQUE (idempotency_key)
);

-- ---------------------------------------------------------------------------
-- The idempotent load, as ONE transaction.
--
-- Run all the INSERTs for a batch between BEGIN and COMMIT. If the Activity is
-- killed mid-batch (Worker crash, timeout), the transaction rolls back and the
-- retry re-runs the WHOLE batch from a clean slate — no half-loaded state. The
-- per-row ON CONFLICT then absorbs any rows a previous *committed* attempt
-- already wrote, so a partially-committed-then-retried batch still converges to
-- exactly one copy of each row.
--
-- AVOID auto-commit-per-row: if each INSERT commits on its own, a crash leaves
-- the batch half-written AND you've thrown away the atomicity the transaction
-- was giving you.
-- ---------------------------------------------------------------------------
BEGIN;

-- One statement per row in the batch (the load Activity binds these as a
-- batched/prepared INSERT). The key is workflowId + batchIndex, so attempt 2 of
-- the same Workflow produces the SAME keys and conflicts harmlessly.
INSERT INTO loaded_rows (idempotency_key, order_id, amount_cents)
VALUES
    ('import-2026-06-16:0', 'ORD-1', 1000),
    ('import-2026-06-16:1', 'ORD-2', 2000),
    ('import-2026-06-16:2', 'ORD-3', 3000)
ON CONFLICT (idempotency_key) DO NOTHING;   -- the dedupe: a duplicate row is skipped, not errored

COMMIT;

-- ---------------------------------------------------------------------------
-- Variant: a dedicated dedup table (when you don't want the key column on the
-- business table, or many tables share one import). Claim the key first; only
-- the first claimant proceeds to write. This is the SQL twin of the DynamoDB
-- `attribute_not_exists(pk)` conditional put.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS import_dedup (
    idempotency_key TEXT PRIMARY KEY,        -- PRIMARY KEY is already UNIQUE
    claimed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    status          TEXT        NOT NULL DEFAULT 'DONE'   -- IN_PROGRESS / DONE, for the UPSERT-status stretch goal
);

-- Inside the same transaction as the business writes: claim the key, and if it
-- was already claimed (RETURNING yields no row), skip the batch entirely.
-- BEGIN;
--   INSERT INTO import_dedup (idempotency_key) VALUES ('import-2026-06-16:batch-0')
--   ON CONFLICT (idempotency_key) DO NOTHING
--   RETURNING idempotency_key;        -- empty result => already loaded, do nothing
--   -- ... if claimed, INSERT the batch's business rows ...
-- COMMIT;

-- ---------------------------------------------------------------------------
-- DO NOTHING vs DO UPDATE: use DO NOTHING for an append-only load (a retry must
-- be a no-op). Use DO UPDATE (UPSERT) when a re-run should refresh a mutable
-- column — e.g. mark the row's status — while still keyed on the same stable key:
--
--   INSERT INTO loaded_rows (idempotency_key, order_id, amount_cents)
--   VALUES ('import-2026-06-16:0', 'ORD-1', 1000)
--   ON CONFLICT (idempotency_key)
--   DO UPDATE SET amount_cents = EXCLUDED.amount_cents, loaded_at = now();
