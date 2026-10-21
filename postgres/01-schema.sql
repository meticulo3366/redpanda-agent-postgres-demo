-- postgres/01-schema.sql
-- Agent memory on PostgreSQL: schema for the Memory Concierge demo.
-- Contract: docs/PORT-CONTRACT.md section 4. All DDL is idempotent and lives in
-- schema agent_memory of the target database. Run as the admin user
-- (scripts/10-postgres-setup.sh, make setup-db).
--
-- Written against GantrySQL (PostgreSQL 18 wire- and SQL-compatible), which
-- differs from vanilla PostgreSQL in ways that shaped this file. Every point
-- below was verified live on the target server:
--   - stored generated columns that call md5() or to_tsvector() make every
--     INSERT fail, so content_hash is a plain column the writer computes and
--     there is no tsvector column
--   - GIN over to_tsvector() is refused; full text uses the engine's own index
--     type (USING fts), with a fallback to GIN so this file also runs on
--     vanilla PostgreSQL
--   - wal_level = replica: no logical decoding, so change capture is the
--     trigger-fed change_log table (02-triggers.sql) polled by Redpanda Connect
--   - casts inside trigger bodies fail, so every key a trigger handles is BIGINT

CREATE SCHEMA IF NOT EXISTS agent_memory;
SET search_path TO agent_memory, public;

-- customers: the people. tax_id is deliberately sensitive (masked by data policy).
CREATE TABLE IF NOT EXISTS customers (
  customer_id UUID        NOT NULL DEFAULT gen_random_uuid(),
  email       TEXT        NOT NULL,                 -- identity key used by the agent
  full_name   TEXT        NOT NULL,
  segment     TEXT        NOT NULL DEFAULT 'retail', -- retail | premier | business
  tax_id      TEXT,                                 -- SENSITIVE
  phone       TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_customers PRIMARY KEY (customer_id),
  CONSTRAINT uq_customers_email UNIQUE (email)
);

-- accounts: system of record. owner_email is denormalised so the change event
-- carries the memory subject without a join. account_number is SENSITIVE and is
-- deliberately left out of the change-log documents (02-triggers.sql).
CREATE TABLE IF NOT EXISTS accounts (
  account_id      BIGINT        NOT NULL,
  customer_id     UUID          NOT NULL REFERENCES customers (customer_id),
  owner_email     TEXT          NOT NULL,
  account_number  TEXT          NOT NULL,            -- SENSITIVE, partially masked
  nickname        TEXT          NOT NULL,
  account_type    TEXT          NOT NULL,            -- checking | savings | credit
  balance         NUMERIC(14,2) NOT NULL DEFAULT 0,
  currency        TEXT          NOT NULL DEFAULT 'USD',
  overdraft_limit NUMERIC(14,2) NOT NULL DEFAULT 0,
  status          TEXT          NOT NULL DEFAULT 'open',
  updated_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
  CONSTRAINT pk_accounts PRIMARY KEY (account_id),
  CONSTRAINT chk_account_type CHECK (account_type IN ('checking','savings','credit')),
  CONSTRAINT chk_status CHECK (status IN ('open','frozen','closed'))
);
CREATE INDEX IF NOT EXISTS idx_accounts_owner ON accounts (owner_email) INCLUDE (nickname, account_type, balance, currency, status);

-- transactions: recent activity, so "why am I overdrawn" has an answer.
CREATE TABLE IF NOT EXISTS transactions (
  txn_id      UUID          NOT NULL DEFAULT gen_random_uuid(),
  account_id  BIGINT        NOT NULL REFERENCES accounts (account_id),
  amount      NUMERIC(14,2) NOT NULL,                -- negative = debit
  merchant    TEXT          NOT NULL,
  category    TEXT          NOT NULL,
  occurred_at TIMESTAMPTZ   NOT NULL DEFAULT now(),
  CONSTRAINT pk_transactions PRIMARY KEY (txn_id)
);
CREATE INDEX IF NOT EXISTS idx_txn_account_time ON transactions (account_id, occurred_at DESC) INCLUDE (amount, merchant, category);

-- memories: the agent's long-term memory. One row per remembered thing.
-- content_hash makes "the same memory stated twice" an upsert, not a duplicate
-- (ON CONFLICT (subject, content_hash) DO UPDATE ...). The writer computes it as
-- md5(lower(subject) || '|' || lower(content)); the agent's INSERT and the
-- observer pipeline both do. Expired rows are removed by the hourly
-- pg-agent-memory-purge pipeline, and that DELETE is logged like any other change.
CREATE TABLE IF NOT EXISTS memories (
  memory_id    BIGINT      GENERATED ALWAYS AS IDENTITY,
  subject      TEXT        NOT NULL,   -- verified caller email, from the gateway
  kind         TEXT        NOT NULL,   -- preference | fact | goal | observation | episode
  content      TEXT        NOT NULL,   -- one sentence, third person ("Prefers short answers")
  importance   SMALLINT    NOT NULL DEFAULT 3,
  source       TEXT        NOT NULL DEFAULT 'agent',  -- agent | event-stream | human
  source_ref   TEXT,                   -- chat date, or accounts/<id>@<change_id> for observations
  content_hash TEXT        NOT NULL,   -- md5(lower(subject) || '|' || lower(content)), writer-computed
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at   TIMESTAMPTZ NOT NULL DEFAULT now() + INTERVAL '90 days',
  CONSTRAINT pk_memories PRIMARY KEY (memory_id),
  CONSTRAINT uq_memories_subject_content UNIQUE (subject, content_hash),
  CONSTRAINT chk_kind CHECK (kind IN ('preference','fact','goal','observation','episode')),
  CONSTRAINT chk_importance CHECK (importance BETWEEN 1 AND 5),
  CONSTRAINT chk_source CHECK (source IN ('agent','event-stream','human'))
);
CREATE INDEX IF NOT EXISTS idx_memories_recall ON memories (subject, expires_at, importance DESC, created_at DESC) INCLUDE (kind, content, source);

-- Full-text recall: to_tsvector('english', content) @@ plainto_tsquery('english', $1).
-- GantrySQL indexes that with its own access method; vanilla PostgreSQL with GIN.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'agent_memory' AND indexname = 'idx_memories_search') THEN
    BEGIN
      EXECUTE 'CREATE INDEX idx_memories_search ON agent_memory.memories USING fts (content) WITH (analyzer = ''english'')';
    EXCEPTION WHEN OTHERS THEN
      EXECUTE 'CREATE INDEX idx_memories_search ON agent_memory.memories USING GIN (to_tsvector(''english'', content))';
    END;
  END IF;
END $$;

-- change_log: the change stream. AFTER triggers (02-triggers.sql) append one row
-- per memory insert/update/delete and per account update; every second the
-- pg-agent-memory-cdc pipeline claims the unpublished rows (claimed_at, a
-- 30-second lease), publishes them to Redpanda and stamps published_at.
-- At-least-once: consumers are idempotent on change_id.
CREATE TABLE IF NOT EXISTS change_log (
  change_id    BIGINT      GENERATED ALWAYS AS IDENTITY,
  table_name   TEXT        NOT NULL,    -- memories | accounts
  op           TEXT        NOT NULL,    -- insert | update | delete
  row_key      BIGINT      NOT NULL,    -- memory_id or account_id
  subject      TEXT,                    -- memories.subject or accounts.owner_email
  before_doc   JSONB,                   -- null for insert
  after_doc    JSONB,                   -- null for delete
  changed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  claimed_at   TIMESTAMPTZ,             -- 30-second lease taken by the cdc pipeline's poll
  published_at TIMESTAMPTZ,             -- null until published to Redpanda
  CONSTRAINT pk_change_log PRIMARY KEY (change_id),
  CONSTRAINT chk_change_log_table CHECK (table_name IN ('memories','accounts')),
  CONSTRAINT chk_change_log_op CHECK (op IN ('insert','update','delete'))
);
ALTER TABLE change_log ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;  -- installs made before the lease existed
CREATE INDEX IF NOT EXISTS idx_change_log_pending ON change_log (change_id) WHERE published_at IS NULL;

-- memory_audit: append-only history materialised from the Redpanda topic by the
-- pg-agent-memory-audit pipeline. change_id is the idempotency key.
CREATE TABLE IF NOT EXISTS memory_audit (
  audit_id    BIGINT      GENERATED ALWAYS AS IDENTITY,
  change_id   BIGINT      NOT NULL,     -- change_log.change_id carried in the event
  memory_id   BIGINT      NOT NULL,
  subject     TEXT,                     -- after.subject, else before.subject
  op          TEXT        NOT NULL,     -- insert | update | delete
  before_doc  JSONB,
  after_doc   JSONB,
  changed_at  TIMESTAMPTZ NOT NULL,     -- when the trigger fired in PostgreSQL
  audited_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_memory_audit PRIMARY KEY (audit_id),
  CONSTRAINT uq_memory_audit_change UNIQUE (change_id),
  CONSTRAINT chk_audit_op CHECK (op IN ('insert','update','delete'))
);
CREATE INDEX IF NOT EXISTS idx_memory_audit_subject ON memory_audit (subject, audited_at DESC) INCLUDE (op, memory_id);
