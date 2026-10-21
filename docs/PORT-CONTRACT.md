# Port contract: the Memory Concierge on PostgreSQL (GantrySQL), 2026-10-21

Binding for every file in this repository. It replaced the pre-port plan (now `origin/POSTGRES-PORT-plan.md`), which
assumed logical replication and database roles, because this contract was written against the live target server and
that plan was not. Connection details of the target server live only in the gitignored `.postgres.env` and `.env`;
they never appear in a tracked file, this one included.

## 1. The target server, as probed live

- `PostgreSQL 18.4 (GantrySQL 0.9.1)`, TLS required (`sslmode=require`; TLSv1.3). One database, one user without
  `SUPERUSER`, `CREATEROLE`, `CREATEDB` or `REPLICATION`.
- `wal_level = replica`: no logical replication, so the Redpanda Connect `postgres_cdc` input cannot be used.
  Change capture is **triggers into a change-log table, polled by a Connect pipeline** (sections 4 and 5).
- The user may `CREATE SCHEMA`, tables, functions, triggers and indexes. Everything lives in schema **`agent_memory`**.
  Every statement the scripts, the pipelines and the agent's system prompt run is schema-qualified
  (`agent_memory.memories`), and the agent DSN carries no `search_path`. The three files in `postgres/` begin with
  `SET search_path TO agent_memory, public;`: `01-schema.sql` creates, alters and indexes its tables unqualified and
  `03-seed.sql` inserts and updates them unqualified, relying on that setting; `02-triggers.sql` qualifies every name.
  The admin psql URL carries the same `search_path` for convenience.
- The user may **not** create roles. There is no `agent_rw`: the MCP server and the pipelines connect as the one user.
  Database-level least privilege is therefore absent; the MCP server's blocked patterns (section 7), Cedar and the
  data policy carry the governance story, and the docs say so plainly.
- GantrySQL deviations from vanilla PostgreSQL, each verified:
  - Stored generated columns whose expression calls `md5(...)` or `to_tsvector(...)` make every INSERT fail
    (`ERR_HOST_FN_PLSQL`). `content_hash` is a plain column computed by the writer; there is no `search` column.
  - `ON CONFLICT` does not see values assigned by a BEFORE trigger. The writer supplies `content_hash` in the INSERT.
  - GIN indexes over `to_tsvector(...)` are refused. Full text uses the engine's index
    `CREATE INDEX ... ON agent_memory.memories USING fts (content) WITH (analyzer = 'english')` and the standard query
    `to_tsvector('english', content) @@ plainto_tsquery('english', $2)`.
  - Inside a trigger, `::uuid`/`::text` casts of NEW/OLD fields fail (`ERR_TRIGGER_EMBEDDED_SQL`), `to_jsonb(NEW)`
    loses column names (keys come back as f1, f2 ...) and `RETURN COALESCE(NEW, OLD)` is rejected ("trigger returned
    unsupported scalar value"). Triggers build documents with `jsonb_build_object('col', NEW.col, ...)`, never cast, and
    return `NEW` or `OLD` per branch. Keys are **bigint**: `memories.memory_id bigint GENERATED ALWAYS AS IDENTITY`,
    `change_log.row_key bigint`.
  - An UPDATE whose WHERE cast the uuid key to text (`txn_id::text LIKE ...`) failed once on freshly inserted rows with
    `covering scan failed to project ... CorruptedData` against the covering index on `transactions`; the seed compares
    uuids with `BETWEEN` instead.
  - Works as in PostgreSQL: identity columns, `ON CONFLICT (subject, content_hash) DO UPDATE ... RETURNING`,
    `UPDATE ... RETURNING` on a subquery (the claim pattern), `FOR UPDATE SKIP LOCKED`, `count(*) FILTER (WHERE ...)`,
    `jsonb_build_object` with numeric and timestamptz values, `uuid DEFAULT gen_random_uuid()` on tables without
    triggers (customers, transactions), `CREATE INDEX ... INCLUDE (...)`, partial indexes, `DO $$ ... EXCEPTION ... $$`.
- Extensions present: `vector` only (`pg_trgm` cannot be installed). No `pg_cron`: a Connect pipeline purges expired memories.

## 2. Fixed names

| Thing | Name |
|---|---|
| Schema | `agent_memory` in the target database |
| Tables | `customers`, `accounts`, `transactions`, `memories`, `memory_audit`, `change_log` |
| Triggers | `tr_memories_log` (AFTER INSERT OR UPDATE OR DELETE on memories), `tr_accounts_log` (AFTER UPDATE on accounts); functions `agent_memory.log_memory_change()`, `agent_memory.log_account_change()` |
| Topics | `agent.pg.memory.cdc`, `agent.pg.accounts.cdc` (3 partitions, replication 3, 7 day retention) |
| Consumer groups | `agent-memory-pg-audit`, `agent-memory-pg-observer` |
| SASL user | `agent-memory-pg-demo` (this demo's own; SCRAM-SHA-256) with ACLs: topic prefix `agent.pg.` ALL, group prefix `agent-memory-pg` ALL, cluster `kafka-cluster` DESCRIBE |
| Secrets (scopes SCOPE_MCP_SERVER, SCOPE_REDPANDA_CONNECT, SCOPE_AI_GATEWAY) | `AGENT_MEMORY_PG_DSN`, `AGENT_MEMORY_PG_KAFKA_USER`, `AGENT_MEMORY_PG_KAFKA_PASSWORD` |
| Managed pipelines (Redpanda Cloud) | `pg-agent-memory-cdc`, `pg-agent-memory-audit`, `pg-agent-memory-observer`, `pg-agent-memory-purge` (tags `demo: agent-memory-pg`, `owner: zeke.dean`, `stack: postgresql-redpanda`; `cpu_shares: 100m`, `memory_shares: 400M`) |
| ADP MCP server | `pg-agent-memory` (managed SQL, driver `postgres`, DSN `${secrets.AGENT_MEMORY_PG_DSN}`) |
| ADP agent | `memory-concierge-pg` (display `Memory Concierge (PostgreSQL)`, tags `demo: agent-memory-pg`, `owner: zeke.dean`, `stack: postgresql-redpanda`) |
| Cedar policies | `memory-concierge-pg-mcp` (`resource == McpServer::"pg-agent-memory"`), `memory-concierge-pg-llm` (`when { resource in [LLMProvider::"zeke-anthropic-agent-memory", LLMProvider::"zeke-bedrock-agent-memory"] }`) |
| Providers, guardrail | shared with the CockroachDB demo, unchanged: `zeke-anthropic-agent-memory` (agent), `zeke-bedrock-agent-memory` (guardrail backend, alternative), `agent-memory-guardrail` |
| Demo user | `zeke.dean@redpanda.com` |

The CockroachDB demo's resources (user `agent-memory-demo`, topics `agent.memory.cdc` and `agent.accounts.cdc`,
pipelines `agent-memory-*`, MCP server `crdb-agent-memory`, agent `memory-concierge`, secrets `AGENT_MEMORY_CRDB_DSN`,
`AGENT_MEMORY_KAFKA_USER`, `AGENT_MEMORY_KAFKA_PASSWORD`) stay untouched on the same cluster and environment. Only the
providers and the guardrail are shared, which is why `scripts/90-teardown.sh` keeps them unless `--providers`.

## 3. Environment (`.env`; `.env.example` documents it)

Written once by `scripts/00-preflight.sh` from `.postgres.env` (`PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`,
`PGPASSWORD`, optional `PG_AGENT_USER`, `PG_AGENT_PASSWORD`) plus a generated Kafka password and the fixed names:

`PG_ADMIN_URL` (`postgresql://<user>:<pw>@<host>:<port>/<db>?sslmode=require&options=-c%20search_path%3Dagent_memory,public`),
`PG_HOST` (host:port), `PG_DATABASE`, `PG_SCHEMA` (`agent_memory`), `PG_AGENT_USER`, `PG_AGENT_PASSWORD` (the admin
user and password on this server), `REDPANDA_BROKERS`, `REDPANDA_DATAPLANE_API`, `REDPANDA_SCHEMA_REGISTRY_URL`,
`KAFKA_USER` (`agent-memory-pg-demo`), `KAFKA_PASSWORD`, `TOPIC_MEMORY_CDC`, `TOPIC_ACCOUNTS_CDC`, `ADP_ENV`
(`adp-production`), `AIGW_URL`, `LLM_PROVIDER`, `LLM_MODEL`, `BEDROCK_PROVIDER`, `BEDROCK_MODEL`, `GUARDRAIL_NAME`,
`MCP_SERVER_NAME` (`pg-agent-memory`), `AGENT_NAME` (`memory-concierge-pg`), `DEMO_USER_EMAIL`. `scripts/lib.sh`
validates these names in `load_env`, except `BEDROCK_PROVIDER` and `BEDROCK_MODEL`, which are optional and default to
`zeke-bedrock-agent-memory` and `us.anthropic.claude-sonnet-4-6` when `.env` leaves them out. The agent DSN helper
`pg_agent_url` prints
`postgresql://<user>:<pw>@<host:port>/<db>?sslmode=require` (user and password percent-encoded, no search_path); it is
the value of the `AGENT_MEMORY_PG_DSN` secret and what the MCP server and the pipelines connect with.

## 4. Schema (`postgres/01-schema.sql`), triggers (`postgres/02-triggers.sql`), seed (`postgres/03-seed.sql`)

All idempotent. `01-schema.sql` starts with `CREATE SCHEMA IF NOT EXISTS agent_memory; SET search_path TO agent_memory, public;`.

- `customers`: `customer_id UUID DEFAULT gen_random_uuid()`, `email TEXT UNIQUE`, `full_name`, `segment`, `tax_id`
  (sensitive), `phone`, `created_at TIMESTAMPTZ`.
- `accounts`: `account_id BIGINT`, `customer_id UUID` (FK), `owner_email`, `account_number` (sensitive), `nickname`,
  `account_type` (CHECK checking | savings | credit), `balance NUMERIC(14,2)`, `currency`, `overdraft_limit
  NUMERIC(14,2)`, `status` (CHECK open | frozen | closed), `updated_at`; index `(owner_email) INCLUDE (nickname,
  account_type, balance, currency, status)`.
- `transactions`: `txn_id UUID DEFAULT gen_random_uuid()`, `account_id BIGINT` (FK), `amount NUMERIC(14,2)`,
  `merchant`, `category`, `occurred_at`; index `(account_id, occurred_at DESC) INCLUDE (amount, merchant, category)`.
- `memories`: `memory_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY`, `subject`, `kind` (CHECK preference | fact |
  goal | observation | episode), `content`, `importance SMALLINT` (CHECK 1..5, default 3), `source` (CHECK agent |
  event-stream | human, default agent), `source_ref`, `content_hash TEXT NOT NULL` (plain column:
  `md5(lower(subject) || '|' || lower(content))`, computed by the writer), `created_at`, `updated_at`, `expires_at`
  (default now() + 90 days); `UNIQUE (subject, content_hash)`; index `idx_memories_recall (subject, expires_at,
  importance DESC, created_at DESC) INCLUDE (kind, content, source)`; full-text index `idx_memories_search`, created
  in a `DO $$ ... $$` block that tries `USING fts (content) WITH (analyzer = 'english')` and falls back to
  `USING GIN (to_tsvector('english', content))` on vanilla PostgreSQL.
- `change_log`: `change_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY`, `table_name` (CHECK memories |
  accounts), `op` (CHECK insert | update | delete), `row_key BIGINT`, `subject`, `before_doc JSONB`, `after_doc
  JSONB`, `changed_at TIMESTAMPTZ DEFAULT now()`, `claimed_at TIMESTAMPTZ` (the cdc pipeline's 30-second lease),
  `published_at TIMESTAMPTZ` (null until published); `ALTER TABLE change_log ADD COLUMN IF NOT EXISTS claimed_at`
  for installs made before the lease existed; partial index `idx_change_log_pending (change_id) WHERE published_at
  IS NULL`.
- `memory_audit`: `audit_id BIGINT identity`, `change_id BIGINT NOT NULL UNIQUE` (idempotency key), `memory_id
  BIGINT`, `subject`, `op` (CHECK), `before_doc JSONB`, `after_doc JSONB`, `changed_at TIMESTAMPTZ NOT NULL`,
  `audited_at TIMESTAMPTZ DEFAULT now()`; index `(subject, audited_at DESC) INCLUDE (op, memory_id)`.
- Triggers (`02-triggers.sql`, `CREATE OR REPLACE FUNCTION` + `DROP TRIGGER IF EXISTS` then `CREATE TRIGGER`):
  `agent_memory.log_memory_change()` writes one `change_log` row per memory insert/update/delete with
  `jsonb_build_object('memory_id', X.memory_id, 'subject', X.subject, 'kind', X.kind, 'content', X.content,
  'importance', X.importance, 'source', X.source, 'source_ref', X.source_ref, 'created_at', X.created_at,
  'updated_at', X.updated_at, 'expires_at', X.expires_at)` for before (OLD) and after (NEW); `row_key = memory_id`,
  `subject = subject`; `RETURN NEW` on insert and update, `RETURN OLD` on delete. `agent_memory.log_account_change()`
  on UPDATE of accounts writes `table_name 'accounts'`, `row_key = account_id`, `subject = owner_email`, documents
  with keys `account_id, owner_email, nickname, account_type, balance, currency, overdraft_limit, status,
  updated_at` (never `account_number` or `customer_id`). No casts anywhere in trigger bodies.
- Seed (`03-seed.sql`): the CockroachDB seed, `SET search_path TO agent_memory, public;` first, identical data
  (customer `zeke.dean@redpanda.com`, accounts 1001 to 1003, 25 transactions on 1001 summing to 2450.00, alice and
  bob), every INSERT `ON CONFLICT DO NOTHING`, then the re-anchor UPDATE that shifts the seeded ledger so its newest
  row is about one day old; it selects the seeded rows with `txn_id BETWEEN 'c0ffee00-0000-4000-8000-000000000000'
  AND 'c0ffee00-0000-4000-8000-0000000000ff'` (a uuid range, not a text cast). No memories seeded.

## 5. The change stream: `connect/pg-cdc.yaml` (managed pipeline `pg-agent-memory-cdc`)

Polls `change_log` every second: claim, publish, mark. At-least-once; the consumers are idempotent, the audit table on
`change_id` and observations on `(subject, content_hash)`.

```yaml
input:
  generate: { interval: 1s, mapping: root = {} }
pipeline:
  processors:
    - sql_raw:            # claim with a 30-second lease; returns the claimed rows
        driver: postgres
        dsn: "${secrets.AGENT_MEMORY_PG_DSN}"
        query: >-
          UPDATE agent_memory.change_log SET claimed_at = now()
           WHERE change_id IN (SELECT change_id FROM agent_memory.change_log
                                WHERE published_at IS NULL
                                  AND (claimed_at IS NULL OR claimed_at < now() - INTERVAL '30 seconds')
                                ORDER BY change_id LIMIT 200 FOR UPDATE SKIP LOCKED)
          RETURNING change_id, table_name, op, row_key, subject, before_doc, after_doc, changed_at
    - catch: [ log ERROR 'poll failed: error()', mapping root = deleted() ]   # a failed poll is logged, never silent
    - mapping: root = this.filter(r -> r.type() == "object").sort_by(r -> r.change_id.or(0))   # keep object rows; RETURNING has no order
    - unarchive: { format: json_array }
    - mapping: |          # envelope; non-object rows and rows without the key columns are dropped
        root = if this.type() != "object" || !this.exists("change_id") || !this.exists("table_name") || !this.exists("row_key") { deleted() } else {
          { "change_id": this.change_id, "table": this.table_name, "op": this.op, "row_key": this.row_key, "subject": this.subject,
            "before": if this.before_doc.type() == "string" { this.before_doc.parse_json() } else { this.before_doc },
            "after":  if this.after_doc.type()  == "string" { this.after_doc.parse_json()  } else { this.after_doc },
            "changed_at": this.changed_at } }
        meta kafka_key = this.row_key.string()
    - catch: [ log ERROR, mapping root = deleted() ]
output:
  switch:
    retry_until_success: true
    cases:
      - check: this.table == "memories"
        output: { broker: { pattern: fan_out_sequential, outputs: [ redpanda -> agent.pg.memory.cdc keyed by kafka_key,
                  sql_raw "UPDATE agent_memory.change_log SET published_at = now() WHERE change_id = $1::bigint AND published_at IS NULL" ] } }
      - check: this.table == "accounts"
        output: (same with topic agent.pg.accounts.cdc)
      - output: (mark published without a topic)
```

Settled by the local run: the `jsonb` columns arrive as JSON text from the driver (the mapping also accepts parsed
objects and nulls); an empty claim yields an empty array and no messages; `UPDATE ... RETURNING` through the Go
driver returns the rows; the `redpanda` output takes the same TLS and SASL block as the consumers. Before the lease
existed, two overlapping polls (a second tick firing while the first batch was still publishing) published the same
row twice; the lease makes overlapping polls take disjoint rows.

Envelope on both topics: `{"change_id", "table", "op", "row_key", "subject", "before", "after", "changed_at"}`,
key = `row_key` as text.

## 6. Consumers

- `connect/memory-audit.yaml` (`pg-agent-memory-audit`): topic `agent.pg.memory.cdc`, group `agent-memory-pg-audit`;
  drops anything without `change_id`, `op` and `row_key`; builds `{change_id, memory_id: row_key, subject (envelope,
  else after.subject, else before.subject), op, before_doc (JSON text or null), after_doc (JSON text or null),
  changed_at}`; output `sql_raw` postgres: `INSERT INTO agent_memory.memory_audit (change_id, memory_id, subject, op,
  before_doc, after_doc, changed_at) VALUES ($1::bigint,$2::bigint,$3,$4,$5::jsonb,$6::jsonb,$7::timestamptz) ON
  CONFLICT (change_id) DO NOTHING`; batching `count: 100, period: 1s`, `max_in_flight: 2`.
- `connect/account-observer.yaml` (`pg-agent-memory-observer`): topic `agent.pg.accounts.cdc`, group
  `agent-memory-pg-observer`; `op == "update"` with both documents only; rules overdraft (`before.balance >= 0 &&
  after.balance < 0`) and large deposit (`after.balance - before.balance >= 5000`); content sentences as in the
  CockroachDB demo with the date from `after.updated_at`; `subject` from the envelope, else `after.owner_email`;
  `source_ref = "accounts/<account_id>@<change_id>"`; `content_hash = (subject.lowercase() + "|" +
  content.lowercase()).hash("md5").encode("hex")` (verified equal to the server's `md5()`); output `INSERT INTO
  agent_memory.memories (subject, kind, content, importance, source, source_ref, content_hash, expires_at) VALUES
  ($1,'observation',$2,4,'event-stream',$3,$4, now() + INTERVAL '30 days') ON CONFLICT (subject, content_hash) DO
  NOTHING`.
- `connect/memory-purge.yaml` (`pg-agent-memory-purge`): `generate` every `1h`, `sql_raw` processor with `exec_only`
  `DELETE FROM agent_memory.memories WHERE expires_at < now()`, a log line, output `drop`; the delete trigger makes
  each purge an audited event. `scripts/demo-purge.sh` (`make purge`) runs the same statement on demand.
- Unit tests in `connect/tests/`: `pg-cdc_test.yaml` (the claim statement mocked at `/pipeline/processors/0`;
  envelopes, kafka_key metadata, empty poll, ordering, malformed rows), `memory-audit_test.yaml` (insert, update,
  delete, subject fallback, observation, malformed and mixed batches), `account-observer_test.yaml` (every rule,
  boundaries, string numerics, date fallback, drops, content hashes pinned to independently computed md5 values).
  Run with `rpk connect test connect/tests/*.yaml` (`make test`); lint with `rpk connect lint --skip-env-var-check
  connect/*.yaml`. All three files pass.

## 7. The MCP server and the agent

`adp/mcp-server.yaml`: name `pg-agent-memory`, driver `postgres`, `dsn: ${secrets.AGENT_MEMORY_PG_DSN}`,
`connection_pool` 5/2, `readonly: false`, `allowed_schemas: [agent_memory]` (declared, not enforced), blocked patterns
(enforced on every `query` and `execute`):

```
(?i)\b(DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE|COPY|MERGE|VACUUM|ANALYZE|REINDEX|CLUSTER)\b
(?i)\bDELETE\b
(?i)\b(INSERT\s+INTO|UPDATE)\s+(agent_memory\.)?(memory_audit|accounts|customers|transactions)\b
(?i)\bchange_log\b
(?i)\bSET\s+(SESSION|ROLE|default_transaction|search_path)
(?i)\bset_config\b
(?i)\bpg_(read|ls|stat_file|sleep|terminate|cancel|reload)
(?i)\b(information_schema|pg_catalog)\b
```

`max_rows_default: 50`, `max_rows_limit: 200`, `query_timeout: 15s`, `row_format: ROW_FORMAT_OBJECT`; data policy on
`query`: `tax_id` redacted, `account_number` keep last 4, `max_rows` clamped to 100. On this server these patterns are
the database write boundary (section 1).

`adp/system-prompt.md`: schema-qualified names, bigint ids, and these shapes. Recall: `SELECT memory_id, kind,
content, importance, source, created_at FROM agent_memory.memories WHERE subject = $1 AND expires_at > now() ORDER BY
importance DESC, created_at DESC LIMIT 20`. Remember: `INSERT INTO agent_memory.memories (subject, kind, content,
importance, source, source_ref, content_hash) VALUES ($1, $2, $3, $4::SMALLINT, 'agent', $5, md5(lower($1) || '|' ||
lower($3))) ON CONFLICT (subject, content_hash) DO UPDATE SET importance = excluded.importance, updated_at = now(),
expires_at = now() + INTERVAL '90 days'`. Forget: `UPDATE agent_memory.memories SET expires_at = now(), updated_at =
now() WHERE subject = $1 AND memory_id = $2::BIGINT`. Search: `... AND to_tsvector('english', content) @@
plainto_tsquery('english', $2) LIMIT 10`. Audit: `SELECT op, after_doc->>'content' AS content, before_doc->>'content'
AS previous, changed_at FROM agent_memory.memory_audit WHERE subject = $1 ORDER BY changed_at DESC LIMIT 20`. Account
queries as before with `agent_memory.` prefixes and `$2::BIGINT`. The prose says PostgreSQL, a trigger, Redpanda and
an hourly purge job; it forbids `change_log`, catalogs and writes to the system of record and the audit table.

`adp/agent.template.yaml`: name `memory-concierge-pg`, display `Memory Concierge (PostgreSQL)`, `mcp_servers:
[pg-agent-memory]`, `max_iterations: 16`, placeholders `__LLM_PROVIDER__`, `__LLM_MODEL__`, `__SYSTEM_PROMPT__`
rendered by `scripts/50-deploy-agent.sh` into the gitignored `adp/agent.yaml`. Cedar files
`adp/policies/memory-concierge-pg-{mcp,llm}.cedar` (section 2). `adp/guardrail.json` unchanged (shared guardrail).

## 8. Scripts and Makefile

`scripts/lib.sh`: `pg_admin_psql "<sql>" [args]`, `pg_admin_psql_file <file> [args]` (psql with `PG_ADMIN_URL`,
`-v ON_ERROR_STOP=1 -X -q`), `pg_agent_url`; `load_env` checks the section 3 names; `rp_token`, `rp_api`, `aigw_rpc`,
`rpk_kafka` as in the CockroachDB demo.

`scripts/00-preflight.sh`: tools, ADP login and environment, `.env` from `.postgres.env` when missing (never
overwrites), `.state/`, PostgreSQL connectivity with the server version, `wal_level` and `rolcreaterole` of the
current user, Data Plane API, AI Gateway. `scripts/10-postgres-setup.sh` (`make setup-db`): applies 01, 02, 03,
prints row counts, checks the two triggers and the full-text index, proves `pg_agent_url` connects and says whether
the agent user is the admin user. `scripts/20-redpanda-setup.sh` (`make setup-redpanda`): SASL user, ACLs, topics and
secrets of section 2; `DRY_RUN=1`. `scripts/40-pipelines.sh` (`make pipelines`): the four pipelines, idempotent by
display name, waits for RUNNING, ids in `.state/pipelines.json`; `--local [names]` renders `${secrets.X}` to `${X}`,
exports `AGENT_MEMORY_PG_DSN`, `AGENT_MEMORY_PG_KAFKA_USER`, `AGENT_MEMORY_PG_KAFKA_PASSWORD` and runs `rpk connect
streams` (`make pipelines-local ONLY="..."`). `scripts/50-deploy-agent.sh` (`make deploy-agent`): providers (reused when
present), guardrail, attach, `AGENT_MEMORY_PG_DSN` present, MCP server create/update, Cedar policies create/update,
agent render and apply, wait RUNNING, print the A2A URL.

`scripts/60-verify.sh` (`make verify`): six tables; two triggers; the full-text index (reports fts or GIN); the agent
DSN connects; privilege checks (DELETE and UPDATE denied) only when the agent user differs from the admin user, else
`SKIP`; topics; three secrets; four pipelines RUNNING; no unpublished change older than 30s; change-stream probe
(unless `SKIP_STREAM=1`): insert a memory for `verify@agent-memory.invalid`, expect its `change_log` row, wait up to
30s for `published_at`, up to 30s for the `memory_audit` row, then delete the probe; MCP server enabled with `query`
and `execute`, blocked patterns containing `DELETE`, `change_log` and `memory_audit`; agent RUNNING; provider and
attached guardrail; guardrail READY; then (unless `SKIP_A2A=1`) A2A smoke test, memory round-trip with a pseudo-word
coffee shop name in a new conversation, audit row within 60s, expire the memory (`VERIFY_KEEP_MEMORY=1` keeps it),
optional `VERIFY_GUARDRAIL=1` denied-topic probe.

`scripts/chat.sh` (agent `memory-concierge-pg`), `scripts/observe.sh` (tails `agent.pg.memory.cdc`; polls
`memory_audit` with `memory_id` and `change_id`, and the live memories), `scripts/demo-overdraft.sh` (rent debit
-2,900.00 and balance -450.00 on account 1001 in one transaction), `scripts/demo-purge.sh` (`make purge`),
`scripts/demo-reset.sh [--all]` (balance 2,450.00, demo transaction removed, seed re-applied, observation and expired
memories deleted; `--all` deletes every memory of the demo user). `scripts/90-teardown.sh`: default removes the agent,
the two policies, the MCP server and the four pipelines; `--all` also the topics, ACLs, SASL user, the three secrets
and `DROP SCHEMA agent_memory CASCADE`; `--providers` also the shared providers and the guardrail; `FORCE=1`,
`DRY_RUN=1`. `scripts/30-changefeeds.sh` and `scripts/10-cockroach-setup.sh` are gone, as is `cockroach/`.

`Makefile` targets: `preflight`, `setup-db`, `setup-redpanda`, `pipelines`, `pipelines-local`, `deploy-agent`,
`verify`, `up` (preflight, setup-db, setup-redpanda, pipelines, deploy-agent, verify), `chat`, `observe`,
`overdraft`, `purge`, `reset` (`ALL=1`), `teardown`, `teardown-all` (`PROVIDERS=1`), `lint`, `test`.

## 9. Observed on 2026-10-21

### Locally first

Before the cloud deploy, with the cdc, audit and observer pipelines running on a laptop (`make pipelines-local`)
against the target server and the Redpanda Cloud cluster:

- A memory insert was claimed and `published_at` set within one to two seconds of the write; its `memory_audit` row
  followed within a few seconds. An account update (1830.55 to -10.00 on account 2001) produced an `observation`
  memory with `source_ref accounts/2001@<change_id>`, and that insert was itself published and audited.
- Deleting the probe rows afterwards produced `delete` events that were published and audited the same way; nothing
  was left unpublished.
- Before the lease existed, the observer logged the same overdraft event three times: the one-second `generate` tick
  fired again while the first poll's batch was still publishing, so both polls selected the same unpublished rows.
  The consumers absorbed the duplicates (`ON CONFLICT` on `change_id` and on `(subject, content_hash)`); the claim
  with `claimed_at` removed them.
- `rpk connect lint --skip-env-var-check connect/*.yaml` and the three test files pass; `bash -n scripts/*.sh` passes.
### Then on Redpanda Cloud (17:03 to 17:20 UTC)

- `make setup-redpanda` created the SASL user, the three ACLs and the three secrets; the two topics already existed
  (created through the Kafka API during the local run). `make pipelines` created the four managed pipelines; each
  reached `STATE_RUNNING` about 30 seconds after creation. `make deploy-agent` found the shared providers and the
  guardrail in place, created the MCP server (tools `execute`, `query` discovered on the first refresh), the two Cedar
  policies and the agent, which was `AGENT_STATE_RUNNING` after about 75 seconds.
- First `VERIFY_GUARDRAIL=1 make verify`: 31 of 32 checks passed. The one failure was the change-stream probe's publish
  window: three minutes after the pipelines started, a probe inserted at 17:06:04 was claimed at once, but its publish
  and mark stalled, the 30-second lease expired, the row was claimed again at 17:06:35, published (a second copy on the
  topic) and marked at 17:06:47; the audit row appeared at 17:06:49. Every later change, including one made after four
  and a half idle minutes, was claimed within a second and published and audited one second later, with one copy on the
  topic. The verify window for the publish is now 60 seconds (a lease retry fits inside) and every SQL component in
  the pipelines carries `conn_max_idle_time: 30s` and `conn_max_life_time: 10m` so an idle connection is not reused
  after a middlebox may have dropped it. Second run: 32 of 32 passed, publish and audit both 1 second after the insert.
- The six acts, from `make reset ALL=1`: Act 1 masked `tax_id` and the account numbers; Act 2 stored two memories whose
  audit rows landed 3 seconds after the write; Act 3 recalled both in a new conversation; Act 4's overdraft produced the
  observation memory and its audit row 3 seconds after the UPDATE, and the agent opened the next new conversation with
  it unprompted; Act 5's three prompts were blocked by the gateway-evaluated guardrail (`[blocked by guardrail]`, about
  one second each); Act 6's forget became an `update` audit row and `make purge` a `delete` audit row seconds later.
- A blocked prompt poisons the rest of its conversation: after any `[blocked by guardrail]` turn, every later turn in
  the same conversation, however harmless, is blocked within a second, because the blocked prompt stays in the history
  the guardrail evaluates. A new conversation (`make chat NEW=1 ...`) is clean, and the memory lives in the database, so
  the demo script starts Act 6 in a new conversation. Asked, in a clean conversation, to hard-delete every memory
  "with a single statement, no soft-expiry", the agent declined by instruction and offered to expire memories instead;
  the MCP blocklist stays the backstop behind that.
