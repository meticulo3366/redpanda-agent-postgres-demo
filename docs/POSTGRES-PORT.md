# Porting the code to PostgreSQL

What changes in this repository to run the Memory Concierge against PostgreSQL instead of CockroachDB Cloud. The documentation in this fork already tells the PostgreSQL story; the code does not. This is the plan for the code.

Every Redpanda Connect claim below was checked against the component documentation on 2026-10-19 (links in Sources). Nothing in this plan has been executed against a live PostgreSQL instance, so section 11 is the acceptance test, not a record.

Scope: `cockroach/` becomes `postgres/`, two scripts are replaced, one pipeline is added (two where the managed service has no `pg_cron`), the two existing pipelines change a handful of Bloblang lines, and `.env` renames `CRDB_*` to `PG_*`. The ADP side does not change.

## 1. Prerequisites on the PostgreSQL side

From the `postgres_cdc` documentation:

- PostgreSQL 14 or later.
- `SHOW wal_level;` must return `logical`. Self-hosted: set `wal_level = logical`, keep `max_wal_senders` greater than or equal to `max_replication_slots`, add a `replication` line to `pg_hba.conf` for the replication user and the connector's address, restart. Managed services expose the same switch as a parameter; the documentation page carries setup notes for Amazon RDS, Azure Database for PostgreSQL, Cloud SQL and Neon.
- A role for the input's DSN that may open a logical replication connection and create a slot: the `REPLICATION` attribute, or the service's equivalent (membership in `rds_replication` on RDS and Aurora, in `neon_superuser` on Neon). Section 3 has the per-service grant.
- The input creates the replication slot if it does not exist. It expects a publication named `pglog_stream_<slot_name>`; create it yourself so the replication role does not need CREATE on the database. With an empty `tables` list the input would create a publication `FOR ALL TABLES`; the demo lists its two tables.
- `ALTER TABLE ... REPLICA IDENTITY FULL` on every table whose before-image matters. The documentation: "with the default identity only key columns are present, with REPLICA IDENTITY FULL all columns are present."
- The component requires an enterprise license. Managed pipelines in Redpanda Cloud are covered; `rpk connect streams` on a laptop (`make pipelines-local`) needs a license key, and a 30 day trial key is available.

The demo needs before-images on both tables (the observer compares `before.balance` with `after.balance`; the audit table stores `before_doc`), so both `memories` and `accounts` get `REPLICA IDENTITY FULL`. `FULL` logs every non-generated column of the old row; section 2 covers the two generated columns that logical replication leaves out before PostgreSQL 18.

## 2. Schema: `cockroach/01-schema.sql` to `postgres/01-schema.sql`

| CockroachDB (today) | PostgreSQL | Why |
|---|---|---|
| `STRING` | `TEXT` | type name |
| `INT8` | `BIGINT` | type name |
| `INT2` | `SMALLINT` | type name |
| `DECIMAL(14,2)`, `UUID`, `TIMESTAMPTZ`, `JSONB`, `TSVECTOR` | unchanged | |
| `DEFAULT gen_random_uuid()` | unchanged | built in since PostgreSQL 13 (`pgcrypto` before that) |
| `content_hash STRING NOT NULL AS (md5(lower(subject) \|\| '\|' \|\| lower(content))) STORED` | `content_hash TEXT NOT NULL GENERATED ALWAYS AS (md5(lower(subject) \|\| '\|' \|\| lower(content))) STORED` | `md5`, `lower` and `\|\|` are immutable, so the expression is allowed in a generated column |
| `search TSVECTOR NOT NULL AS (to_tsvector('english', content)) STORED` | `search TSVECTOR NOT NULL GENERATED ALWAYS AS (to_tsvector('english', content)) STORED` | the two-argument form with an explicit configuration is immutable; the one-argument `to_tsvector(content)` is not and PostgreSQL rejects it in a generated column |
| `) WITH (ttl_expiration_expression = 'expires_at', ttl_job_cron = '@hourly')` | removed | no row-level TTL in PostgreSQL; section 8 |
| `CREATE INDEX ... STORING (...)` (`idx_accounts_owner`, `idx_txn_account_time`, `idx_memories_recall`) | `CREATE INDEX ... INCLUDE (...)` | covering index syntax, PostgreSQL 11 or later |
| `CREATE INVERTED INDEX IF NOT EXISTS idx_memories_search ON memories (search)` | `CREATE INDEX IF NOT EXISTS idx_memories_search ON memories USING GIN (search)` | GIN is the tsvector index |
| `memory_audit.updated_hlc STRING NOT NULL` | `updated_lsn TEXT NOT NULL` | the row-version id is the WAL position from the CDC envelope; switch to `pg_lsn` once the emitted format is confirmed |
| `CONSTRAINT uq_memory_audit_event UNIQUE (memory_id, updated_hlc)` | `UNIQUE (memory_id, updated_lsn)` | idempotency key for the audit pipeline |
| `ON CONFLICT (subject, content_hash) DO UPDATE ...` and `DO NOTHING` | unchanged | the UNIQUE constraint exists, so conflict inference works; a stored generated column may be a conflict target |
| `CHECK` constraints, `REFERENCES`, `CREATE TABLE IF NOT EXISTS`, `DEFAULT now()` | unchanged | |
| (none) | `ALTER TABLE memories REPLICA IDENTITY FULL;` `ALTER TABLE accounts REPLICA IDENTITY FULL;` | section 1 |
| (none) | `CREATE PUBLICATION pglog_stream_agent_memory_cdc FOR TABLE memories, accounts;` | name dictated by the slot name (section 5); `CREATE PUBLICATION` has no `IF NOT EXISTS`, so guard it: `SELECT 'CREATE PUBLICATION pglog_stream_agent_memory_cdc FOR TABLE memories, accounts' WHERE NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'pglog_stream_agent_memory_cdc') \gexec` |

Column comments in the file that say "CDC key/HLC" and "changefeed updated MVCC timestamp" become "CDC key/LSN" and "WAL position from the CDC envelope".

Generated columns and logical replication. `content_hash` and `search` are stored generated columns, and logical replication does not publish generated columns before PostgreSQL 18. On PostgreSQL 14 to 17 the `after` and `before` images on `agent.memory.cdc`, and therefore `memory_audit.before_doc` and `after_doc`, lack those two columns, unlike the CockroachDB changefeed. Nothing in the pipelines reads them. On PostgreSQL 18 or later they can be included: `CREATE PUBLICATION pglog_stream_agent_memory_cdc FOR TABLE memories, accounts WITH (publish_generated_columns = stored);`

## 3. Roles and grants: `cockroach/02-roles.sql` to `postgres/02-roles.sql`

| Today | PostgreSQL |
|---|---|
| `CREATE ROLE IF NOT EXISTS agent_rw WITH LOGIN PASSWORD :'agent_password';` | PostgreSQL has no `CREATE ROLE IF NOT EXISTS`: `SELECT format('CREATE ROLE agent_rw LOGIN PASSWORD %L', :'agent_password') WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agent_rw') \gexec`, then the existing `ALTER ROLE agent_rw WITH LOGIN PASSWORD :'agent_password';` keeps the re-run semantics |
| `GRANT CONNECT ON DATABASE agent_memory TO agent_rw;` | unchanged |
| `GRANT USAGE ON SCHEMA agent_memory.public TO agent_rw;` | `GRANT USAGE ON SCHEMA public TO agent_rw;` (PostgreSQL does not qualify a schema by database) |
| `REVOKE CREATE ON SCHEMA agent_memory.public FROM public;` | `REVOKE CREATE ON SCHEMA public FROM PUBLIC;` same idea; the default since PostgreSQL 15, harmless to repeat |
| `REVOKE CREATE ON SCHEMA defaultdb.public, postgres.public FROM public;` | dropped (there is no `defaultdb`); the equivalent fence on the maintenance database is `REVOKE CONNECT ON DATABASE postgres FROM PUBLIC;` where the service allows it |
| `GRANT SELECT ON TABLE customers, accounts, transactions TO agent_rw;` | unchanged |
| `GRANT SELECT, INSERT, UPDATE ON TABLE memories TO agent_rw;` | unchanged: forget is an UPDATE, never a DELETE |
| `GRANT SELECT, INSERT ON TABLE memory_audit TO agent_rw;` | unchanged, with the same caveat (the agent shares the pipeline's INSERT grant) |
| (none) | `cdc_repl`, the CDC pipeline's role: `CREATE ROLE cdc_repl LOGIN PASSWORD ...;` (same `\gexec` guard, password as `:'cdc_password'`). No `REPLICATION` in the `CREATE`: only a superuser or a role that already holds it may grant the attribute, and the admin of a managed service is usually neither. Grant replication the way the service allows, as a per-service line in the script: `ALTER ROLE cdc_repl REPLICATION;` (self-managed, Cloud SQL, Azure Database for PostgreSQL), `GRANT rds_replication TO cdc_repl;` (RDS and Aurora, where `pg_roles.rolreplication` stays false for every role and replication still works), or on Neon create the role in the console, CLI or API so it is a member of `neon_superuser`, which carries the privilege. Then `GRANT CONNECT ON DATABASE agent_memory TO cdc_repl; GRANT USAGE ON SCHEMA public TO cdc_repl; GRANT SELECT ON TABLE memories, accounts TO cdc_repl;` SELECT is what a snapshot and the input's table discovery need; no write grants |
| (none, pipeline purge only) | `memory_purge`, the purge pipeline's role: `CREATE ROLE memory_purge LOGIN PASSWORD ...;` `GRANT CONNECT ON DATABASE agent_memory TO memory_purge; GRANT USAGE ON SCHEMA public TO memory_purge; GRANT SELECT, DELETE ON TABLE memories TO memory_purge;` A `pg_cron` job runs as the role that scheduled it, in the database the job names (section 8); that role needs `USAGE ON SCHEMA cron` in the database that hosts the extension and DELETE on `memories` in `agent_memory`, so `memory_purge` fits there too |

The closing comment "Row-level TTL does the only deleting on memories" becomes "The purge job (section 8) does the only deleting on memories, as `memory_purge` or as the `pg_cron` scheduler's role, by policy."

## 4. Seed: `cockroach/03-seed.sql`

Standard SQL throughout: `INSERT ... ON CONFLICT DO NOTHING`, deterministic UUID literals, `now() - '29 days 9 hours'::INTERVAL`. Review the re-anchoring `UPDATE` at the end of the file for anything CockroachDB-specific; nothing else changes.

## 5. The CDC pipeline: `connect/postgres-cdc.yaml`, managed pipeline `agent-memory-cdc`

Replaces `scripts/30-changefeeds.sh` and its two `CREATE CHANGEFEED` statements. One pipeline, one slot, one publication, both tables, topic chosen by table.

Verified field names: `dsn`, `schema`, `tables`, `slot_name`, `stream_snapshot`, `include_transaction_markers`, `heartbeat_interval`, `checkpoint_limit`, `batching`. Verified metadata on every message: `operation` (`read`, `insert`, `update`, `delete`), `table`, `lsn`, `commit_ts_ms` (not set for snapshot reads), and `before` for updates and deletes. Metadata does not travel to a Kafka topic by itself, so the pipeline folds it into the message value.

```yaml
# agent-memory-cdc
# Reads memories and accounts through logical replication and writes one JSON
# envelope per row change to agent.memory.cdc or agent.accounts.cdc.
input:
  postgres_cdc:
    dsn: "${secrets.AGENT_MEMORY_PG_CDC_DSN}"   # role cdc_repl (replication privilege), sslmode=require
    # Optional, Connect 4.70.0 or later: pin the provider's CA (section 9, TLS).
    # tls:
    #   root_cas: "${secrets.AGENT_MEMORY_PG_CA}"
    schema: public
    tables: [ memories, accounts ]
    slot_name: agent_memory_cdc                 # publication must be pglog_stream_agent_memory_cdc
    stream_snapshot: false                      # topics start empty; seed rows are not history
    include_transaction_markers: false          # no begin/commit messages
    heartbeat_interval: 1h                      # default; keeps the slot advancing on an idle database
    checkpoint_limit: 1024                      # default
    batching:
      count: 100
      period: 1s

pipeline:
  processors:
    - label: cdc_envelope
      mapping: |
        # before arrives as metadata: absent on insert, the old row on update and
        # delete (every non-generated column with REPLICA IDENTITY FULL, key columns otherwise).
        let before = if @before.type() == "string" { @before.parse_json() } else { @before }
        root.op = @operation
        root.table = @table
        root.lsn = @lsn
        root.commit_ts_ms = @commit_ts_ms
        root.before = $before
        # after: the row for insert and update, null on delete.
        root.after = if @operation == "delete" { null } else { this }
        # Kafka key: the primary key as a JSON array, the shape the consumers already parse.
        let pk = if @table == "memories" {
          this.memory_id.or($before.memory_id)
        } else {
          this.account_id.or($before.account_id)
        }
        meta kafka_key = [ $pk ].string()
        meta topic = if @table == "memories" { "agent.memory.cdc" } else { "agent.accounts.cdc" }

output:
  redpanda:
    seed_brokers: [ "${REDPANDA_BROKERS}" ]
    tls:
      enabled: true
    sasl:
      - mechanism: SCRAM-SHA-256
        username: "${secrets.AGENT_MEMORY_KAFKA_USER}"
        password: "${secrets.AGENT_MEMORY_KAFKA_PASSWORD}"
    topic: '${! @topic }'
    key: '${! @kafka_key }'
    max_in_flight: 1

metrics:
  prometheus: {}

logger:
  level: INFO
```

What lands on the topics:

```json
{"op":"update","table":"memories","lsn":"0/1A2B3C4","commit_ts_ms":1760536541500,
 "before":{"memory_id":"8c4e...","subject":"zeke.dean@redpanda.com","content":"...","expires_at":"2027-01-13 14:05:40.000001+00",...},
 "after":{"memory_id":"8c4e...","subject":"zeke.dean@redpanda.com","content":"...","expires_at":"2026-10-15 14:09:01.5+00",...}}
```

with key `["8c4e..."]` on `agent.memory.cdc`, and `[1001]` (or `["1001"]`, see below) on `agent.accounts.cdc`.

Notes, each tied to what the documentation does and does not say:

- `max_in_flight: 1` on the output keeps row order as the slot delivers it. Both consumers are idempotent, so raise it if throughput ever matters.
- The documentation calls `lsn` "the log sequence number in postgres" without saying whether it is the change's own position or its transaction's commit position. If it is the commit position, two changes to one row inside one transaction collide on `(memory_id, updated_lsn)` and the second is dropped by `ON CONFLICT DO NOTHING`. The demo's agent runs one statement per transaction, so it does not hit this; section 11 has the probe, and the fix is a second key component (`commit_ts_ms`, or a per-transaction counter).
- The documentation does not spell out the body of a delete message. The sketch does not depend on it: `before` comes from metadata and `after` is set to null.
- The documented type mapping is TEXT, TIMESTAMP, UUID, VARCHAR to JSON strings, BOOL to booleans, INT4 to numbers, JSONB to objects, TSVECTOR to strings, and "All other data types are mapped to string values". So `balance` (NUMERIC) arrives as `"2450.00"`, which the observer's `.number()` already handles. BIGINT and SMALLINT are not listed, so `account_id` and `importance` may arrive as strings; the mappings use `account_id` only as text (`%v` and `source_ref`), so either works, but pin whichever you observe in the test fixtures. Timestamps arrive as strings whose exact format the page does not pin; the observer's `.slice(0, 10)` fallback exists for that.
- `stream_snapshot: false` because the topics start empty and seeded accounts are not history. If it is ever turned on, `read` operations appear and both consumers must drop them (sections 6 and 7 do).
- Slot management. A slot retains WAL until the consumer confirms it, so a stopped pipeline grows the server's disk; set `max_slot_wal_keep_size` as a ceiling and watch `SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) FROM pg_replication_slots;`. Teardown order: stop and delete the pipeline, then `SELECT pg_drop_replication_slot('agent_memory_cdc');` (refused while the slot is active), then `DROP PUBLICATION pglog_stream_agent_memory_cdc;`. A re-created slot starts at the current WAL position; changes made while no slot existed are not replayed unless `stream_snapshot: true` re-reads the tables as `read` operations.
- Two pipelines with two slots would also work (one per table) at the cost of two slots' WAL retention and two replication connections. One is enough here.

## 6. `connect/memory-audit.yaml`

The envelope changes from `{"after", "before", "updated"}` plus `{"resolved"}` markers to `{"op", "table", "lsn", "commit_ts_ms", "before", "after"}` with no markers. Line numbers refer to the file in this repository.

| Line | Today | Port |
|---|---|---|
| 3 to 5, 35 to 37, 40 to 41 | comments describing the changefeed envelope and resolved markers | describe the section 5 envelope |
| 42 | `root = if this.exists("resolved") \|\| !this.exists("updated") {` | `root = if !["insert","update","delete"].contains(this.op.or("")) \|\| !this.exists("lsn") {` drops snapshot `read` rows, transaction markers and anything that is not a row change |
| 49 to 51 | `metadata("kafka_key").parse_json().index(0).or(this.after.memory_id.or(this.before.memory_id))` | unchanged: the CDC pipeline keeps the key as a JSON array |
| 53 | `"subject": this.after.subject.or(this.before.subject)` | unchanged: deletes have `after: null`, and `before` is complete because of `REPLICA IDENTITY FULL` |
| 54 | `"op": if this.before == null { "insert" } else if this.after == null { "delete" } else { "update" }` | `"op": this.op` The operation is authoritative metadata; inferring it from the presence of `before` would misclassify nothing today but breaks the moment a table loses `REPLICA IDENTITY FULL`, where `before` is present and key-only |
| 56 to 57 | `before_doc`, `after_doc` | unchanged |
| 58 | `"updated_hlc": this.updated.string()` | `"updated_lsn": this.lsn.string()` |
| 65 | `dropping malformed changefeed event` | `dropping malformed CDC event` (cosmetic) |
| 71 | `dsn: "${secrets.AGENT_MEMORY_CRDB_DSN}"` | `dsn: "${secrets.AGENT_MEMORY_PG_DSN}"` |
| 73 to 75 | `INSERT INTO memory_audit (..., updated_hlc) ... ON CONFLICT (memory_id, updated_hlc) DO NOTHING` | `updated_lsn` in both places |
| 79 | `root = [ this.memory_id, this.subject, this.op, this.before_doc, this.after_doc, this.updated_hlc ]` | `this.updated_lsn` |

`connect/tests/memory-audit_test.yaml`: every `content` fixture becomes a section 5 envelope, every expected `updated_hlc` becomes `updated_lsn`, the case "resolved marker is dropped" becomes "snapshot read is dropped" with input `{"op": "read", "table": "memories", "lsn": "0/0", "before": null, "after": {...}}`, and "JSON without an updated timestamp is dropped" becomes "JSON without an op is dropped".

## 7. `connect/account-observer.yaml`

| Line | Today | Port |
|---|---|---|
| 3 to 8, 38 to 40 | comments | describe the section 5 envelope |
| 44 | `let is_update = !this.exists("resolved") && this.before.type() == "object" && this.after.type() == "object"` | `let is_update = this.op == "update" && this.before.type() == "object" && this.after.type() == "object"` |
| 46 to 49 | DECIMAL comment and `this.before.balance.number()` / `this.after.balance.number()` | unchanged: `postgres_cdc` maps NUMERIC to a string and `.number()` still normalises. This is the line that requires `REPLICA IDENTITY FULL` on `accounts`: with the default identity `this.before.balance` is null, `.number()` errors, the `catch` block at line 87 logs and drops the event, and no observation is ever written |
| 58 to 63 | `updated_at` date with the `.slice(0, 10)` fallback | unchanged; the fallback covers whichever timestamp format the input emits |
| 73 | `"source_ref": "accounts/%v@%s".format(this.after.account_id, this.updated.string())` | `"source_ref": "accounts/%v@%s".format(this.after.account_id, this.lsn.string())` so observations point at `accounts/<id>@<lsn>` |
| 81 | same expression in the large-deposit branch | same change |
| 90 | `dropping malformed changefeed event` | cosmetic |
| 100 | `dsn: "${secrets.AGENT_MEMORY_CRDB_DSN}"` | `dsn: "${secrets.AGENT_MEMORY_PG_DSN}"` |
| 102 to 104 | `INSERT INTO memories (...) VALUES (..., now() + INTERVAL '30 days') ON CONFLICT (subject, content_hash) DO NOTHING` | unchanged |

`connect/tests/account-observer_test.yaml`: fixtures become envelopes with `"op": "update"` and `"lsn"`, expected `source_ref` values end in `@<lsn>`, and one new case pins the failure mode: "update with a key-only before (default replica identity) is dropped", input `"before": {"account_id": 1001}`, expected `output_batches: []`.

## 8. Forgetting by policy: the purge

PostgreSQL has no row-level TTL. The agent's "forget" stays `UPDATE memories SET expires_at = now(), updated_at = now() WHERE ...`; something else must delete.

Option A, `pg_cron`, where the managed service offers it. pg_cron is installed in one database per cluster (`cron.database_name`, default `postgres`), and a job runs in the database in which `cron.schedule` was called. A `cron.schedule(...)` typed into `postgres` therefore runs against `postgres` and fails every hour with `relation "memories" does not exist`, and nothing purges. Three steps:

First, enable the extension on the service. RDS and Aurora: add `pg_cron` to `shared_preload_libraries` in the parameter group (and to `rds.allowed_extensions` if that list is in use), reboot, then `CREATE EXTENSION pg_cron;` in `postgres` as a member of `rds_superuser`. Azure Database for PostgreSQL: allowlist the extension and add it to `shared_preload_libraries`, restart. Neon: set `cron.database_name = agent_memory` through the compute endpoint API, restart the compute, then `CREATE EXTENSION IF NOT EXISTS pg_cron;` in `agent_memory`. `CREATE EXTENSION` fails until this service-side step is done.

Second, schedule the job so that it runs in `agent_memory`. Where pg_cron lives in `postgres` (RDS, Aurora and Azure by default), from `postgres`:

```sql
SELECT cron.schedule_in_database('memory-purge', '0 * * * *',
  $$DELETE FROM memories WHERE expires_at < now()$$, 'agent_memory');   -- pg_cron 1.4 or later
```

RDS also documents an alternative: `cron.schedule('memory-purge', '0 * * * *', $$DELETE FROM memories WHERE expires_at < now()$$);` followed by `UPDATE cron.job SET database = 'agent_memory' WHERE jobname = 'memory-purge';` as `rds_superuser`. Use plain `cron.schedule(...)` from inside the database only where `cron.database_name` is the database that holds `memories`: Neon (which does not support `cron.schedule_in_database()`), or a self-managed server with the setting changed.

Third, the job runs as the role that scheduled it, so schedule it as a role that holds DELETE on `memories` (`memory_purge` from section 3, or the admin) and that no conversation can reach; that role also needs `USAGE ON SCHEMA cron` in the database hosting the extension (on RDS `rds_superuser` has it and can grant it). Check the first run in `cron.job_run_details` (`status`, `return_message`) rather than trusting the schedule.

Option B, `connect/memory-purge.yaml`, managed pipeline `agent-memory-purge`. Verified fields: `generate.interval` ("expressed either as a duration string or as a cron expression"), `generate.mapping`, `sql_raw.exec_only` ("Set to `true` to leave the message contents unchanged, which is useful when you are executing inserts, updates, and so on."):

```yaml
input:
  generate:
    interval: 1h
    mapping: 'root = { "purge_started_at": now() }'

pipeline:
  processors:
    - sql_raw:
        driver: postgres
        dsn: "${secrets.AGENT_MEMORY_PG_PURGE_DSN}"   # role memory_purge: SELECT, DELETE on memories only
        query: DELETE FROM memories WHERE expires_at < now()
        exec_only: true
    - log:
        level: INFO
        message: 'agent-memory-purge: run started at ${! this.purge_started_at }'

output:
  drop: {}
```

Either way the DELETE is an ordinary row change: the CDC pipeline emits `op: delete` with the old row's non-generated columns in `before`, and the audit pipeline writes a `delete` row. Forget (the agent's UPDATE of `expires_at`) and purge (the DELETE) stay two separate audited facts, as they are today. With a large backlog, bound each run (`DELETE FROM memories WHERE memory_id IN (SELECT memory_id FROM memories WHERE expires_at < now() LIMIT 1000)`) and let the next hour catch up.

## 9. Scripts, `.env`, ADP manifests

| Today | Port |
|---|---|
| `cockroach/` | `postgres/` with the files from sections 2 to 4 |
| `scripts/10-cockroach-setup.sh` | `scripts/10-postgres-setup.sh`: the same three `psql -v ON_ERROR_STOP=1 -X -q -f` runs against `PG_ADMIN_URL`; passwords still travel as psql variables (`-v agent_password=...`, plus `-v cdc_password=...` and `-v purge_password=...`); the login check keeps `sslmode=require`; the row-count query is unchanged |
| `scripts/30-changefeeds.sh` | `scripts/30-cdc-pipeline.sh`: preflight `SHOW wal_level` = `logical`, `SELECT relname, relreplident FROM pg_class WHERE relname IN ('memories','accounts')` = `f`, the publication in `pg_publication_tables`; then create or update `agent-memory-cdc` through the Data Plane API exactly as `40-pipelines.sh` does for the other two (or add it to `PIPELINE_NAMES` there and make this script the preflight only); print `pg_replication_slots` for `agent_memory_cdc` instead of `SHOW CHANGEFEED JOBS` |
| `scripts/40-pipelines.sh` | `PIPELINE_NAMES` gains `agent-memory-cdc` (and `agent-memory-purge` without `pg_cron`) with `pipeline_file` and `pipeline_description` entries; `--local` exports `AGENT_MEMORY_PG_DSN`, `AGENT_MEMORY_PG_CDC_DSN`, `AGENT_MEMORY_PG_PURGE_DSN` and needs a Connect license key for `postgres_cdc` |
| `scripts/20-redpanda-setup.sh` | secrets: `AGENT_MEMORY_PG_DSN` with scopes `SCOPE_MCP_SERVER`, `SCOPE_REDPANDA_CONNECT`, `SCOPE_AI_GATEWAY` (unchanged set), `AGENT_MEMORY_PG_CDC_DSN` and `AGENT_MEMORY_PG_PURGE_DSN` with `SCOPE_REDPANDA_CONNECT` only; Kafka user, ACLs and topics unchanged |
| `scripts/lib.sh` | `crdb_admin_psql` and `crdb_admin_psql_file` become `pg_admin_psql*` over `PG_ADMIN_URL`; `crdb_agent_url` becomes `pg_agent_url` plus `pg_cdc_url` and `pg_purge_url`; the `load_env` required list follows the `.env` table below |
| `scripts/60-verify.sh` | lines 113 to 120: replace the `SHOW CHANGEFEED JOBS` loop with `SELECT active FROM pg_replication_slots WHERE slot_name = 'agent_memory_cdc'` expecting `t`; line 133: add `agent-memory-cdc` (and the purge pipeline) to the list; the DELETE-denied check at line 73 keeps working (PostgreSQL answers `permission denied for table memories`, which the grep matches); the 60 s audit wait at lines 302 to 321 is unchanged; the forget at line 324 is unchanged |
| `scripts/90-teardown.sh --all` | stop and delete `agent-memory-cdc` first, then `SELECT pg_drop_replication_slot('agent_memory_cdc')`, `DROP PUBLICATION pglog_stream_agent_memory_cdc`, then tables and the three roles |
| `scripts/00-preflight.sh` | no `.cockroachlabs.env` to read: take `PG_ADMIN_URL` from the operator, generate the three passwords, and check `SHOW wal_level` early so the failure is cheap. Once `10-postgres-setup.sh` has created `cdc_repl`, prove it can replicate with a functional test, not with `pg_roles.rolreplication` (false on RDS and Aurora even when `rds_replication` membership makes replication work): `psql "${PG_CDC_URL}&replication=database" -X -c 'IDENTIFY_SYSTEM'` must return one row |
| `scripts/observe.sh`, `demo-overdraft.sh`, `demo-reset.sh`, `chat.sh` | helper renames only; the SQL is standard |
| `adp/mcp-server.yaml` | `dsn: ${secrets.AGENT_MEMORY_PG_DSN}`; description text; the `(?i)\bcrdb_internal\b` blocked pattern is harmless and can go; keep the name `crdb-agent-memory` or rename it together with `adp/policies/*` (the Cedar `resource == McpServer::"..."` clause) and `adp/agent.template.yaml` |
| `adp/system-prompt.md` | wording only where it names CockroachDB; the schema it describes is the same |
| `Makefile` | target `changefeeds` becomes `cdc`; help text |
| `connect/tests/*.yaml` | sections 6 and 7 |

`.env` (`.env.example`):

| Today | Port |
|---|---|
| `CRDB_ADMIN_URL` | `PG_ADMIN_URL` (`postgresql://<admin>:<password>@<host>:5432/agent_memory?sslmode=require`) |
| `CRDB_HOST` | `PG_HOST` (`<host>:5432`) |
| `CRDB_DATABASE` | `PG_DATABASE` (`agent_memory`) |
| `CRDB_AGENT_USER`, `CRDB_AGENT_PASSWORD` | `PG_AGENT_USER` (`agent_rw`), `PG_AGENT_PASSWORD` |
| (new) | `PG_CDC_USER` (`cdc_repl`), `PG_CDC_PASSWORD`, `PG_CDC_SLOT` (`agent_memory_cdc`) |
| (new, pipeline purge only) | `PG_PURGE_USER` (`memory_purge`), `PG_PURGE_PASSWORD` |
| `MCP_SERVER_NAME` | unchanged unless the server is renamed |
| every Redpanda, ADP and runtime-toggle variable | unchanged |

TLS. The code's DSN uses `sslmode=verify-full`, which works against CockroachDB Cloud because its certificate chains to a public CA. A DSN can name a CA only through `sslrootcert=<path>`, a file neither the managed MCP server nor a managed pipeline has, so `verify-full` is possible only where the service's server certificate chains to a publicly trusted root (check per service). Most managed PostgreSQL services (RDS, Aurora and Cloud SQL among them) sign with a private CA, and there the three DSNs use `sslmode=require` (encrypted, server identity not checked). One exception: the `postgres_cdc` input has a `tls` block (`root_cas`, `root_cas_file`, `client_certs`, `skip_cert_verify`; Connect 4.70.0 or later) whose settings replace the TLS settings derived from the DSN, so the CDC pipeline can pin the provider's CA inline through `tls.root_cas` from a secret (`AGENT_MEMORY_PG_CA`, scope `SCOPE_REDPANDA_CONNECT`). The pipeline files say "Written for Redpanda Connect 4.69", so confirm the managed runtime's version before relying on it. `sql_raw` has no TLS fields beyond the DSN's `sslmode`, and the SQL MCP server's DSN is a secret string, so the audit, observer and purge pipelines and the MCP server stay on `require` against a private CA. `ARCHITECTURE.md` says so under Limitations.

## 10. ADP side: unchanged

- The managed SQL MCP server already runs with `driver: postgres`; its documentation says it connects to PostgreSQL databases with the `postgres` or `pgx` driver. Only the DSN secret's value changes.
- The DSN secret must carry `SCOPE_AI_GATEWAY` as well as `SCOPE_MCP_SERVER`; without it the server reports "discovery failed" and the agent fabricates tool calls (verified on the original, `DESIGN.md` section 6).
- `blocked_patterns`, `max_rows_default`, `max_rows_limit`, `query_timeout`, the data policy (`tax_id` redacted, `account_number` partial), the Cedar policies, the guardrail, both providers, the agent manifest and transcripts: no change.
- `allowed_schemas` is still not enforced by the managed SQL MCP server; schema access is still the `agent_rw` grants.

## 11. Verification checklist

Database

- [ ] `SHOW wal_level;` returns `logical`
- [ ] `SELECT relname, relreplident FROM pg_class WHERE relname IN ('memories','accounts');` returns `f` for both
- [ ] `SELECT * FROM pg_publication_tables WHERE pubname = 'pglog_stream_agent_memory_cdc';` lists both tables
- [ ] `\d memories` shows `content_hash` and `search` as generated stored columns; `\di` shows `idx_memories_search` as GIN and `idx_memories_recall` with `INCLUDE`
- [ ] as `agent_rw`: `DELETE FROM memories WHERE 1 = 0` is `permission denied`; `UPDATE accounts SET balance = balance WHERE 1 = 0` is denied; INSERT and UPDATE on `memories` work
- [ ] as `cdc_repl`: `SELECT 1 FROM memories LIMIT 1` works; INSERT is denied
- [ ] as `cdc_repl`, a replication connection opens: `psql "${PG_CDC_URL}&replication=database" -X -c 'IDENTIFY_SYSTEM'` returns one row (this is the test, not `pg_roles.rolreplication`, which stays false on RDS and Aurora)
- [ ] as `memory_purge`: `DELETE FROM memories WHERE 1 = 0` works; UPDATE and INSERT are denied

CDC

- [ ] `agent-memory-cdc` is `STATE_RUNNING`; `SELECT active, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'agent_memory_cdc';` shows `active = t`
- [ ] insert a memory as `agent_rw`; `rpk topic consume agent.memory.cdc -o end -f '%k %v\n'` shows `op: insert`, `before: null`, key `["<uuid>"]` within seconds
- [ ] update it; the envelope's `before` holds every non-generated column (no `content_hash` or `search` before PostgreSQL 18), not only `memory_id`
- [ ] two updates to one row in one transaction produce two audit rows (if not, `lsn` is the commit position; add `commit_ts_ms` or a per-transaction counter to the key)
- [ ] record whether `account_id` and `importance` arrive as numbers or strings and pin the fixtures accordingly
- [ ] slot lag stays near zero while the pipeline runs: `pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)`
- [ ] stop the pipeline for a few minutes, write rows, start it: every row arrives, no duplicates in `memory_audit`

Pipelines

- [ ] `rpk connect lint --skip-env-var-check connect/*.yaml` passes
- [ ] `rpk connect test connect/tests/*.yaml` passes with the rewritten fixtures
- [ ] `make overdraft` yields one `observation` memory within 60 s and one `insert` audit row for it
- [ ] forget (`UPDATE memories SET expires_at = now()`) yields an `update` audit row; the next purge run yields a `delete` audit row for the same `memory_id`. With `pg_cron`, first confirm `SELECT database, username FROM cron.job WHERE jobname = 'memory-purge';` says `agent_memory` and the purge role, and that the latest `cron.job_run_details` row for the job is `succeeded`, not `relation "memories" does not exist`

End to end

- [ ] `make verify` passes with the replaced checks (slot active instead of changefeed running; three or four pipelines)
- [ ] `make teardown-all` leaves `pg_replication_slots` and `pg_publication` empty and the WAL size stable afterwards

## Sources

- `postgres_cdc` input, Redpanda Connect documentation: https://docs.redpanda.com/redpanda-connect/components/inputs/postgres_cdc/ (version requirement, `wal_level`, replication connections, publication naming `pglog_stream_<slot_name>`, slot creation, `REPLICA IDENTITY FULL`, metadata fields, type mapping, enterprise license)
- `sql_raw` processor: https://docs.redpanda.com/redpanda-connect/components/processors/sql_raw/ (`exec_only`, driver list, `postgres://` DSN format)
- `generate` input: https://docs.redpanda.com/redpanda-connect/components/inputs/generate/ (`interval` as a duration or a cron expression)
- pg_cron: https://github.com/citusdata/pg_cron (jobs run in the database where `cron.schedule` was called, one database per cluster, `cron.schedule_in_database`); on RDS and Aurora: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL_pg_cron.html (`shared_preload_libraries`, the `postgres` database, `UPDATE cron.job SET database`, `USAGE ON SCHEMA cron`); on Neon: https://neon.com/docs/extensions/pg_cron (`cron.database_name` through the compute endpoint API, `cron.schedule_in_database()` unsupported)
- Replication privilege: https://www.postgresql.org/docs/current/sql-createrole.html (only superusers or roles with `REPLICATION` can specify `REPLICATION`); on RDS and Aurora: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication.html (`rds_replication`, `rds.logical_replication`); on Neon: https://neon.com/docs/guides/logical-replication-postgres (`neon_superuser` carries `REPLICATION`)
- Generated columns in logical replication: https://www.postgresql.org/docs/current/logical-replication-gencols.html (not published before PostgreSQL 18; `publish_generated_columns = stored`)
