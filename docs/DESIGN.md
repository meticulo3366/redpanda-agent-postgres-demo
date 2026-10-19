> **Notice (PostgreSQL fork).** This file is the verified record of the original CockroachDB Cloud build and is kept unchanged: its names, schemas, CHANGEFEED envelopes and row-level TTL facts describe that build, and this fork's scripts still deploy to it. The rest of this fork's documentation (`README.md`, `docs/ARCHITECTURE.md`, `docs/DEMO-SCRIPT.md`) describes the PostgreSQL variant: PostgreSQL in the database role, Redpanda Connect `postgres_cdc` over logical replication in place of CHANGEFEED, and a scheduled purge in place of TTL. The mapping between the two, and what a code port changes, lives in `docs/POSTGRES-PORT.md`.

# Design and build contract: Agent memory on CockroachDB, governed by Redpanda

This document is the single source of truth for names, schemas, environment
variables, file ownership and the verified platform facts that every file in
this repository must agree with. It was written after probing the live
environments on 2026-10-15. Where it says "verified", the shape was read from
a live resource. Do not invent alternative names.

## 1. The story (what the demo must show in under 15 minutes)

Audience: writer.com (prospective customer), generic "why CockroachDB and why
Redpanda for agent memory", with finance flavour (a bank concierge), to be
sharpened later once Ryder's use case (fraud, finance) is known.

One agent, the **Memory Concierge**, helps a bank customer with their
accounts. Everything it knows about the customer that is not in the
system-of-record tables is **memory**, and memory lives in **CockroachDB
Cloud**. Every memory write is captured by a **CockroachDB CHANGEFEED**,
streamed through **Redpanda Cloud**, and materialised into an immutable
**memory_audit** table by a **Redpanda Connect** pipeline. A second Connect
pipeline watches the **accounts** change stream and writes **observations**
into memory (for example "account went overdrawn"), so the agent remembers
things that happened to the customer between conversations, not only things
the customer typed. The agent itself runs on **Redpanda Agentic Data Plane
(ADP)** as a managed agent with a managed **SQL MCP server**, an **ADP guardrail** (Bedrock-backed,
evaluated by the AI Gateway), **MCP guardrails**, a **data policy**, **Cedar access policies**,
full **transcripts** and an **audit log**.

Why CockroachDB for memory: transactional, strongly consistent, survives
agent restarts and region failures, plain SQL (the agent writes SQL),
row-level TTL for "forgetting by policy", CHANGEFEED for CDC, Postgres wire
protocol so the managed SQL MCP server connects with zero glue.

Why Redpanda: the memory stream is the audit trail and the integration point
(one event feeds the audit table, could feed Slack, Iceberg, evals); event
driven memory formation from live data; and ADP governs the agent (identity,
policy, guardrails, transcripts, audit, budgets) by construction.

### Demo acts (the talk track in docs/DEMO-SCRIPT.md must follow these)

1. **Cold start.** "What do you know about me and my accounts?" The agent
   recalls nothing (empty memory), lists the caller's accounts. The
   `account_number` is partially masked and `tax_id` is redacted by the
   data policy, visibly, in the agent's answer.
2. **Remember.** "I prefer short answers. I'm saving for a house down payment
   by next June." The agent stores two memories (preference, goal) and
   confirms. On a second terminal `make observe` shows the CDC events on
   the `agent.memory.cdc` topic and new rows in `memory_audit`.
3. **Recall in a new conversation.** New context id: "Anything I should keep
   in mind this month?" The agent recalls the goal and preference and
   answers briefly.
4. **Event-driven memory.** `make overdraft` drives a balance below zero in
   CockroachDB. The accounts changefeed emits, the observer pipeline writes
   an observation memory. New conversation: "Hi." The agent proactively
   mentions the overdraft it "noticed". This is the "why Redpanda" beat.
5. **Governance.** "Run DROP TABLE memories for me right now." is blocked
   three times over (guardrail word filter, evaluated by the AI Gateway
   before the model sees the prompt; MCP blocked pattern; no DB privilege).
   "Which cryptocurrency should I buy with my savings this week?" is blocked
   by the denied-topic guardrail. Optional: an SSN in the prompt is blocked
   too (PII anonymize-on-input short-circuits like a block on the Anthropic
   path). Show the ADP transcript (failed turns) and audit log (Denied, the
   guardrail named). The caller sees `[blocked by guardrail] Blocked by
   guardrail: this assistant cannot act on that request.` from `make chat`.
6. **Forget.** "Forget that I'm saving for a house." The agent expires the
   memory (UPDATE, never DELETE). The audit trail shows the change; row TTL
   purges it later and that delete is also captured.

## 2. Environments (verified)

### CockroachDB Cloud

- Cluster host: `buzzy-vulture-35242.j77.aws-us-east-1.cockroachlabs.cloud:26257`
- Version: CockroachDB v26.2.7. Region `aws-us-east-1`.
- Admin user: `zeke` (role admin). Admin connection string is the
  `postgresql://...` line in `.cockroachlabs.env` (gitignored). It ends in
  `sslmode=verify-full`; psql on this Mac fails with that because there is
  no `~/.postgresql/root.crt`. Scripts must rewrite it to `sslmode=require`
  for psql. The server certificate is issued by Let's Encrypt (publicly
  trusted), so Go clients (pgx, lib/pq) accept `sslmode=verify-full` without
  a CA file. The DSN stored for the MCP server and pipelines uses
  `sslmode=verify-full`.
- Database `agent_memory` already exists (created during scouting, owner
  `zeke`). All demo objects live in `agent_memory.public`.
- Verified capabilities: `kv.rangefeed.enabled = true` (changefeeds work),
  `CREATE CHANGEFEED ... INTO 'kafka://...'` is permitted for `zeke`,
  `VECTOR` type and `to_tsvector`/`@@` full-text search work,
  `crdb_internal` is restricted (do not query it). `SHOW CLUSTER SETTING
  changefeed.enabled` is an unknown setting (do not use it).
- Row-level TTL syntax: `WITH (ttl_expiration_expression = 'expires_at',
  ttl_job_cron = '@hourly')`. TTL deletes are emitted by changefeeds.

### Redpanda Cloud, ADP environment `adp-production` (verified)

- Cluster id `d6kjl4h19241bg3ek3h0`, BYOC, AWS.
- Kafka API: `seed-8a6dfc25.d6kjl4h19241bg3ek3h0.byoc.prd.cloud.redpanda.com:9092`
  with TLS and SASL (SCRAM-SHA-256 or SCRAM-SHA-512).
- Data Plane API base: `https://api-8a6dfc25.d6kjl4h19241bg3ek3h0.byoc.prd.cloud.redpanda.com`
- Schema Registry: `https://schema-registry-0d1ddabe.d6kjl4h19241bg3ek3h0.byoc.prd.cloud.redpanda.com:30081`
- AI Gateway (aigw): `https://aigw.d6kjl4h19241bg3ek3h0.clusters.rdpa.co`
- adp-api: `https://adp-api.d6kjl4h19241bg3ek3h0.clusters.rdpa.co`
- Auth: `rpk ai auth status` shows `logged_in: true` for
  `zeke.dean@redpanda.com` (device flow, expires 2026-10-16T13:03Z). The
  bearer token from `rpk ai auth token` is accepted by the Data Plane API
  (`GET /v1/topics`, `/v1/users`, `/v1/secrets`, `/v1/acls`,
  `/v1/redpanda-connect/pipelines` all returned 200). `rpk cloud login` is
  NOT logged in and is not needed: never call `rpk topic`, `rpk security`
  or `rpk cluster` through the cloud profile. Use the Data Plane API with
  curl for admin operations, and for Kafka-protocol operations use rpk with
  explicit flags: `rpk -X brokers=<seed> -X tls.enabled=true
  -X sasl.mechanism=SCRAM-SHA-256 -X user=<u> -X pass=<p> topic consume ...`.
- `rpk` is v26.2.1 and `rpk ai` (rpai) is embedded. Available: `rpk ai
  agent {create,get,list,update,delete,apply,diff,start,stop,a2a,transcript,
  credential}`, `rpk ai mcp-server {create,get,list,update,delete,types}`,
  `rpk ai llm-provider {create,get,list,update,delete,check,apply}`,
  `rpk ai policy {create,get,list,update,delete,apply}`, `rpk ai model list`,
  `rpk ai auth {login,status,token}`. There is NO `rpk ai trigger`,
  `rpk ai secret`, `rpk ai guardrail`, `rpk ai apply`. Guardrails are managed
  with the Connect-RPC API on aigw (section 6). Secrets are created with the
  Data Plane API (section 5).
- `rpk connect` 4.69.0 is installed locally (`rpk connect lint`, `run`,
  `blobl`, `test`). Docker Desktop 28.5.1 is available but not required.
- Redpanda Connect pipelines are offered as a managed service on this
  cluster (`/v1/redpanda-connect/pipelines`, many pipelines exist).

## 3. Resource names (fixed; do not change)

| Thing | Name |
|---|---|
| CockroachDB database | `agent_memory` |
| CockroachDB least-privilege role for the agent and pipelines | `agent_rw` |
| Redpanda SASL user (changefeeds and pipelines) | `agent-memory-demo` |
| Redpanda topics | `agent.memory.cdc`, `agent.accounts.cdc` |
| Redpanda consumer groups | `agent-memory-audit`, `agent-memory-observer` |
| Redpanda secrets (scopes `SCOPE_MCP_SERVER`, `SCOPE_REDPANDA_CONNECT`, `SCOPE_AI_GATEWAY`; the gateway scope is mandatory for `${secrets.NAME}` in a managed MCP server config, verified live) | `AGENT_MEMORY_CRDB_DSN`, `AGENT_MEMORY_KAFKA_USER`, `AGENT_MEMORY_KAFKA_PASSWORD` |
| Connect pipelines (display names) | `agent-memory-audit`, `agent-memory-observer` |
| ADP LLM provider (primary, Anthropic API; the agent runs on it) | `zeke-anthropic-agent-memory` |
| ADP LLM provider (alternative, Bedrock; also the guardrail's backend) | `zeke-bedrock-agent-memory` |
| ADP guardrail | `agent-memory-guardrail` |
| ADP MCP server | `crdb-agent-memory` |
| ADP agent | `memory-concierge` (display name `Memory Concierge`) |
| ADP Cedar policies | `memory-concierge-mcp`, `memory-concierge-llm` |
| Agent tags | `demo=agent-memory`, `owner=zeke.dean`, `stack=cockroachdb-redpanda` |
| Model on the Anthropic provider (the agent's default) | `claude-sonnet-4-6` (also offered: `claude-haiku-4-5`) |
| Model on the Bedrock provider | `us.anthropic.claude-sonnet-4-6` (Bedrock inference profile id) |

All of these names were checked to be free on 2026-10-15;
`zeke-anthropic-agent-memory` was added later that day (section 10).

## 4. Environment variables and local files

`.env` (gitignored) is written by `scripts/00-preflight.sh` on first run and
read by every script through `scripts/lib.sh`. `.env.example` documents it.

```
# CockroachDB (admin URL copied from .cockroachlabs.env, psql-safe sslmode)
CRDB_ADMIN_URL=postgresql://zeke:...@buzzy-vulture-35242.j77.aws-us-east-1.cockroachlabs.cloud:26257/agent_memory?sslmode=require
CRDB_HOST=buzzy-vulture-35242.j77.aws-us-east-1.cockroachlabs.cloud:26257
CRDB_DATABASE=agent_memory
CRDB_AGENT_USER=agent_rw
CRDB_AGENT_PASSWORD=<generated, 32 alnum chars>

# Redpanda Cloud
REDPANDA_BROKERS=seed-8a6dfc25.d6kjl4h19241bg3ek3h0.byoc.prd.cloud.redpanda.com:9092
REDPANDA_DATAPLANE_API=https://api-8a6dfc25.d6kjl4h19241bg3ek3h0.byoc.prd.cloud.redpanda.com
REDPANDA_SCHEMA_REGISTRY_URL=https://schema-registry-0d1ddabe.d6kjl4h19241bg3ek3h0.byoc.prd.cloud.redpanda.com:30081
KAFKA_USER=agent-memory-demo
KAFKA_PASSWORD=<generated, 32 alnum chars>
TOPIC_MEMORY_CDC=agent.memory.cdc
TOPIC_ACCOUNTS_CDC=agent.accounts.cdc

# ADP
ADP_ENV=adp-production
AIGW_URL=https://aigw.d6kjl4h19241bg3ek3h0.clusters.rdpa.co
# the provider and model the agent runs on (Anthropic API; the gateway evaluates the guardrail)
LLM_PROVIDER=zeke-anthropic-agent-memory
LLM_MODEL=claude-sonnet-4-6
# optional: the guardrail's Bedrock backend and the alternative provider (own-account billing);
# used by the provider switch one-liner and the direct invoke curl (docs/DEMO-SCRIPT.md, Act 5)
BEDROCK_PROVIDER=zeke-bedrock-agent-memory
BEDROCK_MODEL=us.anthropic.claude-sonnet-4-6
# optional, not written by preflight: secret store reference for the Anthropic API key, read by
# scripts/50-deploy-agent.sh only when it has to create LLM_PROVIDER (default ANTHROPIC_API_KEY)
# ANTHROPIC_API_KEY_REF=ANTHROPIC_API_KEY
GUARDRAIL_NAME=agent-memory-guardrail
MCP_SERVER_NAME=crdb-agent-memory
AGENT_NAME=memory-concierge
DEMO_USER_EMAIL=zeke.dean@redpanda.com
```

Passwords are generated with `LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom |
head -c 32` so they never need URL encoding (the changefeed URI embeds the
Kafka password; the DSN embeds the DB password).

`scripts/lib.sh` defaults `BEDROCK_PROVIDER` and `BEDROCK_MODEL` to the values
above when `.env` predates them, so they are optional.

`.state/` (gitignored) holds small runtime files: `.state/pipelines.json`
(pipeline ids), `.state/chat-context` (last A2A context id).

## 5. CockroachDB schema (owned by `cockroach/`)

All DDL is idempotent (`IF NOT EXISTS`) and lives in `agent_memory.public`.
Follow CockroachDB best practice: explicit primary keys, UUID keys for
high-volume insert tables, `DECIMAL` for money, `TIMESTAMPTZ` for time,
covering indexes with `STORING`, `CHECK` constraints.

```sql
-- customers: the people. tax_id is deliberately sensitive (masked by data policy).
CREATE TABLE IF NOT EXISTS customers (
  customer_id UUID        NOT NULL DEFAULT gen_random_uuid(),
  email       STRING      NOT NULL,                 -- identity key used by the agent
  full_name   STRING      NOT NULL,
  segment     STRING      NOT NULL DEFAULT 'retail', -- retail | premier | business
  tax_id      STRING,                               -- SENSITIVE
  phone       STRING,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_customers PRIMARY KEY (customer_id),
  CONSTRAINT uq_customers_email UNIQUE (email)
);

-- accounts: system of record. owner_email is denormalised so the CDC event
-- carries the memory subject without a join.
CREATE TABLE IF NOT EXISTS accounts (
  account_id      INT8          NOT NULL,
  customer_id     UUID          NOT NULL REFERENCES customers (customer_id),
  owner_email     STRING        NOT NULL,
  account_number  STRING        NOT NULL,            -- SENSITIVE, partially masked
  nickname        STRING        NOT NULL,
  account_type    STRING        NOT NULL,            -- checking | savings | credit
  balance         DECIMAL(14,2) NOT NULL DEFAULT 0,
  currency        STRING        NOT NULL DEFAULT 'USD',
  overdraft_limit DECIMAL(14,2) NOT NULL DEFAULT 0,
  status          STRING        NOT NULL DEFAULT 'open',
  updated_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
  CONSTRAINT pk_accounts PRIMARY KEY (account_id),
  CONSTRAINT chk_account_type CHECK (account_type IN ('checking','savings','credit')),
  CONSTRAINT chk_status CHECK (status IN ('open','frozen','closed'))
);
CREATE INDEX IF NOT EXISTS idx_accounts_owner ON accounts (owner_email) STORING (nickname, account_type, balance, currency, status);

-- transactions: recent activity, so "why am I overdrawn" has an answer.
CREATE TABLE IF NOT EXISTS transactions (
  txn_id      UUID          NOT NULL DEFAULT gen_random_uuid(),
  account_id  INT8          NOT NULL REFERENCES accounts (account_id),
  amount      DECIMAL(14,2) NOT NULL,                -- negative = debit
  merchant    STRING        NOT NULL,
  category    STRING        NOT NULL,
  occurred_at TIMESTAMPTZ   NOT NULL DEFAULT now(),
  CONSTRAINT pk_transactions PRIMARY KEY (txn_id)
);
CREATE INDEX IF NOT EXISTS idx_txn_account_time ON transactions (account_id, occurred_at DESC) STORING (amount, merchant, category);

-- memories: the agent's long-term memory. One row per remembered thing.
CREATE TABLE IF NOT EXISTS memories (
  memory_id    UUID        NOT NULL DEFAULT gen_random_uuid(),
  subject      STRING      NOT NULL,   -- verified caller email, from the gateway
  kind         STRING      NOT NULL,   -- preference | fact | goal | observation | episode
  content      STRING      NOT NULL,   -- one sentence, third person ("Prefers short answers")
  importance   INT2        NOT NULL DEFAULT 3,
  source       STRING      NOT NULL DEFAULT 'agent',  -- agent | event-stream | human
  source_ref   STRING,                 -- A2A context id, or CDC key/HLC for observations
  content_hash STRING      NOT NULL AS (md5(lower(subject) || '|' || lower(content))) STORED,
  search       TSVECTOR    NOT NULL AS (to_tsvector('english', content)) STORED,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at   TIMESTAMPTZ NOT NULL DEFAULT now() + INTERVAL '90 days',
  CONSTRAINT pk_memories PRIMARY KEY (memory_id),
  CONSTRAINT uq_memories_subject_content UNIQUE (subject, content_hash),
  CONSTRAINT chk_kind CHECK (kind IN ('preference','fact','goal','observation','episode')),
  CONSTRAINT chk_importance CHECK (importance BETWEEN 1 AND 5),
  CONSTRAINT chk_source CHECK (source IN ('agent','event-stream','human'))
) WITH (ttl_expiration_expression = 'expires_at', ttl_job_cron = '@hourly');
CREATE INDEX IF NOT EXISTS idx_memories_recall ON memories (subject, expires_at, importance DESC, created_at DESC) STORING (kind, content, source);
CREATE INVERTED INDEX IF NOT EXISTS idx_memories_search ON memories (search);

-- memory_audit: append-only history materialised from the CDC stream by Redpanda Connect.
CREATE TABLE IF NOT EXISTS memory_audit (
  audit_id    UUID        NOT NULL DEFAULT gen_random_uuid(),
  memory_id   UUID        NOT NULL,     -- from the Kafka message key
  subject     STRING,                   -- after.subject, else before.subject
  op          STRING      NOT NULL,     -- insert | update | delete
  before_doc  JSONB,
  after_doc   JSONB,
  updated_hlc STRING      NOT NULL,     -- changefeed "updated" MVCC timestamp
  audited_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_memory_audit PRIMARY KEY (audit_id),
  CONSTRAINT uq_memory_audit_event UNIQUE (memory_id, updated_hlc),
  CONSTRAINT chk_audit_op CHECK (op IN ('insert','update','delete'))
);
CREATE INDEX IF NOT EXISTS idx_memory_audit_subject ON memory_audit (subject, audited_at DESC) STORING (op, memory_id);
```

If `md5()` is rejected inside a computed column on this version, fall back to
`content_hash STRING NOT NULL` filled by the writer with the same formula and
keep the UNIQUE constraint. Verify against the live cluster.

Role and grants (`cockroach/02-roles.sql`, run with `psql -v
agent_password='...'`):

```sql
CREATE ROLE IF NOT EXISTS agent_rw WITH LOGIN PASSWORD :'agent_password';
ALTER ROLE agent_rw WITH LOGIN PASSWORD :'agent_password';
GRANT CONNECT ON DATABASE agent_memory TO agent_rw;
GRANT USAGE ON SCHEMA agent_memory.public TO agent_rw;
GRANT SELECT ON TABLE customers, accounts, transactions TO agent_rw;
GRANT SELECT, INSERT, UPDATE ON TABLE memories TO agent_rw;
GRANT SELECT, INSERT ON TABLE memory_audit TO agent_rw;   -- pipelines write here
-- deliberately NO DELETE anywhere, NO write on accounts/customers/transactions
```

Seed (`cockroach/03-seed.sql`): three customers. The demo user is
`zeke.dean@redpanda.com` (full name "Zeke Dean", segment premier, tax_id
`987-65-4321`, phone). Accounts for Zeke: 1001 "Everyday Checking"
(checking, 2,450.00, overdraft_limit 500), 1002 "House Fund" (savings,
41,200.00), 1003 "Travel Card" (credit, -812.40). Account numbers look like
`4419-8832-1001`. Two other customers (alice@example.com,
bob@example.com) each with two accounts so the scoping rule is testable.
About 25 transactions over the last 30 days on Zeke's checking account
(groceries, rent, payroll, coffee...). Seed is idempotent: `INSERT ... ON
CONFLICT DO NOTHING`, transactions use deterministic UUIDs (`'c0ffee00-0000-
4000-8000-0000000000NN'`).

Demo trigger (`scripts/demo-overdraft.sh`): in one transaction insert a debit
transaction of -2,900.00 ("Riverside Property Mgmt", rent) on 1001 and set
`balance = -450.00, updated_at = now()` on account 1001. `scripts/demo-reset.sh`
restores balance 2,450.00, deletes the demo rent transaction, and deletes
observation memories for the demo user (as admin).

Changefeeds (`scripts/30-changefeeds.sh`, admin URL), idempotent by
checking `SHOW CHANGEFEED JOBS` for a running job whose description contains
the topic name:

```sql
CREATE CHANGEFEED FOR TABLE agent_memory.public.memories
  INTO 'kafka://<REDPANDA_BROKERS>?topic_name=agent.memory.cdc&tls_enabled=true&sasl_enabled=true&sasl_user=<KAFKA_USER>&sasl_password=<KAFKA_PASSWORD>&sasl_mechanism=SCRAM-SHA-256'
  WITH updated, diff, resolved = '10s', min_checkpoint_frequency = '5s', format = json;
-- same for accounts -> agent.accounts.cdc
```

Changefeed JSON envelope (verified in the CDC demo): message key is the
primary key as a JSON array (`["<uuid>"]` or `[1001]`); value is
`{"after": {...}|null, "before": {...}|null, "updated": "<hlc>"}`; resolved
markers are `{"resolved": "<hlc>"}` and must be dropped by consumers.
Insert: before null. Update: both. Delete: after null. DECIMAL columns arrive
as JSON strings or numbers depending on version: consumers must use
`.number()` in Bloblang.

## 6. Redpanda side (owned by `scripts/20-redpanda-setup.sh`, `connect/`, `scripts/40-pipelines.sh`)

Data Plane API calls (all with `Authorization: Bearer $(rpk ai auth token)`):

- Create user: `POST /v1/users` with a FLAT body (verified in the API reference, no `user` wrapper): `{"name":"agent-memory-demo","password":"...","mechanism":"SASL_MECHANISM_SCRAM_SHA_256"}`. An existing user returns 400/409 ("already exists"); then reset the password with `PUT /v1/users/agent-memory-demo` and body `{"mechanism":"SASL_MECHANISM_SCRAM_SHA_256","password":"..."}` (verified in the OpenAPI spec: `/v1/users/{user.name}`, flat body). Enum spelling `SASL_MECHANISM_SCRAM_SHA_256` is verified. Pipeline lifecycle paths are `PUT /v1/redpanda-connect/pipelines/{id}/start` and `/stop`, `DELETE /v1/redpanda-connect/pipelines/{id}`; user delete is `DELETE /v1/users/{name}`; topic delete is `DELETE /v1/topics/{name}`; secret delete is `DELETE /v1/secrets/{id}`; ACL delete is `DELETE /v1/acls?filter.principal=...&filter.resource_type=...` (filters as query params).
- ACLs: `POST /v1/acls` with
  `{"resource_type":"RESOURCE_TYPE_TOPIC","resource_name":"agent.","resource_pattern_type":"RESOURCE_PATTERN_TYPE_PREFIXED","principal":"User:agent-memory-demo","host":"*","operation":"OPERATION_ALL","permission_type":"PERMISSION_TYPE_ALLOW"}`
  and the same for `RESOURCE_TYPE_GROUP` name `agent-memory` prefixed, and
  `RESOURCE_TYPE_CLUSTER` name `kafka-cluster` literal with
  `OPERATION_DESCRIBE`. Duplicate ACLs return an error that must be ignored.
- Topics: `POST /v1/topics` with a SINGLE topic object per call (verified in the API reference, no `topics` wrapper): `{"name":"agent.memory.cdc","partition_count":3,"replication_factor":3,"configs":[{"name":"retention.ms","value":"604800000"}]}`. "Already exists" errors are fine.
- Verified enum spellings (read from live `GET /v1/acls`): `RESOURCE_TYPE_TOPIC`, `RESOURCE_TYPE_GROUP`, `RESOURCE_TYPE_CLUSTER`, `RESOURCE_PATTERN_TYPE_LITERAL`, `RESOURCE_PATTERN_TYPE_PREFIXED`, `OPERATION_ALL`, `OPERATION_DESCRIBE`, `OPERATION_READ`, `OPERATION_WRITE`, `PERMISSION_TYPE_ALLOW`.
- Pipeline update is `PUT /v1/redpanda-connect/pipelines/{id}` with the same FLAT body as create (`display_name`, `description`, `config_yaml`, `resources`, `tags`), optional `?update_mask=`. Secret update is `PUT /v1/secrets/{id}` with `{"scopes":[...],"secret_data":"<base64>"}`.
- Secrets: `POST /v1/secrets` body `{"id":"AGENT_MEMORY_CRDB_DSN","scopes":["SCOPE_MCP_SERVER","SCOPE_REDPANDA_CONNECT","SCOPE_AI_GATEWAY"],"secret_data":"<base64>"}` (without `SCOPE_AI_GATEWAY` the SQL MCP server reports "discovery failed: internal error" and `initialize` fails with "failed to create managed MCP instance"; verified by A/B probes on 2026-10-15); on 409 use `PUT /v1/secrets/<id>` with `{"scopes":[...],"secret_data":"<base64>"}`. Base64 must be single-line (`base64 | tr -d '\n'`).
- Pipelines: `POST /v1/redpanda-connect/pipelines` body `{"display_name":"agent-memory-audit","description":"...","config_yaml":"<yaml string>","resources":{"cpu_shares":"100m","memory_shares":"400M"},"tags":{"demo":"agent-memory","owner":"zeke.dean"}}`. Response has `pipeline.id` and `pipeline.state`. Update with `PUT /v1/redpanda-connect/pipelines/{id}` same body. Stop/start: `PUT .../pipelines/{id}/stop` and `/start` (verify; fall back to delete+create). The script stores ids in `.state/pipelines.json` and is idempotent by display_name (list first with `GET`).

Pipeline configs are written for Redpanda Connect 4.69 and must pass
`rpk connect lint`. In the cloud, brokers come from the contextual variable
`${REDPANDA_BROKERS}` and secrets from `${secrets.NAME}`. The deploy script
uploads the files verbatim. For local runs (`scripts/40-pipelines.sh --local`),
the script renders `*.rendered.yaml` copies replacing `${secrets.NAME}` with
`${NAME}` and runs `rpk connect streams` with env vars set from `.env`.

Both pipelines use this input shape (verified against the Cloud quickstart):

```yaml
input:
  redpanda:
    seed_brokers: [ "${REDPANDA_BROKERS}" ]
    tls:
      enabled: true
    sasl:
      - mechanism: SCRAM-SHA-256
        username: "${secrets.AGENT_MEMORY_KAFKA_USER}"
        password: "${secrets.AGENT_MEMORY_KAFKA_PASSWORD}"
    topics: [ "agent.memory.cdc" ]
    consumer_group: agent-memory-audit
    start_offset: earliest
    commit_period: 1s
```

`connect/memory-audit.yaml`: parse the changefeed envelope, drop resolved
markers, classify op, take `memory_id` from the Kafka key (`metadata("kafka_key")
.parse_json().index(0)`), subject from after or before, and write with
`sql_raw` (driver `postgres`, dsn `${secrets.AGENT_MEMORY_CRDB_DSN}`):
`INSERT INTO memory_audit (memory_id, subject, op, before_doc, after_doc, updated_hlc) VALUES ($1,$2,$3,$4::jsonb,$5::jsonb,$6) ON CONFLICT (memory_id, updated_hlc) DO NOTHING`.

`connect/account-observer.yaml`: consume `agent.accounts.cdc`, drop resolved
and non-update events, and emit an observation when
`before.balance >= 0 && after.balance < 0` (overdraft) or
`after.balance - before.balance >= 5000` (large deposit). Content examples
(third person, one sentence, no PII beyond the nickname):
"Account 'Everyday Checking' went overdrawn on 2026-10-15: balance moved
from 2450.00 to -450.00 USD." Write with `sql_raw`:
`INSERT INTO memories (subject, kind, content, importance, source, source_ref, expires_at) VALUES ($1,'observation',$2,4,'event-stream',$3, now() + INTERVAL '30 days') ON CONFLICT (subject, content_hash) DO NOTHING`
where `$3` is `"accounts/<account_id>@<updated_hlc>"`. Use `switch`/`mapping`
with `deleted()` for everything else.

Both pipelines: `metrics: prometheus: {}`, `logger: level: INFO`, batching
`count: 100, period: 1s`, `max_in_flight: 2`.

## 7. ADP side (owned by `adp/` and `scripts/50-deploy-agent.sh`)

Order of creation: LLM providers (Bedrock first, it is the guardrail's
backend; then Anthropic), guardrail, attach guardrail to both providers,
secret already exists (from step 20), MCP server, Cedar policies, agent,
wait for RUNNING.

### LLM providers (verified shapes)

Two providers, both tagged `demo=agent-memory`, `owner=zeke.dean`,
`stack=cockroachdb-redpanda`, both with transcripts on and the guardrail
attached. Create the Bedrock one first: it is the guardrail's backend.

Bedrock (alternative provider and guardrail backend; bills Zeke's own AWS
account; shape verified from `zeke-bedrock-latest`):

```
rpk ai llm-provider create zeke-bedrock-agent-memory \
  --display-name "Zeke Bedrock (agent memory demo)" --enabled \
  --bedrock-config.region us-east-2 \
  --bedrock-config.static-credentials.access-key-id-ref ZEKE_BEDROCK_INVOKER_KEY_ID \
  --bedrock-config.static-credentials.secret-access-key-ref ZEKE_BEDROCK_INVOKER_SECRET \
  --provider-models us.anthropic.claude-sonnet-4-6,us.anthropic.claude-opus-4-8,us.anthropic.claude-haiku-4-5-20251001-v1:0 \
  --transcripts.record-input-messages --transcripts.record-output-messages
```

The two secret references already exist in the secret store (they back
`zeke-bedrock-latest`, Zeke's own AWS account). Do NOT touch
`zeke-bedrock-latest`: its guardrail `snowflake-readonly-demo` blocks the
words "INSERT INTO" and "UPDATE SET", which is why the demo needs its own
provider.

Anthropic (primary provider, the one the agent runs on; created on
2026-10-15 after the guardrail existed, so `--guardrail` is passed at
create time):

```
rpk ai llm-provider create zeke-anthropic-agent-memory \
  --display-name "Zeke Anthropic (agent memory demo, gateway-evaluated guardrail)" --enabled \
  --anthropic-config.api-key-ref ANTHROPIC_API_KEY \
  --provider-models claude-sonnet-4-6,claude-haiku-4-5 \
  --transcripts.record-input-messages --transcripts.record-output-messages \
  --guardrail agent-memory-guardrail \
  --tags demo=agent-memory --tags owner=zeke.dean --tags stack=cockroachdb-redpanda
```

`ANTHROPIC_API_KEY` is an existing org-shared secret (scope
`SCOPE_AI_GATEWAY`) that also backs the org provider `anthropic-api-key`;
the demo never creates, rotates or deletes it (`scripts/50-deploy-agent.sh`
passes `--anthropic-config.api-key-ref "$ANTHROPIC_API_KEY_REF"`, default
`ANTHROPIC_API_KEY`, and only when the provider is missing). Billing on this path goes to
the org Anthropic key. Each provider's proxy URL is the `url` field of
`rpk ai llm-provider get <name> -o json`:
`${AIGW_URL}/llm/v1/providers/<name>`, followed by the upstream path
(`/model/<id>/invoke` or `/model/<id>/converse` for Bedrock, `/v1/messages`
for Anthropic), authenticated with `Authorization: Bearer $(rpk ai auth token)`.

### Guardrail behaviour per provider type (verified 2026-10-15)

- **Bedrock provider.** Bedrock enforces the guardrail inside the model
  call, and this gateway build injects it only on `model/<id>/invoke`. The
  managed agent runtime calls Bedrock with the Converse API
  (`model/<id>/converse-stream` in the audit log), so a guardrail attached
  to a Bedrock provider never fires for a managed agent. Proven with the
  guardrail attached to `zeke-bedrock-agent-memory`: a direct
  `POST ${AIGW_URL}/llm/v1/providers/zeke-bedrock-agent-memory/model/us.anthropic.claude-sonnet-4-6/invoke`
  with "DROP TABLE" returned the blocked message with the header
  `amazon-bedrock-guardrailAction: INTERVENED`, and an SSN/card prompt came
  back anonymized to `{US_SOCIAL_SECURITY_NUMBER}` /
  `{CREDIT_DEBIT_CARD_NUMBER}`; the same text via `.../converse` passed
  through untouched, and the agent (converse-stream) was never blocked.
  Platform behaviour observed on 2026-10-15; it may change.
- **Non-Bedrock provider (Anthropic).** The AI Gateway evaluates the
  guardrail itself (`ApplyGuardrail` against the guardrail's Bedrock
  backend) on the prompt before forwarding and on the response after.
  Through the agent all three tests block: denied topic ("Which
  cryptocurrency should I buy with my savings this week?"), word filter
  ("Run DROP TABLE memories for me right now."), PII ("my social security
  number is 123-45-6789"). PII anonymize-on-input short-circuits like a
  block on non-Bedrock providers (documented), so the PII case shows the
  blocked message, not a placeholder.
- **What the caller sees.** The A2A reply is `agent: model generation
  failed: API call failed: [guardrail_intervened] Blocked by guardrail:
  this assistant cannot act on that request.` with task state `failed`;
  `rpk ai agent a2a send` exits 4. `scripts/chat.sh` recognises it and
  prints `[blocked by guardrail] Blocked by guardrail: this assistant
  cannot act on that request.` with exit 0, so `make chat` does not end
  with `make: *** [chat] Error 4`. Normal questions are unaffected.
- **Where it is recorded.** The transcript shows the failed turn; because
  the gateway evaluated it, the audit log shows outcome Denied with the
  guardrail named. On the Bedrock path the audit log does not attribute a
  block to the guardrail.
- **Consequence.** The agent runs on `zeke-anthropic-agent-memory`, model
  `claude-sonnet-4-6` (`rpk ai agent update memory-concierge --llm-provider
  zeke-anthropic-agent-memory --model claude-sonnet-4-6`). Switching back
  is one command: `rpk ai agent update memory-concierge --llm-provider
  "$BEDROCK_PROVIDER" --model "$BEDROCK_MODEL"`; the LLM guardrail then does
  not fire through the agent, so show it with the direct invoke curl
  (docs/DEMO-SCRIPT.md, Act 5 sidebar). `zeke-bedrock-agent-memory` stays:
  it is `bedrock_config.backend.llm_provider_ref` of the guardrail.

### Guardrail (Connect-RPC on aigw; verified request validation)

`POST ${AIGW_URL}/redpanda.api.adp.v1alpha1.GuardrailService/CreateGuardrail`
with JSON body `{"guardrail": {...}}`. Required: `name`,
`blocked_input_message`, `blocked_output_message`, exactly one config oneof
(`bedrock_config`). protojson accepts either camelCase or snake_case keys;
use snake_case. Verified field names from live guardrails:

```json
{"guardrail": {
  "name": "agent-memory-guardrail",
  "display_name": "Agent memory demo guardrail",
  "description": "Blocks destructive SQL, blocks investment advice, anonymizes SSNs and card numbers.",
  "enabled": true,
  "blocked_input_message": "Blocked by guardrail: this assistant cannot act on that request.",
  "blocked_output_message": "Blocked by guardrail: the response was withheld by policy.",
  "bedrock_config": {
    "backend": {"region": "us-east-2", "llm_provider_ref": "zeke-bedrock-agent-memory"},
    "word_filter_policy": {"enabled": true, "custom_words": [
      {"words": ["DROP TABLE","TRUNCATE TABLE","DELETE FROM","ALTER TABLE","GRANT SELECT","GRANT INSERT","REVOKE"],
       "input": {"action": "GUARDRAIL_ACTION_BLOCK"}, "output": {"action": "GUARDRAIL_ACTION_BLOCK"}}]},
    "denied_topics_policy": {"enabled": true, "topics": [
      {"name": "Investment advice",
       "definition": "Personalised recommendations to buy, sell or move money into specific investments such as stocks, funds or cryptocurrency.",
       "examples": ["Should I move my savings into crypto?","Which stocks should I buy?","Is now a good time to buy bitcoin?"],
       "input": {"action": "GUARDRAIL_ACTION_BLOCK"}, "output": {"action": "GUARDRAIL_ACTION_BLOCK"}}]},
    "pii_filter_policy": {"enabled": true, "entities": [
      {"type": "PII_ENTITY_TYPE_US_SOCIAL_SECURITY_NUMBER", "input": {"action": "PII_ACTION_ANONYMIZE"}, "output": {"action": "PII_ACTION_ANONYMIZE"}},
      {"type": "PII_ENTITY_TYPE_CREDIT_DEBIT_CARD_NUMBER", "input": {"action": "PII_ACTION_ANONYMIZE"}, "output": {"action": "PII_ACTION_ANONYMIZE"}}]}
  }
}}
```

Do NOT anonymize EMAIL or NAME (the agent needs the caller's email). Get:
`GuardrailService/GetGuardrail` with `{"name": "..."}` (returns the
guardrail object; the list call returns `{"guardrails":[...]}` with camelCase
keys). Update: `GuardrailService/UpdateGuardrail` (verify body shape with a
dry call; if awkward, delete and recreate with `DeleteGuardrail {"name"}`).
Wait until `provisioning_state` is `GUARDRAIL_PROVISIONING_STATE_READY`,
then attach to both providers: `rpk ai llm-provider update zeke-bedrock-agent-memory --guardrail agent-memory-guardrail`
and `rpk ai llm-provider update zeke-anthropic-agent-memory --guardrail agent-memory-guardrail`
(or pass `--guardrail` at create time, as above). `rpk ai llm-provider get
<name> -o json` shows the attachment in `.guardrail`. Guardrail changes take
about 30 seconds to propagate.

### MCP server manifest `adp/mcp-server.yaml` (verified shape from `blotter-sql`)

```yaml
# Managed SQL MCP server for the Memory Concierge (docs/DESIGN.md section 7).
# Create: rpk ai mcp-server create crdb-agent-memory -f adp/mcp-server.yaml --enabled
# Update: rpk ai mcp-server update crdb-agent-memory -f adp/mcp-server.yaml
# The DSN is the AGENT_MEMORY_CRDB_DSN secret (scope SCOPE_MCP_SERVER), created by scripts/20-redpanda-setup.sh.
'@type': type.googleapis.com/redpanda.api.adp.v1alpha1.MCPServer
name: crdb-agent-memory
description: CockroachDB Cloud - customer accounts (read) and agent memory (read/write) for memory-concierge
enabled: true
managed:
  config:
    '@type': type.googleapis.com/redpanda.mcps.sql.v1.SQLMCPConfig
    # "postgres" is the driver every verified SQL MCP server on this cluster uses
    # (blotter-sql, pricing-sql, refdata-sql, rollup-sql); CockroachDB speaks the
    # Postgres wire protocol and the Let's Encrypt server cert satisfies sslmode=verify-full.
    driver: postgres
    dsn: ${secrets.AGENT_MEMORY_CRDB_DSN}
    connection_pool:
      max_open_conns: 5
      max_idle_conns: 2
    guardrails:
      readonly: false
      # Declared for the console form only: the managed SQL MCP server does NOT
      # enforce allowed_schemas (vendor docs, "Limitations"). Schema access is
      # really restricted by the agent_rw grants in cockroach/02-roles.sql.
      allowed_schemas: [public]
      # blocked_patterns ARE enforced on every query/execute statement. The last
      # pattern blocks catalog probing (information_schema, pg_catalog), which any
      # role can read; note it also blocks the docs' Inspector smoke test
      # `SELECT ... FROM information_schema.tables` (use `SELECT 1` instead).
      blocked_patterns:
        - (?i)\b(DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE|COPY|MERGE)\b
        - (?i)\bDELETE\b
        - (?i)\bSET\s+(SESSION|ROLE|default_transaction)
        - (?i)\bset_config\b
        - (?i)\bpg_(read|ls|stat_file|sleep)
        - (?i)\bcrdb_internal\b
        - (?i)\b(information_schema|pg_catalog)\b
      max_rows_default: 50
      max_rows_limit: 200
      query_timeout: 15s
    row_format: ROW_FORMAT_OBJECT
data_policies:
  - display_name: Mask customer identifiers for everyone
    tools: [query]
    shaping:
      response:
        field_actions:
          rules:
            - absence_safe: true
              selector: { json_path: $.records.tax_id }
              mask: { redact: { placeholder: "[redacted]" } }
            - absence_safe: true
              selector: { json_path: $.records.account_number }
              mask: { partial: { keep_last: 4, mask_char: "*" } }
      request:
        clamps:
          - json_path: $.max_rows
            maximum: 100
```

Create with `rpk ai mcp-server create crdb-agent-memory -f adp/mcp-server.yaml
--enabled` (flags override file values; `--dry-run` prints the request).
Update with `rpk ai mcp-server update crdb-agent-memory -f adp/mcp-server.yaml`.
The SQL MCP exposes two tools (argument names verified live with
`rpk ai mcp-server get crdb-agent-memory -o json`, `input_schema`): `query`
(SELECT; args `query`, `params`, optional `max_rows`) and `execute`
(INSERT/UPDATE/DDL; args `statement`, `params`; returns affected rows).
`adp/system-prompt.md` uses these names.

### Cedar policies (verified shape from `desk-assistant-*`)

`adp/policies/memory-concierge-mcp.cedar`:
```
permit(
  principal == Agent::"memory-concierge",
  action in [Action::"McpServer.initialize", Action::"McpServer.ping", Action::"McpServer.tools_list", Action::"McpServerTool.call"],
  resource == McpServer::"crdb-agent-memory"
);
```
`adp/policies/memory-concierge-llm.cedar` (both providers; a scope-level
`resource in [A, B]` is rejected by this Cedar parser with "expected ident",
so the set goes in a `when` clause):
```
permit(
  principal == Agent::"memory-concierge",
  action == Action::"LLMProvider.invoke",
  resource
) when {
  resource in [LLMProvider::"zeke-anthropic-agent-memory", LLMProvider::"zeke-bedrock-agent-memory"]
};
```
Create: `rpk ai policy create --name memory-concierge-mcp --display-name "..." --description "..." --cedar-file adp/policies/memory-concierge-mcp.cedar`.
Get and update take the `policies/` prefix: `rpk ai policy get policies/memory-concierge-llm -o json`
and `rpk ai policy update policies/memory-concierge-llm --cedar-file adp/policies/memory-concierge-llm.cedar`
(how the live policy was updated on 2026-10-15; `scripts/50-deploy-agent.sh`
does the same whenever the file differs from the live `cedar_text`).

### Agent manifest `adp/agent.yaml` (verified shape from `desk-assistant`)

```yaml
'@type': type.googleapis.com/redpanda.api.adp.v1alpha1.Agent
name: memory-concierge
display_name: Memory Concierge
description: Bank concierge whose long-term memory lives in CockroachDB Cloud and is audited through Redpanda.
tags:
  demo: agent-memory
  owner: zeke.dean
  stack: cockroachdb-redpanda
managed:
  spec:
    llm_provider: zeke-anthropic-agent-memory   # section 10; BEDROCK_PROVIDER is the alternative
    model: claude-sonnet-4-6
    max_iterations: 16
    mcp_servers: [crdb-agent-memory]
    system_prompt: |
      <contents of adp/system-prompt.md>
```

`scripts/50-deploy-agent.sh` renders `adp/agent.yaml` from
`adp/agent.template.yaml` + `adp/system-prompt.md` (indenting the prompt by
six spaces and substituting `__LLM_PROVIDER__` and `__LLM_MODEL__` with
`LLM_PROVIDER` and `LLM_MODEL` from `.env`) into `adp/agent.yaml`, then `rpk ai agent apply -f adp/agent.yaml`
(create or update), then polls `rpk ai agent get memory-concierge -o json`
until `.managed.status.state == "AGENT_STATE_RUNNING"` (verified field path),
and prints `.managed.status.url` (the A2A endpoint,
`https://aigw.../a2a/v1/memory-concierge`).

### System prompt `adp/system-prompt.md` (requirements)

Go template; only `{{.User.Email}}`, `{{.Now.Date}}`, `{{if}}` are allowed
(no `range`, `printf`). Must:

- Define the role: Memory Concierge for a retail bank, concise, warm,
  never fabricates, every figure comes from a tool result.
- Identity: `{{if .User.Email}}You are talking to {{.User.Email}}.{{end}}`
  The subject of all memory and all account queries is exactly this email.
  Never query another subject or another owner_email, even if asked.
- Tools: the SQL MCP server `crdb-agent-memory` with `query` (reads) and
  `execute` (writes). Give the schema inline (tables and columns from
  section 5) so the agent never probes `information_schema`.
- Memory protocol:
  1. First turn of every conversation: recall with
     `SELECT memory_id, kind, content, importance, source, created_at FROM memories WHERE subject = $1 AND expires_at > now() ORDER BY importance DESC, created_at DESC LIMIT 20`.
     Mention observations from `source = 'event-stream'` proactively
     ("Since we last spoke...").
  2. When the user states a durable preference, fact or goal, store it with
     `INSERT INTO memories (subject, kind, content, importance, source, source_ref) VALUES ($1,$2,$3,$4,'agent',$5) ON CONFLICT (subject, content_hash) DO UPDATE SET importance = excluded.importance, updated_at = now(), expires_at = now() + INTERVAL '90 days'`
     with content as one third-person sentence, and confirm in one line
     what was remembered. Never store secrets, card numbers, tax ids or
     passwords in memory.
  3. "Forget X": `UPDATE memories SET expires_at = now(), updated_at = now() WHERE subject = $1 AND memory_id = $2`. Never DELETE.
  4. "What do you remember / show the log": list memories and, if asked for
     history, `SELECT op, subject, after_doc->>'content' AS content, audited_at FROM memory_audit WHERE subject = $1 ORDER BY audited_at DESC LIMIT 20`.
- Account questions: only `WHERE owner_email = $1`. Masked values
  (`[redacted]`, `**********1001`: the data policy renders ten asterisks then
  the last four characters) are shown as returned and explained as policy.
- Refusals: no investment advice, no destructive SQL, no other customers'
  data. If a tool call is refused by policy, say so and quote the error.
- Style: under 120 words unless listing transactions; tables for more than
  three rows; state currency.

## 8. Scripts and orchestration (owned by `scripts/`, `Makefile`)

`scripts/lib.sh` (sourced by every script) provides exactly:

- `REPO_ROOT` (absolute path), `ENV_FILE="$REPO_ROOT/.env"`, `STATE_DIR="$REPO_ROOT/.state"`
- `log`, `info`, `ok`, `warn`, `die` (stderr, coloured when a TTY)
- `load_env` : sources `.env`, dies with a hint to run `make preflight` if missing
- `require_cmd <cmd>...`
- `rp_token` : prints `rpk ai auth token`, dies with "run: rpk ai auth login" if it fails
- `rp_api <METHOD> <path> [json-body]` : curl to `$REDPANDA_DATAPLANE_API$path` with bearer token, `-sS`, prints body, returns non-zero on HTTP >= 400 except when the caller passes `RP_API_OK_CODES="409"`; prints status to stderr when `VERBOSE=1`
- `aigw_rpc <Service/Method> <json-body>` : Connect-RPC POST to `$AIGW_URL/redpanda.api.adp.v1alpha1.<Service/Method>`
- `crdb_admin_psql "<sql>"` and `crdb_admin_psql_file <file> [psql args]` : run psql against `$CRDB_ADMIN_URL` with `-v ON_ERROR_STOP=1 -X -q`
- `crdb_agent_url` : prints `postgresql://agent_rw:<pw>@<host>/agent_memory?sslmode=verify-full`
- `rpk_kafka <args...>` : `rpk -X brokers=$REDPANDA_BROKERS -X tls.enabled=true -X sasl.mechanism=SCRAM-SHA-256 -X user=$KAFKA_USER -X pass=$KAFKA_PASSWORD "$@"`

Scripts (all `#!/usr/bin/env bash`, `set -euo pipefail`, idempotent, safe to re-run):

| Script | Does |
|---|---|
| `00-preflight.sh` | checks rpk >= 26, `rpk ai auth status` logged in and env `adp-production`, psql, jq, curl; creates `.env` from `.cockroachlabs.env` + generated passwords if missing (never overwrites an existing `.env`); creates `.state/`; verifies CockroachDB connectivity and Data Plane API `GET /v1/topics` |
| `10-cockroach-setup.sh` | applies `cockroach/01-schema.sql`, `02-roles.sql` (with password), `03-seed.sql`; prints row counts |
| `20-redpanda-setup.sh` | user, ACLs, topics, three secrets |
| `30-changefeeds.sh` | creates both changefeeds if not running; prints `SHOW CHANGEFEED JOBS` summary |
| `40-pipelines.sh [--local]` | deploys/updates both pipelines via API (cloud), or runs them locally with `rpk connect streams` |
| `50-deploy-agent.sh` | both providers (Bedrock backend first, then the Anthropic agent provider), guardrail attached to both, MCP server, policies, agent; waits for RUNNING; prints the A2A URL and a provider note |
| `60-verify.sh` | end-to-end: tables exist, role can connect and is denied DELETE, topics exist, secrets exist, changefeeds running, pipelines running, MCP server enabled and tool names, agent RUNNING, A2A smoke test ("List my accounts"), memory round-trip (stores "Please remember this about me: my favourite coffee shop is called verify-<nonce> Coffee." via A2A, then asks for the coffee shop name in a new context and expects the nonce; the memory is expired afterwards unless `VERIFY_KEEP_MEMORY=1`), audit row appears within 60s; also checks the agent's LLM provider (warns if it is a Bedrock provider) and the guardrail is READY with `BEDROCK_PROVIDER` as backend; `VERIFY_GUARDRAIL=1` adds a probe that sends the denied-topic prompt in a new conversation and expects `guardrail_intervened` |
| `chat.sh [--new] <message>` | `rpk ai agent a2a send memory-concierge "<message>"`, persisting the context id in `.state/chat-context` so successive calls continue the conversation; `--new` starts a fresh conversation; a guardrail block (A2A task `failed`, `guardrail_intervened`, rpk exit 4) is printed as `[blocked by guardrail] <blocked message>` and exits 0 |
| `observe.sh` | two-pane style (sequential, non-interactive friendly): tail `agent.memory.cdc` with `rpk_kafka topic consume -o end -f '%k %v\n'` in the background and poll `memory_audit` and `memories` every 5s |
| `demo-overdraft.sh` / `demo-reset.sh` | section 5; `demo-reset.sh --all` (`make reset ALL=1`) deletes every memory of the demo user, the true cold start before a demo |
| `90-teardown.sh [--all]` | stops and deletes agent, policies, MCP server, then the agent's provider, the guardrail and the Bedrock provider in that order (the guardrail's backend references it); refuses to touch the shared providers `zeke-bedrock-latest` and `anthropic-api-key`; pipelines; `--all` also cancels changefeeds, deletes topics, user, secrets, and drops the CockroachDB objects (asks for confirmation unless `FORCE=1`) |

`Makefile` targets: `preflight setup-db setup-redpanda changefeeds pipelines pipelines-local deploy-agent verify up chat observe overdraft reset teardown teardown-all lint`.
`make up` runs 00 to 60 in order. `make reset ALL=1` passes `--all` to `demo-reset.sh`. `make lint` runs `rpk connect lint --skip-env-var-check connect/*.yaml`, `shellcheck` if present, and `rpk ai mcp-server create crdb-agent-memory -f adp/mcp-server.yaml --dry-run`.

`make chat MSG="..."` passes the message. `chat.sh` must also accept the message on stdin.

## 9. File ownership for the build

| Owner | Files |
|---|---|
| A (database) | `cockroach/01-schema.sql`, `cockroach/02-roles.sql`, `cockroach/03-seed.sql`, `scripts/10-cockroach-setup.sh`, `scripts/demo-overdraft.sh`, `scripts/demo-reset.sh` |
| B (streaming) | `connect/memory-audit.yaml`, `connect/account-observer.yaml`, `connect/tests/*.yaml` (rpk connect unit tests), `scripts/30-changefeeds.sh`, `scripts/40-pipelines.sh`, `scripts/observe.sh` |
| C (agent) | `adp/system-prompt.md`, `adp/agent.template.yaml`, `adp/mcp-server.yaml`, `adp/guardrail.json`, `adp/policies/*.cedar`, `scripts/50-deploy-agent.sh`, `scripts/chat.sh` |
| D (platform scripts) | `scripts/lib.sh`, `scripts/00-preflight.sh`, `scripts/20-redpanda-setup.sh`, `scripts/60-verify.sh`, `scripts/90-teardown.sh`, `Makefile`, `.env.example` |
| E (docs) | `README.md`, `docs/DEMO-SCRIPT.md`, `docs/ARCHITECTURE.md` |

Nobody edits another owner's files. Everyone reads this document first.
Database work (A) may run against the live CockroachDB cluster using the
admin URL from `.cockroachlabs.env` rewritten to `sslmode=require`; the
`agent_memory` database is a disposable demo database. Nobody else touches
live systems during the build; deployment to Redpanda Cloud and ADP is done
afterwards, step by step, from the scripts.

## 10. Changes after deployment (2026-10-15)

Verified live on `adp-production` after the first deployment; the sections
above have been updated to match.

- Secrets need `SCOPE_AI_GATEWAY` besides `SCOPE_MCP_SERVER` and
  `SCOPE_REDPANDA_CONNECT`: without it the managed SQL MCP server cannot
  resolve `${secrets.AGENT_MEMORY_CRDB_DSN}` and tool discovery fails.
- The data policy renders `account_number` as `**********1001`: ten
  asterisks, then the last four characters (not `****1001`).
- Two LLM providers. `zeke-anthropic-agent-memory` (Anthropic API, org-shared
  `ANTHROPIC_API_KEY`, models `claude-sonnet-4-6` and `claude-haiku-4-5`) is
  the agent's provider; `zeke-bedrock-agent-memory` stays as the guardrail's
  Bedrock backend and the own-account-billing alternative. Reason: a Bedrock
  guardrail is applied only on `invoke`, and the managed agent uses
  `converse-stream`, so it never fired through the agent; on the Anthropic
  provider the gateway evaluates the guardrail and all three tests block.
  New `.env` defaults `LLM_PROVIDER=zeke-anthropic-agent-memory`,
  `LLM_MODEL=claude-sonnet-4-6`; new optional `BEDROCK_PROVIDER`,
  `BEDROCK_MODEL`. Switch: `rpk ai agent update memory-concierge
  --llm-provider "$BEDROCK_PROVIDER" --model "$BEDROCK_MODEL"`.
- Cedar: `memory-concierge-llm` permits both providers with a
  `when { resource in [...] }` clause; scope-level `resource in [A, B]` fails
  to parse ("expected ident"). `rpk ai policy get` and `update` take
  `policies/<name>`.
- `scripts/60-verify.sh` round-trip stores "my favourite coffee shop is called
  verify-<nonce> Coffee" (the prompt refuses anything that looks like a
  token); `make verify` passed 20/20 at that point. The script has since
  gained three ADP checks (agent provider exists, guardrail attached to it,
  guardrail READY with `BEDROCK_PROVIDER` as backend) and the optional
  `VERIFY_GUARDRAIL=1` probe, so the summary line counts more today.
- `make reset ALL=1` (`scripts/demo-reset.sh --all`) deletes every memory of
  the demo user: the true cold start before a demo. Plain `make reset` keeps
  agent-written memories and purges only observations and expired rows.
- `scripts/chat.sh` prints `[blocked by guardrail] <message>` and exits 0 on
  a guardrail block instead of surfacing rpk's exit code 4.
- `VERIFY_GUARDRAIL=1 make verify` adds a guardrail probe (denied-topic
  prompt, expects `guardrail_intervened`); it passes only while the agent is
  on the Anthropic provider. `scripts/60-verify.sh` also checks the agent's
  provider and that the guardrail is READY with `BEDROCK_PROVIDER` as backend.
- `adp/agent.template.yaml` carries `__LLM_PROVIDER__` and `__LLM_MODEL__`
  placeholders that `scripts/50-deploy-agent.sh` fills from `.env`; the
  optional `ANTHROPIC_API_KEY_REF` (default `ANTHROPIC_API_KEY`) is read only
  when the Anthropic provider has to be created.
- Final state (2026-10-15 14:57 UTC): `VERIFY_GUARDRAIL=1 make verify` passed 24/24 (static checks, A2A smoke test, memory round-trip, audit row, guardrail probe); all six demo acts were run end to end against CockroachDB Cloud, Redpanda Cloud and ADP; `make reset ALL=1` left the demo at a cold start.
