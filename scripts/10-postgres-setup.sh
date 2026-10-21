#!/usr/bin/env bash
# scripts/10-postgres-setup.sh
# Applies the PostgreSQL side of the demo, as the admin user, in order:
#   postgres/01-schema.sql    schema agent_memory, tables, indexes          (IF NOT EXISTS)
#   postgres/02-triggers.sql  change-capture triggers into change_log       (CREATE OR REPLACE)
#   postgres/03-seed.sql      customers, accounts, transactions             (ON CONFLICT DO NOTHING,
#                             then re-anchors the seeded ledger to the last 30 days)
# then prints row counts and proves the agent user can log in.
# Contract: docs/DESIGN.md sections 4 and 8. Idempotent: safe to re-run.
#
# Usage: scripts/10-postgres-setup.sh        (make setup-db)
# Needs .env from scripts/00-preflight.sh (PG_ADMIN_URL, PG_AGENT_* ...).
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"
load_env
require_cmd psql jq

SQL_DIR="$REPO_ROOT/postgres"
for f in 01-schema.sql 02-triggers.sql 03-seed.sql; do
  [[ -f "$SQL_DIR/$f" ]] || die "missing $SQL_DIR/$f"
done

info "Applying schema ($SQL_DIR/01-schema.sql)"
pg_admin_psql_file "$SQL_DIR/01-schema.sql" 2> >(grep -v 'already exists, skipping' >&2 || true)
ok "schema $PG_SCHEMA applied in $PG_DATABASE"

info "Applying change-capture triggers ($SQL_DIR/02-triggers.sql)"
pg_admin_psql_file "$SQL_DIR/02-triggers.sql"
ok "triggers tr_memories_log (insert/update/delete) and tr_accounts_log (update) write to ${PG_SCHEMA}.change_log"

info "Applying seed data ($SQL_DIR/03-seed.sql)"
pg_admin_psql_file "$SQL_DIR/03-seed.sql"
ok "seed applied (seeded transactions re-anchored so the newest is ~1 day old)"

info "Row counts in ${PG_DATABASE}.${PG_SCHEMA}"
pg_admin_psql "
SELECT 'customers'    AS table_name, count(*) AS row_count FROM ${PG_SCHEMA}.customers
UNION ALL SELECT 'accounts',     count(*) FROM ${PG_SCHEMA}.accounts
UNION ALL SELECT 'transactions', count(*) FROM ${PG_SCHEMA}.transactions
UNION ALL SELECT 'memories',     count(*) FROM ${PG_SCHEMA}.memories
UNION ALL SELECT 'change_log',   count(*) FROM ${PG_SCHEMA}.change_log
UNION ALL SELECT 'memory_audit', count(*) FROM ${PG_SCHEMA}.memory_audit
ORDER BY 1;"

info "Checking the change-capture objects"
n="$(pg_admin_psql "SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgname IN ('tr_memories_log','tr_accounts_log')" -At)"
[[ "$n" == "2" ]] || die "expected 2 triggers, found $n"
ok "2 triggers present"
idx="$(pg_admin_psql "SELECT indexdef FROM pg_indexes WHERE schemaname = '${PG_SCHEMA}' AND indexname = 'idx_memories_search'" -At)"
[[ -n "$idx" ]] || die "full-text index idx_memories_search is missing"
case "$idx" in
  *"USING fts"*) ok "full-text index: GantrySQL fts (analyzer english)" ;;
  *"USING gin"*|*"USING GIN"*) ok "full-text index: GIN over to_tsvector (vanilla PostgreSQL)" ;;
  *) ok "full-text index: $idx" ;;
esac

info "Checking that $PG_AGENT_USER can log in with the agent DSN"
who="$(psql "$(pg_agent_url)" -X -q -v ON_ERROR_STOP=1 -At -c 'SELECT current_user' 2>&1)" \
  || die "$PG_AGENT_USER cannot connect: $who"
[[ "$who" == "$PG_AGENT_USER" ]] || die "connected as '$who', expected '$PG_AGENT_USER'"
admin="$(pg_admin_psql "SELECT current_user" -At)"
if [[ "$who" == "$admin" ]]; then
  ok "$PG_AGENT_USER can connect (same user as the admin: this server has no CREATE ROLE, so the MCP server's blocked patterns carry the write restrictions)"
else
  ok "$PG_AGENT_USER can connect to $PG_DATABASE (least-privilege role)"
fi

ok "PostgreSQL setup complete. Next: make setup-redpanda"
