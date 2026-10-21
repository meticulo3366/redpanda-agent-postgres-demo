#!/usr/bin/env bash
# scripts/demo-reset.sh
# Puts the demo back to its seeded state, as the admin (docs/PORT-CONTRACT.md section 4):
#   - account 1001 balance back to 2,450.00 (updated_at = now())
#   - removes the demo rent transaction posted by scripts/demo-overdraft.sh
#   - re-applies postgres/03-seed.sql: restores any missing seed rows and
#     re-anchors the seeded transactions so the newest is ~1 day old again
#     (the agent's spending queries look at the last 30 days)
#   - deletes observation memories for the demo user (the agent itself can never DELETE)
#   - purges the demo user's already-expired memories, which is exactly what the
#     hourly purge pipeline would do; done now, a sentence re-stated in act 2 is a
#     fresh INSERT instead of an upsert that revives the expired row as an UPDATE
# --all   deletes ALL memories for the demo user instead: the true cold start
#         before a demo (docs/DEMO-SCRIPT.md pre-demo checklist and reset procedure).
#
# memory_audit is never touched: it is the append-only trail, and these admin
# deletes reach it through the trigger and the cdc pipeline like any other change.
# Idempotent: safe to run any number of times.
#
# Usage: scripts/demo-reset.sh [--all]    (make reset)
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"
load_env
require_cmd psql
: "${PG_ADMIN_URL:?PG_ADMIN_URL missing from .env (run: make preflight)}"
S="${PG_SCHEMA:-agent_memory}"

usage() {
  cat <<USAGE
Usage: $(basename "$0") [--all]
  Restores account 1001 to 2,450.00, removes the demo rent transaction,
  re-anchors the seeded ledger to the last 30 days, deletes observation
  memories and purges expired memories for \$DEMO_USER_EMAIL.
  --all   deletes every memory for the demo user instead (cold start).
USAGE
}

RESET_ALL=0
for arg in "$@"; do
  case "$arg" in
    --all)     RESET_ALL=1 ;;
    -h|--help) usage; exit 0 ;;
    *)         usage >&2; die "unknown argument: $arg" ;;
  esac
done

DEMO_ACCOUNT_ID=1001
DEMO_TXN_ID='c0ffee00-0000-4000-8000-000000000999'   # fixed in scripts/demo-overdraft.sh
SEED_BALANCE='2450.00'
SEED_FILE="$REPO_ROOT/postgres/03-seed.sql"
[[ -f "$SEED_FILE" ]] || die "missing $SEED_FILE"

SUBJECT="${DEMO_USER_EMAIL:-zeke.dean@redpanda.com}"
# The subject is interpolated into SQL literals below, so insist it looks like
# an email (apostrophes are allowed: o'brien@example.com) and double any quote.
# Note: "${x//\'/\'\'}" is NOT portable (bash 3.2, the /bin/bash on macOS, emits
# literal backslashes), hence the variable.
email_re="^[A-Za-z0-9._%+'-]+@[A-Za-z0-9.-]+$"
[[ "$SUBJECT" =~ $email_re ]] || die "DEMO_USER_EMAIL does not look like an email address: $SUBJECT"
q="'"
SUBJECT_SQL="${SUBJECT//$q/$q$q}"

info "Restoring account ${DEMO_ACCOUNT_ID} to balance ${SEED_BALANCE} and removing the demo rent transaction"
pg_admin_psql "
BEGIN;
UPDATE ${S}.accounts SET balance = ${SEED_BALANCE}, updated_at = now() WHERE account_id = ${DEMO_ACCOUNT_ID};
DELETE FROM ${S}.transactions WHERE txn_id = '${DEMO_TXN_ID}';
COMMIT;"

info "Re-applying seed (restores missing seed rows, re-anchors seeded transactions to the last 30 days)"
pg_admin_psql_file "$SEED_FILE"
pg_admin_psql "SELECT count(*) AS seeded_txns_on_${DEMO_ACCOUNT_ID},
       count(*) FILTER (WHERE occurred_at > now() - INTERVAL '30 days') AS within_30_days,
       max(occurred_at) AS newest
  FROM ${S}.transactions WHERE account_id = ${DEMO_ACCOUNT_ID};"

if (( RESET_ALL )); then
  info "--all: deleting every memory for ${SUBJECT} (cold start)"
  pg_admin_psql "WITH d AS (
    DELETE FROM ${S}.memories WHERE subject = '${SUBJECT_SQL}' RETURNING memory_id
  ) SELECT count(*) AS memories_deleted FROM d;"
else
  info "Deleting observation memories and purging expired memories for ${SUBJECT}"
  pg_admin_psql "WITH d AS (
    DELETE FROM ${S}.memories
     WHERE subject = '${SUBJECT_SQL}' AND (kind = 'observation' OR expires_at <= now())
    RETURNING kind
  ) SELECT count(*) FILTER (WHERE kind = 'observation')  AS observations_deleted,
           count(*) FILTER (WHERE kind <> 'observation') AS expired_purged
      FROM d;"
fi

info "State now:"
pg_admin_psql "SELECT account_id, nickname, balance, currency, updated_at
                 FROM ${S}.accounts WHERE account_id = ${DEMO_ACCOUNT_ID};"
pg_admin_psql "SELECT kind, count(*) AS live_memories FROM ${S}.memories
                 WHERE subject = '${SUBJECT_SQL}' AND expires_at > now() GROUP BY kind ORDER BY kind;"

live="$(pg_admin_psql "SELECT count(*) FROM ${S}.memories WHERE subject = '${SUBJECT_SQL}' AND expires_at > now()" -At)"
if (( RESET_ALL == 0 )) && (( live > 0 )); then
  warn "${live} live memories remain for ${SUBJECT}. For a cold start (act 1 recalls nothing,"
  warn "act 2 events are inserts) run: scripts/demo-reset.sh --all"
fi
ok "Demo reset complete"
