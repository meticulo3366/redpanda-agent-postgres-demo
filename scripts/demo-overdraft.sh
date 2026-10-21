#!/usr/bin/env bash
# scripts/demo-overdraft.sh
# Demo act 4 ("event-driven memory"): drive the demo user's Everyday Checking
# (account 1001) overdrawn. The accounts trigger logs the update, the
# pg-agent-memory-cdc pipeline publishes it on agent.pg.accounts.cdc and the
# pg-agent-memory-observer pipeline turns the before.balance >= 0 ->
# after.balance < 0 transition into an observation memory.
#
# In ONE transaction (docs/DESIGN.md section 4), as the admin:
#   - insert a -2,900.00 rent debit ("Riverside Property Mgmt") on account 1001
#   - set balance = -450.00, updated_at = now() on account 1001
# Idempotent: the debit has a fixed txn_id (ON CONFLICT DO NOTHING) and the UPDATE
# is absolute. The observer only fires on the >= 0 -> < 0 transition, so to
# re-trigger the observation run scripts/demo-reset.sh first.
#
# Usage: scripts/demo-overdraft.sh        (make overdraft)
# Undo:  scripts/demo-reset.sh            (make reset)
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"
load_env
require_cmd psql
: "${PG_ADMIN_URL:?PG_ADMIN_URL missing from .env (run: make preflight)}"

DEMO_ACCOUNT_ID=1001
DEMO_TXN_ID='c0ffee00-0000-4000-8000-000000000999'   # fixed: demo-reset.sh removes exactly this row
RENT_AMOUNT='-2900.00'
OVERDRAWN_BALANCE='-450.00'
TOPIC="${TOPIC_ACCOUNTS_CDC:-agent.pg.accounts.cdc}"
S="${PG_SCHEMA:-agent_memory}"

account_sql="SELECT account_id, nickname, balance, currency, overdraft_limit, status, updated_at
             FROM ${S}.accounts WHERE account_id = ${DEMO_ACCOUNT_ID};"

balance_before="$(psql "$PG_ADMIN_URL" -X -q -v ON_ERROR_STOP=1 -At \
  -c "SELECT balance FROM ${S}.accounts WHERE account_id = ${DEMO_ACCOUNT_ID}")"
[[ -n "$balance_before" ]] || die "account ${DEMO_ACCOUNT_ID} not found; run: make setup-db"

info "Account ${DEMO_ACCOUNT_ID} before: balance ${balance_before}"
if [[ "$balance_before" == -* ]]; then
  warn "account ${DEMO_ACCOUNT_ID} is already overdrawn (${balance_before}); the observer reacts only to a >= 0 -> < 0 transition. Run scripts/demo-reset.sh first to re-trigger it."
fi

info "Posting rent debit ${RENT_AMOUNT} and setting balance to ${OVERDRAWN_BALANCE} in one transaction"
pg_admin_psql "
BEGIN;
INSERT INTO ${S}.transactions (txn_id, account_id, amount, merchant, category, occurred_at)
  VALUES ('${DEMO_TXN_ID}', ${DEMO_ACCOUNT_ID}, ${RENT_AMOUNT}, 'Riverside Property Mgmt', 'rent', now())
  ON CONFLICT (txn_id) DO NOTHING;
UPDATE ${S}.accounts SET balance = ${OVERDRAWN_BALANCE}, updated_at = now()
  WHERE account_id = ${DEMO_ACCOUNT_ID};
COMMIT;"

info "Account ${DEMO_ACCOUNT_ID} after:"
pg_admin_psql "$account_sql"
info "Latest transactions on account ${DEMO_ACCOUNT_ID}:"
pg_admin_psql "SELECT occurred_at, amount, merchant, category FROM ${S}.transactions
                 WHERE account_id = ${DEMO_ACCOUNT_ID} ORDER BY occurred_at DESC LIMIT 3;"

ok "Overdraft posted: balance ${balance_before} -> ${OVERDRAWN_BALANCE}."
log "    The trigger logs the update, the cdc pipeline publishes it on ${TOPIC} and the observer writes an"
log "    'observation' memory within seconds (watch: make observe)."
log "    Then start a NEW conversation: scripts/chat.sh --new \"Hi\""
