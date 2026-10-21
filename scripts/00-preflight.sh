#!/usr/bin/env bash
# 00-preflight.sh - check tools and logins, create .env on first run, verify
# PostgreSQL and Redpanda Data Plane API connectivity.
#
# .env is written from .postgres.env (gitignored; PGHOST, PGPORT, PGDATABASE,
# PGUSER, PGPASSWORD, optionally PG_AGENT_USER and PG_AGENT_PASSWORD when the
# server lets you create a least-privilege role) plus the fixed names in
# docs/DESIGN.md section 2 and a generated Kafka password.
# Idempotent: an existing .env is never overwritten.
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

# Fixed values from docs/DESIGN.md sections 2-3.
FIXED_PG_SCHEMA="agent_memory"
FIXED_REDPANDA_BROKERS="seed-8a6dfc25.d6kjl4h19241bg3ek3h0.byoc.prd.cloud.redpanda.com:9092"
FIXED_REDPANDA_DATAPLANE_API="https://api-8a6dfc25.d6kjl4h19241bg3ek3h0.byoc.prd.cloud.redpanda.com"
FIXED_REDPANDA_SCHEMA_REGISTRY_URL="https://schema-registry-0d1ddabe.d6kjl4h19241bg3ek3h0.byoc.prd.cloud.redpanda.com:30081"
# This demo's own SASL user (other demos on the same cluster have their own).
FIXED_KAFKA_USER="agent-memory-pg-demo"
FIXED_TOPIC_MEMORY_CDC="agent.pg.memory.cdc"
FIXED_TOPIC_ACCOUNTS_CDC="agent.pg.accounts.cdc"
FIXED_ADP_ENV="adp-production"
FIXED_AIGW_URL="https://aigw.d6kjl4h19241bg3ek3h0.clusters.rdpa.co"
# The agent runs on the Anthropic provider: for non-Bedrock providers the AI
# Gateway evaluates the guardrail itself, whereas a guardrail attached to a
# Bedrock provider never fires on the agent's Converse calls (.env.example).
FIXED_LLM_PROVIDER="zeke-anthropic-agent-memory"
FIXED_LLM_MODEL="claude-sonnet-4-6"
# Bedrock provider: the guardrail's backend and the own-account billing alternative.
FIXED_BEDROCK_PROVIDER="zeke-bedrock-agent-memory"
FIXED_BEDROCK_MODEL="us.anthropic.claude-sonnet-4-6"
FIXED_GUARDRAIL_NAME="agent-memory-guardrail"
FIXED_MCP_SERVER_NAME="pg-agent-memory"
FIXED_AGENT_NAME="memory-concierge-pg"
FIXED_DEMO_USER_EMAIL="zeke.dean@redpanda.com"

# 32 URL-safe alphanumeric characters (no percent-encoding needed in DSNs/URIs).
gen_password() {
  local p
  p="$(LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c 32 || true)"
  [[ ${#p} -eq 32 ]] || die "password generation failed"
  printf '%s' "$p"
}

# pg_env_get <file> <KEY>: the value of KEY=value in a .postgres.env file,
# without sourcing it (so PGPASSWORD never lands in this shell's environment).
# Surrounding single or double quotes are stripped.
pg_env_get() {
  local v
  v="$(grep -E "^[[:space:]]*(export[[:space:]]+)?$2=" "$1" | head -1 | sed -E 's/^[[:space:]]*(export[[:space:]]+)?[A-Z_]+=//' | tr -d '\r' || true)"
  v="${v%\"}"; v="${v#\"}"; v="${v%\'}"; v="${v#\'}"
  printf '%s' "$v"
}
urlenc() { jq -rn --arg v "$1" '$v|@uri'; }

# ---------------------------------------------------------------------------
info "Checking required tools"
require_cmd rpk psql jq curl

rpk_ver="$(rpk version 2>/dev/null | grep -oE 'v[0-9]+\.[0-9]+(\.[0-9]+)?' | head -1 || true)"
[[ -n "$rpk_ver" ]] || die "could not determine the rpk version (is rpk on PATH?)"
rpk_major="${rpk_ver#v}"; rpk_major="${rpk_major%%.*}"
(( rpk_major >= 26 )) || die "rpk $rpk_ver is too old; this demo needs rpk >= v26 (rpk ai embedded)"
ok "rpk $rpk_ver"
ok "psql $(psql --version 2>/dev/null | awk '{print $3}')"
ok "jq $(jq --version 2>/dev/null)"
ok "curl $(curl --version 2>/dev/null | head -1 | awk '{print $2}')"

# ---------------------------------------------------------------------------
if [[ -f "$ENV_FILE" ]]; then
  info ".env already exists; leaving it untouched ($ENV_FILE)"
else
  info "Creating .env from .postgres.env"
  pgenv="$REPO_ROOT/.postgres.env"
  [[ -f "$pgenv" ]] || die "$pgenv not found. Create it (gitignored, mode 600) with PGHOST, PGPORT, PGDATABASE, PGUSER and PGPASSWORD for the target server (see .env.example) and re-run."

  pghost="$(pg_env_get "$pgenv" PGHOST)"
  pgport="$(pg_env_get "$pgenv" PGPORT)"; [[ -n "$pgport" ]] || pgport=5432
  pgdb="$(pg_env_get "$pgenv" PGDATABASE)"
  pguser="$(pg_env_get "$pgenv" PGUSER)"
  pgpass="$(pg_env_get "$pgenv" PGPASSWORD)"
  [[ -n "$pghost" && -n "$pgdb" && -n "$pguser" && -n "$pgpass" ]] || die "$pgenv must set PGHOST, PGDATABASE, PGUSER and PGPASSWORD"
  # Least-privilege role, when the server lets you create one; else the admin user.
  # Both halves or neither: a role name with the admin's password would only fail later.
  agent_user="$(pg_env_get "$pgenv" PG_AGENT_USER)"
  agent_pass="$(pg_env_get "$pgenv" PG_AGENT_PASSWORD)"
  if [[ -n "$agent_user" && -z "$agent_pass" ]] || [[ -z "$agent_user" && -n "$agent_pass" ]]; then
    die "set both PG_AGENT_USER and PG_AGENT_PASSWORD in $pgenv, or neither"
  fi
  [[ -n "$agent_user" ]] || agent_user="$pguser"
  [[ -n "$agent_pass" ]] || agent_pass="$pgpass"
  # .env is sourced by bash, and these two values come from the server, not from a
  # generator: write them as bash-safe literals so quotes, dollars, backslashes and
  # backticks in a password cannot break or execute anything in load_env.
  agent_user_q="$(printf '%q' "$agent_user")"
  agent_pass_q="$(printf '%q' "$agent_pass")"

  admin_url="postgresql://$(urlenc "$pguser"):$(urlenc "$pgpass")@${pghost}:${pgport}/${pgdb}?sslmode=require&options=-c%20search_path%3D${FIXED_PG_SCHEMA},public"
  pw_kafka="$(gen_password)"

  old_umask="$(umask)"
  umask 077
  tmp="$(mktemp "$REPO_ROOT/.env.XXXXXX")"
  cat >"$tmp" <<ENVEOF
# Generated by scripts/00-preflight.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ). Gitignored. Do not commit.
# See .env.example for documentation of every variable.

# PostgreSQL (admin URL built from .postgres.env; TLS required; search_path for psql)
PG_ADMIN_URL="$admin_url"
PG_HOST="${pghost}:${pgport}"
PG_DATABASE="$pgdb"
PG_SCHEMA="$FIXED_PG_SCHEMA"
# User the MCP server and the pipelines connect as (the admin user when the server has no CREATE ROLE)
PG_AGENT_USER=$agent_user_q
PG_AGENT_PASSWORD=$agent_pass_q

# Redpanda Cloud
REDPANDA_BROKERS="$FIXED_REDPANDA_BROKERS"
REDPANDA_DATAPLANE_API="$FIXED_REDPANDA_DATAPLANE_API"
REDPANDA_SCHEMA_REGISTRY_URL="$FIXED_REDPANDA_SCHEMA_REGISTRY_URL"
KAFKA_USER="$FIXED_KAFKA_USER"
KAFKA_PASSWORD="$pw_kafka"
TOPIC_MEMORY_CDC="$FIXED_TOPIC_MEMORY_CDC"
TOPIC_ACCOUNTS_CDC="$FIXED_TOPIC_ACCOUNTS_CDC"

# ADP
ADP_ENV="$FIXED_ADP_ENV"
AIGW_URL="$FIXED_AIGW_URL"
# Provider the agent uses (Anthropic type: the gateway evaluates the guardrail itself)
LLM_PROVIDER="$FIXED_LLM_PROVIDER"
LLM_MODEL="$FIXED_LLM_MODEL"
# Bedrock provider: the guardrail's backend and the own-account billing alternative.
# Switch with: rpk ai agent update $FIXED_AGENT_NAME --llm-provider "\$BEDROCK_PROVIDER" --model "\$BEDROCK_MODEL"
BEDROCK_PROVIDER="$FIXED_BEDROCK_PROVIDER"
BEDROCK_MODEL="$FIXED_BEDROCK_MODEL"
GUARDRAIL_NAME="$FIXED_GUARDRAIL_NAME"
MCP_SERVER_NAME="$FIXED_MCP_SERVER_NAME"
AGENT_NAME="$FIXED_AGENT_NAME"
DEMO_USER_EMAIL="$FIXED_DEMO_USER_EMAIL"
ENVEOF
  mv "$tmp" "$ENV_FILE"
  chmod 600 "$ENV_FILE"
  umask "$old_umask"
  ok "wrote $ENV_FILE (mode 600; database $pgdb on ${pghost}:${pgport}; generated KAFKA_PASSWORD)"
fi

mkdir -p "$STATE_DIR"
ok "state directory $STATE_DIR"

load_env
ok "agent provider $LLM_PROVIDER ($LLM_MODEL); guardrail backend $BEDROCK_PROVIDER ($BEDROCK_MODEL)"

# ---------------------------------------------------------------------------
info "Verifying PostgreSQL connectivity (as the admin user)"
if ! out="$(pg_admin_psql "SELECT version()" -At 2>&1)"; then
  die "cannot connect with PG_ADMIN_URL: $out
Hints: the database '$PG_DATABASE' must exist and accept TLS connections; if the password changed, delete .env and re-run."
fi
ok "PostgreSQL reachable: $PG_HOST, database $PG_DATABASE"
log "    server: ${out:0:100}"
wal="$(pg_admin_psql "SHOW wal_level" -At 2>/dev/null || echo unknown)"
if [[ "$wal" == "logical" ]]; then
  log "    wal_level = logical (the trigger-fed change_log still works; a postgres_cdc input would be possible too)"
else
  log "    wal_level = $wal: no logical decoding, so change capture is the trigger-fed change_log table (docs/DESIGN.md section 1)"
fi
can_role="$(pg_admin_psql "SELECT rolcreaterole FROM pg_roles WHERE rolname = current_user" -At 2>/dev/null || echo "?")"
if [[ "$can_role" == "t" ]]; then
  log "    current user may CREATE ROLE: set PG_AGENT_USER/PG_AGENT_PASSWORD in .postgres.env for a least-privilege agent role"
else
  log "    current user cannot CREATE ROLE: the agent connects as $PG_AGENT_USER; the MCP server's blocked patterns carry the write restrictions"
fi

# ---------------------------------------------------------------------------
info "Checking ADP login (rpk ai auth status)"
status_json="$(rpk ai auth status -o json 2>/dev/null || true)"
logged_in="$(jq -r '.logged_in // false' <<<"$status_json" 2>/dev/null || echo false)"
env_name="$(jq -r '.environment // ""' <<<"$status_json" 2>/dev/null || true)"
[[ "$logged_in" == "true" ]] || die "rpk ai is not logged in; run: rpk ai auth login"
[[ "$env_name" == "$FIXED_ADP_ENV" ]] || die "rpk ai environment is '${env_name:-unset}', expected '$FIXED_ADP_ENV'; run: rpk ai env use $FIXED_ADP_ENV"
ok "logged in as $(jq -r '.user_email // "?"' <<<"$status_json") to $env_name (token expires $(jq -r '.expires_at // "?"' <<<"$status_json"))"

# ---------------------------------------------------------------------------
info "Verifying Redpanda Data Plane API (GET /v1/topics?page_size=1)"
if ! out="$(rp_api GET '/v1/topics?page_size=1')"; then
  die "Data Plane API call failed: $out"
fi
jq -e '.topics' >/dev/null <<<"$out" || die "unexpected Data Plane API response: $out"
ok "Data Plane API reachable: $REDPANDA_DATAPLANE_API"

if out="$(aigw_rpc GuardrailService/ListGuardrails '{}')" && jq -e '.guardrails' >/dev/null <<<"$out"; then
  ok "AI Gateway reachable: $AIGW_URL"
else
  warn "AI Gateway check failed (GuardrailService/ListGuardrails); scripts/50-deploy-agent.sh needs it"
fi

info "Preflight complete. Next: make setup-db (or make up for everything)."
