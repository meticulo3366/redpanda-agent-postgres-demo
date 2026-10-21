#!/usr/bin/env bash
# scripts/lib.sh - shared helpers for every script in this repository.
#
# Source it, never execute it:   source "$(dirname "$0")/lib.sh"
#
# Public surface (docs/DESIGN.md section 8; other scripts call these by name):
#   REPO_ROOT ENV_FILE STATE_DIR
#   log info ok warn die
#   load_env require_cmd
#   rp_token rp_api aigw_rpc
#   pg_admin_psql pg_admin_psql_file pg_agent_url
#   rpk_kafka
# Names starting with "_" are private to this file.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$REPO_ROOT/.env"
STATE_DIR="$REPO_ROOT/.state"

# ---------------------------------------------------------------------------
# Logging (all to stderr; coloured only when stderr is a TTY and NO_COLOR unset)
# ---------------------------------------------------------------------------
if [[ -t 2 && -z "${NO_COLOR:-}" ]]; then
  _C_RED=$'\033[31m'; _C_GREEN=$'\033[32m'; _C_YELLOW=$'\033[33m'; _C_BLUE=$'\033[34m'; _C_RESET=$'\033[0m'
else
  _C_RED=''; _C_GREEN=''; _C_YELLOW=''; _C_BLUE=''; _C_RESET=''
fi

log()  { printf '%s\n' "$*" >&2; }
info() { printf '%s==> %s%s\n' "$_C_BLUE" "$*" "$_C_RESET" >&2; }
ok()   { printf '%s  ok%s  %s\n' "$_C_GREEN" "$_C_RESET" "$*" >&2; }
warn() { printf '%sWARN%s  %s\n' "$_C_YELLOW" "$_C_RESET" "$*" >&2; }
die()  { printf '%sERROR%s %s\n' "$_C_RED" "$_C_RESET" "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
# load_env: source .env (exporting every variable so psql/rpk/rpk connect see
# them). Dies with a hint when .env is missing.
load_env() {
  [[ -f "$ENV_FILE" ]] || die ".env not found at $ENV_FILE. Run: make preflight"
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
  # Every required variable documented in .env.example (docs/DESIGN.md
  # section 3); the scripts dereference all of them under set -u.
  local v
  for v in PG_ADMIN_URL PG_HOST PG_DATABASE PG_SCHEMA PG_AGENT_USER PG_AGENT_PASSWORD \
           REDPANDA_BROKERS REDPANDA_DATAPLANE_API REDPANDA_SCHEMA_REGISTRY_URL \
           KAFKA_USER KAFKA_PASSWORD TOPIC_MEMORY_CDC TOPIC_ACCOUNTS_CDC \
           ADP_ENV AIGW_URL LLM_PROVIDER LLM_MODEL GUARDRAIL_NAME MCP_SERVER_NAME \
           AGENT_NAME DEMO_USER_EMAIL; do
    [[ -n "${!v:-}" ]] || die "$v is not set in $ENV_FILE (compare with .env.example)"
  done
  # Optional, with defaults: the Bedrock provider is the guardrail's backend and
  # the alternative provider for own-account billing (.env.example).
  : "${BEDROCK_PROVIDER:=zeke-bedrock-agent-memory}"
  : "${BEDROCK_MODEL:=us.anthropic.claude-sonnet-4-6}"
  export BEDROCK_PROVIDER BEDROCK_MODEL
}

# require_cmd <cmd>...: die if any command is missing from PATH.
require_cmd() {
  local c
  for c in "$@"; do
    command -v "$c" >/dev/null 2>&1 || die "required command not found: $c"
  done
}

# ---------------------------------------------------------------------------
# Redpanda Cloud / ADP HTTP helpers
# ---------------------------------------------------------------------------
# rp_token: print the ADP bearer token (rpk ai auth token).
rp_token() {
  local t
  if ! t="$(rpk ai auth token 2>/dev/null)" || [[ -z "$t" ]]; then
    die "could not obtain an ADP bearer token; run: rpk ai auth login"
  fi
  printf '%s' "$t"
}

# Cache the token for the lifetime of the calling shell (not a subshell).
_rp_token_cached() {
  if [[ -z "${__RP_TOKEN:-}" ]]; then
    __RP_TOKEN="$(rp_token)" || exit 1
  fi
}

# _http <METHOD> <url> [json-body]
# Prints the response body on stdout. Returns 1 on HTTP >= 400 unless that
# status is listed in RP_API_OK_CODES (space or comma separated, e.g. "404 409").
# With VERBOSE=1 the method, URL and status are logged to stderr.
_http() {
  local method="$1" url="$2" body="${3:-}"
  local tmp code c
  _rp_token_cached
  tmp="$(mktemp)"
  local -a args=(-sS -o "$tmp" -w '%{http_code}' -X "$method"
                 -H "Authorization: Bearer $__RP_TOKEN" -H 'Accept: application/json')
  if [[ -n "$body" ]]; then
    args+=(-H 'Content-Type: application/json' --data-binary "$body")
  fi
  if ! code="$(curl "${args[@]}" "$url")"; then
    rm -f "$tmp"
    die "curl failed: $method $url"
  fi
  if [[ "${VERBOSE:-0}" == "1" ]]; then
    log "[$method $url] HTTP $code"
  fi
  cat "$tmp"
  rm -f "$tmp"
  if (( code >= 400 )); then
    # RP_API_OK_CODES is usually unset: default it, because bash >= 4 raises
    # "unbound variable" for ${unset//,/ } under set -u (bash 3.2 tolerates it).
    local codes="${RP_API_OK_CODES:-}"
    # shellcheck disable=SC2086  # intentional word split of "404 409"
    for c in ${codes//,/ }; do
      [[ "$c" == "$code" ]] && return 0
    done
    return 1
  fi
  return 0
}

# rp_api <METHOD> <path> [json-body]: Data Plane API call, e.g.
#   rp_api GET '/v1/topics?page_size=1'
#   out="$(RP_API_OK_CODES=409 rp_api POST /v1/secrets "$body")"
rp_api() {
  local method="$1" path="$2" body="${3:-}"
  _http "$method" "${REDPANDA_DATAPLANE_API:?REDPANDA_DATAPLANE_API unset; call load_env}${path}" "$body"
}

# aigw_rpc <Service/Method> [json-body]: Connect-RPC POST to the AI Gateway, e.g.
#   aigw_rpc GuardrailService/GetGuardrail '{"name":"agent-memory-guardrail"}'
aigw_rpc() {
  local method="$1" body="${2:-}"
  [[ -n "$body" ]] || body='{}'
  _http POST "${AIGW_URL:?AIGW_URL unset; call load_env}/redpanda.api.adp.v1alpha1.${method}" "$body"
}

# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------
# pg_admin_psql "<sql>" [extra psql args]   e.g. pg_admin_psql "SELECT 1" -At
# PG_ADMIN_URL carries sslmode=require and search_path=agent_memory,public, but
# the SQL in this repository schema-qualifies every table anyway.
pg_admin_psql() {
  local sql="$1"; shift
  psql "${PG_ADMIN_URL:?PG_ADMIN_URL unset; call load_env}" -v ON_ERROR_STOP=1 -X -q "$@" -c "$sql"
}

# pg_admin_psql_file <file> [extra psql args]   e.g. pg_admin_psql_file postgres/01-schema.sql
pg_admin_psql_file() {
  local file="$1"; shift
  [[ -f "$file" ]] || die "SQL file not found: $file"
  psql "${PG_ADMIN_URL:?PG_ADMIN_URL unset; call load_env}" -v ON_ERROR_STOP=1 -X -q "$@" -f "$file"
}

# pg_agent_url: the DSN used by the MCP server and the Connect pipelines (the
# value of the AGENT_MEMORY_PG_DSN secret). On a server without CREATE ROLE the
# agent user is the admin user; the SQL that reaches the database is then
# limited by the MCP server's blocked patterns, not by grants. User and password
# are percent-encoded; TLS is required; no search_path (every statement the
# pipelines and the agent run is schema-qualified).
pg_agent_url() {
  local user pw
  user="$(jq -rn --arg v "${PG_AGENT_USER:?}" '$v|@uri')"
  pw="$(jq -rn --arg v "${PG_AGENT_PASSWORD:?}" '$v|@uri')"
  printf 'postgresql://%s:%s@%s/%s?sslmode=require\n' "$user" "$pw" "${PG_HOST:?}" "${PG_DATABASE:?}"
}

# ---------------------------------------------------------------------------
# Kafka protocol via rpk with explicit flags (the cloud profile is not logged in)
# ---------------------------------------------------------------------------
# rpk_kafka <args...>   e.g. rpk_kafka topic consume agent.pg.memory.cdc -o end -f '%k %v\n'
rpk_kafka() {
  rpk -X brokers="${REDPANDA_BROKERS:?}" -X tls.enabled=true -X sasl.mechanism=SCRAM-SHA-256 \
      -X user="${KAFKA_USER:?}" -X pass="${KAFKA_PASSWORD:?}" "$@"
}
