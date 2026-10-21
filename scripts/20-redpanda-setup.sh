#!/usr/bin/env bash
# 20-redpanda-setup.sh - Redpanda Cloud side: this demo's SASL user, ACLs, topics
# and the three secrets, all through the Data Plane API (docs/PORT-CONTRACT.md
# section 2). Everything is named apart from the CockroachDB demo that shares the
# cluster (user agent-memory-pg-demo, topics agent.pg.*, secrets AGENT_MEMORY_PG_*).
#
# Idempotent: every resource is looked up first (GET) and only created when
# missing; the user password and the secret values are re-asserted from .env.
# DRY_RUN=1 performs only the GETs and prints what would change.
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"
load_env
require_cmd rpk curl jq base64

DRY_RUN="${DRY_RUN:-0}"
# SCOPE_AI_GATEWAY is required: the AI Gateway can only resolve ${secrets.NAME} in a
# managed MCP server config for secrets that carry that scope (verified live: without it
# the SQL MCP server reports "discovery failed: internal error" and every initialize fails).
SECRET_SCOPES='["SCOPE_MCP_SERVER","SCOPE_REDPANDA_CONNECT","SCOPE_AI_GATEWAY"]'

b64()    { printf '%s' "$1" | base64 | tr -d '\n'; }
urlenc() { jq -rn --arg v "$1" '$v|@uri'; }
would()  { log "DRY_RUN: would $*"; }

# True when a response body is a Data Plane API error object ({code, message, ...}).
is_error_body() { jq -e 'type == "object" and has("code") and has("message")' <<<"$1" >/dev/null 2>&1; }

# Short, credential-free summary of a response body for log messages.
summarize() {
  local s
  s="$(jq -r 'if type == "object" then ((.code // "") + " " + (.message // "")) else tostring end' <<<"$1" 2>/dev/null || printf '%s' "$1")"
  s="${s//"$KAFKA_PASSWORD"/***}"
  s="${s//"$PG_AGENT_PASSWORD"/***}"
  printf '%s' "${s:0:400}"
}

# ---------------------------------------------------------------------------
info "SASL user $KAFKA_USER (SCRAM-SHA-256)"
out="$(rp_api GET "/v1/users?filter.name=$(urlenc "$KAFKA_USER")")" || die "GET /v1/users failed: $(summarize "$out")"
user_exists=0
if jq -e --arg n "$KAFKA_USER" 'any(.users[]?; .name == $n)' <<<"$out" >/dev/null; then
  user_exists=1
fi

create_body="$(jq -n --arg n "$KAFKA_USER" --arg p "$KAFKA_PASSWORD" \
  '{name: $n, password: $p, mechanism: "SASL_MECHANISM_SCRAM_SHA_256"}')"
update_body="$(jq -n --arg p "$KAFKA_PASSWORD" \
  '{mechanism: "SASL_MECHANISM_SCRAM_SHA_256", password: $p}')"

if (( user_exists == 0 )); then
  if (( DRY_RUN )); then
    would "POST /v1/users (create $KAFKA_USER)"
  else
    out="$(RP_API_OK_CODES="400 409" rp_api POST /v1/users "$create_body")" || die "POST /v1/users failed: $(summarize "$out")"
    if is_error_body "$out"; then
      if grep -qiE 'already exist|ALREADY_EXISTS' <<<"$out"; then
        user_exists=1
      else
        die "POST /v1/users rejected: $(summarize "$out")"
      fi
    else
      ok "created user $KAFKA_USER"
    fi
  fi
fi

if (( user_exists )); then
  if (( DRY_RUN )); then
    would "PUT /v1/users/$KAFKA_USER (reset password to the value in .env)"
  elif out="$(rp_api PUT "/v1/users/$KAFKA_USER" "$update_body")"; then
    ok "user $KAFKA_USER exists; password reset from .env"
  else
    warn "PUT /v1/users/$KAFKA_USER failed ($(summarize "$out")); falling back to delete + create"
    out="$(RP_API_OK_CODES=404 rp_api DELETE "/v1/users/$KAFKA_USER")" || die "DELETE /v1/users/$KAFKA_USER failed: $(summarize "$out")"
    out="$(rp_api POST /v1/users "$create_body")" || die "POST /v1/users (recreate) failed: $(summarize "$out")"
    ok "recreated user $KAFKA_USER"
  fi
fi

# ---------------------------------------------------------------------------
info "ACLs for User:$KAFKA_USER"

# ensure_acl <resource_type> <resource_name> <pattern_type> <operation>
ensure_acl() {
  local rtype="$1" rname="$2" rpat="$3" op="$4"
  local principal="User:$KAFKA_USER"
  local label="$rtype $rname ($rpat) $op"
  local q out body
  q="filter.resource_type=$rtype&filter.resource_name=$(urlenc "$rname")&filter.resource_pattern_type=$rpat"
  q="$q&filter.principal=$(urlenc "$principal")&filter.host=$(urlenc '*')&filter.operation=$op&filter.permission_type=PERMISSION_TYPE_ALLOW"

  out="$(rp_api GET "/v1/acls?$q")" || die "GET /v1/acls failed: $(summarize "$out")"
  if jq -e '(.resources // []) | length > 0' <<<"$out" >/dev/null; then
    ok "ACL exists: $label"
    return 0
  fi
  if (( DRY_RUN )); then
    would "POST /v1/acls ($label)"
    return 0
  fi
  body="$(jq -n --arg t "$rtype" --arg n "$rname" --arg p "$rpat" --arg pr "$principal" --arg op "$op" \
    '{resource_type: $t, resource_name: $n, resource_pattern_type: $p, principal: $pr, host: "*", operation: $op, permission_type: "PERMISSION_TYPE_ALLOW"}')"
  out="$(RP_API_OK_CODES="400 409" rp_api POST /v1/acls "$body")" || die "POST /v1/acls failed ($label): $(summarize "$out")"
  if is_error_body "$out"; then
    if grep -qiE 'already exist|duplicate|ALREADY_EXISTS' <<<"$out"; then
      ok "ACL already present: $label"
    else
      die "POST /v1/acls rejected ($label): $(summarize "$out")"
    fi
  else
    ok "created ACL: $label"
  fi
}

ensure_acl RESOURCE_TYPE_TOPIC   "agent.pg."        RESOURCE_PATTERN_TYPE_PREFIXED OPERATION_ALL
ensure_acl RESOURCE_TYPE_GROUP   "agent-memory-pg"  RESOURCE_PATTERN_TYPE_PREFIXED OPERATION_ALL
ensure_acl RESOURCE_TYPE_CLUSTER "kafka-cluster" RESOURCE_PATTERN_TYPE_LITERAL  OPERATION_DESCRIBE

# ---------------------------------------------------------------------------
info "Topics"

# ensure_topic <name>: 3 partitions, RF 3, 7 day retention
ensure_topic() {
  local name="$1" out body
  out="$(rp_api GET "/v1/topics?filter.name_contains=$(urlenc "$name")&page_size=100")" || die "GET /v1/topics failed: $(summarize "$out")"
  if jq -e --arg n "$name" 'any(.topics[]?; .name == $n)' <<<"$out" >/dev/null; then
    ok "topic exists: $name"
    return 0
  fi
  if (( DRY_RUN )); then
    would "POST /v1/topics (create $name: 3 partitions, replication 3, retention.ms 604800000)"
    return 0
  fi
  body="$(jq -n --arg n "$name" \
    '{name: $n, partition_count: 3, replication_factor: 3, configs: [{name: "retention.ms", value: "604800000"}]}')"
  out="$(RP_API_OK_CODES="400 409" rp_api POST /v1/topics "$body")" || die "POST /v1/topics failed ($name): $(summarize "$out")"
  if is_error_body "$out"; then
    if grep -qiE 'already exist|ALREADY_EXISTS' <<<"$out"; then
      ok "topic already present: $name"
    else
      die "POST /v1/topics rejected ($name): $(summarize "$out")"
    fi
  else
    ok "created topic: $name"
  fi
}

ensure_topic "$TOPIC_MEMORY_CDC"
ensure_topic "$TOPIC_ACCOUNTS_CDC"

# ---------------------------------------------------------------------------
info "Secrets (scopes SCOPE_MCP_SERVER, SCOPE_REDPANDA_CONNECT, SCOPE_AI_GATEWAY)"

# ensure_secret <id> <plaintext value>: POST, or PUT when it already exists.
ensure_secret() {
  local id="$1" value="$2"
  local data out body exists=0
  data="$(b64 "$value")"

  out="$(RP_API_OK_CODES=404 rp_api GET "/v1/secrets/$id")" || die "GET /v1/secrets/$id failed: $(summarize "$out")"
  # 200 wraps the object: {"secret":{"id":...,"scopes":[...]}} (verified live); 404 is {code, message}.
  if jq -e --arg id "$id" '(.id // .secret.id) == $id' <<<"$out" >/dev/null 2>&1; then
    exists=1
  fi

  if (( exists == 0 )); then
    if (( DRY_RUN )); then
      would "POST /v1/secrets (create $id)"
      return 0
    fi
    body="$(jq -n --arg id "$id" --arg d "$data" --argjson s "$SECRET_SCOPES" '{id: $id, scopes: $s, secret_data: $d}')"
    out="$(RP_API_OK_CODES=409 rp_api POST /v1/secrets "$body")" || die "POST /v1/secrets failed ($id): $(summarize "$out")"
    if is_error_body "$out"; then
      if grep -qiE 'already exist|ALREADY_EXISTS' <<<"$out"; then
        exists=1
      else
        die "POST /v1/secrets rejected ($id): $(summarize "$out")"
      fi
    else
      ok "created secret $id"
      return 0
    fi
  fi

  if (( DRY_RUN )); then
    would "PUT /v1/secrets/$id (update value)"
    return 0
  fi
  body="$(jq -n --arg d "$data" --argjson s "$SECRET_SCOPES" '{scopes: $s, secret_data: $d}')"
  out="$(rp_api PUT "/v1/secrets/$id" "$body")" || die "PUT /v1/secrets/$id failed: $(summarize "$out")"
  ok "updated secret $id"
}

ensure_secret AGENT_MEMORY_PG_DSN            "$(pg_agent_url)"
ensure_secret AGENT_MEMORY_PG_KAFKA_USER     "$KAFKA_USER"
ensure_secret AGENT_MEMORY_PG_KAFKA_PASSWORD "$KAFKA_PASSWORD"

# ---------------------------------------------------------------------------
info "Redpanda setup complete"
log "  user:    $KAFKA_USER"
log "  ACLs:    topic agent.pg.* (ALL), group agent-memory-pg* (ALL), cluster DESCRIBE"
log "  topics:  $TOPIC_MEMORY_CDC, $TOPIC_ACCOUNTS_CDC"
log "  secrets: AGENT_MEMORY_PG_DSN, AGENT_MEMORY_PG_KAFKA_USER, AGENT_MEMORY_PG_KAFKA_PASSWORD"
log "Next: make pipelines"
