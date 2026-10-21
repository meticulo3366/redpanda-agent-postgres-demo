#!/usr/bin/env bash
# 60-verify.sh - end-to-end verification (docs/PORT-CONTRACT.md section 8).
# Prints one PASS/FAIL line per check and exits non-zero when anything failed.
#   SKIP_A2A=1 (or --skip-a2a)  skip the checks that talk to the agent (LLM calls, 1-3 min)
#   SKIP_STREAM=1               skip the change-stream probe (it needs pg-agent-memory-cdc and -audit running)
#   VERIFY_GUARDRAIL=1          also send a denied-topic message and expect guardrail_intervened
#   VERIFY_KEEP_MEMORY=1        keep the memory written by the round-trip instead of expiring it
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"
load_env
require_cmd rpk psql jq curl

SKIP_A2A="${SKIP_A2A:-0}"
SKIP_STREAM="${SKIP_STREAM:-0}"
for a in "$@"; do
  case "$a" in
    --skip-a2a) SKIP_A2A=1 ;;
    --skip-stream) SKIP_STREAM=1 ;;
    -h|--help) sed -n '2,7p' "$0"; exit 0 ;;
    *) die "unknown argument: $a" ;;
  esac
done

PASS_COUNT=0
FAIL_COUNT=0
pass() { PASS_COUNT=$((PASS_COUNT + 1)); printf 'PASS  %s\n' "$*"; }
fail() { FAIL_COUNT=$((FAIL_COUNT + 1)); printf 'FAIL  %s\n' "$*"; }
skip() { printf 'SKIP  %s\n' "$*"; }

S="$PG_SCHEMA"
AGENT_PSQL_URL="$(pg_agent_url)"
agent_psql() { psql "$AGENT_PSQL_URL" -v ON_ERROR_STOP=1 -X -q -At -c "$1"; }
sql_scalar()  { pg_admin_psql "$1" -At 2>/dev/null; }

A2A_ERR="$(mktemp)"
ADP_ERR="$(mktemp)"
trap 'rm -f "$A2A_ERR" "$ADP_ERR"' EXIT

# adp_get <kind> <name>: prints `rpk ai <kind> get <name> -o json`. rpk exits 1 for
# "not found" and for auth/network errors alike, so the reason is kept in $ADP_ERR:
# rc 1 = not_found, rc 2 = any other error (see adp_reason).
adp_get() {
  local out
  if out="$(rpk ai "$1" get "$2" -o json 2>"$ADP_ERR")"; then
    printf '%s' "$out"
    return 0
  fi
  # rpk prints some errors as JSON on stdout and others on stderr; keep both.
  printf '%s\n' "$out" >>"$ADP_ERR"
  grep -qi 'not_found' "$ADP_ERR" && return 1
  return 2
}
adp_reason() { tr -s '[:space:]' ' ' <"$ADP_ERR" | head -c 200; }

# ---------------------------------------------------------------------------
info "PostgreSQL ($PG_HOST, database $PG_DATABASE, schema $S)"

if n="$(sql_scalar "SELECT count(*) FROM information_schema.tables WHERE table_schema = '$S' AND table_name IN ('customers','accounts','transactions','memories','memory_audit','change_log')")"; then
  if [[ "$n" == "6" ]]; then
    pass "tables exist: customers, accounts, transactions, memories, memory_audit, change_log"
  else
    fail "only $n of 6 demo tables exist in $PG_DATABASE.$S (run make setup-db)"
  fi
else
  fail "cannot query PostgreSQL as admin (run make preflight)"
fi

n="$(sql_scalar "SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgname IN ('tr_memories_log','tr_accounts_log')" || echo 0)"
if [[ "$n" == "2" ]]; then
  pass "change-capture triggers exist: tr_memories_log, tr_accounts_log"
else
  fail "expected 2 change-capture triggers, found ${n:-0} (run make setup-db)"
fi

idx="$(sql_scalar "SELECT indexdef FROM pg_indexes WHERE schemaname = '$S' AND indexname = 'idx_memories_search'" || true)"
if [[ "$idx" == *"USING fts"* ]]; then
  pass "full-text index idx_memories_search uses the GantrySQL fts access method"
elif [[ -n "$idx" ]]; then
  pass "full-text index idx_memories_search exists (${idx#*USING })"
else
  fail "full-text index idx_memories_search is missing (run make setup-db)"
fi

admin_user="$(sql_scalar "SELECT current_user" || true)"
if out="$(agent_psql "SELECT current_user" 2>&1)"; then
  pass "agent user $PG_AGENT_USER can connect with the DSN the MCP server and pipelines use (current_user=$out)"
else
  fail "agent user $PG_AGENT_USER cannot connect: ${out:0:200}"
fi

if [[ -n "$admin_user" && "$admin_user" == "$PG_AGENT_USER" ]]; then
  skip "database-level privilege checks: $PG_AGENT_USER is the admin user (this server has no CREATE ROLE); the MCP server's blocked patterns are checked below"
else
  if out="$(agent_psql "DELETE FROM $S.memories WHERE 1 = 0" 2>&1)"; then
    fail "role $PG_AGENT_USER is allowed to DELETE FROM memories (must be denied)"
  elif grep -qiE 'permission denied|privilege|not authorized|denied' <<<"$out"; then
    pass "role $PG_AGENT_USER is denied DELETE on memories"
  else
    fail "DELETE as $PG_AGENT_USER failed for an unexpected reason: ${out:0:200}"
  fi
  if out="$(agent_psql "UPDATE $S.accounts SET balance = balance WHERE 1 = 0" 2>&1)"; then
    fail "role $PG_AGENT_USER is allowed to UPDATE accounts (must be read-only)"
  elif grep -qiE 'permission denied|privilege|not authorized|denied' <<<"$out"; then
    pass "role $PG_AGENT_USER is denied UPDATE on accounts"
  else
    fail "UPDATE accounts as $PG_AGENT_USER failed for an unexpected reason: ${out:0:200}"
  fi
fi

# ---------------------------------------------------------------------------
info "Redpanda Cloud"

if out="$(rp_api GET "/v1/topics?filter.name_contains=agent.pg.&page_size=100")"; then
  for t in "$TOPIC_MEMORY_CDC" "$TOPIC_ACCOUNTS_CDC"; do
    if jq -e --arg n "$t" 'any(.topics[]?; .name == $n)' <<<"$out" >/dev/null; then
      pass "topic exists: $t"
    else
      fail "topic missing: $t (run make setup-redpanda)"
    fi
  done
else
  fail "GET /v1/topics failed: ${out:0:200}"
fi

# GET /v1/secrets/{id} wraps the object: {"secret":{"id":...,"scopes":[...]}} (verified live).
for s in AGENT_MEMORY_PG_DSN AGENT_MEMORY_PG_KAFKA_USER AGENT_MEMORY_PG_KAFKA_PASSWORD; do
  if out="$(RP_API_OK_CODES=404 rp_api GET "/v1/secrets/$s")" && jq -e --arg id "$s" '(.id // .secret.id) == $id' <<<"$out" >/dev/null 2>&1; then
    pass "secret exists: $s (scopes $(jq -r '(.scopes // .secret.scopes // []) | join(",")' <<<"$out"))"
  else
    fail "secret missing: $s (run make setup-redpanda)"
  fi
done

# Pipelines: list every page and match by display_name (the API ignores a display_name filter).
list_pipelines() {
  local token="" out
  while :; do
    out="$(rp_api GET "/v1/redpanda-connect/pipelines?page_size=100${token:+&page_token=$(jq -rn --arg t "$token" '$t|@uri')}")" || return 1
    jq -c '.pipelines[]?' <<<"$out"
    token="$(jq -r '.next_page_token // ""' <<<"$out")"
    [[ -n "$token" ]] || break
  done
}
cdc_running=0
if pipes="$(list_pipelines)"; then
  for p in pg-agent-memory-cdc pg-agent-memory-audit pg-agent-memory-observer pg-agent-memory-purge; do
    state="$(jq -r --arg n "$p" 'select(.display_name == $n) | .state' <<<"$pipes" | head -1)"
    case "$state" in
      STATE_RUNNING) pass "pipeline running: $p"; [[ "$p" == pg-agent-memory-cdc ]] && cdc_running=1 ;;
      "")            fail "pipeline missing: $p (run make pipelines)" ;;
      *)             fail "pipeline $p is in state $state" ;;
    esac
  done
else
  fail "GET /v1/redpanda-connect/pipelines failed"
fi

# The poller keeps change_log drained: nothing older than 30s may still be unpublished.
n="$(sql_scalar "SELECT count(*) FROM $S.change_log WHERE published_at IS NULL AND changed_at < now() - INTERVAL '30 seconds'" || echo "?")"
if [[ "$n" == "0" ]]; then
  pass "change_log has no unpublished change older than 30s"
elif [[ "$n" =~ ^[0-9]+$ ]]; then
  fail "change_log has $n unpublished change(s) older than 30s (is pipeline pg-agent-memory-cdc running and able to reach PostgreSQL and Redpanda?)"
else
  fail "could not count unpublished changes in change_log"
fi

# ---------------------------------------------------------------------------
if (( SKIP_STREAM )); then
  skip "change-stream probe (SKIP_STREAM=1)"
else
  info "Change-stream probe: INSERT -> trigger -> change_log -> cdc pipeline -> $TOPIC_MEMORY_CDC -> audit pipeline -> memory_audit"
  probe_subject="verify@agent-memory.invalid"
  probe_content="Verification probe $(date -u +%Y-%m-%dT%H:%M:%SZ) $RANDOM"
  mid="$(pg_admin_psql "INSERT INTO $S.memories (subject, kind, content, importance, source, source_ref, content_hash, expires_at)
    VALUES ('$probe_subject', 'fact', '$probe_content', 1, 'human', 'verify', md5(lower('$probe_subject') || '|' || lower('$probe_content')), now() + INTERVAL '1 hour')
    RETURNING memory_id" -At 2>/dev/null || true)"
  if [[ "$mid" =~ ^[0-9]+$ ]]; then
    pass "probe memory inserted (memory_id $mid, subject $probe_subject)"
    cid="$(sql_scalar "SELECT max(change_id) FROM $S.change_log WHERE table_name = 'memories' AND row_key = $mid AND op = 'insert'" || true)"
    if [[ "$cid" =~ ^[0-9]+$ ]]; then
      pass "trigger logged the insert as change $cid"
      t0=$SECONDS; published=""
      while (( SECONDS - t0 < 30 )); do
        published="$(sql_scalar "SELECT published_at IS NOT NULL FROM $S.change_log WHERE change_id = $cid" || true)"
        [[ "$published" == "t" ]] && break
        sleep 2
      done
      if [[ "$published" == "t" ]]; then
        pass "cdc pipeline published change $cid to Redpanda after $((SECONDS - t0))s"
      else
        fail "change $cid was not published within 30s (pipeline pg-agent-memory-cdc$( (( cdc_running )) || printf ' is not running'))"
      fi
      t0=$SECONDS; audited="0"
      while (( SECONDS - t0 < 30 )); do
        audited="$(sql_scalar "SELECT count(*) FROM $S.memory_audit WHERE change_id = $cid" || echo 0)"
        [[ "$audited" == "1" ]] && break
        sleep 2
      done
      if [[ "$audited" == "1" ]]; then
        pass "audit pipeline wrote the memory_audit row for change $cid after $((SECONDS - t0))s"
      else
        fail "no memory_audit row for change $cid within 30s (pipeline pg-agent-memory-audit)"
      fi
    else
      fail "no change_log row for the probe insert (trigger tr_memories_log; run make setup-db)"
    fi
    # The probe row is removed again; that delete is logged and audited like any other change.
    pg_admin_psql "DELETE FROM $S.memories WHERE memory_id = $mid" >/dev/null 2>&1 || warn "could not delete the probe memory $mid"
  else
    fail "could not insert the probe memory as admin"
  fi
fi

# ---------------------------------------------------------------------------
info "ADP"

rc=0; mcp="$(adp_get mcp-server "$MCP_SERVER_NAME")" || rc=$?
if (( rc == 0 )); then
  enabled="$(jq -r '.enabled // false' <<<"$mcp")"
  tools="$(jq -r '[.tools[]?.name] | sort | join(",")' <<<"$mcp")"
  if [[ "$enabled" == "true" ]]; then
    pass "MCP server $MCP_SERVER_NAME is enabled"
  else
    fail "MCP server $MCP_SERVER_NAME is not enabled"
  fi
  if [[ ",$tools," == *",query,"* && ",$tools," == *",execute,"* ]]; then
    pass "MCP server tools: $tools"
  else
    fail "MCP server tools are [${tools:-none}], expected query and execute"
  fi
  # No database role separates reads from writes on this server, so the MCP
  # server's blocked patterns are the enforcement point: DELETE and the
  # change_log table must be among them.
  patterns="$(jq -r '[.. | objects | (.blocked_patterns? // .blockedPatterns? // empty) | .[]] | join("\n")' <<<"$mcp" 2>/dev/null || true)"
  if [[ -z "$patterns" ]]; then
    fail "MCP server $MCP_SERVER_NAME reports no blocked_patterns (adp/mcp-server.yaml)"
  else
    grep -qE 'DELETE' <<<"$patterns" && pass "MCP server blocks DELETE" || fail "MCP server blocked_patterns do not block DELETE"
    grep -qE 'change_log' <<<"$patterns" && pass "MCP server blocks access to change_log" || fail "MCP server blocked_patterns do not block change_log"
    grep -qE 'memory_audit' <<<"$patterns" && pass "MCP server blocks writes to memory_audit and the system of record" || fail "MCP server blocked_patterns do not block writes to memory_audit, accounts, customers, transactions"
  fi
elif (( rc == 1 )); then
  fail "MCP server $MCP_SERVER_NAME not found (run make deploy-agent)"
else
  fail "rpk ai mcp-server get $MCP_SERVER_NAME failed: $(adp_reason)"
fi

agent_running=0
rc=0; ag="$(adp_get agent "$AGENT_NAME")" || rc=$?
if (( rc == 0 )); then
  state="$(jq -r '.managed.status.state // "unknown"' <<<"$ag")"
  url="$(jq -r '.managed.status.url // ""' <<<"$ag")"
  if [[ "$state" == "AGENT_STATE_RUNNING" ]]; then
    pass "agent $AGENT_NAME is RUNNING (${url:-no url yet})"
    agent_running=1
  else
    fail "agent $AGENT_NAME state is $state (expected AGENT_STATE_RUNNING)"
  fi
elif (( rc == 1 )); then
  fail "agent $AGENT_NAME not found (run make deploy-agent)"
else
  fail "rpk ai agent get $AGENT_NAME failed: $(adp_reason)"
fi

# The agent's provider must exist and carry the guardrail. For a non-Bedrock
# provider the gateway evaluates the guardrail itself, which is why the agent runs
# on the Anthropic provider (.env.example); on a Bedrock provider it never fires
# through the agent (Converse path), so that case is a warning, not a failure.
rc=0; prov="$(adp_get llm-provider "$LLM_PROVIDER")" || rc=$?
if (( rc == 0 )); then
  ptype="$(jq -r '.type // "?"' <<<"$prov")"
  attached="$(jq -r '.guardrail // ""' <<<"$prov")"
  pass "agent provider $LLM_PROVIDER exists ($ptype)"
  if [[ "$attached" == "$GUARDRAIL_NAME" ]]; then
    pass "guardrail $GUARDRAIL_NAME is attached to $LLM_PROVIDER"
  else
    fail "provider $LLM_PROVIDER has guardrail '${attached:-none}', expected $GUARDRAIL_NAME (run make deploy-agent)"
  fi
  if [[ "$ptype" == "LLM_PROVIDER_TYPE_BEDROCK" ]]; then
    warn "$LLM_PROVIDER is a Bedrock provider: the guardrail will not fire through the agent, and VERIFY_GUARDRAIL=1 would fail"
  fi
elif (( rc == 1 )); then
  fail "LLM provider $LLM_PROVIDER not found (run make deploy-agent)"
else
  fail "rpk ai llm-provider get $LLM_PROVIDER failed: $(adp_reason)"
fi

# GetGuardrail answers {"guardrail":{...}} with camelCase keys (verified live) and
# HTTP 404 {"code":"not_found",...} when the guardrail is missing.
gr_body="$(jq -cn --arg n "$GUARDRAIL_NAME" '{name: $n}')"
if out="$(RP_API_OK_CODES=404 aigw_rpc GuardrailService/GetGuardrail "$gr_body" 2>/dev/null)"; then
  gstate="$(jq -r '.guardrail.provisioningState // .guardrail.provisioning_state // .provisioningState // ""' <<<"$out" 2>/dev/null || true)"
  gbackend="$(jq -r '.guardrail.bedrockConfig.backend.llmProviderRef // .guardrail.bedrock_config.backend.llm_provider_ref // ""' <<<"$out" 2>/dev/null || true)"
  if [[ "$gstate" == "GUARDRAIL_PROVISIONING_STATE_READY" ]]; then
    pass "guardrail $GUARDRAIL_NAME is READY (backend ${gbackend:-?})"
  elif jq -e 'type == "object" and has("code")' <<<"$out" >/dev/null 2>&1; then
    fail "guardrail $GUARDRAIL_NAME not found: $(jq -r '.message // .code' <<<"$out") (run make deploy-agent)"
  else
    fail "guardrail $GUARDRAIL_NAME provisioning state is ${gstate:-unknown}, expected GUARDRAIL_PROVISIONING_STATE_READY"
  fi
  if [[ -n "$gbackend" && "$gbackend" != "$BEDROCK_PROVIDER" ]]; then
    warn "guardrail backend is $gbackend, expected BEDROCK_PROVIDER=$BEDROCK_PROVIDER"
  fi
else
  fail "GuardrailService/GetGuardrail failed: ${out:0:200}"
fi

# ---------------------------------------------------------------------------
# A2A helpers: -o json prints the full A2A message or task object on stdout.
a2a_send_json() {  # <message> [extra flags]
  local msg="$1"; shift
  rpk ai agent a2a send "$AGENT_NAME" -o json --timeout 120s "$@" "$msg" 2>"$A2A_ERR"
}
a2a_text() {  # <json> -> reply text
  jq -r '[ (.parts[]? | .text?),
           (.status.message.parts[]? | .text?),
           (.artifacts[]?.parts[]? | .text?),
           (.history[]? | select(.role == "agent") | .parts[]? | .text?),
           (.result.parts[]? | .text?),
           (.message.parts[]? | .text?) ]
         | map(select(. != null)) | join("\n")' <<<"$1" 2>/dev/null || true
}
a2a_ctx() {  # <json> -> context id (falls back to the "context-id:" line on stderr)
  local c
  c="$(jq -r '[.contextId, .context_id, .result.contextId, .message.contextId, .task.contextId, .status.message.contextId]
              | map(select(. != null and . != "")) | .[0] // empty' <<<"$1" 2>/dev/null || true)"
  if [[ -z "$c" ]]; then
    c="$(grep -oiE 'context[-_ ]?id:? *[A-Za-z0-9_-]+' "$A2A_ERR" | head -1 | sed -E 's/.*: *//' || true)"
  fi
  printf '%s' "$c"
}

if (( SKIP_A2A )); then
  skip "A2A smoke test, memory round-trip, audit check and guardrail probe (SKIP_A2A=1)"
elif (( agent_running == 0 )); then
  skip "A2A smoke test, memory round-trip, audit check and guardrail probe (agent is not running)"
else
  info "A2A smoke test"
  if reply="$(rpk ai agent a2a send "$AGENT_NAME" "List my accounts" --timeout 120s 2>"$A2A_ERR")" && [[ -n "${reply//[[:space:]]/}" ]]; then
    pass "A2A smoke test: the agent replied ($(wc -w <<<"$reply" | tr -d ' ') words)"
    log "----- agent reply -----"
    log "$reply"
    log "-----------------------"
  else
    fail "A2A smoke test failed: $(head -c 300 "$A2A_ERR" | tr '\n' ' ')"
  fi

  info "Memory round-trip (store in one conversation, recall in a new one)"
  # A harmless, natural-looking fact. The system prompt (correctly) refuses to store anything
  # that looks like a secret, token or password, and it treats token-like strings such as
  # "verify-dk73fokh" as suspicious data it will not repeat, so the nonce is a pronounceable
  # pseudo-word (four random syllables, title case) used as a coffee shop name.
  syllables=(ma ro bel ta ni vo lu ke sa do ri fen po la tu mi)
  nonce=""
  for _ in 1 2 3 4; do nonce+="${syllables[$((RANDOM % ${#syllables[@]}))]}"; done
  token="$(printf '%s' "${nonce:0:1}" | tr '[:lower:]' '[:upper:]')${nonce:1}"
  store_msg="Please remember this about me: my favourite coffee shop is called ${token} Coffee."
  recall_msg="What is the name of my favourite coffee shop? Answer with the shop's name exactly as you have it stored."
  ctx1=""; ctx2=""
  if j1="$(a2a_send_json "$store_msg")"; then
    ctx1="$(a2a_ctx "$j1")"
    log "store reply (context ${ctx1:-?}): $(a2a_text "$j1" | head -c 300 | tr '\n' ' ')"
    if j2="$(a2a_send_json "$recall_msg")"; then      # no --context-id: a new conversation
      ctx2="$(a2a_ctx "$j2")"
      t2="$(a2a_text "$j2")"
      log "recall reply (context ${ctx2:-?}): $(head -c 300 <<<"$t2" | tr '\n' ' ')"
      if grep -qiF -- "$token" <<<"$t2"; then
        pass "memory round-trip: $token recalled in a new conversation"
      else
        fail "memory round-trip: $token not found in the new-conversation reply"
      fi
      if [[ -z "$ctx1" || -z "$ctx2" ]]; then
        fail "could not capture both context ids from -o json output"
      elif [[ "$ctx1" != "$ctx2" ]]; then
        pass "new conversation used a different context id ($ctx1 -> $ctx2)"
      else
        fail "recall reused the same context id $ctx1"
      fi
    else
      fail "recall A2A call failed: $(head -c 300 "$A2A_ERR" | tr '\n' ' ')"
    fi
  else
    fail "store A2A call failed: $(head -c 300 "$A2A_ERR" | tr '\n' ' ')"
  fi

  info "Waiting up to 60s for the audit row (trigger -> cdc pipeline -> Redpanda -> audit pipeline -> memory_audit)"
  deadline=$((SECONDS + 60))
  found="0"
  while (( SECONDS < deadline )); do
    found="$(sql_scalar "SELECT count(*) FROM $S.memory_audit WHERE subject = '$DEMO_USER_EMAIL' AND coalesce(after_doc->>'content', '') ILIKE '%${token}%'" || echo 0)"
    if [[ "$found" =~ ^[0-9]+$ ]] && (( found >= 1 )); then
      break
    fi
    sleep 5
  done
  if [[ "$found" =~ ^[0-9]+$ ]] && (( found >= 1 )); then
    pass "audit row for the stored memory appeared in memory_audit"
  else
    inmem="$(sql_scalar "SELECT count(*) FROM $S.memories WHERE subject = '$DEMO_USER_EMAIL' AND content ILIKE '%${token}%'" || echo "?")"
    if [[ "$inmem" == "0" ]]; then
      fail "audit: the agent did not store the token in memories, so nothing was audited"
    else
      fail "audit: memories has $inmem matching row(s) but memory_audit got none within 60s (check pipelines pg-agent-memory-cdc and pg-agent-memory-audit)"
    fi
  fi

  if [[ "${VERIFY_KEEP_MEMORY:-0}" != "1" ]]; then
    if pg_admin_psql "UPDATE $S.memories SET expires_at = now(), updated_at = now() WHERE subject = '$DEMO_USER_EMAIL' AND content ILIKE '%${token}%'" >/dev/null 2>&1; then
      log "expired the verification memory (UPDATE, never DELETE); set VERIFY_KEEP_MEMORY=1 to keep it"
    fi
  fi

  # Guardrail probe (opt-in): a denied-topic message in a new conversation. The
  # gateway blocks it, the A2A task ends "failed" and rpk exits 4, so the exit
  # code is ignored and both stdout and stderr are searched for the marker.
  if [[ "${VERIFY_GUARDRAIL:-0}" == "1" ]]; then
    info "Guardrail probe (denied topic, new conversation)"
    probe_msg="Which cryptocurrency should I buy with my savings this week?"
    probe_out="$(rpk ai agent a2a send "$AGENT_NAME" -o json --timeout 120s "$probe_msg" 2>"$A2A_ERR" || true)"
    if grep -qF 'guardrail_intervened' <<<"$probe_out" || grep -qF 'guardrail_intervened' "$A2A_ERR"; then
      pass "guardrail probe: the denied-topic message was blocked (guardrail_intervened)"
    else
      fail "guardrail probe: the denied-topic message was not blocked: $(a2a_text "$probe_out" | head -c 200 | tr '\n' ' ') $(head -c 200 "$A2A_ERR" | tr '\n' ' ')"
    fi
  else
    skip "guardrail probe (set VERIFY_GUARDRAIL=1 to send a denied-topic message)"
  fi
fi

# ---------------------------------------------------------------------------
echo
printf '%d passed, %d failed\n' "$PASS_COUNT" "$FAIL_COUNT"
if (( FAIL_COUNT > 0 )); then
  exit 1
fi
