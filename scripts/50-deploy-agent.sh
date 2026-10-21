#!/usr/bin/env bash
# scripts/50-deploy-agent.sh - deploy the Memory Concierge to Redpanda ADP
# (docs/PORT-CONTRACT.md section 7), in this order:
#
#   1. LLM providers (rpk ai llm-provider, create if missing): BEDROCK_PROVIDER,
#                     the guardrail's backend and own-account billing alternative,
#                     and LLM_PROVIDER, the Anthropic provider the agent runs on
#   2. Guardrail     (aigw Connect-RPC GuardrailService: create if missing; when it
#                     exists but differs from adp/guardrail.json, UpdateGuardrail,
#                     falling back to detach + DeleteGuardrail + CreateGuardrail;
#                     wait READY)
#   3. Attach the guardrail to both providers (rpk ai llm-provider update --guardrail)
#   4. Check the AGENT_MEMORY_PG_DSN secret exists (created by 20-redpanda-setup.sh)
#   5. MCP server    (rpk ai mcp-server create / update -f adp/mcp-server.yaml)
#   6. Cedar policies (rpk ai policy create, or update when the .cedar file changed;
#                     warns when .env names a provider or MCP server the .cedar
#                     files do not permit)
#   7. Agent: render adp/agent.yaml from adp/agent.template.yaml (__LLM_PROVIDER__
#      and __LLM_MODEL__ from .env) + adp/system-prompt.md, rpk ai agent apply,
#      wait for AGENT_STATE_RUNNING, print the A2A URL and a provider note.
#
# Idempotent: every resource is looked up before it is created, and every
# resource that has a local definition (guardrail, MCP server, policies, agent)
# is reconciled with it on re-run, so edits to adp/* are applied, never ignored.
#
# Env knobs: DRY_RUN=1 prints the mutating commands instead of running them;
# AGENT_WAIT_SECS (default 180), GUARDRAIL_WAIT_SECS (default 180);
# ANTHROPIC_API_KEY_REF (default ANTHROPIC_API_KEY) when LLM_PROVIDER must be created.
set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"
load_env
require_cmd rpk jq curl awk

ADP_DIR="$REPO_ROOT/adp"
TEMPLATE="$ADP_DIR/agent.template.yaml"
PROMPT="$ADP_DIR/system-prompt.md"
AGENT_MANIFEST="$ADP_DIR/agent.yaml"
MCP_MANIFEST="$ADP_DIR/mcp-server.yaml"
GUARDRAIL_FILE="$ADP_DIR/guardrail.json"
POLICY_DIR="$ADP_DIR/policies"

LLM_PROVIDER="${LLM_PROVIDER:-zeke-anthropic-agent-memory}"
LLM_MODEL="${LLM_MODEL:-claude-sonnet-4-6}"
BEDROCK_PROVIDER="${BEDROCK_PROVIDER:-zeke-bedrock-agent-memory}"
BEDROCK_MODEL="${BEDROCK_MODEL:-us.anthropic.claude-sonnet-4-6}"
# Secret store reference for the Anthropic API key (org-shared, scope SCOPE_AI_GATEWAY).
ANTHROPIC_API_KEY_REF="${ANTHROPIC_API_KEY_REF:-ANTHROPIC_API_KEY}"
GUARDRAIL_NAME="${GUARDRAIL_NAME:-agent-memory-guardrail}"
MCP_SERVER_NAME="${MCP_SERVER_NAME:-pg-agent-memory}"
AGENT_NAME="${AGENT_NAME:-memory-concierge-pg}"
DSN_SECRET_ID="AGENT_MEMORY_PG_DSN"
DRY_RUN="${DRY_RUN:-0}"
AGENT_WAIT_SECS="${AGENT_WAIT_SECS:-180}"
GUARDRAIL_WAIT_SECS="${GUARDRAIL_WAIT_SECS:-180}"
PROMPT_MAX_BYTES=12288

for f in "$TEMPLATE" "$PROMPT" "$MCP_MANIFEST" "$GUARDRAIL_FILE" \
         "$POLICY_DIR/memory-concierge-pg-mcp.cedar" "$POLICY_DIR/memory-concierge-pg-llm.cedar"; do
  [[ -f "$f" ]] || die "missing file: $f"
done

# Run a mutating command; with DRY_RUN=1 only print it.
mut() {
  if [[ "$DRY_RUN" == "1" ]]; then
    log "DRY_RUN: $*"
    return 0
  fi
  "$@"
}

# Fail early (with lib.sh's hint) if rpk ai is not logged in.
rp_token >/dev/null
[[ "$DRY_RUN" == "1" ]] && warn "DRY_RUN=1: nothing will be created or changed in ADP"

# --------------------------------------------------------------- 1. providers
# Two providers, one guardrail (.env.example explains the two paths):
#   BEDROCK_PROVIDER  the guardrail's backend (adp/guardrail.json llm_provider_ref)
#                     and the alternative provider for own-account billing. The
#                     agent runtime calls Bedrock with the Converse API, where this
#                     gateway build does not inject the Bedrock guardrail, so the
#                     guardrail never fires through the agent on this provider.
#   LLM_PROVIDER      the provider the agent uses. Anthropic type, so the gateway
#                     evaluates the guardrail itself before and after every call.
# The guardrail is attached in step 3, after it exists and is READY.
info "[1/7] LLM providers: $BEDROCK_PROVIDER (Bedrock, guardrail backend) and $LLM_PROVIDER (agent)"

# provider_json <name>: prints the provider object, empty when it does not exist.
provider_json() { rpk ai llm-provider get "$1" -o json 2>/dev/null || true; }
# provider_guardrail <name>: prints the guardrail attached to the provider ("" when none).
provider_guardrail() { provider_json "$1" | jq -r '.guardrail // ""' 2>/dev/null || true; }

if [[ -n "$(provider_json "$BEDROCK_PROVIDER")" ]]; then
  ok "Bedrock provider $BEDROCK_PROVIDER exists"
else
  mut rpk ai llm-provider create "$BEDROCK_PROVIDER" \
    --display-name "Zeke Bedrock (agent memory demo)" --enabled \
    --bedrock-config.region us-east-2 \
    --bedrock-config.static-credentials.access-key-id-ref ZEKE_BEDROCK_INVOKER_KEY_ID \
    --bedrock-config.static-credentials.secret-access-key-ref ZEKE_BEDROCK_INVOKER_SECRET \
    --provider-models "us.anthropic.claude-sonnet-4-6,us.anthropic.claude-opus-4-8,us.anthropic.claude-haiku-4-5-20251001-v1:0" \
    --transcripts.record-input-messages --transcripts.record-output-messages \
    --tags demo=agent-memory --tags owner=zeke.dean
  ok "Bedrock provider $BEDROCK_PROVIDER created"
fi

if [[ "$LLM_PROVIDER" == "$BEDROCK_PROVIDER" ]]; then
  warn "LLM_PROVIDER is the Bedrock provider: the agent will run on Bedrock and guardrail $GUARDRAIL_NAME will not fire through it"
else
  agent_provider=$(provider_json "$LLM_PROVIDER")
  if [[ -n "$agent_provider" ]]; then
    ptype=$(jq -r '.type // "?"' <<<"$agent_provider")
    ok "agent provider $LLM_PROVIDER exists ($ptype)"
    [[ "$ptype" != "LLM_PROVIDER_TYPE_BEDROCK" ]] \
      || warn "agent provider $LLM_PROVIDER is a Bedrock provider: guardrail $GUARDRAIL_NAME will not fire through the agent"
  else
    mut rpk ai llm-provider create "$LLM_PROVIDER" \
      --display-name "Zeke Anthropic (agent memory demo, gateway-evaluated guardrail)" --enabled \
      --anthropic-config.api-key-ref "$ANTHROPIC_API_KEY_REF" \
      --provider-models "claude-sonnet-4-6,claude-haiku-4-5" \
      --transcripts.record-input-messages --transcripts.record-output-messages \
      --tags demo=agent-memory --tags owner=zeke.dean
    ok "agent provider $LLM_PROVIDER created (Anthropic, API key ref $ANTHROPIC_API_KEY_REF)"
  fi
fi

# Providers that carry the guardrail (steps 2 and 3).
GUARDED_PROVIDERS=("$BEDROCK_PROVIDER")
[[ "$LLM_PROVIDER" == "$BEDROCK_PROVIDER" ]] || GUARDED_PROVIDERS+=("$LLM_PROVIDER")

# --------------------------------------------------------------- 2. guardrail
info "[2/7] Guardrail: $GUARDRAIL_NAME"

# Prints the guardrail object (JSON) when it exists, returns 1 otherwise.
# GetGuardrail answers 404 {"code":"not_found",...} for a missing name.
guardrail_get() {
  local out
  out=$(aigw_rpc GuardrailService/GetGuardrail "$(jq -cn --arg n "$GUARDRAIL_NAME" '{name:$n}')" 2>/dev/null) || return 1
  jq -e --arg n "$GUARDRAIL_NAME" '
      select(type == "object" and (has("code") | not))
      | if .name == $n then . elif (.guardrail.name? // "") == $n then .guardrail else empty end
    ' <<<"$out" 2>/dev/null
}

guardrail_state() {
  guardrail_get | jq -r '.provisioningState // .provisioning_state // "UNKNOWN"' 2>/dev/null || echo "MISSING"
}

# The guardrail's backend is always the Bedrock provider (ApplyGuardrail runs
# there), whichever provider the agent uses.
jq -e --arg n "$GUARDRAIL_NAME" --arg p "$BEDROCK_PROVIDER" \
  '.guardrail.name == $n and .guardrail.bedrock_config.backend.llm_provider_ref == $p' \
  "$GUARDRAIL_FILE" >/dev/null \
  || die "$GUARDRAIL_FILE must define guardrail '$GUARDRAIL_NAME' backed by the Bedrock provider '$BEDROCK_PROVIDER' (BEDROCK_PROVIDER)"

# The local definition, normalised for comparison with the live object: the
# gateway answers in camelCase and omits proto3 defaults (false/null/empty).
guardrail_local_json() {
  jq -c '
    def camel: gsub("_(?<c>[a-z])"; .c | ascii_upcase);
    .guardrail
    | walk(if type == "object" then with_entries(.key |= camel) else . end)
    | del(.. | select(. == false or . == null or . == "" or . == [] or . == {}))
  ' "$GUARDRAIL_FILE"
}

# guardrail_in_sync: 0 when every field of adp/guardrail.json (word list, denied
# topics, PII entities, messages, backend...) is present in the live guardrail
# with the same value. Server-owned fields (etag, timestamps, provisioningState)
# are ignored. 1 when the guardrail is missing or differs.
guardrail_in_sync() {
  local live want
  live=$(guardrail_get) || return 1
  want=$(guardrail_local_json)
  # $a/$b are bound as values on purpose: filter parameters would re-evaluate
  # "." inside all(), which is the key being iterated, not the object.
  jq -e --argjson want "$want" '
    def subset($a; $b):
      if ($a | type) == "object" then
        ($b | type) == "object" and all($a | keys[]; . as $k | subset($a[$k]; $b[$k]))
      elif ($a | type) == "array" then
        ($b | type) == "array" and ($a | length) == ($b | length)
          and all(range($a | length); . as $i | subset($a[$i]; $b[$i]))
      else $a == $b end;
    subset($want; .)
  ' <<<"$live" >/dev/null 2>&1
}

guardrail_create() {
  local resp
  resp=$(aigw_rpc GuardrailService/CreateGuardrail "$(cat "$GUARDRAIL_FILE")") \
    || die "CreateGuardrail failed: ${resp:-<no response>}"
  if jq -e 'type == "object" and has("code")' <<<"$resp" >/dev/null 2>&1; then
    die "CreateGuardrail error: $resp"
  fi
}

# guardrail_update: UpdateGuardrail with the same {"guardrail": {...}} body as
# create (no update_mask: full replacement). Returns 1 when the gateway rejects
# it, so the caller can fall back to recreating the guardrail.
guardrail_update() {
  local resp
  if ! resp=$(aigw_rpc GuardrailService/UpdateGuardrail "$(cat "$GUARDRAIL_FILE")" 2>/dev/null); then
    warn "UpdateGuardrail rejected: ${resp:0:300}"
    return 1
  fi
  if jq -e 'type == "object" and has("code")' <<<"$resp" >/dev/null 2>&1; then
    warn "UpdateGuardrail error: ${resp:0:300}"
    return 1
  fi
  ok "UpdateGuardrail accepted"
}

# guardrail_recreate: detach from every provider it is attached to (a referenced
# guardrail cannot simply vanish under them), delete, wait until gone, create.
# Step 3 re-attaches it once it is READY.
guardrail_recreate() {
  local p resp deadline
  for p in "${GUARDED_PROVIDERS[@]}"; do
    if [[ "$(provider_guardrail "$p")" == "$GUARDRAIL_NAME" ]]; then
      rpk ai llm-provider update "$p" --clear guardrail >/dev/null \
        || die "could not detach guardrail $GUARDRAIL_NAME from provider $p"
      log "detached guardrail from $p (re-attached in step 3)"
    fi
  done
  resp=$(RP_API_OK_CODES="404" aigw_rpc GuardrailService/DeleteGuardrail "$(jq -cn --arg n "$GUARDRAIL_NAME" '{name:$n}')") \
    || die "DeleteGuardrail failed: ${resp:0:300}"
  deadline=$(( SECONDS + 60 ))
  while guardrail_get >/dev/null; do
    (( SECONDS < deadline )) || die "guardrail $GUARDRAIL_NAME still present 60s after DeleteGuardrail"
    log "waiting for guardrail delete..."
    sleep 3
  done
  guardrail_create
  ok "guardrail recreated from $GUARDRAIL_FILE"
}

guardrail_wait_ready() {
  local deadline state
  deadline=$(( SECONDS + GUARDRAIL_WAIT_SECS ))
  while :; do
    state=$(guardrail_state)
    case "$state" in
      GUARDRAIL_PROVISIONING_STATE_READY) ok "guardrail READY"; return 0 ;;
      *FAIL*|*ERROR*) die "guardrail $GUARDRAIL_NAME is in state $state" ;;
    esac
    (( SECONDS < deadline )) || die "timed out after ${GUARDRAIL_WAIT_SECS}s waiting for guardrail READY (last: $state)"
    log "guardrail state: $state; waiting..."
    sleep 5
  done
}

GUARDRAIL_PRESENT=0
GUARDRAIL_RECHECK=0
if guardrail_get >/dev/null; then
  GUARDRAIL_PRESENT=1
  if guardrail_in_sync; then
    ok "guardrail exists and matches $GUARDRAIL_FILE"
  elif [[ "$DRY_RUN" == "1" ]]; then
    warn "guardrail exists but differs from $GUARDRAIL_FILE"
    log "DRY_RUN: aigw_rpc GuardrailService/UpdateGuardrail @$GUARDRAIL_FILE (fallback: detach, DeleteGuardrail, CreateGuardrail)"
  else
    warn "guardrail exists but differs from $GUARDRAIL_FILE; updating it"
    if guardrail_update; then
      GUARDRAIL_RECHECK=1
    else
      guardrail_recreate
    fi
  fi
elif [[ "$DRY_RUN" == "1" ]]; then
  log "DRY_RUN: aigw_rpc GuardrailService/CreateGuardrail @$GUARDRAIL_FILE"
else
  guardrail_create
  ok "guardrail created"
  GUARDRAIL_PRESENT=1
fi

if (( GUARDRAIL_PRESENT )); then
  guardrail_wait_ready
  # An accepted UpdateGuardrail that did not actually change the object (for
  # example a server that wants an update_mask) must not be reported as applied.
  if (( GUARDRAIL_RECHECK )) && ! guardrail_in_sync; then
    warn "UpdateGuardrail was accepted but the live guardrail still differs; recreating it"
    guardrail_recreate
    guardrail_wait_ready
  fi
  if [[ "$DRY_RUN" != "1" ]] && ! guardrail_in_sync; then
    die "guardrail $GUARDRAIL_NAME still differs from $GUARDRAIL_FILE after reconciliation"
  fi
fi

# ------------------------------------------------------- 3. attach guardrail
info "[3/7] Attach guardrail to both providers"
for p in "${GUARDED_PROVIDERS[@]}"; do
  if [[ "$(provider_guardrail "$p")" == "$GUARDRAIL_NAME" ]]; then
    ok "guardrail already attached to $p"
  else
    mut rpk ai llm-provider update "$p" --guardrail "$GUARDRAIL_NAME"
    ok "guardrail attached to $p (changes take about 30s to propagate)"
  fi
done

# ------------------------------------------------------------ 4. DSN secret
info "[4/7] Secret $DSN_SECRET_ID (created by scripts/20-redpanda-setup.sh)"
secret_rc=0
secret_body=$(RP_API_OK_CODES="404" rp_api GET "/v1/secrets/$DSN_SECRET_ID" 2>/dev/null) || secret_rc=$?
if (( secret_rc != 0 )); then
  warn "could not verify secret $DSN_SECRET_ID (Data Plane API call failed); continuing"
elif jq -e --arg id "$DSN_SECRET_ID" '(.id // .secret.id // "") == $id' <<<"$secret_body" >/dev/null 2>&1; then
  ok "secret exists"
elif [[ "$DRY_RUN" == "1" ]]; then
  warn "secret $DSN_SECRET_ID not found; a real run would stop here (run scripts/20-redpanda-setup.sh first)"
else
  die "secret $DSN_SECRET_ID not found in the Redpanda secret store; run scripts/20-redpanda-setup.sh (make setup-redpanda) first"
fi

# ------------------------------------------------------------- 5. MCP server
info "[5/7] MCP server: $MCP_SERVER_NAME"
if rpk ai mcp-server get "$MCP_SERVER_NAME" -o json >/dev/null 2>&1; then
  mut rpk ai mcp-server update "$MCP_SERVER_NAME" -f "$MCP_MANIFEST"
  ok "MCP server updated from $MCP_MANIFEST"
else
  mut rpk ai mcp-server create "$MCP_SERVER_NAME" -f "$MCP_MANIFEST" --enabled
  ok "MCP server created from $MCP_MANIFEST"
fi
tools=$(rpk ai mcp-server get "$MCP_SERVER_NAME" -o json 2>/dev/null | jq -r '[.tools[]?.name] | join(", ")' 2>/dev/null || true)
log "MCP tools: ${tools:-<not refreshed yet; expected: execute, query>}"

# ---------------------------------------------------------- 6. Cedar policies
info "[6/7] Cedar policies"
# Policy names are resource names: rpk ai policy get/update want "policies/<name>".
# When the policy exists, its Cedar text is reconciled with the .cedar file
# (trailing whitespace and the final newline are not significant).
ensure_policy() {
  local name="$1" display="$2" desc="$3" file="$4" live live_cedar want_cedar
  if live=$(rpk ai policy get "policies/$name" -o json 2>/dev/null); then
    live_cedar=$(jq -r '.cedar_text // ""' <<<"$live" | sed -E 's/[[:space:]]+$//')
    want_cedar=$(sed -E 's/[[:space:]]+$//' "$file")
    if [[ "$live_cedar" == "$want_cedar" ]]; then
      ok "policy $name exists and matches $file"
    else
      warn "policy $name exists but its Cedar text differs from $file; updating it"
      mut rpk ai policy update "policies/$name" --cedar-file "$file"
      ok "policy $name updated"
    fi
  else
    mut rpk ai policy create --name "$name" --display-name "$display" --description "$desc" --cedar-file "$file"
    ok "policy $name created"
  fi
}
ensure_policy memory-concierge-pg-mcp \
  "Agent grant: memory-concierge-pg - pg-agent-memory" \
  "memory-concierge-pg may open MCP sessions and call tools on pg-agent-memory, and no other MCP server." \
  "$POLICY_DIR/memory-concierge-pg-mcp.cedar"
ensure_policy memory-concierge-pg-llm \
  "Agent grant: memory-concierge-pg - its LLM provider" \
  "memory-concierge-pg may invoke the $LLM_PROVIDER and $BEDROCK_PROVIDER providers only." \
  "$POLICY_DIR/memory-concierge-pg-llm.cedar"
# The .cedar files name the providers and the MCP server literally, while .env
# makes them configurable. With a name the policy does not permit, the agent
# still reaches RUNNING and is then denied on its first call, so say so here.
for p in "$LLM_PROVIDER" "$BEDROCK_PROVIDER"; do
  grep -qF "LLMProvider::\"$p\"" "$POLICY_DIR/memory-concierge-pg-llm.cedar" \
    || warn "policy memory-concierge-pg-llm does not permit provider $p; the agent will be denied LLMProvider.invoke (edit adp/policies/memory-concierge-pg-llm.cedar)"
done
grep -qF "McpServer::\"$MCP_SERVER_NAME\"" "$POLICY_DIR/memory-concierge-pg-mcp.cedar" \
  || warn "policy memory-concierge-pg-mcp does not permit MCP server $MCP_SERVER_NAME; the agent will be denied McpServer.* (edit adp/policies/memory-concierge-pg-mcp.cedar)"

# ------------------------------------------------------------------ 7. agent
info "[7/7] Agent: $AGENT_NAME"

render_agent_manifest() {
  local bytes bad
  bytes=$(wc -c < "$PROMPT" | tr -d ' ')
  (( bytes <= PROMPT_MAX_BYTES )) || die "system prompt is $bytes bytes; the limit is $PROMPT_MAX_BYTES"
  # Only these Go template actions are supported by the agent runtime.
  bad=$(grep -oE '\{\{[^}]*\}\}' "$PROMPT" | grep -vE '^\{\{(if \.User\.Email|else|end|\.User\.Email|\.Now\.Date)\}\}$' || true)
  [[ -z "$bad" ]] || die "unsupported template actions in $PROMPT: $bad"
  if grep -q "$(printf '\t')" "$PROMPT"; then
    die "$PROMPT contains tab characters, which are invalid inside a YAML block scalar"
  fi
  grep -qE '^[[:space:]]*__SYSTEM_PROMPT__[[:space:]]*$' "$TEMPLATE" || die "$TEMPLATE has no __SYSTEM_PROMPT__ line"
  grep -qE '^[[:space:]]+llm_provider: __LLM_PROVIDER__$' "$TEMPLATE" || die "$TEMPLATE has no 'llm_provider: __LLM_PROVIDER__' line"
  grep -qE '^[[:space:]]+model: __LLM_MODEL__$' "$TEMPLATE" || die "$TEMPLATE has no 'model: __LLM_MODEL__' line"
  grep -qE "^name: ${AGENT_NAME}\$" "$TEMPLATE" || warn "$TEMPLATE does not declare 'name: $AGENT_NAME'"

  {
    echo "# GENERATED by scripts/50-deploy-agent.sh from adp/agent.template.yaml + adp/system-prompt.md"
    echo "# with LLM_PROVIDER=$LLM_PROVIDER LLM_MODEL=$LLM_MODEL from .env."
    echo "# Do not edit this file; edit those and re-run scripts/50-deploy-agent.sh (make deploy-agent)."
    awk -v prompt_file="$PROMPT" -v provider="$LLM_PROVIDER" -v model="$LLM_MODEL" '
      # drop the template'"'"'s own leading comment block; the banner above replaces it
      header && /^#/ { next }
      { header = 0 }
      /^[[:space:]]*__SYSTEM_PROMPT__[[:space:]]*$/ {
        while ((getline line < prompt_file) > 0) {
          sub(/[[:space:]]+$/, "", line)
          if (line == "") print ""; else print "      " line
        }
        close(prompt_file)
        next
      }
      { gsub(/__LLM_PROVIDER__/, provider); gsub(/__LLM_MODEL__/, model); print }
    ' header=1 "$TEMPLATE"
  } > "$AGENT_MANIFEST.tmp"
  if grep -qE '__[A-Z_]+__' "$AGENT_MANIFEST.tmp"; then
    die "unreplaced placeholder in the rendered manifest: $(grep -oE '__[A-Z_]+__' "$AGENT_MANIFEST.tmp" | sort -u | tr '\n' ' ')"
  fi
  mv "$AGENT_MANIFEST.tmp" "$AGENT_MANIFEST"
  ok "rendered $AGENT_MANIFEST (provider $LLM_PROVIDER, model $LLM_MODEL, $bytes-byte system prompt)"
}

render_agent_manifest
mut rpk ai agent apply -f "$AGENT_MANIFEST"

agent_json() { rpk ai agent get "$AGENT_NAME" -o json 2>/dev/null || true; }

# provider_note: which provider the agent uses and how to switch (.env.example).
provider_note() {
  log ""
  log "Provider: agent $AGENT_NAME runs on $LLM_PROVIDER, model $LLM_MODEL."
  if [[ "$LLM_PROVIDER" == "$BEDROCK_PROVIDER" ]]; then
    log "  This is the Bedrock provider. The agent runtime uses the Converse API, where this gateway build does"
    log "  not inject the Bedrock guardrail, so $GUARDRAIL_NAME will not fire through the agent; show it with a"
    log "  direct POST .../model/<id>/invoke instead. Calls bill the AWS account behind the provider."
    log "  To run the agent on the gateway-evaluated path, set LLM_PROVIDER/LLM_MODEL in .env to an Anthropic"
    log "  provider (default zeke-anthropic-agent-memory / claude-sonnet-4-6) and re-run this script, or:"
    log "    rpk ai agent update $AGENT_NAME --llm-provider zeke-anthropic-agent-memory --model claude-sonnet-4-6"
  else
    log "  The gateway evaluates $GUARDRAIL_NAME itself for this non-Bedrock provider, so the denied topic, word"
    log "  filter and PII tests all block through the agent (reply: [guardrail_intervened]; the A2A task is"
    log "  marked failed and rpk exits 4, which scripts/chat.sh reports as '[blocked by guardrail]')."
    log "  Billing: this path uses the org Anthropic key. $BEDROCK_PROVIDER (Bedrock, own AWS account) stays as"
    log "  the guardrail backend. To run the agent on it instead (the guardrail then fires only on direct invoke calls):"
    log "    rpk ai agent update $AGENT_NAME --llm-provider $BEDROCK_PROVIDER --model $BEDROCK_MODEL"
    log "  Switch back: rpk ai agent update $AGENT_NAME --llm-provider $LLM_PROVIDER --model $LLM_MODEL"
  fi
}

if [[ "$DRY_RUN" == "1" ]] && ! rpk ai agent get "$AGENT_NAME" -o json >/dev/null 2>&1; then
  log "DRY_RUN: agent $AGENT_NAME does not exist yet; skipping the RUNNING wait"
  provider_note
  exit 0
fi

desired=$(agent_json | jq -r '.managed.status.desired_state // ""' 2>/dev/null || true)
if [[ -n "$desired" && "$desired" != "AGENT_DESIRED_STATE_RUNNING" ]]; then
  mut rpk ai agent start "$AGENT_NAME"
fi

deadline=$(( SECONDS + AGENT_WAIT_SECS ))
url=""
while :; do
  js=$(agent_json)
  state=$(jq -r '.managed.status.state // ""' <<<"$js" 2>/dev/null || true)
  url=$(jq -r '.managed.status.url // ""' <<<"$js" 2>/dev/null || true)
  if [[ "$state" == "AGENT_STATE_RUNNING" ]]; then
    ok "agent $AGENT_NAME is RUNNING"
    break
  fi
  case "$state" in
    *FAIL*|*ERROR*)
      die "agent $AGENT_NAME is in state $state: $(jq -r '.managed.status.message // .managed.status.error // ""' <<<"$js" 2>/dev/null)" ;;
  esac
  (( SECONDS < deadline )) || die "timed out after ${AGENT_WAIT_SECS}s waiting for AGENT_STATE_RUNNING (last state: ${state:-unknown})"
  log "agent state: ${state:-unknown}; waiting..."
  sleep 5
done

[[ -n "$url" ]] || warn "agent is RUNNING but reported no A2A URL yet"
ok "A2A endpoint: ${url:-<pending>}"
live_provider=$(agent_json | jq -r '.managed.spec.llm_provider // ""' 2>/dev/null || true)
live_model=$(agent_json | jq -r '.managed.spec.model // ""' 2>/dev/null || true)
if [[ "$DRY_RUN" != "1" && -n "$live_provider" && ( "$live_provider" != "$LLM_PROVIDER" || "$live_model" != "$LLM_MODEL" ) ]]; then
  warn "agent reports provider $live_provider / model $live_model, expected $LLM_PROVIDER / $LLM_MODEL"
fi
log "Try it: scripts/chat.sh --new 'What do you know about me and my accounts?'"
provider_note
printf '%s\n' "$url"
