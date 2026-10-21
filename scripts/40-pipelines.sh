#!/usr/bin/env bash
# 40-pipelines.sh : deploy the four Redpanda Connect pipelines.
#
#   ./scripts/40-pipelines.sh          cloud mode: create or update the managed
#                                      pipelines pg-agent-memory-cdc, -audit,
#                                      -observer and -purge through the Data
#                                      Plane API, store ids in .state/pipelines.json
#                                      and print their state.
#   ./scripts/40-pipelines.sh --local  run the pipelines on this machine with
#                                      `rpk connect streams`, using the Kafka user
#                                      and the agent DSN from .env.
#   Any extra argument is a pipeline name: only those are deployed or run
#   (e.g. --local pg-agent-memory-cdc).
#
# The config files in connect/ are uploaded verbatim (they reference
# REDPANDA_BROKERS and secrets.NAME interpolations, which Redpanda Cloud
# resolves). For --local the script renders .state/connect/*.rendered.yaml
# copies with secrets.NAME -> NAME and moves metrics/logger (service-wide
# fields, not allowed inside stream files) into a streams-mode root config.
# Idempotent by pipeline display_name. Contract: docs/PORT-CONTRACT.md sections 5, 6, 8.
set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

usage() {
  sed -n '2,19p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

MODE=cloud
ONLY=()
for arg in "$@"; do
  case "$arg" in
    --local) MODE=local ;;
    -h|--help) usage 0 ;;
    pg-agent-memory-*) ONLY+=("$arg") ;;
    *) warn "unknown argument: $arg"; usage 1 ;;
  esac
done

load_env
require_cmd jq rpk mktemp

CONNECT_DIR="$REPO_ROOT/connect"
STATE_FILE="$STATE_DIR/pipelines.json"
LIST_FILE=""   # temp copy of the pipeline list (cloud mode); removed by the EXIT trap
PIPELINE_NAMES=(pg-agent-memory-cdc pg-agent-memory-audit pg-agent-memory-observer pg-agent-memory-purge)
if (( ${#ONLY[@]} > 0 )); then PIPELINE_NAMES=("${ONLY[@]}"); fi
PIPELINE_WAIT_SECONDS="${PIPELINE_WAIT_SECONDS:-120}"

# Static per-pipeline facts (bash 3 compatible: no associative arrays).
pipeline_file() {
  case "$1" in
    pg-agent-memory-cdc)      echo "$CONNECT_DIR/pg-cdc.yaml" ;;
    pg-agent-memory-audit)    echo "$CONNECT_DIR/memory-audit.yaml" ;;
    pg-agent-memory-observer) echo "$CONNECT_DIR/account-observer.yaml" ;;
    pg-agent-memory-purge)    echo "$CONNECT_DIR/memory-purge.yaml" ;;
    *) die "unknown pipeline: $1" ;;
  esac
}
pipeline_description() {
  case "$1" in
    pg-agent-memory-cdc)      echo "Agent memory demo (PostgreSQL): polls agent_memory.change_log every second and publishes memory and account changes to agent.pg.memory.cdc / agent.pg.accounts.cdc." ;;
    pg-agent-memory-audit)    echo "Agent memory demo (PostgreSQL): materialises agent.pg.memory.cdc into the append-only agent_memory.memory_audit table." ;;
    pg-agent-memory-observer) echo "Agent memory demo (PostgreSQL): watches agent.pg.accounts.cdc and writes observation memories (overdraft, large deposit) into agent_memory.memories." ;;
    pg-agent-memory-purge)    echo "Agent memory demo (PostgreSQL): deletes expired memories once an hour (the only DELETE on memories; each one is logged by the trigger)." ;;
    *) die "unknown pipeline: $1" ;;
  esac
}

lint_configs() {
  local files=()
  local name
  for name in "${PIPELINE_NAMES[@]}"; do files+=("$(pipeline_file "$name")"); done
  info "rpk connect lint ${files[*]#"$REPO_ROOT"/}"
  # REDPANDA_BROKERS and secrets.NAME are resolved by Redpanda Cloud, not here.
  rpk connect lint --skip-env-var-check "${files[@]}"
}

# ---------------------------------------------------------------------------
# cloud mode
# ---------------------------------------------------------------------------

# list_pipelines <out-file> : writes a JSON array of every pipeline (follows
# pagination). Pages are large (config_yaml is inlined), so everything goes
# through files rather than argv.
list_pipelines() {
  local out="$1" token="" path page
  page="$(mktemp "${TMPDIR:-/tmp}/pipelines-page.XXXXXX")"
  printf '[]' >"$out"
  while :; do
    path="/v1/redpanda-connect/pipelines?page_size=100"
    if [[ -n "$token" ]]; then
      path="${path}&page_token=$(jq -rn --arg t "$token" '$t | @uri')"
    fi
    if ! rp_api GET "$path" >"$page"; then
      die "listing pipelines failed: $(cat "$page")"
    fi
    jq -c -s '.[0] + (.[1].pipelines // [])' "$out" "$page" >"$out.new"
    mv "$out.new" "$out"
    token="$(jq -r '.next_page_token // ""' "$page")"
    [[ -z "$token" ]] && break
  done
  rm -f "$page"
}

# existing_field <list-file> <name> <field> : prints that field of the pipeline
# with display_name <name>, or nothing.
existing_field() {
  jq -r --arg n "$2" --arg f "$3" \
    '[.[] | select(.display_name == $n)] | first | .[$f] // empty' "$1"
}

# build_body <name> : JSON request body for create/update, config_yaml verbatim.
build_body() {
  local name="$1"
  jq -n \
    --arg name "$name" \
    --arg desc "$(pipeline_description "$name")" \
    --rawfile cfg "$(pipeline_file "$name")" \
    '{
      display_name: $name,
      description: $desc,
      config_yaml: $cfg,
      resources: { cpu_shares: "100m", memory_shares: "400M" },
      tags: { demo: "agent-memory-pg", owner: "zeke.dean", stack: "postgresql-redpanda" }
    }'
}

# state_set <name> <id> <state> <url> : merge one entry into .state/pipelines.json
state_set() {
  local name="$1" id="$2" state="$3" url="$4" cur='{}' tmp
  mkdir -p "$STATE_DIR"
  [[ -s "$STATE_FILE" ]] && cur="$(cat "$STATE_FILE")"
  tmp="$(mktemp "${STATE_DIR}/pipelines.XXXXXX")"
  jq --arg n "$name" --arg id "$id" --arg st "$state" --arg url "$url" \
     --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
     '.[$n] = { id: $id, state: $st, url: $url, updated_at: $ts }' <<<"$cur" >"$tmp"
  mv "$tmp" "$STATE_FILE"
}

# get_pipeline <id> : prints the pipeline object (dies with the API body on failure).
get_pipeline() {
  local out
  if ! out="$(rp_api GET "/v1/redpanda-connect/pipelines/$1")"; then
    die "GET pipeline $1 failed: $out"
  fi
  jq -c '.pipeline // .' <<<"$out"
}

# wait_for_running <name> <id> : polls until STATE_RUNNING or timeout; prints the
# final state (STATE_UNKNOWN when the API call itself failed).
wait_for_running() {
  local name="$1" id="$2" deadline state
  deadline=$(( $(date +%s) + PIPELINE_WAIT_SECONDS ))
  while :; do
    # Explicit failure check: this function runs as an `if` condition, where
    # set -e is suspended, so a failed GET must not fall through as an empty
    # state and be retried until the deadline.
    if ! state="$(get_pipeline "$id" | jq -r '.state // "STATE_UNSPECIFIED"')"; then
      printf 'STATE_UNKNOWN\n'; return 1
    fi
    case "$state" in
      STATE_RUNNING)
        printf '%s\n' "$state"; return 0 ;;
      STATE_ERROR|STATE_COMPLETED)
        printf '%s\n' "$state"; return 1 ;;
    esac
    if (( $(date +%s) >= deadline )); then
      printf '%s\n' "$state"; return 1
    fi
    info "$name ($id) is $state; waiting"
    sleep 5
  done
}

deploy_cloud() {
  lint_configs
  require_cmd curl
  rp_token >/dev/null

  # LIST_FILE is deliberately file scope, not `local`: the EXIT trap runs after
  # this function has returned, when a local would be unbound (set -u would then
  # turn a successful deploy into exit 1 and leak the file).
  LIST_FILE="$(mktemp "${TMPDIR:-/tmp}/pipelines-list.XXXXXX")"
  trap 'rm -f "$LIST_FILE"' EXIT
  info "listing existing pipelines"
  list_pipelines "$LIST_FILE"
  info "$(jq -r 'length' "$LIST_FILE") pipelines exist on the cluster"

  local name body id resp cur_cfg cur_desc state url rc=0
  for name in "${PIPELINE_NAMES[@]}"; do
    body="$(build_body "$name")"
    id="$(existing_field "$LIST_FILE" "$name" id)"

    if [[ -z "$id" ]]; then
      info "creating pipeline $name"
      if ! resp="$(rp_api POST /v1/redpanda-connect/pipelines "$body")"; then
        die "create $name failed: $resp"
      fi
      id="$(jq -r '.pipeline.id // empty' <<<"$resp")"
      if [[ -z "$id" ]]; then
        # Possibly created concurrently: look it up again before giving up.
        list_pipelines "$LIST_FILE"
        id="$(existing_field "$LIST_FILE" "$name" id)"
        [[ -n "$id" ]] || die "create $name returned no pipeline id: $resp"
      fi
      ok "created $name ($id)"
    else
      cur_cfg="$(existing_field "$LIST_FILE" "$name" config_yaml)"
      cur_desc="$(existing_field "$LIST_FILE" "$name" description)"
      if [[ "$cur_cfg" == "$(cat "$(pipeline_file "$name")")" && "$cur_desc" == "$(pipeline_description "$name")" ]]; then
        ok "$name ($id) is up to date"
      else
        info "updating pipeline $name ($id)"
        if ! resp="$(rp_api PUT "/v1/redpanda-connect/pipelines/$id" "$body")"; then
          die "update $name failed: $resp"
        fi
        ok "updated $name ($id)"
      fi
    fi

    state="$(get_pipeline "$id" | jq -r '.state // "STATE_UNSPECIFIED"')"
    if [[ "$state" == "STATE_STOPPED" ]]; then
      info "$name ($id) is stopped; starting"
      if ! resp="$(rp_api PUT "/v1/redpanda-connect/pipelines/$id/start")"; then
        warn "start $name failed (start it from the console, or delete it and re-run): $resp"
      fi
    fi

    if state="$(wait_for_running "$name" "$id")"; then
      ok "$name ($id) is $state"
    elif [[ "$state" == "STATE_UNKNOWN" ]]; then
      die "could not read the state of $name ($id) from the Data Plane API (see the error above)"
    else
      warn "$name ($id) is $state (waited up to ${PIPELINE_WAIT_SECONDS}s; check the pipeline logs in the Redpanda console)"
      rc=1
    fi
    url="$(get_pipeline "$id" | jq -r '.url // ""')"
    state_set "$name" "$id" "$state" "$url"
  done

  log "pipeline state ($STATE_FILE)"
  jq -r 'to_entries[] | "  \(.key)\t\(.value.id)\t\(.value.state)"' "$STATE_FILE"
  return "$rc"
}

# ---------------------------------------------------------------------------
# local mode
# ---------------------------------------------------------------------------

# render_local <src> <dst> : secrets.NAME -> NAME interpolations; drop the
# top-level metrics:/logger: blocks (streams mode rejects service-wide fields
# inside stream files; they move to the -o root config).
render_local() {
  local src="$1" dst="$2"
  sed -E 's/\$\{secrets\.([A-Za-z0-9_]+)\}/${\1}/g' "$src" \
    | awk '
        /^(metrics|logger):/ { skip = 1; next }
        skip && /^[^[:space:]#]/ { skip = 0 }
        !skip { print }
      ' >"$dst"
}

run_local() {
  require_cmd awk sed

  local out_dir="$STATE_DIR/connect"
  mkdir -p "$out_dir"

  local name src dst rendered=()
  for name in "${PIPELINE_NAMES[@]}"; do
    src="$(pipeline_file "$name")"
    dst="$out_dir/$(basename "${src%.yaml}").rendered.yaml"
    render_local "$src" "$dst"
    rendered+=("$dst")
  done

  # Loopback only, and the exec below passes --no-api: the unauthenticated
  # streams management API (create/update/delete streams) must not be reachable
  # from the network while a process holding the Kafka password and the agent
  # DSN is running. /metrics and /ready stay available on localhost.
  cat >"$out_dir/observability.yaml" <<'YAML'
# Streams-mode root config (service-wide fields) for scripts/40-pipelines.sh --local
http:
  address: 127.0.0.1:4195
metrics:
  prometheus: {}
logger:
  level: INFO
YAML

  # Same names the cloud secret store uses, so the rendered configs read 1:1.
  export REDPANDA_BROKERS
  export AGENT_MEMORY_PG_KAFKA_USER="$KAFKA_USER"
  export AGENT_MEMORY_PG_KAFKA_PASSWORD="$KAFKA_PASSWORD"
  AGENT_MEMORY_PG_DSN="$(pg_agent_url)"
  export AGENT_MEMORY_PG_DSN

  info "rpk connect lint (rendered copies, env resolved)"
  rpk connect lint "${rendered[@]}"

  log "running ${PIPELINE_NAMES[*]} locally (metrics: http://127.0.0.1:4195/metrics, loopback only, streams API disabled; Ctrl-C to stop)"
  exec rpk connect streams --no-api -o "$out_dir/observability.yaml" "${rendered[@]}"
}

# ---------------------------------------------------------------------------

case "$MODE" in
  cloud) deploy_cloud ;;
  local) run_local ;;
esac
