#!/usr/bin/env bash
# observe.sh : watch the agent's memory stream end to end.
#
# Two "panes", sequential and non-interactive friendly:
#   - background : tail the agent.pg.memory.cdc topic from its end, printing
#                  "<key> <value>" for every change event as it arrives
#   - foreground : every OBSERVE_INTERVAL seconds (default 5) print the latest
#                  memory_audit rows and the active memories
# Ctrl-C stops both. `--once` prints a single snapshot (no topic tail) and exits.
#
# Env knobs: OBSERVE_INTERVAL (5), OBSERVE_LIMIT (10),
#            OBSERVE_SUBJECT (DEMO_USER_EMAIL; empty = all subjects)
# Contract: docs/DESIGN.md section 8 (observe.sh).
set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

ONCE=0
for arg in "$@"; do
  case "$arg" in
    --once) ONCE=1 ;;
    -h|--help) sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown argument: $arg" ;;
  esac
done

load_env
require_cmd rpk psql

: "${TOPIC_MEMORY_CDC:=agent.pg.memory.cdc}"
S="${PG_SCHEMA:-agent_memory}"
INTERVAL="${OBSERVE_INTERVAL:-5}"
LIMIT="${OBSERVE_LIMIT:-10}"
SUBJECT="${OBSERVE_SUBJECT-${DEMO_USER_EMAIL:-}}"

# SQL literal for the optional subject filter (single quotes doubled). The quote
# is held in a variable because bash 3.2 (macOS /bin/bash) keeps the backslashes
# when the pattern and replacement of a //-expansion are written as escaped quotes.
SUBJECT_FILTER=""
if [[ -n "$SUBJECT" ]]; then
  q="'"
  SUBJECT_FILTER="AND subject = '${SUBJECT//$q/$q$q}'"
fi

snapshot() {
  local now
  now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "===== ${now}  memory_audit (latest ${LIMIT}${SUBJECT:+, subject ${SUBJECT}}) ====="
  pg_admin_psql "SELECT op, memory_id AS memory, change_id AS change, subject,
       left(coalesce(after_doc->>'content', before_doc->>'content'), 60) AS content,
       audited_at
FROM ${S}.memory_audit
WHERE true ${SUBJECT_FILTER}
ORDER BY audited_at DESC
LIMIT ${LIMIT};" || warn "memory_audit query failed (is the schema applied? make setup-db)"
  echo "===== memories (active${SUBJECT:+, subject ${SUBJECT}}) ====="
  pg_admin_psql "SELECT memory_id, kind, left(content, 70) AS content, importance, source, created_at, expires_at
FROM ${S}.memories
WHERE expires_at > now() ${SUBJECT_FILTER}
ORDER BY created_at DESC
LIMIT ${LIMIT};" || warn "memories query failed (is the schema applied? make setup-db)"
}

if (( ONCE )); then
  snapshot
  exit 0
fi

CONSUMER_PID=""
cleanup() {
  if [[ -n "$CONSUMER_PID" ]]; then
    pkill -TERM -P "$CONSUMER_PID" 2>/dev/null || true
    kill "$CONSUMER_PID" 2>/dev/null || true
  fi
}
# INT and TERM must terminate explicitly: a Ctrl-C that lands inside one of the
# `psql ... || warn` calls in snapshot() is otherwise swallowed by the `|| warn`
# and the loop would carry on without the topic tail. cleanup is idempotent, so
# the EXIT trap running it again is harmless.
trap cleanup EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM

log "tailing ${TOPIC_MEMORY_CDC} from the end (key value); polling PostgreSQL every ${INTERVAL}s; Ctrl-C to stop"
(
  rpk_kafka topic consume "$TOPIC_MEMORY_CDC" -o end -f '%k %v\n' 2>&1 \
    | while IFS= read -r line; do
        printf '[%s] cdc %s\n' "$(date +%H:%M:%S)" "$line"
      done
) &
CONSUMER_PID=$!

while :; do
  snapshot
  if ! kill -0 "$CONSUMER_PID" 2>/dev/null; then
    die "the ${TOPIC_MEMORY_CDC} tail (rpk topic consume) has exited; see its output above"
  fi
  sleep "$INTERVAL"
done
