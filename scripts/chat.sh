#!/usr/bin/env bash
# scripts/chat.sh - talk to the Memory Concierge over A2A and keep the conversation going.
#
# Usage:
#   scripts/chat.sh [--new] <message>
#   echo "<message>" | scripts/chat.sh [--new]
#
#   --new   start a fresh conversation (drop the saved context id)
#
# The agent's reply is printed to stdout. rpk prints the A2A ids on stderr as
# "context-id: <id>" lines; the last one is saved in .state/chat-context and
# passed back with --context-id on the next call, so successive calls continue
# the same conversation. The message may come from the argument(s) or stdin.
#
# When the ADP guardrail intervenes the A2A task ends "failed" and rpk exits 4
# with "[guardrail_intervened] ..." in its output (stderr or stdout; both are
# checked). That is an expected demo outcome, so the blocked message is printed
# on stdout as "[blocked by guardrail] ..." and the script exits 0. Any other
# rpk failure keeps its exit code.
set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"
load_env
require_cmd rpk

AGENT_NAME="${AGENT_NAME:-memory-concierge-pg}"
CTX_FILE="$STATE_DIR/chat-context"

usage() {
  sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'
}

NEW=0
ARGS=()
while (( $# )); do
  case "$1" in
    --new) NEW=1 ;;
    -h|--help) usage; exit 0 ;;
    --) shift; ARGS+=("$@"); break ;;
    *) ARGS+=("$1") ;;
  esac
  shift
done

MSG="${ARGS[*]+"${ARGS[*]}"}"
if [[ -z "$MSG" || "$MSG" == "-" ]]; then
  if [[ -t 0 ]]; then
    usage >&2
    die "no message given (pass it as an argument or on stdin)"
  fi
  MSG=$(cat)
fi
[[ -n "${MSG//[[:space:]]/}" ]] || die "empty message"

mkdir -p "$STATE_DIR"
if (( NEW )); then
  rm -f "$CTX_FILE"
  log "starting a new conversation with $AGENT_NAME"
fi

send_args=()
ctx=""
if [[ -s "$CTX_FILE" ]]; then
  ctx=$(tr -d '[:space:]' < "$CTX_FILE")
  if [[ -n "$ctx" ]]; then
    send_args+=(--context-id "$ctx")
  fi
fi

outf=$(mktemp "${TMPDIR:-/tmp}/chat-stdout.XXXXXX")
errf=$(mktemp "${TMPDIR:-/tmp}/chat-stderr.XXXXXX")
trap 'rm -f "$outf" "$errf"' EXIT

# The message goes in on stdin so that any text (even one starting with "-")
# reaches the agent unchanged. rpk blocks until the reply is complete (no
# --stream), so both streams are captured and replayed below: the reply text
# goes to stdout and the ids and state to stderr, but a failed turn's error can
# appear on either, so both are searched for the guardrail marker (as
# 60-verify.sh does). `-o table` is pinned on purpose: rpk honours RPAI_FORMAT,
# and in json/yaml mode the context id moves from stderr into the stdout JSON,
# which would break the "context-id:" parsing below and print raw JSON instead
# of the reply.
rc=0
printf '%s' "$MSG" | rpk ai agent a2a send "$AGENT_NAME" -o table ${send_args[@]+"${send_args[@]}"} >"$outf" 2>"$errf" || rc=$?

blocked=0
if (( rc != 0 )) && grep -qF 'guardrail_intervened' "$errf" "$outf"; then
  blocked=1
fi

# Replay the reply on stdout and rpk's status lines (context-id, task-id, state,
# errors) on stderr; in the blocked case the raw error line is replaced by the
# clean message below. Then remember the context id for the next call (also
# when blocked: the conversation continues, only this turn failed).
if (( blocked )); then
  grep -vF 'guardrail_intervened' "$errf" >&2 || true
else
  cat "$outf"
  cat "$errf" >&2
fi
new_ctx=$(grep -iE '^[[:space:]]*context[-_]?id:' "$errf" | tail -n 1 | sed -E 's/^[^:]*:[[:space:]]*//' | tr -d '[:space:]' || true)
if [[ -n "$new_ctx" ]]; then
  printf '%s\n' "$new_ctx" > "$CTX_FILE"
fi

if (( blocked )); then
  # e.g. "agent: model generation failed: API call failed: [guardrail_intervened] Blocked by guardrail: ..."
  msg=$(grep -hF 'guardrail_intervened' "$errf" "$outf" | head -n 1 | sed -E 's/.*\[guardrail_intervened\][[:space:]]*//; s/[[:space:]]+$//')
  printf '[blocked by guardrail] %s\n' "${msg:-Blocked by guardrail.}"
  log "note: the ADP guardrail $GUARDRAIL_NAME intervened on provider $LLM_PROVIDER; the A2A task is marked failed (rpk exit $rc), which is the expected outcome for this demo"
  exit 0
fi

if (( rc != 0 )); then
  if [[ -n "$ctx" ]]; then
    warn "the call failed while continuing conversation $ctx; if it is stale, retry with --new"
  fi
  exit "$rc"
fi
