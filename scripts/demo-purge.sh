#!/usr/bin/env bash
# scripts/demo-purge.sh
# Runs the retention purge now instead of waiting for the hourly
# pg-agent-memory-purge pipeline: deletes every memory whose expires_at has
# passed, as the admin. "Forget" in the agent only sets expires_at; this is the
# one statement that deletes memories, and the trigger logs each delete into
# change_log, so the deletes reach agent.pg.memory.cdc and memory_audit within
# seconds (docs/DEMO-SCRIPT.md act 6).
#
# Usage: scripts/demo-purge.sh        (make purge)
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"
load_env
require_cmd psql

info "Deleting expired memories (what pg-agent-memory-purge does once an hour)"
pg_admin_psql "WITH d AS (
  DELETE FROM ${PG_SCHEMA}.memories WHERE expires_at < now() RETURNING memory_id, subject, kind
) SELECT count(*) AS memories_purged, count(DISTINCT subject) AS subjects FROM d;"
ok "purge complete; each delete is in the change stream (watch: make observe)"
