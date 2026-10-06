#!/usr/bin/env bash
#
# Low-overhead shell supervisor for the order flow engine.
# Auto-restarts the Python engine within 10s of any crash and forces a ledger
# recovery read on every boot (the engine itself does this on start).
#
set -u

ENGINE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-python3}"
RESTART_DELAY="${RESTART_DELAY:-5}"
ENGINE_ARGS="${ENGINE_ARGS:---stats-interval 30}"
LOG_FILE="${LOG_FILE:-$ENGINE_DIR/orderflow_engine.supervisor.log}"

cd "$ENGINE_DIR" || exit 1

log() { printf '%s [watchdog] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$LOG_FILE"; }

log "supervisor started (pid $$) -> $PYTHON orderflow_engine.py $ENGINE_ARGS"

while true; do
    start=$(date +%s)
    # shellcheck disable=SC2086
    "$PYTHON" "$ENGINE_DIR/orderflow_engine.py" $ENGINE_ARGS
    rc=$?
    uptime=$(( $(date +%s) - start ))
    log "engine exited rc=$rc after ${uptime}s; restarting in ${RESTART_DELAY}s"
    sleep "$RESTART_DELAY"
done
