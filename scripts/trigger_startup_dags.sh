#!/usr/bin/env bash

set -euo pipefail

DAG_ID="${STARTUP_DAG_ID:-linkedin_jobs_ingestion_startup}"
MAX_ATTEMPTS="${STARTUP_DAG_MAX_ATTEMPTS:-30}"
RETRY_DELAY="${STARTUP_DAG_RETRY_DELAY:-5}"

if command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
else
    echo "[startup-trigger] Error: neither python nor python3 is available in PATH." >&2
    exit 1
fi

log() {
    echo "[startup-trigger] $*"
}

fail() {
    log "Error: $*"
    exit 1
}

dag_is_listed() {
    airflow dags list 2>/dev/null | grep -qE "(^|[[:space:]])${DAG_ID}([[:space:]]|$)"
}

dag_is_unpaused() {
    local dags_json

    dags_json="$(airflow dags list --output json 2>/dev/null || true)"
    if [ -z "$dags_json" ]; then
        return 1
    fi

    AIRFLOW_DAGS_JSON="$dags_json" "$PYTHON_BIN" - "$DAG_ID" <<'PY'
import json
import os
import sys

dag_id = sys.argv[1]
raw = os.environ.get("AIRFLOW_DAGS_JSON", "")

try:
    data = json.loads(raw)
except json.JSONDecodeError:
    raise SystemExit(1)

for row in data:
    if row.get("dag_id") != dag_id:
        continue

    if row.get("is_paused") in (False, "False", "false", 0, "0"):
        raise SystemExit(0)
    raise SystemExit(1)

raise SystemExit(1)
PY
}

wait_for_airflow_db() {
    log "Waiting for Airflow metadata database availability..."

    for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
        if airflow db check >/dev/null 2>&1; then
            log "Airflow metadata database is available."
            return 0
        fi

        if [ "$attempt" -eq "$MAX_ATTEMPTS" ]; then
            fail "Airflow metadata database is unavailable after ${MAX_ATTEMPTS} attempts."
        fi

        log "Airflow database unavailable, retrying in ${RETRY_DELAY}s..."
        sleep "$RETRY_DELAY"
    done
}

wait_for_dag_discovery() {
    log "Waiting for DAG ${DAG_ID} discovery..."

    for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
        if dag_is_listed; then
            log "DAG ${DAG_ID} detected."
            return 0
        fi

        if [ "$attempt" -eq "$MAX_ATTEMPTS" ]; then
            fail "DAG ${DAG_ID} was not discovered after ${MAX_ATTEMPTS} attempts."
        fi

        log "DAG ${DAG_ID} not available yet, retrying in ${RETRY_DELAY}s..."
        sleep "$RETRY_DELAY"
    done
}

ensure_dag_unpaused() {
    log "Ensuring DAG ${DAG_ID} is unpaused..."
    airflow dags unpause "$DAG_ID" >/dev/null

    if ! dag_is_unpaused; then
        fail "DAG ${DAG_ID} is still paused after the unpause command."
    fi

    log "DAG ${DAG_ID} is unpaused."
}

main() {
    local claim_output
    local claim_status
    local trigger_error

    wait_for_airflow_db
    wait_for_dag_discovery
    ensure_dag_unpaused

    STARTUP_ID="${AIRFLOW_STARTUP_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
    RUN_ID="startup__${STARTUP_ID}"

    log "Prepared startup trigger identifiers: startup_id=${STARTUP_ID} run_id=${RUN_ID}"

    set +e
    claim_output="$(
        "$PYTHON_BIN" -m job_matcher.startup_trigger claim \
            --dag-id "$DAG_ID" \
            --startup-id "$STARTUP_ID" \
            --run-id "$RUN_ID"
    )"
    claim_status=$?
    set -e

    if [ "$claim_status" -eq 0 ]; then
        log "Startup claim acquired: ${claim_output}"
    elif [ "$claim_status" -eq 10 ]; then
        log "Startup already claimed for dag=${DAG_ID} startup_id=${STARTUP_ID}: ${claim_output}"
        exit 0
    else
        fail "Unable to claim startup trigger slot for dag=${DAG_ID} startup_id=${STARTUP_ID}."
    fi

    log "Triggering DAG ${DAG_ID} with run_id ${RUN_ID}..."

    if airflow dags trigger --run-id "$RUN_ID" "$DAG_ID"; then
        "$PYTHON_BIN" -m job_matcher.startup_trigger mark-status \
            --dag-id "$DAG_ID" \
            --startup-id "$STARTUP_ID" \
            --status triggered >/dev/null
        log "DAG ${DAG_ID} triggered successfully."
        exit 0
    fi

    trigger_error="airflow dags trigger failed for dag=${DAG_ID} run_id=${RUN_ID}"
    "$PYTHON_BIN" -m job_matcher.startup_trigger mark-status \
        --dag-id "$DAG_ID" \
        --startup-id "$STARTUP_ID" \
        --status failed \
        --last-error "$trigger_error" >/dev/null
    fail "$trigger_error"
}

main "$@"
