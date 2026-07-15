#!/usr/bin/env bash

set -euo pipefail

DEFAULT_DAG_IDS="linkedin_jobs_ingestion_startup,etat_geneve_jobs_ingestion_startup"
RAW_DAG_IDS="${STARTUP_DAG_IDS:-${STARTUP_DAG_ID:-$DEFAULT_DAG_IDS}}"
read -r -a DAG_IDS <<< "${RAW_DAG_IDS//,/ }"
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
    local dag_id="$1"
    airflow dags list 2>/dev/null | grep -qE "(^|[[:space:]])${dag_id}([[:space:]]|$)"
}

dag_is_unpaused() {
    local dag_id="$1"
    local dags_json

    dags_json="$(airflow dags list --output json 2>/dev/null || true)"
    if [ -z "$dags_json" ]; then
        return 1
    fi

    AIRFLOW_DAGS_JSON="$dags_json" "$PYTHON_BIN" - "$dag_id" <<'PY'
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
    local dag_id="$1"
    log "Waiting for DAG ${dag_id} discovery..."

    for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
        if dag_is_listed "$dag_id"; then
            log "DAG ${dag_id} detected."
            return 0
        fi

        if [ "$attempt" -eq "$MAX_ATTEMPTS" ]; then
            fail "DAG ${dag_id} was not discovered after ${MAX_ATTEMPTS} attempts."
        fi

        log "DAG ${dag_id} not available yet, retrying in ${RETRY_DELAY}s..."
        sleep "$RETRY_DELAY"
    done
}

ensure_dag_unpaused() {
    local dag_id="$1"
    log "Ensuring DAG ${dag_id} is unpaused..."
    airflow dags unpause "$dag_id" >/dev/null

    if ! dag_is_unpaused "$dag_id"; then
        fail "DAG ${dag_id} is still paused after the unpause command."
    fi

    log "DAG ${dag_id} is unpaused."
}

trigger_startup_dag() {
    local dag_id="$1"
    local startup_id="$2"
    local claim_output
    local claim_status
    local trigger_error
    local run_id="startup__${startup_id}"

    log "Prepared startup trigger identifiers for dag=${dag_id}: startup_id=${startup_id} run_id=${run_id}"

    set +e
    claim_output="$(
        "$PYTHON_BIN" -m job_matcher.startup_trigger claim \
            --dag-id "$dag_id" \
            --startup-id "$startup_id" \
            --run-id "$run_id"
    )"
    claim_status=$?
    set -e

    if [ "$claim_status" -eq 0 ]; then
        log "Startup claim acquired: ${claim_output}"
    elif [ "$claim_status" -eq 10 ]; then
        log "Startup already claimed for dag=${dag_id} startup_id=${startup_id}: ${claim_output}"
        return 0
    else
        fail "Unable to claim startup trigger slot for dag=${dag_id} startup_id=${startup_id}."
    fi

    log "Triggering DAG ${dag_id} with run_id ${run_id}..."

    if airflow dags trigger --run-id "$run_id" "$dag_id"; then
        "$PYTHON_BIN" -m job_matcher.startup_trigger mark-status \
            --dag-id "$dag_id" \
            --startup-id "$startup_id" \
            --status triggered >/dev/null
        log "DAG ${dag_id} triggered successfully."
        return 0
    fi

    trigger_error="airflow dags trigger failed for dag=${dag_id} run_id=${run_id}"
    "$PYTHON_BIN" -m job_matcher.startup_trigger mark-status \
        --dag-id "$dag_id" \
        --startup-id "$startup_id" \
        --status failed \
        --last-error "$trigger_error" >/dev/null
    fail "$trigger_error"
}

main() {
    local startup_id
    local dag_id

    if [ "${#DAG_IDS[@]}" -eq 0 ]; then
        fail "No startup DAG IDs were configured."
    fi

    wait_for_airflow_db
    startup_id="${AIRFLOW_STARTUP_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"

    for dag_id in "${DAG_IDS[@]}"; do
        wait_for_dag_discovery "$dag_id"
        ensure_dag_unpaused "$dag_id"
        trigger_startup_dag "$dag_id" "$startup_id"
    done
}

main "$@"
