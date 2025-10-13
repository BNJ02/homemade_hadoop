#!/bin/bash
set -euo pipefail

# Automated harness to launch the MapReduce wordcount job repeatedly on a single
# machine and capture per-run timings as well as the aggregated wordcount output.
#
# Usage:
#   ./run_benchmarks.sh [--runs N]
# Environment overrides:
#   MASTER_HOST     Hostname/IP where the master listens (default: current host)
#   CONTROL_PORT    Control port for the master (default: 5374)
#   SHUFFLE_BASE    Base port for shuffle listeners (default: 6200)
#   SPLIT_ID        Split suffix consumed by the worker (default: 1)
#   RESULTS_ROOT    Parent directory for benchmark artifacts (default: bench_results)

RUNS=10

while [[ $# -gt 0 ]]; do
    case "$1" in
        --runs)
            RUNS="${2:-}"
            if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [[ "$RUNS" -lt 1 ]]; then
                echo "Invalid value for --runs: $RUNS" >&2
                exit 1
            fi
            shift 2
            ;;
        --help|-h)
            awk 'NR>2 {print} NR==2 {print ""}' "$0" | sed -n '1,20p'
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

MASTER_HOST="${MASTER_HOST:-$(hostname -f 2>/dev/null || hostname)}"
CONTROL_PORT="${CONTROL_PORT:-5374}"
SHUFFLE_BASE="${SHUFFLE_BASE:-6200}"
SPLIT_ID="${SPLIT_ID:-1}"
RESULTS_ROOT="${RESULTS_ROOT:-bench_results}"

SPLIT_FILE="split_${SPLIT_ID}.txt"
if [[ ! -f "$SPLIT_FILE" ]]; then
    echo "Expected split file '$SPLIT_FILE' not found in $(pwd)" >&2
    exit 1
fi

timestamp="$(date +%Y%m%d-%H%M%S)"
OUTPUT_DIR="${RESULTS_ROOT}/${timestamp}"
mkdir -p "$OUTPUT_DIR"

TIMINGS_FILE="${OUTPUT_DIR}/timings.csv"
echo "run,elapsed_seconds,user_seconds,sys_seconds,exit_code" > "$TIMINGS_FILE"

echo "Benchmark directory: $OUTPUT_DIR"
echo "Running $RUNS iterations..."

for run in $(seq 1 "$RUNS"); do
    run_label=$(printf "run_%02d" "$run")
    master_log="${OUTPUT_DIR}/${run_label}_master.log"
    worker_log="${OUTPUT_DIR}/${run_label}_worker.log"
    time_tmp="${OUTPUT_DIR}/${run_label}_time.tmp"
    wordcount_file="${OUTPUT_DIR}/${run_label}_wordcounts.txt"
    summary_file="${OUTPUT_DIR}/${run_label}_summary.txt"

    echo "[$run/$RUNS] Starting master..."
    python3 serveur.py \
        --host 0.0.0.0 \
        --port "$CONTROL_PORT" \
        --num-workers 1 \
        > "$master_log" 2>&1 &
    master_pid=$!

    cleanup_master() {
        if kill -0 "$master_pid" 2>/dev/null; then
            kill "$master_pid" 2>/dev/null || true
            wait "$master_pid" 2>/dev/null || true
        fi
    }

    trap 'cleanup_master' INT TERM ERR

    sleep 1

    echo "[$run/$RUNS] Launching worker and timing execution..."
    set +e
    python3 - "$MASTER_HOST" "$CONTROL_PORT" "$SHUFFLE_BASE" "$SPLIT_ID" "$worker_log" "$time_tmp" <<'PYTHON'
import resource
import subprocess
import sys
import time

master_host, control_port, shuffle_base, split_id, log_path, timing_path = sys.argv[1:7]
cmd = [
    "python3",
    "client.py",
    "1",
    master_host,
    "--master-host",
    master_host,
    "--control-port",
    control_port,
    "--shuffle-port-base",
    shuffle_base,
    "--split-id",
    split_id,
]

before = resource.getrusage(resource.RUSAGE_CHILDREN)
start = time.perf_counter()
with open(log_path, "wb") as log_file:
    proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
    exit_code = proc.wait()
elapsed = time.perf_counter() - start
after = resource.getrusage(resource.RUSAGE_CHILDREN)
user_time = after.ru_utime - before.ru_utime
sys_time = after.ru_stime - before.ru_stime

with open(timing_path, "w", encoding="utf-8") as out:
    out.write(f"elapsed={elapsed:.6f}\n")
    out.write(f"user={user_time:.6f}\n")
    out.write(f"sys={sys_time:.6f}\n")
    out.write(f"exit={exit_code}\n")

sys.exit(exit_code)
PYTHON
    client_status=$?
    set -e

    wait "$master_pid" || true
    cleanup_master
    trap - INT TERM ERR

    if [[ ! -s "$time_tmp" ]]; then
        echo "Timing information missing for ${run_label}; aborting." >&2
        exit 1
    fi

    elapsed=$(awk -F= '/^elapsed=/ {print $2}' "$time_tmp")
    user=$(awk -F= '/^user=/ {print $2}' "$time_tmp")
    sys=$(awk -F= '/^sys=/ {print $2}' "$time_tmp")
    exit_recorded=$(awk -F= '/^exit=/ {print $2}' "$time_tmp")
    echo "${run},${elapsed},${user},${sys},${exit_recorded}" >> "$TIMINGS_FILE"
    rm -f "$time_tmp"

    python3 - "$master_log" "$wordcount_file" "$summary_file" <<'PYTHON'
import sys
from pathlib import Path

log_path = Path(sys.argv[1])
wordcount_path = Path(sys.argv[2])
summary_path = Path(sys.argv[3])

counts = []
collect = False
for raw_line in log_path.read_text(encoding="utf-8", errors="ignore").splitlines():
    line = raw_line.strip()
    if not collect:
        if line == "Final wordcount:":
            collect = True
        continue
    if not line:
        break
    if ": " not in line:
        continue
    word, count = line.split(": ", 1)
    try:
        count_int = int(count)
    except ValueError:
        continue
    counts.append((word, count_int))

wordcount_path.write_text(
    "\n".join(f"{word}: {count}" for word, count in counts) + ("\n" if counts else ""),
    encoding="utf-8",
)

total_occurrences = sum(count for _, count in counts)
summary_path.write_text(
    f"mots_distincts={len(counts)}\noccurrences_total={total_occurrences}\n",
    encoding="utf-8",
)
PYTHON

    echo "[$run/$RUNS] Completed with status ${client_status}."
done

echo "All runs finished. Timings stored in ${TIMINGS_FILE}"
