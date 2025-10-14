#!/bin/bash
set -euo pipefail

# Script to launch distributed MapReduce wordcount job on cluster machines
# and optionally run it multiple times for benchmarking.
#
# Usage:
#   ./run_cluster.sh [--runs N] [--wait] [--output FILE] [--workers HOST1,HOST2,...] [--warc]
#
# Options:
#   --runs N              Number of times to run the job (default: 1)
#   --wait                Wait for job completion and show results after each run
#   --output FILE         Save output to specified file (in addition to console)
#   --workers HOST1,...   Comma-separated list of worker hosts (worker i processes file i)
#   --master HOST         Master hostname (default: tp-4b01-10)
#   --warc                Use Common Crawl WARC files (0000X.warc.wet) instead of split_X.txt
#   --warc-dir DIR        Directory containing WARC files (default: /cal/commoncrawl)
#   --help                Show this help message
#
# Environment overrides:
#   MASTER          Master hostname (default: tp-4b01-10)
#   WORKERS         Space-separated worker hostnames (default: tp-4b01-11..20)
#   CONTROL_PORT    Control port for master (default: 5374)
#   SHUFFLE_BASE    Base port for shuffle (default: 6200)
#   RESULTS_ROOT    Directory for benchmark results (default: cluster_results)

RUNS=1
WAIT_FOR_COMPLETION=false
OUTPUT_FILE=""
WORKERS_ARG=""
MASTER_ARG=""
USE_WARC=false
WARC_DIR="/cal/commoncrawl"

# Parse command line arguments
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
        --wait)
            WAIT_FOR_COMPLETION=true
            shift
            ;;
        --output)
            OUTPUT_FILE="${2:-}"
            if [[ -z "$OUTPUT_FILE" ]]; then
                echo "Error: --output requires a filename" >&2
                exit 1
            fi
            shift 2
            ;;
        --workers)
            WORKERS_ARG="${2:-}"
            if [[ -z "$WORKERS_ARG" ]]; then
                echo "Error: --workers requires a comma-separated list" >&2
                exit 1
            fi
            shift 2
            ;;
        --master)
            MASTER_ARG="${2:-}"
            if [[ -z "$MASTER_ARG" ]]; then
                echo "Error: --master requires a hostname" >&2
                exit 1
            fi
            shift 2
            ;;
        --warc)
            USE_WARC=true
            shift
            ;;
        --warc-dir)
            WARC_DIR="${2:-}"
            if [[ -z "$WARC_DIR" ]]; then
                echo "Error: --warc-dir requires a directory path" >&2
                exit 1
            fi
            USE_WARC=true
            shift 2
            ;;
        --help|-h)
            awk 'NR>2 && /^#/ {sub(/^# ?/, ""); print} /^[^#]/ && NR>2 {exit}' "$0"
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            echo "Use --help for usage information" >&2
            exit 1
            ;;
    esac
done

# Configuration
USER="${USER:-blepourt-25}"

# Handle master configuration
if [[ -n "$MASTER_ARG" ]]; then
    MASTER="$MASTER_ARG"
else
    MASTER="${MASTER:-tp-4b01-10}"
fi

# Handle workers configuration
if [[ -n "$WORKERS_ARG" ]]; then
    # Convert comma-separated list to space-separated
    WORKERS="${WORKERS_ARG//,/ }"
else
    WORKERS="${WORKERS:-tp-4b01-11 tp-4b01-12 tp-4b01-13 tp-4b01-14 tp-4b01-15 tp-4b01-16 tp-4b01-17 tp-4b01-18 tp-4b01-19 tp-4b01-20}"
fi

CONTROL_PORT="${CONTROL_PORT:-5374}"
SHUFFLE_BASE="${SHUFFLE_BASE:-6200}"
RESULTS_ROOT="${RESULTS_ROOT:-cluster_results}"

# Convert workers to array
WORKERS_ARRAY=($WORKERS)
NUM_WORKERS=${#WORKERS_ARRAY[@]}

# Function to output to both console and file (if specified)
log_output() {
    echo "$@"
    if [[ -n "$OUTPUT_FILE" ]]; then
        echo "$@" >> "$OUTPUT_FILE"
    fi
}

# Initialize output file if specified
if [[ -n "$OUTPUT_FILE" ]]; then
    # Create or truncate the output file
    : > "$OUTPUT_FILE"
    echo "Output will be saved to: $OUTPUT_FILE"
fi

log_output "=============================================="
log_output "Distributed MapReduce Cluster Benchmark"
log_output "=============================================="
log_output "Master:       $MASTER"
log_output "Workers:      ${WORKERS_ARRAY[0]} ... ${WORKERS_ARRAY[-1]} ($NUM_WORKERS total)"
log_output "Runs:         $RUNS"
log_output "Control port: $CONTROL_PORT"
log_output "Shuffle base: $SHUFFLE_BASE"
if [[ "$USE_WARC" == true ]]; then
    log_output "Input mode:   Common Crawl WARC files"
    log_output "WARC dir:     $WARC_DIR"
else
    log_output "Input mode:   Split files (split_X.txt)"
fi
if [[ -n "$OUTPUT_FILE" ]]; then
    log_output "Output file:  $OUTPUT_FILE"
fi
log_output ""
log_output "Worker → Input file mapping:"
for i in "${!WORKERS_ARRAY[@]}"; do
    worker_num=$((i + 1))
    file_index=$i
    if [[ "$USE_WARC" == true ]]; then
        file_name=$(printf "CC-MAIN-20230320083513-20230320113513-%05d.warc.wet" $file_index)
        log_output "  Worker $worker_num (${WORKERS_ARRAY[$i]}) → $WARC_DIR/$file_name"
    else
        log_output "  Worker $worker_num (${WORKERS_ARRAY[$i]}) → ~/split_${worker_num}.txt"
    fi
done
log_output "=============================================="
log_output ""

# Create results directory if running multiple times
if [[ $RUNS -gt 1 ]] || [[ "$WAIT_FOR_COMPLETION" == true ]]; then
    timestamp="$(date +%Y%m%d-%H%M%S)"
    OUTPUT_DIR="${RESULTS_ROOT}/${timestamp}"
    
    # Create directory locally
    mkdir -p "$OUTPUT_DIR"
    echo "Results directory: $OUTPUT_DIR"
    
    TIMINGS_FILE="${OUTPUT_DIR}/timings.csv"
    echo "run,start_time,end_time,elapsed_seconds,exit_code" > "$TIMINGS_FILE"
fi

# Function to cleanup processes on all machines
cleanup_cluster() {
    log_output ""
    log_output "Cleaning up cluster processes..."
    ssh "$MASTER" "pkill -f 'python3.*serveur.py' 2>/dev/null || true" &
    for worker in "${WORKERS_ARRAY[@]}"; do
        ssh "$worker" "pkill -f 'python3.*client.py' 2>/dev/null || true" &
    done
    wait
    sleep 1
}

# Function to check if master is still running
is_master_running() {
    ssh "$MASTER" "pgrep -f 'python3.*serveur.py' >/dev/null 2>&1"
}

# Function to count running workers
count_running_workers() {
    local count=0
    for worker in "${WORKERS_ARRAY[@]}"; do
        if ssh "$worker" "pgrep -f 'python3.*client.py' >/dev/null 2>&1"; then
            ((count++)) || true
        fi
    done
    echo "$count"
}

# Function to wait for job completion
wait_for_job() {
    local timeout=600  # 10 minutes timeout
    local elapsed=0
    local check_interval=2
    
    log_output "Waiting for job completion (timeout: ${timeout}s)..."
    
    while [[ $elapsed -lt $timeout ]]; do
        if ! is_master_running; then
            log_output "Master process completed"
            return 0
        fi
        
        local running_workers=$(count_running_workers)
        if [[ $running_workers -eq 0 ]]; then
            log_output "All workers completed"
            sleep 1
            if ! is_master_running; then
                return 0
            fi
        fi
        
        sleep $check_interval
        ((elapsed += check_interval))
        
        # Show progress every 10 seconds
        if [[ $((elapsed % 10)) -eq 0 ]]; then
            log_output "  [$elapsed/${timeout}s] Master running, $running_workers/$NUM_WORKERS workers active"
        fi
    done
    
    log_output "WARNING: Timeout reached after ${timeout}s"
    return 1
}

# Function to launch the cluster
launch_cluster() {
    local run_label="$1"
    
    log_output "[$run_label] Launching master on ${MASTER}..."
    ssh "$MASTER" \
        "nohup python3 ~/serveur.py --host 0.0.0.0 --port ${CONTROL_PORT} --num-workers ${NUM_WORKERS} > ~/mapreduce_master.log 2>&1 &"
    
    sleep 2  # Give master time to start
    
    # Verify master is running
    if ! is_master_running; then
        log_output "ERROR: Master failed to start"
        return 1
    fi
    
    log_output "[$run_label] Master started successfully"
    
    # Launch workers
    local worker_id=1
    local worker_index=0
    for worker in "${WORKERS_ARRAY[@]}"; do
        if [[ "$USE_WARC" == true ]]; then
            # Use WARC files starting from index 0
            local warc_file=$(printf "${WARC_DIR}/CC-MAIN-20230320083513-20230320113513-%05d.warc.wet" $worker_index)
            log_output "[$run_label] Launching worker ${worker_id} on ${worker} (WARC file: index $worker_index)..."
            ssh "$worker" \
                "nohup python3 ~/client.py ${worker_id} ${WORKERS} --master-host ${MASTER} --control-port ${CONTROL_PORT} --shuffle-port-base ${SHUFFLE_BASE} --split-id \"${warc_file}\" > ~/mapreduce_worker_${worker_id}.log 2>&1 &" &
        else
            # Use split_X.txt files (default mode)
            log_output "[$run_label] Launching worker ${worker_id} on ${worker} (split_${worker_id}.txt)..."
            ssh "$worker" \
                "nohup python3 ~/client.py ${worker_id} ${WORKERS} --master-host ${MASTER} --control-port ${CONTROL_PORT} --shuffle-port-base ${SHUFFLE_BASE} > ~/mapreduce_worker_${worker_id}.log 2>&1 &" &
        fi
        ((worker_id++))
        ((worker_index++))
    done
    
    wait  # Wait for all SSH commands to complete
    
    sleep 1
    
    # Verify workers started
    local running_workers=$(count_running_workers)
    log_output "[$run_label] $running_workers/$NUM_WORKERS workers started"
    
    if [[ $running_workers -lt $NUM_WORKERS ]]; then
        log_output "WARNING: Not all workers started successfully"
    fi
}

# Function to collect logs
collect_logs() {
    local run_label="$1"
    local output_dir="$2"
    
    log_output "[$run_label] Collecting logs..."
    
    # Collect master log
    if scp "${MASTER}:~/mapreduce_master.log" "${output_dir}/${run_label}_master.log" 2>/dev/null; then
        log_output "[$run_label] Master log retrieved"
    else
        log_output "[$run_label] WARNING: Could not retrieve master log"
    fi
    
    # Collect worker logs
    local worker_id=1
    for worker in "${WORKERS_ARRAY[@]}"; do
        if scp "${worker}:~/mapreduce_worker_${worker_id}.log" \
            "${output_dir}/${run_label}_worker_${worker_id}.log" 2>/dev/null; then
            : # Success, do nothing
        else
            log_output "[$run_label] WARNING: Could not retrieve worker ${worker_id} log"
        fi
        ((worker_id++))
    done
}

# Function to extract and display results
show_results() {
    local log_file="$1"
    
    if [[ ! -f "$log_file" ]]; then
        log_output "Log file not found: $log_file"
        return 1
    fi
    
    log_output ""
    log_output "==================== Results ===================="
    
    # Extract worker registration info
    local registered=$(grep -c "Worker .* registered" "$log_file" 2>/dev/null || echo "0")
    log_output "Workers registered: $registered/$NUM_WORKERS"
    
    # Extract map completion info
    if grep -q "All map_finished received" "$log_file" 2>/dev/null; then
        log_output "Map phase: ✓ Completed"
    else
        log_output "Map phase: ✗ Not completed"
    fi
    
    # Extract reduce completion info
    if grep -q "All.*reduce.*sent" "$log_file" 2>/dev/null; then
        log_output "Reduce phase: ✓ Completed"
    else
        log_output "Reduce phase: ✗ Not completed"
    fi
    
    # Show final wordcount summary (top 10 words)
    if grep -q "Final wordcount:" "$log_file" 2>/dev/null; then
        log_output ""
        log_output "Top 10 words:"
        while IFS= read -r line; do
            log_output "  $line"
        done < <(awk '/Final wordcount:/{flag=1; next} flag && /^[^ ]+: [0-9]+$/ {print $0} flag && !/^[^ ]+: [0-9]+$/ && NF>0 {exit}' "$log_file" | head -10)
        
        # Count total unique words and occurrences
        local unique_words=$(awk '/Final wordcount:/{flag=1; next} flag && /^[^ ]+: [0-9]+$/ {count++} END{print count}' "$log_file")
        local total_occurrences=$(awk '/Final wordcount:/{flag=1; next} flag && /^[^ ]+: [0-9]+$/ {total+=$2} END{print total}' "$log_file")
        log_output ""
        log_output "Total unique words: $unique_words"
        log_output "Total occurrences: $total_occurrences"
    else
        log_output ""
        log_output "WARNING: No final wordcount found in log"
    fi
    
    log_output "================================================="
    log_output ""
}

# Main execution loop
for run in $(seq 1 "$RUNS"); do
    run_label="run_$(printf "%02d" "$run")"
    
    log_output ""
    log_output "=============================================="
    log_output "Starting run $run/$RUNS"
    log_output "=============================================="
    
    # Cleanup any existing processes
    if [[ $run -gt 1 ]]; then
        cleanup_cluster
    fi
    
    # Record start time
    start_time=$(date +%s)
    start_timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    # Launch cluster
    if ! launch_cluster "$run_label"; then
        log_output "ERROR: Failed to launch cluster for $run_label"
        if [[ $RUNS -gt 1 ]]; then
            echo "0,${start_timestamp},N/A,0,1" >> "$TIMINGS_FILE"
        fi
        cleanup_cluster
        continue
    fi
    
    # Wait for completion if requested or if running multiple times
    if [[ "$WAIT_FOR_COMPLETION" == true ]] || [[ $RUNS -gt 1 ]]; then
        if wait_for_job; then
            exit_code=0
        else
            exit_code=1
        fi
        
        end_time=$(date +%s)
        end_timestamp=$(date '+%Y-%m-%d %H:%M:%S')
        elapsed=$((end_time - start_time))
        
        log_output "[$run_label] Completed in ${elapsed}s (exit code: $exit_code)"
        
        # Record timing
        if [[ $RUNS -gt 1 ]] || [[ "$WAIT_FOR_COMPLETION" == true ]]; then
            echo "${run},${start_timestamp},${end_timestamp},${elapsed},${exit_code}" >> "$TIMINGS_FILE"
            
            # Collect logs
            collect_logs "$run_label" "$OUTPUT_DIR"
            
            # Show results
            show_results "${OUTPUT_DIR}/${run_label}_master.log"
        fi
        
        # Cleanup for next run
        cleanup_cluster
    else
        log_output ""
        log_output "[$run_label] Job launched in background"
        log_output ""
        log_output "To monitor progress:"
        log_output "  ssh $MASTER 'tail -f ~/mapreduce_master.log'"
        log_output ""
        log_output "To view results:"
        log_output "  ssh $MASTER 'tail -n 50 ~/mapreduce_master.log'"
        log_output ""
        log_output "To stop the cluster:"
        log_output "  ssh $MASTER 'pkill -f serveur.py'"
        for worker in "${WORKERS_ARRAY[@]}"; do
            log_output "  ssh $worker 'pkill -f client.py'"
        done
    fi
done

# Final summary
if [[ $RUNS -gt 1 ]] || [[ "$WAIT_FOR_COMPLETION" == true ]]; then
    log_output ""
    log_output "=============================================="
    log_output "Benchmark Summary"
    log_output "=============================================="
    log_output "Results directory: $OUTPUT_DIR"
    log_output "Timings file: $TIMINGS_FILE"
    log_output ""
    
    if [[ -f "$TIMINGS_FILE" ]]; then
        log_output "Run timings:"
        if [[ -n "$OUTPUT_FILE" ]]; then
            column -t -s, "$TIMINGS_FILE" | tee -a "$OUTPUT_FILE"
        else
            column -t -s, "$TIMINGS_FILE"
        fi
        log_output ""
        
        # Calculate statistics (always show, even for 1 run)
        completed_runs=$(awk -F, 'NR>1 && $5==0 {count++} END {print count+0}' "$TIMINGS_FILE")
        total_runs=$(awk -F, 'NR>1 {count++} END {print count+0}' "$TIMINGS_FILE")
        
        log_output "Statistics:"
        log_output "  Completed runs: ${completed_runs}/${total_runs}"
        
        if [[ $completed_runs -gt 0 ]]; then
            avg_time=$(awk -F, 'NR>1 && $5==0 {sum+=$4; count++} END {if(count>0) printf "%.1f", sum/count; else print 0}' "$TIMINGS_FILE")
            min_time=$(awk -F, 'NR>1 && $5==0 {if(min=="" || $4<min) min=$4} END {print min}' "$TIMINGS_FILE")
            max_time=$(awk -F, 'NR>1 && $5==0 {if(max=="" || $4>max) max=$4} END {print max}' "$TIMINGS_FILE")
            
            log_output "  Execution time:"
            if [[ $completed_runs -gt 1 ]]; then
                log_output "    Average: ${avg_time}s"
                log_output "    Min: ${min_time}s"
                log_output "    Max: ${max_time}s"
            else
                log_output "    Total: ${max_time}s"
            fi
        fi
    fi
fi

log_output ""
log_output "Done!"

if [[ -n "$OUTPUT_FILE" ]]; then
    echo ""
    echo "Full output saved to: $OUTPUT_FILE"
fi
