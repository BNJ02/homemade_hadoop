#!/bin/bash
# Script to display benchmark summary from cluster_results directory

DIR="$1"
if [[ -z "$DIR" ]]; then
    # Use the most recent results directory
    DIR=$(ls -td ~/cluster_results/*/ 2>/dev/null | head -1)
fi

if [[ -z "$DIR" ]] || [[ ! -d "$DIR" ]]; then
    echo "No results directory found"
    echo "Usage: $0 [results_directory]"
    echo "Example: $0 ~/cluster_results/20251014-013031"
    exit 1
fi

echo "=============================================="
echo "Benchmark Summary"
echo "=============================================="
echo "Results directory: $DIR"
echo ""

if [[ -f "$DIR/timings.csv" ]]; then
    echo "Run timings:"
    column -t -s, "$DIR/timings.csv"
    echo ""
    
    awk -F, 'NR>1 && $5==0 {sum+=$4; count++; if(min=="" || $4<min) min=$4; if(max=="" || $4>max) max=$4} END {
        print "Statistics:"
        print "  Completed runs: " count "/" (NR-1)
        if(count>0) {
            avg=sum/count
            printf "  Average time: %.1fs\n", avg
            print "  Min time: " min "s"
            print "  Max time: " max "s"
        }
    }' "$DIR/timings.csv"
    echo ""
    
    # Show number of log files
    master_logs=$(ls -1 "$DIR"/run_*_master.log 2>/dev/null | wc -l)
    echo "Log files collected: $master_logs master logs"
    echo ""
    echo "To view detailed results of a specific run:"
    echo "  grep -A 20 'Final wordcount' $DIR/run_0X_master.log | head -25"
else
    echo "No timings.csv found in $DIR"
fi
