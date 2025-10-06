#!/bin/bash
set -euo pipefail

USER=blepourt-25
MASTER="tp-1a207-37"
WORKERS=(tp-1a207-34 tp-1a207-35 tp-1a207-36)
CONTROL_PORT=5374
SHUFFLE_BASE=6200

host_args="${WORKERS[*]}"

echo "Launching master on ${MASTER}..."
ssh "${MASTER}" "nohup python3 ~/serveur.py --host 0.0.0.0 --port ${CONTROL_PORT} --num-workers ${#WORKERS[@]} > ~/mapreduce_master.log 2>&1 &"
sleep 1

for idx in "${!WORKERS[@]}"; do
  host="${WORKERS[$idx]}"
  worker_id=$((idx + 1))
  echo "Launching worker ${worker_id} on ${host}..."
  ssh "${host}" \
    "nohup python3 ~/client.py ${worker_id} ${host_args} --master-host ${MASTER} --control-port ${CONTROL_PORT} --shuffle-port-base ${SHUFFLE_BASE} > ~/mapreduce_worker_${worker_id}.log 2>&1 &"
done

echo "All tasks launched."
echo "Master log: ssh ${USER}@${MASTER} 'tail -n 20 ~/mapreduce_master.log'"
