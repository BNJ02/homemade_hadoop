#!/bin/bash
set -euo pipefail   # Quitte si erreur, variable non définie ou échec de pipeline

USER=blepourt-25
MASTER="tp-4b01-00"
WORKERS=(tp-4b01-01 tp-4b01-02 tp-4b01-03)
CONTROL_PORT=5374
SHUFFLE_BASE=6200

# Liste des hôtes travailleurs (séparés par des espaces)
host_args="${WORKERS[*]}"

echo "Lancement du maître sur ${MASTER}..."
# Démarre le serveur maître en arrière-plan via SSH
ssh "${MASTER}" \
  "nohup python3 ~/serveur.py --host 0.0.0.0 --port ${CONTROL_PORT} --num-workers ${#WORKERS[@]} > ~/mapreduce_master.log 2>&1 &"
sleep 1  # Laisse le temps au maître de démarrer

# Boucle sur chaque nœud travailleur
for idx in "${!WORKERS[@]}"; do
  host="${WORKERS[$idx]}"
  worker_id=$((idx + 1))
  echo "Lancement du worker ${worker_id} sur ${host}..."
  # Démarre le client MapReduce sur l'hôte courant
  ssh "${host}" \
    "nohup python3 ~/client.py ${worker_id} ${host_args} --master-host ${MASTER} --control-port ${CONTROL_PORT} --shuffle-port-base ${SHUFFLE_BASE} > ~/mapreduce_worker_${worker_id}.log 2>&1 &"
done

echo -e "Toutes les tâches sont lancées."
echo "Log maître : ssh ${USER}@${MASTER} 'tail -n 20 ~/mapreduce_master.log'"
