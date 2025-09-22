#!/bin/bash
# Ultra-minimal : démarre les serveurs, lance le client, puis envoie SHUTDOWN.
set -e

HOSTS=(tp-1a207-34 tp-1a207-35 tp-1a207-36)
PORT=5374

# 1) Démarrer les serveurs (détachés, sans PID)
# nohup pour éviter que le serveur meurt si la connexion SSH est fermée
for h in "${HOSTS[@]}"; do
  ssh "$h" 'nohup python3 ~/serveur.py </dev/null >/dev/null 2>&1 &'
done

# (optionnel) petite pause pour laisser l’écoute s’ouvrir
sleep 0.5

# 2) Lancer le client multi-hôtes
python3 ~/client.py "${HOSTS[@]}"

# 3) Arrêt propre via commande SHUTDOWN (socket)
for h in "${HOSTS[@]}"; do
  ssh "$h" "python3 -c 'import socket,struct; s=socket.create_connection((\"127.0.0.1\",$PORT),2); m=b\"SHUTDOWN\"; s.sendall(struct.pack(\">I\",len(m))+m)'"
done

echo "FIN TP1"
