#!/bin/bash
set -euo pipefail

HOSTS=(tp-1a207-34 tp-1a207-35 tp-1a207-36)
PORT=5374

start_server() {
  local host="$1"
  # - Utilise un shell distant (bash -lc) pour avoir une expansion correcte
  # - nohup détache des SIGHUP, redirection pour détacher I/O
  # - echo \$! renvoie le PID du python distant (attention à l'antislash)
  ssh "$host" 'bash -lc "nohup python3 ~/serveur.py > ~/serveur.log 2>&1 & echo \$! > ~/serveur.pid"'
}

# Arrêt doux via socket : envoie la commande SHUTDOWN au serveur local
stop_server_via_socket() {
  local host="$1"
  ssh "$host" "python3 - <<'PY' $PORT
import socket,struct,sys
port = int(sys.argv[1])
try:
    s = socket.create_connection(('127.0.0.1', port), timeout=2)
except Exception:
    sys.exit(1)
msg = b'SHUTDOWN'
s.sendall(struct.pack('>I', len(msg))); s.sendall(msg)
# Optionnel: lire la réponse
try:
    l = s.recv(4)
    if len(l) == 4:
        n = struct.unpack('>I', l)[0]
        _ = s.recv(n)
except Exception:
    pass
PY" || return 1
}

# Arrêt via PID (SIGTERM)
stop_server_via_pid() {
  local host="$1"
  ssh "$host" 'bash -lc "[ -s ~/serveur.pid ] && kill -TERM \$(cat ~/serveur.pid) || exit 1"'
}

# Dernier recours : matcher le process par motif
stop_server_force() {
  local host="$1"
  ssh "$host" "pkill -f 'python3 ~/serveur.py' || true"
}

cleanup() {
  echo "Interruption/fin détectée : arrêt des serveurs..."
  for h in "${HOSTS[@]}"; do
    stop_server_via_socket "$h" || stop_server_via_pid "$h" || stop_server_force "$h" || true
    ssh "$h" 'rm -f ~/serveur.pid || true' || true
  done
}

trap cleanup INT TERM EXIT

### Démarrage
for h in "${HOSTS[@]}"; do
  start_server "$h"
done
echo "Serveurs démarrés."

### Client multi-hôtes
python3 ~/client.py "${HOSTS[@]}"

### Arrêt propre
echo "Arrêt des serveurs..."
for h in "${HOSTS[@]}"; do
  stop_server_via_socket "$h" || stop_server_via_pid "$h" || stop_server_force "$h"
  ssh "$h" 'rm -f ~/serveur.pid || true'
done

echo "FIN TP1"
