# serveur.py
import socket
import struct
import signal

HOST = "0.0.0.0"
PORT = 5374

running = True
bonjour_count = 0

def handle_stop(signum, frame):
    # Appelé sur SIGINT/SIGTERM -> on sort proprement des boucles
    global running
    print("Signal reçu, arrêt propre du serveur...")
    running = False

signal.signal(signal.SIGINT, handle_stop)
signal.signal(signal.SIGTERM, handle_stop)

def recv_all(conn, n):
    data = b""
    while len(data) < n:
        packet = conn.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # Redémarrage rapide sans 'Address already in use'
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen()
    s.settimeout(1.0)  # pour vérifier régulièrement 'running'
    print(f"Serveur en attente de connexion sur {HOST}:{PORT}...")
    try:
        while running:
            try:
                conn, addr = s.accept()
            except socket.timeout:
                continue
            print("Connecté par", addr)
            with conn:
                while running:
                    raw_size = recv_all(conn, 4)
                    if raw_size is None:
                        break
                    msg_size = struct.unpack(">I", raw_size)[0]
                    msg_data = recv_all(conn, msg_size)
                    if msg_data is None:
                        break

                    message = msg_data.decode()
                    # Commande d'arrêt à distance
                    if message == "SHUTDOWN":
                        resp_bytes = b"bye"
                        conn.sendall(struct.pack(">I", len(resp_bytes)) + resp_bytes)
                        running = False
                        break

                    # Logique existante 'bonjour' -> 'ok'/'salut'
                    if message == "bonjour":
                        bonjour_count += 1
                        if bonjour_count == 1000:
                            response = "salut"
                            bonjour_count = 0
                        else:
                            response = "ok"
                    else:
                        response = "inconnu"

                    resp_bytes = response.encode()
                    conn.sendall(struct.pack(">I", len(resp_bytes)))
                    conn.sendall(resp_bytes)
    finally:
        print("Arrêt du serveur.")
