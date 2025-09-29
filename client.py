# client.py
import socket
import struct
import sys
import threading
from time import sleep

PORT = 5374

count = 0  # Variable globale sans protection

def recv_all(sock, n):
    data = b""
    # Lire exactement n octets
    while len(data) < n:
        # Attendre la réception de données
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError(
                f"Connexion fermée par le serveur (attendu {n} octets, reçu {len(data)})"
            )
        data += chunk
    return data

def client_worker(host):
    global count
    message = b"bonjour"
    length_bytes = struct.pack(">I", len(message))

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, PORT))
            for _ in range(1000):
                s.sendall(length_bytes)
                s.sendall(message)

                # Lire la réponse
                resp_length_bytes = recv_all(s, 4)
                resp_length = struct.unpack(">I", resp_length_bytes)[0]
                resp_data = recv_all(s, resp_length)

                # PROBLÈME : Opération non-atomique sans protection
                temp = count          # 1. Lire la valeur actuelle
                sleep(0.00000001)  # Simuler un petit délai pour augmenter les chances de collision
                temp = temp + 1       # 2. Calculer la nouvelle valeur
                sleep(0.00000001)  # Simuler un petit délai pour augmenter les chances de collision
                count = temp          # 3. Écrire la nouvelle valeur
                # count += 1  # Incrémentation sans protection

            print(f"[{host}] Terminé, dernière réponse = {resp_data.decode()}")
    except Exception as e:
        print(f"[{host}] Erreur : {e}")

if __name__ == "__main__":
    # Vérification des arguments
    if len(sys.argv) < 2:
        print("Usage: python client.py <HOST1> <HOST2> ...")
        sys.exit(1)

    # Liste des hôtes à contacter
    hosts = sys.argv[1:]

    # Lancer un thread pour chaque hôte
    threads = []
    for h in hosts:
        print(f"Démarrage du client pour {h}")
        t = threading.Thread(target=client_worker, args=(h,))
        t.start()
        threads.append(t)

    # Attendre que tous les threads se terminent
    for t in threads:
        t.join()
    print("Tous les clients ont terminé.")
    print(f"Valeur finale de count = {count}")