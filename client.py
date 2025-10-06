# client.py
import socket
import struct
import sys
import threading

PORT = 5374

count = 0
count_lock = threading.Lock()

def recv_all(sock, n):
    data = b""
    while len(data) < n:
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

                resp_length_bytes = recv_all(s, 4)
                resp_length = struct.unpack(">I", resp_length_bytes)[0]
                resp_data = recv_all(s, resp_length)

                with count_lock:
                    count += 1

            print(f"[{host}] Terminé, dernière réponse = {resp_data.decode()}")
    except Exception as e:
        print(f"[{host}] Erreur : {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python client.py <HOST1> <HOST2> ...")
        sys.exit(1)

    hosts = sys.argv[1:]
    threads = []
    for h in hosts:
        print(f"Démarrage du client pour {h}")
        t = threading.Thread(target=client_worker, args=(h,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
    print("Tous les clients ont terminé.")
    print(f"Valeur finale de count = {count}")
