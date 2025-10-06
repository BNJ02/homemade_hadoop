# client.py
import socket
import struct
import sys
import threading

PORT = 5374

count = 0
count_lock = threading.Lock()


def recv_all(sock, expected):
    data = b""
    while len(data) < expected:
        chunk = sock.recv(expected - len(data))
        if not chunk:
            raise ConnectionError(
                f"Connexion fermee par le serveur (attendu {expected} octets, recu {len(data)})"
            )
        data += chunk
    return data

def client_worker(host):
    global count
    message = b"bonjour"
    payload = struct.pack(">I", len(message)) + message
    local_count = 0
    last_response = b""

    try:
        with socket.create_connection((host, PORT)) as sock:
            for _ in range(100000):
                sock.sendall(payload)

                resp_length_bytes = recv_all(sock, 4)
                resp_length = struct.unpack(">I", resp_length_bytes)[0]
                resp_data = recv_all(sock, resp_length)

                last_response = resp_data
                local_count += 1

        with count_lock:
            count += local_count
        print(f"[{host}] Termine, derniere reponse = {last_response.decode()}")
    except Exception as exc:
        print(f"[{host}] Erreur : {exc}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python client.py <HOST1> <HOST2> ...")
        sys.exit(1)

    hosts = sys.argv[1:]
    threads = []
    for host in hosts:
        print(f"Demarrage du client pour {host}")
        thread = threading.Thread(target=client_worker, args=(host,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
    print("Tous les clients ont termine.")
    print(f"Valeur finale de count = {count}")
