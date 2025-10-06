# serveur.py
import socket
import struct
import signal
import threading

HOST = "0.0.0.0"
PORT = 5374

bonjour_count = 0
bonjour_lock = threading.Lock()
running_event = threading.Event()
running_event.set()

def handle_stop(signum, frame):
    if running_event.is_set():
        print("Signal recu, arret propre du serveur...")
    running_event.clear()


signal.signal(signal.SIGINT, handle_stop)
signal.signal(signal.SIGTERM, handle_stop)


def recv_all(conn, expected):
    data = b""
    while len(data) < expected:
        try:
            chunk = conn.recv(expected - len(data))
        except socket.timeout:
            if not running_event.is_set():
                return None
            continue
        if not chunk:
            return None
        data += chunk
    return data


def handle_client(conn, addr):
    global bonjour_count
    conn.settimeout(1.0)
    with conn:
        while running_event.is_set():
            raw_size = recv_all(conn, 4)
            if raw_size is None:
                break
            msg_size = struct.unpack(">I", raw_size)[0]
            msg_data = recv_all(conn, msg_size)
            if msg_data is None:
                break

            message = msg_data.decode()

            if message == "SHUTDOWN":
                resp_bytes = b"bye"
                conn.sendall(struct.pack(">I", len(resp_bytes)) + resp_bytes)
                running_event.clear()
                break

            if message == "bonjour":
                with bonjour_lock:
                    bonjour_count += 1
                    if bonjour_count == 100000:
                        response = "salut"
                        bonjour_count = 0
                    else:
                        response = "ok"
            else:
                response = "inconnu"

            resp_bytes = response.encode()
            conn.sendall(struct.pack(">I", len(resp_bytes)) + resp_bytes)

if __name__ == "__main__":
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((HOST, PORT))
        server_socket.listen()
        server_socket.settimeout(1.0)
        print(f"Serveur en attente de connexion sur {HOST}:{PORT}...")

        try:
            while running_event.is_set():
                try:
                    conn, addr = server_socket.accept()
                except socket.timeout:
                    continue
                print("Connecte par", addr)
                thread = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
                thread.start()
        finally:
            running_event.clear()
            print("Arret du serveur.")
