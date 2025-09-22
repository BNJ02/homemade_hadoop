import socket
import struct

HOST = "0.0.0.0"
PORT = 5000

bonjour_count = 0

def recv_all(conn, n):
    data = b""
    while len(data) < n:
        packet = conn.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    print(f"Serveur en attente de connexion sur {HOST}:{PORT}...")
    try:
        while True:
            conn, addr = s.accept()
            with conn:
                print("Connecté par", addr)
                while True:
                    raw_size = recv_all(conn, 4)
                    if raw_size is None:
                        break  # client closed
                    msg_size = struct.unpack(">I", raw_size)[0]
                    msg_data = recv_all(conn, msg_size)
                    if msg_data is None:
                        break
                    message = msg_data.decode()
                    if message == "bonjour":
                        bonjour_count += 1
                        if bonjour_count == 100:
                            response = "salut"
                            bonjour_count = 0
                        else:
                            response = "ok"
                    else:
                        response = "inconnu"
                    resp_bytes = response.encode()
                    conn.sendall(struct.pack(">I", len(resp_bytes)))
                    conn.sendall(resp_bytes)
    except KeyboardInterrupt:
        print("Arrêt du serveur.")