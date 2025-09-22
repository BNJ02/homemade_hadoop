import socket
import struct

HOST = "0.0.0.0"
PORT = 5000

def recv_all(conn, n):
    """Lit exactement n octets depuis la connexion."""
    data = b""
    while len(data) < n:
        packet = conn.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen(1)
    print(f"Serveur en attente de connexion sur {HOST}:{PORT}...")

    conn, addr = s.accept()
    with conn:
        print("Connecté par", addr)
        # 1. Lire d'abord 4 octets pour la taille
        raw_size = recv_all(conn, 4)
        if raw_size is None:
            print("Erreur lors de la réception de la taille.")
        else:
            # 2. Décoder la taille avec struct
            msg_size = struct.unpack(">I", raw_size)[0]
            print(f"Taille du message à recevoir : {msg_size} octets")
            # 3. Lire le message complet
            msg_data = recv_all(conn, msg_size)
            if msg_data is None:
                print("Erreur lors de la réception du message.")
            else:
                print("Reçu :", msg_data.decode())
                # 4. Réponse (exemple : "salut")
                response = "salut".encode()
                resp_size = len(response)
                # Encoder la taille avec struct
                conn.sendall(struct.pack(">I", resp_size))
                conn.sendall(response)
