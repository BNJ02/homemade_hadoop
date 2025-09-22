import socket
import struct
import sys

if len(sys.argv) < 2:
    print("Usage: python client.py <HOST>")
    sys.exit(1)

HOST = sys.argv[1]
PORT = 5000

def recv_all(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("Connexion fermée par le serveur (attendu %d octets, reçu %d)" % (n, len(data)))
        data += chunk
    return data

message = b"bonjour"
length_bytes = struct.pack(">I", len(message))

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    for _ in range(100):
        s.sendall(length_bytes)
        s.sendall(message)

        # Lire exactement 4 octets pour la longueur
        resp_length_bytes = recv_all(s, 4)
        resp_length = struct.unpack(">I", resp_length_bytes)[0]

        # Lire exactement resp_length octets de payload
        resp_data = recv_all(s, resp_length)

        # Évite de spammer la console : n'affiche que les premières/dernières réponses
        # print("Réponse du serveur :", resp_data.decode())
