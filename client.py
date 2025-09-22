import socket
import struct

HOST = "tp-1a207-36"        # IP de la machine où tourne le serveur
PORT = 5000                 # même port que dans le serveur

message = "bonjour".encode()
msg_length = len(message)

# Encode la taille sur 4 octets en big endian
length_bytes = struct.pack(">I", msg_length)  # '>' = big endian, 'I' = unsigned int (4 bytes)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    s.sendall(length_bytes)    # Envoie la taille
    s.sendall(message)         # Envoie le message
    # Réception de la réponse (même protocole : d'abord la taille, puis le message)
    resp_length_bytes = s.recv(4)
    resp_length = struct.unpack(">I", resp_length_bytes)[0]
    resp_data = b""
    while len(resp_data) < resp_length:
        chunk = s.recv(resp_length - len(resp_data))
        if not chunk:
            break
        resp_data += chunk
    print("Réponse du serveur :", resp_data.decode())
