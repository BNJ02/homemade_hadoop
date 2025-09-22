import socket
import struct
import sys

if len(sys.argv) < 2:
    print("Usage: python client.py <HOST>")
    sys.exit(1)

HOST = sys.argv[1]           # IP ou nom de la machine passée en argument
PORT = 5000                  # même port que dans le serveur

message = "bonjour".encode()
msg_length = len(message)

length_bytes = struct.pack(">I", msg_length)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    s.sendall(length_bytes)
    s.sendall(message)
    resp_length_bytes = s.recv(4)
    resp_length = struct.unpack(">I", resp_length_bytes)[0]
    resp_data = b""
    while len(resp_data) < resp_length:
        chunk = s.recv(resp_length - len(resp_data))
        if not chunk:
            break
        resp_data += chunk
    print("Réponse du serveur :", resp_data.decode())
