import socket

HOST = "tp-1a201-36"        # IP de la machine où tourne le serveur
PORT = 5000                 # même port que dans le serveur

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    s.sendall(b"bonjour")
    data = s.recv(1024)
    print("Réponse du serveur :", data.decode())
