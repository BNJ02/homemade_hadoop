import socket
import os

HOST = "tp-1a201-36"        # IP de la machine où tourne le serveur
PORT = 5000                 # même port que dans le serveur
FILENAME = "fichier_10Mo.txt"  # fichier à envoyer
RECV_FILENAME = "poeme_recu_du_serveur.txt"  # nom du fichier reçu du serveur

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    # Envoi du fichier
    filesize = os.path.getsize(FILENAME)
    s.sendall(f"{os.path.basename(FILENAME)}|{filesize}".encode())
    ack = s.recv(2)
    if ack != b"OK":
        print("Erreur d'acknowledgement du serveur.")
        exit(1)
    with open(FILENAME, "rb") as f:
        while True:
            bytes_read = f.read(4096)
            if not bytes_read:
                break
            s.sendall(bytes_read)
    print(f"Fichier {FILENAME} envoyé au serveur.")

    # Réception du fichier envoyé par le serveur
    header = s.recv(4096).decode()
    if '|' not in header:
        print("Erreur de réception du header du serveur.")
        exit(1)
    srv_filename, srv_filesize = header.split('|')
    srv_filesize = int(srv_filesize)
    s.sendall(b"OK")
    with open(RECV_FILENAME, "wb") as f:
        total_received = 0
        while total_received < srv_filesize:
            chunk = s.recv(min(4096, srv_filesize - total_received))
            if not chunk:
                break
            f.write(chunk)
            total_received += len(chunk)
    print(f"Fichier reçu du serveur et sauvegardé sous {RECV_FILENAME}.")
