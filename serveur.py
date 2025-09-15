import socket
import os

HOST = "0.0.0.0"   # écoute sur toutes les interfaces réseau
PORT = 5000        # port choisi (>=1024) car pas administrateur
FILENAME_TO_SEND = "poeme_lafontaine.txt"  # fichier à envoyer au client
RECV_FILENAME = "fichier_recu_du_client.txt"  # nom du fichier reçu du client

# Création du socket TCP
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen(1)
    print(f"Serveur en attente de connexion sur {HOST}:{PORT}...")

    conn, addr = s.accept()
    with conn:
        print("Connecté par", addr)
        # Réception du fichier du client
        header = conn.recv(4096).decode()
        if '|' not in header:
            print("Erreur de réception du header du client.")
            conn.close()
            exit(1)
        cli_filename, cli_filesize = header.split('|')
        cli_filesize = int(cli_filesize)
        conn.sendall(b"OK")
        with open(RECV_FILENAME, "wb") as f:
            total_received = 0
            while total_received < cli_filesize:
                chunk = conn.recv(min(4096, cli_filesize - total_received))
                if not chunk:
                    break
                f.write(chunk)
                total_received += len(chunk)
        print(f"Fichier reçu du client et sauvegardé sous {RECV_FILENAME}.")

        # Envoi du fichier poeme_lafontaine.txt au client
        filesize = os.path.getsize(FILENAME_TO_SEND)
        conn.sendall(f"{os.path.basename(FILENAME_TO_SEND)}|{filesize}".encode())
        ack = conn.recv(2)
        if ack != b"OK":
            print("Erreur d'acknowledgement du client.")
            exit(1)
        with open(FILENAME_TO_SEND, "rb") as f:
            while True:
                bytes_read = f.read(4096)
                if not bytes_read:
                    break
                conn.sendall(bytes_read)
        print(f"Fichier {FILENAME_TO_SEND} envoyé au client.")
