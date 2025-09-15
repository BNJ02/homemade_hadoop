import socket

HOST = "0.0.0.0"   # écoute sur toutes les interfaces réseau
PORT = 5000        # port choisi (>=1024)

# Création du socket TCP
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen(1)
    print(f"Serveur en attente de connexion sur {HOST}:{PORT}...")

    conn, addr = s.accept()
    with conn:
        print("Connecté par", addr)
        data = conn.recv(1024)  # reçoit un message
        if data:
            print("Reçu :", data.decode())
            conn.sendall(b"salut")  # envoie la réponse
