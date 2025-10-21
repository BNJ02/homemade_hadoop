from __future__ import annotations

import argparse
import collections
import hashlib
import json
import re
import socket
import struct
import sys
import threading
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

"""Worker client for the distributed MapReduce wordcount demo.

Chaque worker se connecte au master pour recevoir les ordres START_MAP et
START_REDUCE. La phase map lit split_<id>.txt et répartit les mots via une
fonction de hachage MD5%N. Les messages arrivent en parallèle via un thread
"shuffle" dédié et sont cumulés pour la réduction locale.
"""

WORD_RE = re.compile(r"\w+")


class MapReduceClient:
    def __init__(
        self,
        machine_index: int,
        worker_id: int,
        split_id: str,
        hosts: Iterable[str],
        master_host: str,
        control_port: int,
        shuffle_port_base: int,
        encoding: str,
        max_lines: Optional[int],
    ) -> None:
        self.machine_index = machine_index
        self.worker_id = worker_id
        self.split_id = split_id
        self.hosts = list(hosts)
        self.master_host = master_host
        self.control_port = control_port
        self.shuffle_base_port = shuffle_port_base
        self.shuffle_port = shuffle_port_base + machine_index
        self.encoding = encoding
        self.max_lines = max_lines

        self._incoming_counts = collections.Counter()
        self._incoming_lock = threading.Lock()
        self._shutdown_event = threading.Event()
        self._outgoing_sockets: Dict[int, socket.socket] = {}
        self._pending_frames: Dict[int, bytearray] = {}
        self._flush_threshold = 64 * 1024
        self._listener_thread: Optional[threading.Thread] = None
        self._master_socket: Optional[socket.socket] = None

    # Boucle principale du client
    def start(self) -> None:
        # Démarre l'écoute shuffle avant d'informer le master pour garantir
        # que les autres workers peuvent nous pousser des clés immédiatement.
        self._start_shuffle_listener()
        self._connect_master()
        try:
            self._control_loop()
        finally:
            self._shutdown_event.set()
            self._close_outgoing()
            if self._master_socket is not None:
                self._master_socket.close()
            self._poke_listener()

    # Démarre le thread d'écoute pour la phase shuffle.
    def _start_shuffle_listener(self) -> None:
        # Thread d'écoute asynchrone pour recevoir les paires (mot, 1)
        # qui nous sont destinées pendant la phase map des autres workers.
        def run_listener() -> None:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
                server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server_sock.bind(("0.0.0.0", self.shuffle_port))
                server_sock.listen()
                server_sock.settimeout(1.0)
                while not self._shutdown_event.is_set():
                    try:
                        conn, _ = server_sock.accept()
                    except socket.timeout:
                        continue
                    conn.settimeout(1.0)
                    threading.Thread(
                        target=self._consume_shuffle_stream,
                        args=(conn,),
                        daemon=True,
                    ).start()

        self._listener_thread = threading.Thread(target=run_listener, daemon=True)
        self._listener_thread.start()

    # Force la terminaison du thread d'écoute.
    def _poke_listener(self) -> None:
        # Force accept() à sortir pour que le thread se termine proprement.
        try:
            with socket.create_connection(("127.0.0.1", self.shuffle_port), timeout=0.5):
                pass
        except OSError:
            pass
        if self._listener_thread is not None:
            self._listener_thread.join(timeout=1.0)

    # Connexion au master et enregistrement
    def _connect_master(self) -> None:
        self._master_socket = socket.create_connection((self.master_host, self.control_port))
        self._master_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        register_payload = {
            "type": "register",
            "machine_index": self.worker_id,
            "split_id": self.split_id,
            "shuffle_port": self.shuffle_port,
        }
        self._send_control(register_payload)

    # Boucle principale de contrôle pour recevoir les ordres du master.
    def _control_loop(self) -> None:
        assert self._master_socket is not None
        while not self._shutdown_event.is_set():
            message = self._recv_control()
            if message is None:
                break
            msg_type = message.get("type")
            if msg_type == "start_map":
                status, error = self._run_map_stage()
                payload = {
                    "type": "map_finished",
                    "machine_index": self.worker_id,
                    "success": status,
                }
                if error is not None:
                    payload["error"] = error
                self._send_control(payload)
            elif msg_type == "start_reduce":
                results, error = self._run_reduce_stage()
                payload = {
                    "type": "reduce_finished",
                    "machine_index": self.worker_id,
                    "success": error is None,
                }
                if results is not None:
                    payload["results"] = results
                if error is not None:
                    payload["error"] = error
                self._send_control(payload)
            elif msg_type == "shutdown":
                break
            else:
                print(
                    f"[worker {self.worker_id}] Unknown control message: {message}",
                    file=sys.stderr,
                )

    # Lit le split local et envoie les mots aux autres workers.
    def _run_map_stage(self) -> Tuple[bool, Optional[str]]:
        try:
            # Vider d'abord les accumulations pour ne pas mélanger deux jobs.
            with self._incoming_lock:
                self._incoming_counts.clear()
            
            # If split_id contains a path separator, use it as-is, otherwise use split_X.txt format
            if "/" in self.split_id:
                path = Path(self.split_id)
            else:
                path = Path(f"split_{self.split_id}.txt")
            
            if not path.exists():
                raise FileNotFoundError(f"split file missing: {path}")
            for word in self._iter_words(path):
                destination = self._hash_to_index(word)
                self._send_word(destination, word)
            return True, None
        except Exception as exc:  # pylint: disable=broad-except
            return False, str(exc)
        finally:
            self._flush_all_outgoing()
            self._close_outgoing()

    # Réduit les paires (mot, count) accumulées localement.
    def _run_reduce_stage(self) -> Tuple[Optional[List[Tuple[str, int]]], Optional[str]]:
        try:
            with self._incoming_lock:
                snapshot = list(self._incoming_counts.items())
            snapshot.sort()
            return snapshot, None
        except Exception as exc:  # pylint: disable=broad-except
            return None, str(exc)

    # Itère sur les mots dans le fichier split.
    def _iter_words(self, path: Path) -> Iterable[str]:
        # Extraction naïve des tokens alphanumériques pour le wordcount.
        with path.open("r", encoding=self.encoding) as handle:
            max_lines = self.max_lines
            lines_read = 0
            for line in handle:
                for raw_word in WORD_RE.findall(line.lower()):
                    word = raw_word.strip()
                    if word:
                        yield word
                if max_lines is not None:
                    lines_read += 1
                    if lines_read >= max_lines:
                        break

    # Fonction de hachage MD5 pour répartir les mots entre workers.
    def _hash_to_index(self, word: str) -> int:
        digest = hashlib.md5(word.encode(self.encoding)).digest()
        value = int.from_bytes(digest, byteorder="big")
        return value % len(self.hosts)

    # Envoie un mot à un autre worker (ou à soi-même).
    def _send_word(self, destination: int, word: str) -> None:
        if destination == self.machine_index:
            with self._incoming_lock:
                self._incoming_counts[word] += 1
            return
        sock = self._get_outgoing_socket(destination)
        payload = word.encode(self.encoding)
        header = struct.pack(">I", len(payload))
        buffer = self._pending_frames.setdefault(destination, bytearray())
        buffer.extend(header)
        buffer.extend(payload)
        if len(buffer) >= self._flush_threshold:
            self._flush_outgoing(destination)

    # Obtient (ou crée) une connexion socket vers un autre worker.
    def _get_outgoing_socket(self, destination: int) -> socket.socket:
        sock = self._outgoing_sockets.get(destination)
        if sock is not None:
            return sock
        host = self.hosts[destination]
        port = self.shuffle_base_port + destination
        for attempt in range(5):
            try:
                sock = socket.create_connection((host, port), timeout=5.0)
                break
            except OSError:
                if attempt == 4:
                    raise
                time.sleep(0.2)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._outgoing_sockets[destination] = sock
        return sock

    def _flush_outgoing(self, destination: int) -> None:
        buffer = self._pending_frames.get(destination)
        if not buffer:
            return
        sock = self._outgoing_sockets.get(destination)
        if sock is None:
            sock = self._get_outgoing_socket(destination)
        if not buffer:
            return
        try:
            sock.sendall(buffer)
        finally:
            buffer.clear()

    def _flush_all_outgoing(self) -> None:
        for destination in list(self._pending_frames.keys()):
            self._flush_outgoing(destination)

    # Ferme toutes les connexions sortantes.
    def _close_outgoing(self) -> None:
        self._flush_all_outgoing()
        for destination, sock in list(self._outgoing_sockets.items()):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
            self._pending_frames.pop(destination, None)
        self._outgoing_sockets.clear()

    # Consomme un flux de paires (mot, 1) en provenance d'un autre worker.
    def _consume_shuffle_stream(self, conn: socket.socket) -> None:
        with conn:
            while not self._shutdown_event.is_set():
                try:
                    length_bytes = self._recv_exact(conn, 4)
                    if not length_bytes:
                        break
                    size = struct.unpack(">I", length_bytes)[0]
                    payload = self._recv_exact(conn, size)
                    if not payload:
                        break
                except socket.timeout:
                    if self._shutdown_event.is_set():
                        break
                    continue
                word = payload.decode(self.encoding)
                if not word:
                    continue
                with self._incoming_lock:
                    self._incoming_counts[word] += 1

    # Envoi de données avec un en-tête de taille
    def _send_frame(self, sock: socket.socket, payload: bytes) -> None:
        header = struct.pack(">I", len(payload))
        sock.sendall(header + payload)

    # Réception de données exactes
    def _recv_exact(self, sock: socket.socket, size: int) -> Optional[bytes]:
        data = b""
        while len(data) < size:
            chunk = sock.recv(size - len(data))
            if not chunk:
                return None
            data += chunk
        return data

    # Envoi des messages de contrôle au master
    def _send_control(self, payload: Dict[str, object]) -> None:
        assert self._master_socket is not None
        data = json.dumps(payload).encode(self.encoding)
        self._send_frame(self._master_socket, data)

    # Réception des messages de contrôle du master
    def _recv_control(self) -> Optional[Dict[str, object]]:
        assert self._master_socket is not None
        length_bytes = self._recv_exact(self._master_socket, 4)
        if not length_bytes:
            return None
        size = struct.unpack(">I", length_bytes)[0]
        payload = self._recv_exact(self._master_socket, size)
        if not payload:
            return None
        return json.loads(payload.decode(self.encoding))

# Analyse des arguments de la ligne de commande
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MapReduce wordcount client")
    parser.add_argument("worker_id", type=int, help="One-based worker identifier")
    parser.add_argument(
        "hosts",
        nargs="+",
        help="Ordered list of worker hostnames (including this machine)",
    )
    parser.add_argument(
        "--split-id",
        dest="split_id",
        help="Suffix used for split_<id>.txt (defaults to worker_id)",
    )
    parser.add_argument(
        "--master-host",
        dest="master_host",
        default="tp-1a207-37",
        help="Hostname of the master node",
    )
    parser.add_argument(
        "--control-port",
        dest="control_port",
        type=int,
        default=5374,
        help="TCP port for the control plane",
    )
    parser.add_argument(
        "--shuffle-port-base",
        dest="shuffle_port_base",
        type=int,
        default=6200,
        help="Base port for the shuffle phase",
    )
    parser.add_argument(
        "--encoding",
        dest="encoding",
        default="utf-8",
        help="Text encoding for split files",
    )
    parser.add_argument(
        "--max-lines",
        dest="max_lines",
        type=int,
        help="Optional limit on the number of input lines processed during the map stage",
    )
    return parser.parse_args()


### Point d'entrée principal ###
if __name__ == "__main__":
    args = parse_args()
    hosts = args.hosts
    worker_id = args.worker_id
    num_hosts = len(hosts)
    if worker_id < 1 or worker_id > num_hosts:
        print("worker_id must be between 1 and the number of hosts", file=sys.stderr)
        sys.exit(1)
    machine_index = worker_id - 1
    split_id = args.split_id or str(worker_id)
    client = MapReduceClient(
        machine_index=machine_index,
        worker_id=worker_id,
        split_id=split_id,
        hosts=hosts,
        master_host=args.master_host,
        control_port=args.control_port,
        shuffle_port_base=args.shuffle_port_base,
        encoding=args.encoding,
        max_lines=args.max_lines,
    )
    try:
        client.start()
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Client terminated with error: {exc}", file=sys.stderr)
        sys.exit(1)
