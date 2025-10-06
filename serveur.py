from __future__ import annotations

import argparse
import collections
import json
import socket
import struct
import threading
from typing import Dict, Optional, Tuple


class ClientInfo:
    def __init__(self, sock: socket.socket, addr: Tuple[str, int]) -> None:
        self.sock = sock
        self.addr = addr
        self.lock = threading.Lock()
        self.machine_index: Optional[int] = None
        self.split_id: Optional[str] = None
        self.shuffle_port: Optional[int] = None


def recv_exact(sock: socket.socket, size: int) -> Optional[bytes]:
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            return None
        data += chunk
    return data


def recv_json(sock: socket.socket) -> Optional[Dict[str, object]]:
    length_bytes = recv_exact(sock, 4)
    if not length_bytes:
        return None
    size = struct.unpack(">I", length_bytes)[0]
    payload = recv_exact(sock, size)
    if not payload:
        return None
    return json.loads(payload.decode("utf-8"))


def send_json(sock: socket.socket, payload: Dict[str, object]) -> None:
    data = json.dumps(payload).encode("utf-8")
    header = struct.pack(">I", len(data))
    sock.sendall(header + data)


class MasterServer:
    def __init__(self, host: str, port: int, expected_workers: int) -> None:
        self.host = host
        self.port = port
        self.expected_workers = expected_workers

        self._server_socket: Optional[socket.socket] = None
        self._stop_event = threading.Event()
        self._clients: Dict[int, ClientInfo] = {}
        self._map_finished: Dict[int, Dict[str, object]] = {}
        self._reduce_results: Dict[int, Dict[str, int]] = {}
        self._condition = threading.Condition()
        self._start_map_sent = False
        self._start_reduce_sent = False

    def start(self) -> None:
        accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        accept_thread.start()
        try:
            self._coordinate()
        finally:
            self._stop_event.set()
            if self._server_socket is not None:
                try:
                    self._server_socket.close()
                except OSError:
                    pass
            accept_thread.join(timeout=2.0)
            self._close_all_clients()

    def _accept_loop(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_sock.bind((self.host, self.port))
            server_sock.listen()
            self._server_socket = server_sock
            while not self._stop_event.is_set():
                try:
                    conn, addr = server_sock.accept()
                except OSError:
                    break
                info = ClientInfo(conn, addr)
                threading.Thread(target=self._handle_client, args=(info,), daemon=True).start()

    def _handle_client(self, info: ClientInfo) -> None:
        sock = info.sock
        try:
            while not self._stop_event.is_set():
                message = recv_json(sock)
                if message is None:
                    break
                self._process_message(info, message)
        finally:
            sock.close()
            with self._condition:
                if info.machine_index is not None:
                    self._clients.pop(info.machine_index, None)
                self._condition.notify_all()

    def _process_message(self, info: ClientInfo, message: Dict[str, object]) -> None:
        msg_type = message.get("type")
        if msg_type == "register":
            machine_index = int(message.get("machine_index"))
            info.machine_index = machine_index
            info.split_id = str(message.get("split_id"))
            info.shuffle_port = int(message.get("shuffle_port"))
            with self._condition:
                existing = self._clients.get(machine_index)
                if existing is not None and existing is not info:
                    existing.sock.close()
                self._clients[machine_index] = info
                print(f"Worker {machine_index} registered from {info.addr}")
                self._condition.notify_all()
        elif msg_type == "map_finished":
            machine_index = int(message.get("machine_index"))
            success = bool(message.get("success", False))
            error = message.get("error")
            with self._condition:
                self._map_finished[machine_index] = {"success": success, "error": error}
                status = "ok" if success else f"failed: {error}"
                print(f"Map from worker {machine_index}: {status}")
                self._condition.notify_all()
        elif msg_type == "reduce_finished":
            machine_index = int(message.get("machine_index"))
            success = bool(message.get("success", False))
            error = message.get("error")
            raw_results = message.get("results")
            results_dict: Dict[str, int] = {}
            if isinstance(raw_results, list):
                for entry in raw_results:
                    if isinstance(entry, list) and len(entry) == 2:
                        word, count = entry
                        if isinstance(word, str) and isinstance(count, int):
                            results_dict[word] = count
            with self._condition:
                if success:
                    self._reduce_results[machine_index] = results_dict
                    print(f"Reduce from worker {machine_index}: ok ({len(results_dict)} keys)")
                else:
                    print(f"Reduce from worker {machine_index} failed: {error}")
                    self._reduce_results[machine_index] = {}
                self._condition.notify_all()
        else:
            print(f"Unknown message from {info.addr}: {message}")

    def _coordinate(self) -> None:
        while not self._stop_event.is_set():
            with self._condition:
                if not self._start_map_sent and len(self._clients) >= self.expected_workers:
                    self._broadcast({"type": "start_map"})
                    self._start_map_sent = True
                    print("All workers registered. start_map sent.")
                    continue
                if (
                    self._start_map_sent
                    and not self._start_reduce_sent
                    and len(self._map_finished) >= self.expected_workers
                ):
                    self._broadcast({"type": "start_reduce"})
                    self._start_reduce_sent = True
                    print("All map_finished received. start_reduce sent.")
                    continue
                if (
                    self._start_reduce_sent
                    and len(self._reduce_results) >= self.expected_workers
                ):
                    self._emit_final_result()
                    self._broadcast({"type": "shutdown"})
                    self._stop_event.set()
                    return
                self._condition.wait()

    def _broadcast(self, payload: Dict[str, object]) -> None:
        for index, info in list(self._clients.items()):
            try:
                with info.lock:
                    send_json(info.sock, payload)
            except OSError:
                print(f"Failed to send to worker {index}, closing connection")
                info.sock.close()
                self._clients.pop(index, None)

    def _emit_final_result(self) -> None:
        final_counts = collections.Counter()
        for partial in self._reduce_results.values():
            for word, count in partial.items():
                final_counts[word] += count
        print("Final wordcount:")
        for word, count in final_counts.most_common():
            print(f"{word}: {count}")

    def _close_all_clients(self) -> None:
        for info in list(self._clients.values()):
            try:
                info.sock.close()
            except OSError:
                pass
        self._clients.clear()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MapReduce master server")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address for the master")
    parser.add_argument("--port", type=int, default=5374, help="Control port")
    parser.add_argument(
        "--num-workers",
        type=int,
        required=True,
        help="Number of expected worker connections",
    )
    return parser.parse_args()

### Entry point ###
if __name__ == "__main__":
    args = parse_args()
    server = MasterServer(host=args.host, port=args.port, expected_workers=args.num_workers)
    server.start()
