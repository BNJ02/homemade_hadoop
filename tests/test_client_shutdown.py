import socket
import struct
import sys
import threading
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from client import MapReduceClient


class FakeSocket:
    def __init__(self) -> None:
        self.send_calls = []
        self.shutdown_called_with = None
        self.closed = False
        self.lock = threading.Lock()

    def sendall(self, data: bytes) -> None:  # pragma: no cover - simple recorder
        with self.lock:
            self.send_calls.append(bytes(data))

    def shutdown(self, how: int) -> None:  # pragma: no cover - simple recorder
        self.shutdown_called_with = how

    def close(self) -> None:  # pragma: no cover - simple recorder
        self.closed = True


@pytest.fixture()
def mapreduce_client() -> MapReduceClient:
    client = MapReduceClient(
        machine_index=0,
        worker_id=1,
        split_id="1",
        hosts=["local", "remote"],
        master_host="master",
        control_port=9999,
        shuffle_port_base=7000,
        encoding="utf-8",
        max_lines=None,
        flush_threshold=1024,
    )
    client._start_sender_threads()
    try:
        yield client
    finally:
        if client._sender_threads:
            client._close_outgoing()


def test_flush_all_outgoing_batches_before_shutdown(mapreduce_client: MapReduceClient) -> None:
    fake_socket = FakeSocket()

    def fake_get_socket(destination: int) -> FakeSocket:
        return fake_socket

    mapreduce_client._outgoing_sockets[1] = fake_socket
    mapreduce_client._get_outgoing_socket = fake_get_socket  # type: ignore[assignment]

    mapreduce_client._send_word(1, "foo")
    mapreduce_client._send_word(1, "bar")

    mapreduce_client._flush_all_outgoing()

    # Ensure data sent in a single batch respecting framing protocol.
    assert fake_socket.send_calls
    combined = b"".join(fake_socket.send_calls)
    expected = struct.pack(">I", 3) + b"foo" + struct.pack(">I", 3) + b"bar"
    assert combined == expected

    # Closing again should not resend data but should shutdown sockets cleanly.
    mapreduce_client._close_outgoing()

    assert fake_socket.closed
    assert fake_socket.shutdown_called_with == socket.SHUT_RDWR
    assert not mapreduce_client._sender_threads

