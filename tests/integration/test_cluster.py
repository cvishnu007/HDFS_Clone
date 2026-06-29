import socket
import json
import base64
import hashlib
import time
import pytest

NAMENODE = ("127.0.0.1", 5000)


def send_namenode(payload: dict, timeout=30) -> dict:
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8_000_000)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8_000_000)
    s.settimeout(timeout)
    s.connect(NAMENODE)
    s.sendall(json.dumps(payload).encode())
    try:
        s.shutdown(socket.SHUT_WR)
    except:
        pass
    chunks = []
    while True:
        try:
            part = s.recv(65536)
            if not part:
                break
            chunks.append(part)
        except socket.timeout:
            break
    s.close()
    return json.loads(b"".join(chunks).decode())


def read_chunk_from(host: str, port: int, chunk_name: str) -> bytes:
    s = socket.socket()
    s.settimeout(30)
    s.connect((host, port))
    s.sendall(json.dumps({"action": "read", "filename": chunk_name}).encode())
    try:
        s.shutdown(socket.SHUT_WR)
    except:
        pass
    chunks = []
    while True:
        try:
            part = s.recv(65536)
            if not part:
                break
            chunks.append(part)
        except socket.timeout:
            break
    s.close()
    res = json.loads(b"".join(chunks).decode())
    if res.get("status") == "ok":
        return base64.b64decode(res["content_b64"])
    return None


# ---------- TESTS ----------

def test_cluster_status():
    res = send_namenode({"action": "status"})
    assert res["status"] == "ok"
    assert "nodes" in res
    assert len(res["nodes"]) >= 1


def test_upload_small_file():
    raw = b"hello from integration test"
    content_b64 = base64.b64encode(raw).decode("ascii")
    res = send_namenode({
        "action": "upload",
        "filename": "integration_test_small.txt",
        "content_b64": content_b64
    })
    assert res["status"] == "uploaded"
    assert res["chunks"] >= 1


def test_download_matches_upload():
    filename = "integration_test_roundtrip.txt"
    raw = b"roundtrip test data " * 100

    # Upload
    content_b64 = base64.b64encode(raw).decode("ascii")
    res = send_namenode({
        "action": "upload",
        "filename": filename,
        "content_b64": content_b64
    })
    assert res["status"] == "uploaded"

    # Verify via checksum in status
    status = send_namenode({"action": "status"})
    assert filename in status["files"]

    expected_checksum = status["files"][filename]["file_checksum"]
    actual_checksum = hashlib.sha256(raw).hexdigest()
    assert expected_checksum == actual_checksum

    # Verify chunk count
    man = send_namenode({"action": "download", "filename": filename})
    assert man["status"] == "ok"
    assert len(man["chunks"]) >= 1
    for ch in man["chunks"]:
        assert len(ch["replicas"]) >= 1

def test_upload_binary_file():
    raw = bytes(range(256)) * 100
    content_b64 = base64.b64encode(raw).decode("ascii")
    res = send_namenode({
        "action": "upload",
        "filename": "integration_test_binary.bin",
        "content_b64": content_b64
    })
    assert res["status"] == "uploaded"


def test_list_shows_uploaded_files():
    # Upload a file first
    raw = b"list test"
    content_b64 = base64.b64encode(raw).decode("ascii")
    send_namenode({
        "action": "upload",
        "filename": "integration_test_list.txt",
        "content_b64": content_b64
    })

    res = send_namenode({"action": "list"})
    assert res["status"] == "ok"
    filenames = [f["filename"] for f in res["files"]]
    assert "integration_test_list.txt" in filenames


def test_delete_removes_file():
    filename = "integration_test_delete.txt"
    raw = b"delete me"
    content_b64 = base64.b64encode(raw).decode("ascii")

    # Upload
    send_namenode({
        "action": "upload",
        "filename": filename,
        "content_b64": content_b64
    })

    # Delete
    res = send_namenode({"action": "delete", "filename": filename})
    assert res["status"] == "deleted"

    # Confirm gone
    res = send_namenode({"action": "download", "filename": filename})
    assert res["status"] == "error"


def test_checksum_integrity():
    filename = "integration_test_checksum.txt"
    raw = b"checksum verification test " * 500

    content_b64 = base64.b64encode(raw).decode("ascii")
    send_namenode({
        "action": "upload",
        "filename": filename,
        "content_b64": content_b64
    })

    status = send_namenode({"action": "status"})
    expected = status["files"][filename]["file_checksum"]
    actual = hashlib.sha256(raw).hexdigest()
    assert expected == actual