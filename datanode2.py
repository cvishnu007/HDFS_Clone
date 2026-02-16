#!/usr/bin/env python3

import socket
import threading
import os
import json
import time

# ---------------- CONFIG ----------------
DATANODE_NAME = "datanode2"
DATANODE_HOST = "172.22.194.94"
DATANODE_PORT = 5002

NAMENODE_HOST = "172.22.194.120"
NAMENODE_HEARTBEAT_PORT = 6000

DATA_DIR = "data_blocks"
HEARTBEAT_INTERVAL_SEC = 5

os.makedirs(DATA_DIR, exist_ok=True)


# ========== NEW: full receive to avoid truncation ==========
def recv_all(conn):  # NEW
    buf = []
    while True:
        part = conn.recv(65536)
        if not part:
            break
        buf.append(part)
    return b"".join(buf)


# ========== NEW: heartbeat sender (UDP) ==========
def send_heartbeat():  # NEW
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    while True:
        try:
            sock.sendto(DATANODE_NAME.encode(), (NAMENODE_HOST, NAMENODE_HEARTBEAT_PORT))
        except Exception as e:
            print(f"[HB] send error: {e}")
        time.sleep(HEARTBEAT_INTERVAL_SEC)


def handle_conn(conn, addr):
    try:
        # CHANGED: was conn.recv(1_000_000); now read full request
        data = recv_all(conn)  # CHANGED
        if not data:
            return
        req = json.loads(data.decode())
        action = req.get("action")

        if action == "store":
            fname = req["filename"]
            content_str = req["content"]
            blob = content_str.encode("latin-1", errors="ignore")
            with open(os.path.join(DATA_DIR, fname), "wb") as f:
                f.write(blob)
            conn.sendall(json.dumps({"status": "stored"}).encode())

        elif action == "read":
            fname = req["filename"]
            path = os.path.join(DATA_DIR, fname)
            if not os.path.exists(path):
                conn.sendall(json.dumps({"status": "error", "msg": "not found"}).encode())
            else:
                with open(path, "rb") as f:
                    blob = f.read()
                content_str = blob.decode("latin-1", errors="ignore")
                conn.sendall(json.dumps({"status": "ok", "content": content_str}).encode())

        else:
            conn.sendall(json.dumps({"status": "error", "msg": "unknown action"}).encode())

    except Exception as e:
        try:
            conn.sendall(json.dumps({"status": "error", "msg": str(e)}).encode())
        except:
            pass
    finally:
        conn.close()


def start_server():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", DATANODE_PORT))
    srv.listen(32)
    print(f"💾 DataNode2 listening on {DATANODE_HOST}:{DATANODE_PORT}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_conn, args=(conn, addr), daemon=True).start()


def main():
    # NEW: start heartbeat thread so NN can detect up/down
    threading.Thread(target=send_heartbeat, daemon=True).start()  # NEW
    start_server()


# ========== CHANGED: correct the main guard ==========
if __name__ == "__main__":  # CHANGED (fixes _name_/_main_ bug)
    main()
