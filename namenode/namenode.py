#!/usr/bin/env python3
# NameNode – Mini HDFS (FIXED VERSION)
# All functionality working: Upload, Download, Status, Heartbeats, Replication

import socket
import threading
import json
import os
import time
import hashlib
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict, Tuple, List
import base64
from prometheus_client import (
    start_http_server,
    Gauge, Counter, Histogram, REGISTRY
)

# ---------- PROMETHEUS METRICS ----------

HDFS_FILES_TOTAL           = Gauge("hdfs_files_total",              "Number of files in namespace")
HDFS_CHUNKS_TOTAL          = Gauge("hdfs_chunks_total",             "Total chunks across all files")
HDFS_UNDER_REPLICATED      = Gauge("hdfs_blocks_under_replicated",  "Chunks with fewer replicas than RF")
HDFS_DATANODES_ALIVE       = Gauge("hdfs_datanodes_alive",          "Number of live DataNodes")
HDFS_BYTES_STORED          = Gauge("hdfs_bytes_stored_total",       "Total bytes stored across all files")

HDFS_UPLOADS_TOTAL         = Counter("hdfs_upload_requests_total",   "Cumulative upload operations")
HDFS_DOWNLOADS_TOTAL       = Counter("hdfs_download_requests_total", "Cumulative download operations")
HDFS_DELETES_TOTAL         = Counter("hdfs_delete_requests_total",   "Cumulative delete operations")

HDFS_RPC_LATENCY           = Histogram(
    "hdfs_rpc_latency_seconds",
    "RPC latency in seconds",
    labelnames=["op"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

# ---------- CONFIG ----------

REPLICATION_FACTOR = int(os.environ.get("REPLICATION_FACTOR", "2"))
NAMENODE_HOST = os.environ.get("NAMENODE_HOST", "0.0.0.0")
NAMENODE_PORT = int(os.environ.get("NAMENODE_PORT", "5000"))

DATANODES: Dict[str, Tuple[str, int]] = {
    k: (v.split(":")[0], int(v.split(":")[1]))
    for k, v in (
        item.split("=")
        for item in os.environ.get(
            "DATANODES",
            "datanode1=127.0.0.1:5001,datanode2=127.0.0.1:5002"
        ).split(",")
    )
}

HEARTBEAT_UDP_PORT = 6000
HEARTBEAT_TIMEOUT_SEC = 15
HEARTBEAT_CHECK_EVERY = 5

CHUNK_SIZE = 2 * 1024 * 1024  # 2MB
MAX_UPLOAD_BYTES = 50 * 1024 * 1024  # 50MB
METADATA_FILE = "metadata.json"

metadata = {"files": {}, "datanode_status": {}}
lock = threading.Lock()

# ---------- LOGGING ----------
def setup_logs():
    os.makedirs("logs", exist_ok=True)
    handler = RotatingFileHandler("logs/namenode.log", maxBytes=1_000_000, backupCount=3)
    logging.basicConfig(
        level=logging.INFO,
        handlers=[handler],
        format="%(asctime)s %(levelname)s %(message)s"
    )

def log_console(msg: str):
    print(f"[NameNode] {msg}")
    logging.info(msg)

# ---------- UTILS ----------
def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

def save_metadata():
    with lock:
        with open(METADATA_FILE, "w") as f:
            json.dump(metadata, f, indent=2)

def load_metadata():
    global metadata
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            metadata = json.load(f)
    else:
        save_metadata()

# ---------- SOCKET HELPERS ----------
def recv_all(conn: socket.socket, timeout=120) -> bytes:
    """Receive all data from socket until EOF or timeout."""
    conn.settimeout(timeout)
    chunks = []
    total = 0
    
    while True:
        try:
            part = conn.recv(65536)
            if not part:
                break
            chunks.append(part)
            total += len(part)
            
            # Log progress every 1MB
            if total % (1024 * 1024) == 0:
                log_console(f"📥 Received {total // (1024*1024)} MB so far...")
                
        except socket.timeout:
            log_console(f"⏱ Timeout after receiving {total} bytes")
            break  # ✅ FIXED: Exit on timeout instead of continue
        except Exception as e:
            log_console(f"recv_all exception: {e}")
            break
            
    data = b"".join(chunks)
    log_console(f"📥 Received {len(data)} bytes total ({len(data)/1024/1024:.2f} MB)")
    return data

def alive_nodes() -> List[str]:
    with lock:
        return list(metadata["datanode_status"].keys())

# ---------- HEARTBEATS ----------
def heartbeat_listener():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", HEARTBEAT_UDP_PORT))
    log_console(f"💓 Heartbeat listener active on 0.0.0.0:{HEARTBEAT_UDP_PORT}")
    while True:
        try:
            data, _ = sock.recvfrom(512)
            node_name = data.decode(errors="ignore").strip()
            log_console(f"✅ Heartbeat from {node_name}")
            with lock:
                metadata["datanode_status"][node_name] = time.time()
            save_metadata()
        except Exception as e:
            log_console(f"⚠ Heartbeat error: {e}")

def update_gauges():
    """Refresh all Prometheus gauges from current metadata. Call after any mutation."""
    with lock:
        files     = metadata["files"]
        alive_set = set(metadata["datanode_status"].keys())

    file_count    = len(files)
    chunk_count   = sum(len(e["chunks"]) for e in files.values())
    bytes_total   = sum(e.get("size_bytes", 0) for e in files.values())
    under_count   = sum(
        1
        for e in files.values()
        for ch in e["chunks"]
        if len([r for r in ch["replicas"] if r in alive_set]) < len(ch["replicas"])
    )

    HDFS_FILES_TOTAL.set(file_count)
    HDFS_CHUNKS_TOTAL.set(chunk_count)
    HDFS_BYTES_STORED.set(bytes_total)
    HDFS_UNDER_REPLICATED.set(under_count)
    HDFS_DATANODES_ALIVE.set(len(alive_set))


def monitor_nodes():
    while True:
        time.sleep(HEARTBEAT_CHECK_EVERY)
        now = time.time()
        changed = False
        with lock:
            for node, last in list(metadata["datanode_status"].items()):
                if now - last > HEARTBEAT_TIMEOUT_SEC:
                    log_console(f"⚠ {node} is DOWN (no heartbeat)")
                    del metadata["datanode_status"][node]
                    changed = True
        if changed:
            save_metadata()
        update_gauges()
def re_replicate():
    while True:
        time.sleep(HEARTBEAT_CHECK_EVERY)
        with lock:
            alive = list(metadata["datanode_status"].keys())
            files_snapshot = {
                fname: entry
                for fname, entry in metadata["files"].items()
            }

        for fname, entry in files_snapshot.items():
            for ch in entry["chunks"]:
                chunk_name = ch["chunk_name"]
                current_replicas = ch["replicas"]
                alive_replicas = [n for n in current_replicas if n in alive and n in DATANODES]
                dead_replicas = [n for n in current_replicas if n not in alive and n in DATANODES]

                need = REPLICATION_FACTOR - len(alive_replicas)
                if need <= 0:
                    continue

                # Find nodes that don't already have this chunk
                candidates = [n for n in alive if n not in current_replicas and n in DATANODES]
                if not candidates:
                    log_console(f"⚠ No candidates to re-replicate {chunk_name}")
                    continue

                # Read chunk from a surviving replica
                source = alive_replicas[0] if alive_replicas else None
                if not source:
                    log_console(f"⚠ No alive source for {chunk_name} — data lost")
                    continue

                res = read_chunk(source, chunk_name)
                if res.get("status") != "ok":
                    log_console(f"⚠ Could not read {chunk_name} from {source}")
                    continue

                content_b64 = res["content_b64"]

                # Push to candidates up to `need` count
                new_replicas = []
                for target in candidates[:need]:
                    if store_chunk(target, chunk_name, content_b64):
                        new_replicas.append(target)
                        log_console(f"♻ Re-replicated {chunk_name} → {target}")

                if new_replicas:
                    with lock:
                        updated = [n for n in current_replicas if n in alive] + new_replicas
                        metadata["files"][fname]["chunks"] = [
                            {**c, "replicas": updated} if c["chunk_name"] == chunk_name else c
                            for c in metadata["files"][fname]["chunks"]
                        ]
                    save_metadata()

        time.sleep(HEARTBEAT_CHECK_EVERY)

# ---------- DATANODE RPC ----------
def store_chunk(node: str, chunk_name: str, content_str: str) -> bool:
    """Store a chunk on a DataNode."""
    if node not in alive_nodes():
        log_console(f"⚠ Skipping {node} (not alive)")
        return False

    ip, port = DATANODES[node]
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Disable Nagle's algorithm for faster small writes
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # Increase socket buffer sizes for large chunks
        s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8_000_000)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8_000_000)
        s.settimeout(90)  # ✅ 90 seconds for large chunks
        s.connect((ip, port))
        
        payload = {"action": "store", "filename": chunk_name, "content_b64": content_str}
        req_data = json.dumps(payload).encode()
        
        log_console(f"📤 Sending {len(req_data)//1024} KB to {node}")
        s.sendall(req_data)
        
        # Shutdown write side to signal end of request
        try:
            s.shutdown(socket.SHUT_WR)
        except:
            pass
        
        data = recv_all(s, timeout=90)  # ✅ 90 seconds to receive response
        
        if data:
            res = json.loads(data.decode())
            if res.get("status") == "stored":
                log_console(f"✅ Stored {chunk_name} on {node}")
                return True
        
        log_console(f"⚠ Store failed on {node}")
        return False
    except socket.timeout:
        log_console(f"❌ Timeout storing on {node}")
        return False
    except Exception as e:
        log_console(f"❌ Store on {node} failed: {e}")
        return False
    finally:
        if s:
            try:
                s.close()
            except:
                pass

def read_chunk(node: str, chunk_name: str) -> dict:
    ip, port = DATANODES[node]
    try:
        s = socket.socket()
        s.settimeout(30)
        s.connect((ip, port))
        s.sendall(json.dumps({"action": "read", "filename": chunk_name}).encode())
        try:
            s.shutdown(socket.SHUT_WR)
        except:
            pass
        data = recv_all(s, timeout=30)
        s.close()
        if data:
            return json.loads(data.decode())
        return {}
    except Exception as e:
        log_console(f"❌ Read from {node} failed: {e}")
        return {}
# ---------- CLIENT HANDLERS ----------
def handle_upload(filename: str, content_str: str = None, content_b64: str = None) -> dict:
    """Handle file upload with chunking and replication."""
    t_start = time.time()
    try:
        log_console(f"🔼 Starting upload for {filename}")
        
        # Support both latin-1 and base64 encoding
        if content_b64:
            import base64
            log_console(f"📦 Decoding base64 content ({len(content_b64)} chars)")
            raw = base64.b64decode(content_b64)
        elif content_str:
            log_console(f"📦 Decoding latin-1 content ({len(content_str)} chars)")
            raw = content_str.encode("latin-1", errors="ignore")
        else:
            return {"status": "error", "msg": "No content provided"}
        
        log_console(f"📊 File size: {len(raw)} bytes ({len(raw)/1024/1024:.2f} MB)")
        
        if len(raw) > MAX_UPLOAD_BYTES:
            return {"status": "error", "msg": "File too large (> 50 MB)"}

        alive = alive_nodes()
        if not alive:
            return {"status": "error", "msg": "No DataNodes alive"}

        # Split into chunks
        parts = [raw[i:i + CHUNK_SIZE] for i in range(0, len(raw), CHUNK_SIZE)]
        if not parts:
            parts = [b""]  # Empty file

        log_console(f"📦 File will be split into {len(parts)} chunks ({CHUNK_SIZE/1024/1024:.1f}MB each)")
        chunks_meta = []

        for i, part in enumerate(parts):
            chunk_name = f"{filename}_part{i}"
            payload = base64.b64encode(part).decode("ascii")
            chunk_hash = sha256_bytes(part)
            
            log_console(f"📤 Chunk {i+1}/{len(parts)}: {chunk_name} ({len(part)} bytes)")

            # Replicate to all available nodes
            # Replicate to min(RF, alive_nodes) nodes
            replicas = []
            targets = alive[:REPLICATION_FACTOR]
            for node in targets:
                log_console(f"  → Storing on {node}...")
                if store_chunk(node, chunk_name, payload):
                    replicas.append(node)
                    log_console(f"  ✅ {node}")
                else:
                    log_console(f"  ❌ {node}")

            if not replicas:
                log_console(f"❌ No replicas for chunk {chunk_name}")
                return {"status": "error", "msg": f"Failed to store {chunk_name}"}

            chunks_meta.append({
                "chunk_name": chunk_name,
                "replicas": replicas,
                "checksum": chunk_hash
            })
            
            log_console(f"✅ Chunk {i+1}/{len(parts)} stored on {len(replicas)} node(s)")

        # Save metadata
        with lock:
            metadata["files"][filename] = {
                "chunks": chunks_meta,
                "file_checksum": sha256_bytes(raw),
                "size_bytes": len(raw),
                "uploaded_at": time.strftime("%Y-%m-%d %H:%M:%S")
            }
        save_metadata()
        HDFS_UPLOADS_TOTAL.inc()
        HDFS_RPC_LATENCY.labels(op="upload").observe(time.time() - t_start)
        update_gauges()

        log_console(f"✅✅ UPLOAD COMPLETE: {filename} in {len(chunks_meta)} chunks")
        return {"status": "uploaded", "chunks": len(chunks_meta)}

    except Exception as e:
        log_console(f"❌ Upload error: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "msg": str(e)}

def handle_download(filename: str) -> dict:
    """Return chunk manifest for download."""
    t_start = time.time()
    with lock:
        entry = metadata["files"].get(filename)
        if not entry:
            return {"status": "error", "msg": "File not found"}
        result = {"status": "ok", "chunks": entry["chunks"]}
    HDFS_DOWNLOADS_TOTAL.inc()
    HDFS_RPC_LATENCY.labels(op="download").observe(time.time() - t_start)
    return result

def handle_delete(filename: str) -> dict:
    with lock:
        entry = metadata["files"].get(filename)
        if not entry:
            return {"status": "error", "msg": "File not found"}
        chunks = entry["chunks"]

    # Delete from each replica node
    failed = []
    for ch in chunks:
        for node in ch["replicas"]:
            ip, port = DATANODES[node]
            try:
                s = socket.socket()
                s.settimeout(10)
                s.connect((ip, port))
                s.sendall(json.dumps({"action": "delete", "filename": ch["chunk_name"]}).encode())
                try:
                    s.shutdown(socket.SHUT_WR)
                except:
                    pass
                data = recv_all(s, timeout=10)
                s.close()
                if data:
                    res = json.loads(data.decode())
                    if res.get("status") != "deleted":
                        failed.append((ch["chunk_name"], node))
            except Exception as e:
                log_console(f"❌ Delete {ch['chunk_name']} from {node} failed: {e}")
                failed.append((ch["chunk_name"], node))

    # Remove from metadata regardless (best-effort delete)
    with lock:
        del metadata["files"][filename]
    save_metadata()

    HDFS_DELETES_TOTAL.inc()
    update_gauges()

    if failed:
        return {"status": "partial", "msg": f"Deleted from namespace, {len(failed)} chunk(s) failed on nodes"}
    return {"status": "deleted"}

def handle_list() -> dict:
    with lock:
        files = []
        for fname, entry in metadata["files"].items():
            files.append({
                "filename": fname,
                "size_bytes": entry.get("size_bytes", 0),
                "chunks": len(entry["chunks"]),
                "uploaded_at": entry.get("uploaded_at", "unknown"),
                "checksum": entry.get("file_checksum", "")[:16]
            })
    return {"status": "ok", "files": files}

def handle_status() -> dict:
    """Return cluster status."""
    with lock:
        now = time.time()
        nodes = {}
        
        for n in DATANODES:
            ts = metadata["datanode_status"].get(n)
            nodes[n] = {
                "state": "alive" if ts else "down",
                "last_heartbeat_age_sec": (None if ts is None else round(now - ts, 1))
            }
        
        # Check for under-replicated chunks
        under = []
        for fname, entry in metadata["files"].items():
            for ch in entry["chunks"]:
                alive_replicas = [n for n in ch["replicas"] if n in metadata["datanode_status"]]
                if len(alive_replicas) < len(ch["replicas"]):
                    under.append({
                        "file": fname,
                        "chunk": ch["chunk_name"],
                        "have": alive_replicas,
                        "want": ch["replicas"]
                    })
        
        res = {
            "status": "ok",
            "nodes": nodes,
            "files": metadata["files"],
            "under_replicated": under
        }
        
        log_console(f"✅ Status: {len(metadata['files'])} files, {len(nodes)} nodes")
        return res

# ---------- CLIENT CONNECTION HANDLER ----------
def client_handler(conn: socket.socket, addr):
    try:
        # Increase buffer sizes for large uploads
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8_000_000)
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8_000_000)
        
        log_console(f"📞 New connection from {addr}")
        
        raw = recv_all(conn, timeout=180)  # ✅ 3 minutes for large uploads
        
        if not raw:
            log_console(f"⚠ Empty request from {addr}")
            return

        try:
            req = json.loads(raw.decode())
        except json.JSONDecodeError as e:
            log_console(f"❌ JSON decode error from {addr}: {e}")
            return

        action = req.get("action")
        log_console(f"📩 Request: {action} from {addr}")

        # Route to handlers
        if action == "upload":
            content_b64 = req.get("content_b64")
            content_str = req.get("content")
            res = handle_upload(req["filename"], content_str=content_str, content_b64=content_b64)
        elif action == "download":
            res = handle_download(req["filename"])
        elif action == "status":
            res = handle_status()
        elif action=="delete":
            res = handle_delete(req["filename"])
        elif action=="list":
            res = handle_list()
        else:
            res = {"status": "error", "msg": "Invalid action"}

        # Send response
        response_data = json.dumps(res).encode()
        conn.sendall(response_data)
        
        log_console(f"📤 Sent {action} response ({len(response_data)} bytes) to {addr}")

    except Exception as e:
        log_console(f"❌ Handler error: {e}")
        import traceback
        traceback.print_exc()
        try:
            conn.sendall(json.dumps({"status": "error", "msg": str(e)}).encode())
        except:
            pass
    finally:
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except:
            pass
        conn.close()
        log_console(f"🔒 Connection closed with {addr}")

# ---------- MAIN ----------
def start_namenode():
    setup_logs()
    load_metadata()
    update_gauges()  # Initialise metrics from persisted metadata on restart

    # Start Prometheus metrics HTTP server
    metrics_port = int(os.environ.get("METRICS_PORT", "8080"))
    start_http_server(metrics_port)
    log_console(f"📊 Prometheus metrics server started on :{metrics_port}/metrics")

    # Start background threads
    threading.Thread(target=heartbeat_listener, daemon=True).start()
    threading.Thread(target=monitor_nodes, daemon=True).start()
    threading.Thread(target=re_replicate, daemon=True).start()

    log_console(f"🧠 NameNode listening on 0.0.0.0:{NAMENODE_PORT}")
    
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", NAMENODE_PORT))
    srv.listen(32)
    
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=client_handler, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    start_namenode()