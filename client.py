#!/usr/bin/env python3
# Client GUI – Mini HDFS (FIXED VERSION)
# All functionality working: Status, Upload, Download with Debug Console

import socket
import json
import streamlit as st
from datetime import datetime
import hashlib
import time
import base64
import os
# ---------- CONFIG ----------
NAMENODE = (
    os.environ.get("NAMENODE_HOST", "127.0.0.1"),
    int(os.environ.get("NAMENODE_PORT", "5000"))
)
DATANODES = {
    k: (v.split(":")[0], int(v.split(":")[1]))
    for k, v in (
        item.split("=")
        for item in os.environ.get(
            "DATANODES",
            "datanode1=127.0.0.1:5001,datanode2=127.0.0.1:5002"
        ).split(",")
    )
}
MAX_UPLOAD_BYTES = 50 * 1024 * 1024

# ---------- DEBUG LOGGING ----------
if 'debug_logs' not in st.session_state:
    st.session_state.debug_logs = []

def debug(msg):
    """Log debug messages to both terminal and Streamlit session."""
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line)
    st.session_state.debug_logs.append(line)
    if len(st.session_state.debug_logs) > 200:
        st.session_state.debug_logs.pop(0)

# ---------- SOCKET HELPERS ----------
def recv_all(s: socket.socket, timeout=120) -> bytes:
    """Receive all data from socket until EOF or timeout."""
    s.settimeout(timeout)
    chunks = []
    total = 0
    
    while True:
        try:
            part = s.recv(65536)
            if not part:
                break
            chunks.append(part)
            total += len(part)
            
            # Log progress
            if total % (512 * 1024) == 0:  # Every 512KB
                debug(f"📥 Received {total // 1024} KB...")
                
        except socket.timeout:
            debug(f"⚠ Socket timeout after {total} bytes")
            break
        except Exception as e:
            debug(f"⚠ Socket error: {e}")
            break

    full = b"".join(chunks)
    debug(f"📥 Total received: {len(full)} bytes ({len(full)/1024:.1f} KB)")
    return full

def send_namenode(payload: dict, timeout=240) -> dict:
    """Send request to NameNode and get response."""
    debug(f"→ Connecting to NameNode: {NAMENODE}")
    s = socket.socket()
    
    try:
        # Increase buffer sizes for large uploads
        s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8_000_000)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8_000_000)
        s.settimeout(timeout)
        s.connect(NAMENODE)
        debug(f"✅ Connected to NameNode")

        # Send request
        req = json.dumps(payload).encode()
        size_mb = len(req) / 1024 / 1024
        debug(f"📤 Sending {len(req)} bytes ({size_mb:.2f} MB)")
        
        s.sendall(req)
        debug(f"✅ Sent complete")
        
        # Graceful shutdown to signal end of request
        try:
            s.shutdown(socket.SHUT_WR)
        except:
            pass

        # Receive response
        data = recv_all(s, timeout=timeout)
        
        if not data:
            debug("❌ Empty response from NameNode")
            return {"status": "error", "msg": "Empty response from NameNode"}
        
        debug(f"✅ Got response from NameNode")
        return json.loads(data.decode())

    except Exception as e:
        debug(f"❌ NameNode connection failed: {e}")
        return {"status": "error", "msg": str(e)}
    finally:
        s.close()

def read_chunk_from(node_key: str, chunk_name: str):
    """Read a specific chunk from a DataNode."""
    ip, port = DATANODES[node_key]
    debug(f"→ Requesting {chunk_name} from {node_key}")
    
    s = socket.socket()
    # Increase buffer sizes
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4_000_000)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4_000_000)
    s.settimeout(60)  # ✅ Increased from 10s to 60s
    
    try:
        s.connect((ip, port))
        request = json.dumps({"action": "read", "filename": chunk_name}).encode()
        s.sendall(request)
        
        # Shutdown write side
        try:
            s.shutdown(socket.SHUT_WR)
        except:
            pass
        
        data = recv_all(s, timeout=60)  # ✅ Increased from 10s to 60s
        
        if not data:
            debug(f"❌ No data from {node_key}")
            return None
        
        res = json.loads(data.decode())
        
        if res.get("status") == "ok":
            debug(f"✅ Got {chunk_name} from {node_key}")
            return base64.b64decode(res["content_b64"])
        
        debug(f"⚠ Read failed: {res}")
        return None
        
    except Exception as e:
        debug(f"❌ Failed to read from {node_key}: {e}")
        return None
    finally:
        s.close()

def sha256_bytes(b: bytes) -> str:
    """Calculate SHA-256 hash of bytes."""
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

# ---------- CLIENT ACTIONS ----------
def upload_file(filename: str, raw_bytes: bytes):
    """Upload a file to HDFS using base64 encoding."""
    size_mb = len(raw_bytes) / 1024 / 1024
    debug(f"🔼 Uploading {filename} ({len(raw_bytes)} bytes = {size_mb:.2f} MB)")
    
    if len(raw_bytes) > MAX_UPLOAD_BYTES:
        return {"status": "error", "msg": "File exceeds 50 MB limit"}

    # Use base64 encoding - more efficient and reliable than latin-1
    import base64
    content_b64 = base64.b64encode(raw_bytes).decode('ascii')
    debug(f"📦 Encoded to base64: {len(content_b64)} chars")
    
    payload = {
        "action": "upload",
        "filename": filename,
        "content_b64": content_b64  # Use base64 key
    }
    
    # Calculate estimated time: ~5 seconds per MB
    estimated_time = max(3000, int(size_mb * 60))
    debug(f"⏱ Estimated time: {estimated_time}s")
    
    return send_namenode(payload, timeout=estimated_time)

def download_manifest(filename: str):
    """Get download manifest from NameNode."""
    return send_namenode({"action": "download", "filename": filename})

def get_status():
    """Get cluster status from NameNode."""
    return send_namenode({"action": "status"})

def download_file(filename: str):
    """Download and reconstruct a file from chunks."""
    debug(f"🔽 Downloading {filename}")
    
    # Get manifest
    man = download_manifest(filename)
    if man.get("status") != "ok":
        return None, "File not found or NameNode unavailable"
    
    chunks = man["chunks"]
    
    # Get node health to prefer alive replicas
    status = get_status()
    nodes_health = status.get("nodes", {}) if status.get("status") == "ok" else {}

    out = bytearray()
    
    for ch in chunks:
        chunk_name = ch["chunk_name"]
        replicas = ch["replicas"]
        
        # Sort replicas: alive nodes first
        replicas_sorted = sorted(
            replicas,
            key=lambda n: 0 if nodes_health.get(n, {}).get("state") == "alive" else 1
        )
        
        got = False
        for node in replicas_sorted:
            blob = read_chunk_from(node, chunk_name)
            if blob is not None:
                out.extend(blob)
                got = True
                break
        
        if not got:
            return None, f"Missing chunk {chunk_name} (all replicas down)"
    
    debug(f"✅ Downloaded {filename} ({len(out)} bytes)")
    return bytes(out), None

def delete_file(filename: str):
    return send_namenode({"action": "delete", "filename": filename})
# ---------- STREAMLIT UI ----------
st.set_page_config(page_title="Mini HDFS", page_icon="🗂", layout="wide")
st.title("🗂 Mini HDFS Dashboard")

def list_files():
    return send_namenode({"action": "list"})

# Create tabs
tabs = st.tabs(["📊 Status", "📁 Files", "⬆ Upload", "⬇ Download", "🗑 Delete", "🐛 Debug Console"])

# ---------- STATUS TAB ----------
with tabs[0]:
    st.subheader("Cluster Status")
    
    if st.button("🔄 Refresh Status"):
        st.rerun()
    
    status = get_status()
    
    if status.get("status") == "ok":
        nodes = status["nodes"]
        files = status["files"]
        under = status.get("under_replicated", [])

        # Node health display
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("*DataNodes*")
            for n, info in nodes.items():
                badge = "🟢" if info["state"] == "alive" else "🔴"
                age = info["last_heartbeat_age_sec"]
                age_str = "—" if age is None else f"{age}s ago"
                st.write(f"{badge} *{n}*: {info['state']} (last HB: {age_str})")
        
        with col2:
            st.markdown("*Last Refresh*")
            st.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            st.markdown(f"*Total Files*: {len(files)}")

        st.markdown("---")
        
        # Under-replicated warning
        if under:
            st.warning(f"⚠ {len(under)} under-replicated chunk(s) detected:")
            for u in under:
                st.code(f"{u['chunk']} of {u['file']} — have: {u['have']}, want: {u['want']}")
        else:
            st.success("✅ All chunks fully replicated")

        st.markdown("---")
        
        # File details
        st.subheader("Files & Chunk Mapping")
        
        if files:
            for fname, entry in files.items():
                with st.expander(f"📄 *{fname}* (SHA-256: {entry.get('file_checksum', 'N/A')[:16]}...)"):
                    for ch in entry["chunks"]:
                        reps = ", ".join(ch["replicas"])
                        st.code(f"{ch['chunk_name']} → [{reps}] • checksum: {ch.get('checksum', 'N/A')[:16]}...")
        else:
            st.info("No files uploaded yet")
    else:
        st.error(f"❌ Could not fetch status: {status.get('msg', 'unknown error')}")

with tabs[1]:
    st.subheader("All Files")

    if st.button("🔄 Refresh"):
        st.rerun()

    res = list_files()
    if res.get("status") == "ok":
        files = res["files"]
        if not files:
            st.info("No files uploaded yet")
        else:
            for f in files:
                size_kb = f["size_bytes"] / 1024
                size_str = f"{size_kb:.1f} KB" if size_kb < 1024 else f"{size_kb/1024:.2f} MB"
                st.write(f"📄 **{f['filename']}** — {size_str} — {f['chunks']} chunk(s) — uploaded {f['uploaded_at']}")
    else:
        st.error(f"❌ {res.get('msg', 'unknown error')}")
# ---------- UPLOAD TAB ----------
with tabs[2]:
    st.subheader("Upload File (≤ 50 MB)")
    
    up = st.file_uploader("Choose a file", key="uploader")
    
    if up:
        raw = up.read()
        size_kb = len(raw) // 1024
        size_mb = len(raw) / (1024 * 1024)
        
        st.write(f"📁 *Selected*: {up.name}")
        st.write(f"📏 *Size*: {size_kb} KB ({size_mb:.2f} MB)")
        
        if len(raw) > MAX_UPLOAD_BYTES:
            st.error("❌ File exceeds 50 MB limit")
        else:
            if st.button("⬆ Upload to HDFS"):
                with st.spinner("Uploading..."):
                    res = upload_file(up.name, raw)
                    
                    if res.get("status") == "uploaded":
                        st.success(f"✅ Uploaded successfully in {res['chunks']} chunk(s)!")
                        st.balloons()
                    else:
                        st.error(f"❌ Upload failed: {res.get('msg', 'unknown error')}")

# ---------- DOWNLOAD TAB ----------
with tabs[3]:
    st.subheader("Download File")
    
    fname = st.text_input("Enter exact filename to download:")
    
    if st.button("⬇ Download"):
        if not fname:
            st.warning("⚠ Please enter a filename first")
        else:
            with st.spinner("Downloading..."):
                # Get expected checksum
                stat = get_status()
                expected = None
                try:
                    expected = stat["files"][fname]["file_checksum"]
                except:
                    pass

                # Download file
                data, err = download_file(fname)
                
                if err:
                    st.error(f"❌ {err}")
                else:
                    # Verify integrity
                    actual = sha256_bytes(data)
                    
                    if expected and actual == expected:
                        st.success(f"✅ Integrity verified (SHA-256: {actual[:16]}...)")
                    elif expected:
                        st.error(f"⚠ Integrity mismatch!\nExpected: {expected}\nGot: {actual}")
                    else:
                        st.info("ℹ Checksum not available for verification")
                    
                    # Download button
                    st.download_button(
                        label="💾 Download File",
                        data=data,
                        file_name=fname,
                        mime="application/octet-stream"
                    )

with tabs[4]:
    st.subheader("Delete File")
    
    del_fname = st.text_input("Enter exact filename to delete:")
    
    if st.button("🗑 Delete from HDFS"):
        if not del_fname:
            st.warning("⚠ Please enter a filename first")
        else:
            res = delete_file(del_fname)
            if res.get("status") == "deleted":
                st.success(f"✅ {del_fname} deleted from all nodes")
            elif res.get("status") == "partial":
                st.warning(f"⚠ {res.get('msg')}")
            else:
                st.error(f"❌ {res.get('msg', 'unknown error')}")

# ---------- DEBUG CONSOLE TAB ----------
with tabs[5]:
    st.subheader("Live Debug Console")
    st.caption("Real-time client logs for socket activity and operations")
    
    if st.button("🗑 Clear Logs"):
        st.session_state.debug_logs = []
        st.rerun()
    
    # Display logs in reverse order (newest first)
    log_text = "\n".join(reversed(st.session_state.debug_logs[-100:]))
    st.text_area("Debug Logs", log_text, height=400, disabled=True)
