# Mini HDFS — Distributed File System Clone

A from-scratch implementation of the core HDFS (Hadoop Distributed File System) architecture in Python, with a Streamlit web dashboard for cluster management. Built to understand and demonstrate how distributed storage systems work at the protocol level — block splitting, replication, fault detection, and metadata management — without any Hadoop dependencies.

---

## Table of Contents

1. [What This Is](#what-this-is)
2. [Architecture Overview](#architecture-overview)
3. [What Works Right Now](#what-works-right-now)
4. [Known Bugs](#known-bugs)
5. [Project Structure](#project-structure)
6. [How to Run (Current State)](#how-to-run-current-state)
7. [Upgrade Roadmap](#upgrade-roadmap)
   - [Tier 1 — Fixes](#tier-1--fixes-things-that-are-actually-broken)
   - [Tier 2 — Feature Additions](#tier-2--feature-additions)
   - [Tier 3 — Infrastructure](#tier-3--infrastructure)
   - [Tier 4 — Polish](#tier-4--polish)
8. [Resume Impact by Tier](#resume-impact-by-tier)

---

## What This Is

HDFS (Hadoop Distributed File System) is the storage backbone of the Hadoop ecosystem. Real HDFS splits files into fixed-size blocks, distributes and replicates those blocks across a cluster of machines called DataNodes, and uses a central coordinator called the NameNode to track where every block lives. When a machine dies, HDFS automatically re-replicates the lost blocks elsewhere to maintain the configured replication factor.

This project reimplements that core architecture from scratch using raw Python sockets and threading — no Hadoop, no HDFS libraries, no external frameworks in the storage path. The goal is to demonstrate a real understanding of distributed systems internals: the RPC protocol between client and NameNode, the block store protocol between NameNode and DataNodes, heartbeat-based failure detection, and end-to-end data integrity via checksums.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                        CLIENT (Streamlit)                    │
│  • Upload file → NameNode                                    │
│  • Download: get manifest → fetch chunks from DataNodes      │
│  • Status: view cluster health, file list, replication state │
└────────────────────────┬────────────────────────────────────┘
                         │ TCP :5000
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                        NAMENODE                              │
│  • Receives full file from client                            │
│  • Splits into 2MB chunks                                    │
│  • Distributes chunks to all alive DataNodes (replication)   │
│  • Tracks chunk→node mapping in metadata.json                │
│  • SHA-256 checksums per chunk and per file                  │
│  • Listens for UDP heartbeats from DataNodes on :6000        │
│  • Marks nodes dead after 15s of missed heartbeats           │
│  • Detects under-replicated chunks on status request         │
│  • Rotating log file at logs/namenode.log                    │
└──────────┬────────────────────────────┬───────────────────┘
           │ TCP :5001                  │ TCP :5002
           ▼                            ▼
┌─────────────────────┐     ┌─────────────────────┐
│     DATANODE 1       │     │     DATANODE 2       │
│  • Stores chunks     │     │  • Stores chunks     │
│    as raw files in   │     │    as raw files in   │
│    data_blocks/      │     │    data_blocks/      │
│  • Serves reads      │     │  • Serves reads      │
│  • UDP heartbeat     │     │  • UDP heartbeat     │
│    every 5s          │     │    every 5s          │
└─────────────────────┘     └─────────────────────┘
```

**Data flow on upload:**
1. Client base64-encodes the file and sends it as JSON to NameNode over TCP
2. NameNode decodes it, splits into 2MB chunks, iterates over all alive DataNodes
3. For each chunk, NameNode sends `{"action": "store", "filename": "...", "content": "..."}` to each DataNode
4. DataNode writes the chunk to disk and responds `{"status": "stored"}`
5. NameNode records chunk name, which nodes hold replicas, and SHA-256 checksum
6. Full file checksum is stored in metadata.json

**Data flow on download:**
1. Client asks NameNode for the file manifest (chunk names + replica node list)
2. NameNode returns manifest, sorted by alive nodes first
3. Client fetches each chunk directly from a DataNode in order
4. Client reassembles chunks in order and verifies full-file SHA-256

---

## What Works Right Now

| Feature | Status | Notes |
|---|---|---|
| File upload (text + binary) | ✅ Working | Base64 encoded, up to 50MB |
| File chunking | ✅ Working | 2MB fixed chunk size |
| Replication to all DataNodes | ✅ Working | Stores on every alive node |
| File download + reassembly | ✅ Working | Fetches from fastest alive replica |
| End-to-end SHA-256 integrity | ✅ Working | Per-chunk and full-file |
| Per-chunk checksums | ✅ Working | Stored in metadata |
| UDP heartbeat (DataNode → NameNode) | ✅ Working | Every 5 seconds |
| Liveness detection | ✅ Working | 15s timeout marks node dead |
| Under-replication detection | ✅ Working | Shown in dashboard |
| Metadata persistence | ✅ Working | metadata.json survives restarts |
| NameNode rotating logs | ✅ Working | logs/namenode.log |
| Streamlit dashboard | ✅ Working | Status, upload, download, debug tabs |
| Live debug console | ✅ Working | Socket-level trace in browser |
| Prefer alive replicas on download | ✅ Working | Sorted by node health |

---

## Known Bugs

These are confirmed issues in the current codebase, documented here for reference before fixing.

**1. `__name__` guard typo in namenode.py**
The last line reads `if _name_ == "_main_":` — missing the double underscores. The NameNode cannot be launched as `python namenode.py`. Currently must be run by calling `start_namenode()` directly or via a workaround.

**2. datanode1.py and datanode2.py are the same file**
Both files are identical except for the `DATANODE_NAME`, `DATANODE_HOST`, and `DATANODE_PORT` constants. Adding a third DataNode means copying the file again and manually editing it. This makes the system inherently non-scalable and is the biggest architectural smell in the project.

**3. Binary encoding not applied consistently**
Client sends files to NameNode in base64 (correct). NameNode sends chunks to DataNodes using `latin-1` string encoding inside JSON. For some binary byte sequences, `latin-1` with `errors="ignore"` silently drops bytes, causing data corruption on binary files (images, PDFs, compiled executables). The base64 path needs to be extended all the way to the DataNodes.

**4. DataNode `recv_all` has no timeout**
The DataNode's `recv_all` function loops until it receives an empty `recv()` (EOF). If a client or NameNode crashes mid-send, the DataNode thread blocks indefinitely — no timeout, no cleanup. Under sustained traffic this will exhaust the thread pool.

**5. No block re-replication**
Under-replication is detected and shown in the dashboard but nothing acts on it. When a DataNode dies, its chunks are permanently under-replicated. The NameNode needs a background thread that detects this and copies the missing chunks from a surviving replica to another alive node.

**6. All IPs are hardcoded**
Every file contains `172.22.x.x` ZeroTier IP addresses. The project cannot be run by anyone else or deployed anywhere without manually editing all four files.

**7. No file delete operation**
There is no `rm` command. Once a file is uploaded, it cannot be removed from the cluster without manually deleting the metadata.json entry and the block files from each DataNode's `data_blocks/` directory.

---

## Project Structure

```
hdfs-clone/
├── namenode.py          # Central coordinator — metadata, routing, heartbeats
├── datanode1.py         # DataNode instance 1 (hardcoded config)
├── datanode2.py         # DataNode instance 2 (identical to datanode1.py)
├── client.py            # Streamlit web dashboard
├── metadata.json        # Persistent namespace — files, chunks, replica mapping
└── logs/
    └── namenode.log     # Rotating NameNode logs
```

Each DataNode creates a `data_blocks/` directory locally where it stores raw chunk files.

---

## How to Run (Current State)

**Requirements:**
```
pip install streamlit
```

No other external dependencies. Pure Python stdlib for networking.

**Step 1 — Start the NameNode:**
```bash
python namenode.py
# Listens on :5000 (client RPC) and :6000 (heartbeat UDP)
```

**Step 2 — Start DataNode 1** (on a separate machine or terminal):
```bash
python datanode1.py
# Listens on :5001
```

**Step 3 — Start DataNode 2:**
```bash
python datanode2.py
# Listens on :5002
```

**Step 4 — Launch the client dashboard:**
```bash
streamlit run client.py
# Opens at http://localhost:8501
```

> **Note:** The current setup requires ZeroTier or a shared network with the hardcoded `172.22.x.x` IPs. To run locally, change all IPs to `127.0.0.1` across all four files before starting.

---

## Upgrade Roadmap

The following tiers take this from "works on my machine" to a production-aware, resume-worthy distributed systems project. Each tier is self-contained and can be done independently.

---

### Tier 1 — Fixes (Things That Are Actually Broken)

**Goal:** Make the project correct and defensible. These are bugs, not enhancements.

---

#### 1.1 Fix the `__name__` guard in namenode.py

**File:** `namenode.py`, last line  
**Change:** `if _name_ == "_main_":` → `if __name__ == "__main__":`  
**Why it matters:** The NameNode literally cannot be launched as a script in its current state.

---

#### 1.2 Merge datanode1.py + datanode2.py into one file

**New file:** `datanode.py`  
**Approach:** Replace hardcoded constants with environment variables:

```
DATANODE_NAME  = os.environ["DATANODE_NAME"]   # e.g. "datanode1"
DATANODE_HOST  = os.environ["DATANODE_HOST"]   # e.g. "0.0.0.0"
DATANODE_PORT  = int(os.environ["DATANODE_PORT"])  # e.g. 5001
NAMENODE_HOST  = os.environ.get("NAMENODE_HOST", "127.0.0.1")
```

**Why it matters:** This is the single biggest red flag in the current code. One `datanode.py` driven by env vars is how every real distributed system works — it's what makes the Docker/Kubernetes story possible at all.

---

#### 1.3 Fix binary encoding on the NameNode → DataNode path

**File:** `namenode.py` (`store_chunk`) and `datanode.py` (`handle_conn`)  
**Change:** Encode chunk bytes as base64 in the JSON payload sent to DataNodes, same as the client→NameNode path already does. DataNode decodes base64 on receipt and writes raw bytes.  
**Why it matters:** `latin-1` with `errors="ignore"` silently drops bytes. Any PDF, image, or compiled binary uploaded right now may be corrupted at the DataNode layer without the checksum catching it (since the checksum is computed before the encoding round-trip).

---

#### 1.4 Add timeout to DataNode `recv_all`

**File:** `datanode.py`  
**Change:** Add `conn.settimeout(30)` before the recv loop and handle `socket.timeout` with a clean break and log.  
**Why it matters:** Without this, a crashed client leaves a zombie thread in the DataNode permanently. Under any sustained load this silently exhausts resources.

---

#### 1.5 Make IPs configurable

**Files:** All four  
**Change:** Replace all hardcoded `172.22.x.x` addresses with environment variables with `127.0.0.1` defaults.  
**Why it matters:** No one else can run this project. It cannot be Docker-ized or deployed without this change. It also demonstrates you understand the separation between code and configuration.

---

### Tier 2 — Feature Additions

**Goal:** Add the missing operations that a real distributed FS needs. These are the features that make the system complete enough to describe confidently in an interview.

---

#### 2.1 Block re-replication on DataNode failure

**File:** `namenode.py`  
**Where:** New background thread, runs alongside `monitor_nodes()`  
**Logic:**
1. When `monitor_nodes` marks a node dead, add its chunks to a re-replication queue
2. Background thread picks from the queue, reads each chunk from a surviving replica, stores it on a currently-alive node that doesn't already hold that chunk
3. Updates metadata to reflect the new replica set

**Why it matters:** Without this, the under-replication detection in the dashboard is a red light with no fire truck. This is what separates a fault-tolerant system from a fault-detecting one.

---

#### 2.2 File delete

**Files:** `namenode.py`, `datanode.py`, `client.py`  
**New action:** `"action": "delete"`  
**Logic:**
1. Client sends `{"action": "delete", "filename": "foo.txt"}` to NameNode
2. NameNode reads chunk list from metadata, sends `{"action": "delete", "filename": chunk_name}` to each replica node
3. DataNode deletes the file from `data_blocks/`
4. NameNode removes the entry from metadata.json

**Why it matters:** A file system without delete is not a file system. This is also the first operation that tests your consistency story — what happens if NameNode sends delete to datanode1 and datanode2 crashes before receiving it?

---

#### 2.3 File list operation

**Files:** `namenode.py`, `client.py`  
**New action:** `"action": "list"`  
**Returns:** List of filenames, sizes (reconstructed from chunk sizes), chunk counts, upload timestamps  
**Why it matters:** Trivial to implement but makes the dashboard complete. Also lets you add `os.path.getsize`-equivalent reporting from the NameNode's metadata alone, without contacting DataNodes.

---

#### 2.4 Configurable replication factor

**File:** `namenode.py`  
**Change:** Replace "replicate to all nodes" with "replicate to min(RF, alive_nodes)" where RF is a configurable constant (default 2).  
**Why it matters:** Real HDFS uses a replication factor of 3 by default. Replicating to all nodes works with 2 DataNodes but breaks the design for any larger cluster. This also makes the re-replication logic in 2.1 cleaner — the target is always RF copies, not N copies.

---

### Tier 3 — Infrastructure

**Goal:** Make the system deployable by someone who has never seen the code. This is the tier that converts the project from "CS assignment" to "I deployed a distributed system."

---

#### 3.1 Dockerize each component

**New files:**
```
namenode/
├── Dockerfile
└── namenode.py          # (copied, no hardcoded IPs after Tier 1)

datanode/
├── Dockerfile
└── datanode.py          # (single env-var-driven file after Tier 1)

client/
├── Dockerfile
└── client.py
```

**NameNode Dockerfile (sketch):**
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY namenode.py .
RUN pip install --no-cache-dir (any deps)
EXPOSE 5000 6000
CMD ["python", "namenode.py"]
```

**Why it matters:** Docker is the bare minimum for "I can deploy this." Without it, running the project requires manually configuring IPs on 4 different machines or terminals. With it, the entire cluster starts in one command.

---

#### 3.2 docker-compose for local multi-node cluster

**New file:** `docker-compose.yml`

```yaml
version: '3.8'

services:
  namenode:
    build: ./namenode
    ports:
      - "5000:5000"
      - "6000:6000/udp"
      - "8080:8080"       # metrics (Tier 4)
    volumes:
      - nn_data:/app/data
    environment:
      - CHUNK_SIZE_MB=2
      - REPLICATION_FACTOR=2
      - HEARTBEAT_TIMEOUT_SEC=15

  datanode-1:
    build: ./datanode
    environment:
      - DATANODE_NAME=datanode1
      - DATANODE_PORT=5001
      - NAMENODE_HOST=namenode
      - NAMENODE_HEARTBEAT_PORT=6000
    volumes:
      - dn1_data:/app/data_blocks
    depends_on: [namenode]

  datanode-2:
    build: ./datanode
    environment:
      - DATANODE_NAME=datanode2
      - DATANODE_PORT=5001
      - NAMENODE_HOST=namenode
      - NAMENODE_HEARTBEAT_PORT=6000
    volumes:
      - dn2_data:/app/data_blocks
    depends_on: [namenode]

  datanode-3:
    build: ./datanode
    environment:
      - DATANODE_NAME=datanode3
      - DATANODE_PORT=5001
      - NAMENODE_HOST=namenode
      - NAMENODE_HEARTBEAT_PORT=6000
    volumes:
      - dn3_data:/app/data_blocks
    depends_on: [namenode]

  client:
    build: ./client
    ports:
      - "8501:8501"
    environment:
      - NAMENODE_HOST=namenode
      - NAMENODE_PORT=5000
    depends_on: [namenode]

volumes:
  nn_data: {}
  dn1_data: {}
  dn2_data: {}
  dn3_data: {}
```

**Startup command:**
```bash
docker-compose up --scale datanode=3
```

**Why it matters:** This is the artifact that lets you say "clone the repo, run one command, entire distributed cluster is live." The `--scale` flag also demonstrates the value of the single `datanode.py` from Tier 1 — you couldn't do this with separate datanode1.py / datanode2.py files.

---

#### 3.3 Kubernetes manifests

**New directory:** `k8s/`

```
k8s/
├── configmap.yaml          # All cluster config in one place
├── namenode/
│   ├── deployment.yaml     # Single replica (or StatefulSet for HA)
│   ├── service.yaml        # ClusterIP + LoadBalancer for dashboard
│   └── pvc.yaml            # Persistent volume for metadata.json + logs
└── datanode/
    ├── statefulset.yaml    # StatefulSet — stable pod DNS, ordered startup
    ├── service.yaml        # Headless service for pod-to-pod discovery
    └── pdb.yaml            # PodDisruptionBudget — never drop below 2 nodes
```

**Key design decisions to explain in interviews:**

- **DataNodes as StatefulSet, not Deployment.** StatefulSet gives each pod a stable DNS name (`datanode-0`, `datanode-1`, `datanode-2`) that survives restarts. The NameNode needs stable identities to track which node holds which blocks. A Deployment would give pods random names on restart, breaking the replica map.

- **PodDisruptionBudget on DataNodes.** Ensures Kubernetes never voluntarily evicts more than one DataNode at a time during rolling updates or node drains — preserving the replication factor.

- **ConfigMap for all configuration.** Chunk size, replication factor, heartbeat timeout, all moved out of code into a ConfigMap that can be changed without rebuilding images.

- **Liveness probe on NameNode** hitting a `/health` HTTP endpoint.

- **Readiness probe on DataNode** — only ready after it has successfully registered with the NameNode (sent its first heartbeat and received acknowledgment).

- **PersistentVolumeClaim on NameNode** for `metadata.json`. Without this, the namespace is lost when the pod restarts.

**StatefulSet sketch:**
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: hdfs-datanode
spec:
  serviceName: "hdfs-datanode"
  replicas: 3
  selector:
    matchLabels:
      app: hdfs-datanode
  template:
    spec:
      containers:
      - name: datanode
        image: your-registry/hdfs-datanode:latest
        envFrom:
        - configMapRef:
            name: hdfs-config
        livenessProbe:
          httpGet:
            path: /health
            port: 8081
          initialDelaySeconds: 10
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8081
          initialDelaySeconds: 5
  volumeClaimTemplates:
  - metadata:
      name: block-storage
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 5Gi
```

---

### Tier 4 — Polish

**Goal:** Metrics, tests, CI. The things that make open-source contributors and engineering teams trust the project.

---

#### 4.1 Prometheus metrics endpoint

**File:** `namenode.py`  
**New:** HTTP endpoint at `:8080/metrics` (Prometheus text format) exposing:

```
hdfs_files_total                    # Number of files in namespace
hdfs_chunks_total                   # Total chunks across all files
hdfs_blocks_under_replicated        # Chunks with fewer replicas than RF
hdfs_datanodes_alive                # Count of live DataNodes
hdfs_bytes_stored_total             # Sum of chunk sizes
hdfs_upload_requests_total          # Counter of upload operations
hdfs_download_requests_total        # Counter of download operations
hdfs_rpc_latency_seconds{op="upload"}   # Histogram
hdfs_rpc_latency_seconds{op="download"} # Histogram
```

**Also:** Grafana dashboard config (JSON) in `monitoring/grafana/dashboards/hdfs.json`  
**docker-compose addition:** Add `prometheus` and `grafana` services pointing at the metrics endpoint

**Why it matters:** This is the #1 signal that distinguishes "toy project" from "production-aware project" on a resume. Every real system emits metrics. The Grafana dashboard screenshot is also the first thing that goes in the README hero image.

---

#### 4.2 Test suite

**New directory:** `tests/`

```
tests/
├── unit/
│   ├── test_chunking.py          # Split logic, chunk count, size math
│   ├── test_checksum.py          # SHA-256 round-trip correctness
│   ├── test_metadata.py          # Save/load metadata, under-replication detection
│   └── test_encoding.py          # Base64 encode/decode for binary data
└── integration/
    ├── test_upload_download.py   # Full put→get cycle, verify bytes match
    ├── test_node_failure.py      # Kill a DataNode mid-cluster, verify reads still work
    ├── test_replication.py       # Upload, kill node, verify re-replication triggers
    └── test_delete.py            # Upload, delete, verify gone from all nodes
```

**Run:**
```bash
pytest tests/ -v --cov=src --cov-report=html
```

**Integration tests use docker-compose** to spin up the real cluster, run operations against it, and tear it down.

**Why it matters:** Zero tests is the second-biggest red flag in this codebase after the duplicate DataNode files. A 60-70% coverage number on the resume is meaningful. The node failure integration test is particularly strong — it directly tests the fault tolerance story.

---

#### 4.3 GitHub Actions CI

**New file:** `.github/workflows/ci.yml`

```yaml
name: CI

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with: { python-version: '3.11' }
      - run: pip install pytest pytest-cov
      - run: pytest tests/unit/ -v --cov=src

  integration-tests:
    runs-on: ubuntu-latest
    needs: unit-tests
    steps:
      - uses: actions/checkout@v3
      - name: Start cluster
        run: docker-compose up -d
      - name: Wait for cluster
        run: sleep 15
      - name: Run integration tests
        run: pytest tests/integration/ -v
      - name: Tear down
        run: docker-compose down

  build-images:
    runs-on: ubuntu-latest
    needs: integration-tests
    if: github.ref == 'refs/heads/master'
    steps:
      - uses: actions/checkout@v3
      - name: Build NameNode image
        run: docker build -t ghcr.io/${{ github.actor }}/hdfs-namenode:${{ github.sha }} ./namenode
      - name: Build DataNode image
        run: docker build -t ghcr.io/${{ github.actor }}/hdfs-datanode:${{ github.sha }} ./datanode
      - name: Push images
        run: |
          echo ${{ secrets.GITHUB_TOKEN }} | docker login ghcr.io -u ${{ github.actor }} --password-stdin
          docker push ghcr.io/${{ github.actor }}/hdfs-namenode:${{ github.sha }}
          docker push ghcr.io/${{ github.actor }}/hdfs-datanode:${{ github.sha }}
```

**Why it matters:** The green CI badge on the README is immediately visible. It signals that the project is maintained, not abandoned. It also proves the Docker images build and the integration tests pass in a clean environment — not just on your machine.

---

#### 4.4 Architecture diagram in README

Add a proper ASCII or SVG architecture diagram showing the full request lifecycle for both upload and download paths, the heartbeat loop, and the re-replication trigger flow (post Tier 2). This is what interviewers read before looking at the code.

---

## Resume Impact by Tier

| Tier | What You Can Say | Who Notices |
|---|---|---|
| Current state | "Built a distributed file system with chunk replication and heartbeat-based failure detection in Python" | Anyone who reads the code carefully |
| After Tier 1 | "Refactored to env-var-driven config, fixed binary encoding, added defensive networking" | SDE interviews — shows you can find and fix real bugs |
| After Tier 2 | "Implemented automatic block re-replication on node failure and a complete CRUD operation set" | Distributed systems interviews — this is the hard part |
| After Tier 3 | "Deployed the cluster on Docker Compose and Kubernetes with StatefulSets, PVCs, and PodDisruptionBudgets" | Every SDE/MLE role — infra literacy is table stakes now |
| After Tier 4 | "Observability via Prometheus metrics, Grafana dashboard, 70%+ test coverage, GitHub Actions CI with Docker image publishing" | Senior engineers reviewing the repo — this is what makes them share it |

---

## Technical Concepts You Can Speak To (Interview Prep)

After completing all tiers, you should be able to answer any of the following without hesitation:

**On the storage protocol:**
- Why does the client fetch chunks directly from DataNodes instead of going through the NameNode? (NameNode is the bottleneck; DataNodes can scale horizontally)
- Why base64 inside JSON instead of a raw binary TCP stream? (Tradeoff: simplicity vs. ~33% overhead — production HDFS uses protobuf over raw TCP)
- What happens if the NameNode crashes after storing metadata but before responding to the client? (At-least-once vs exactly-once semantics, write-ahead logging)

**On replication:**
- Why is the replication factor 3 in real HDFS? (Rack awareness: 1 local, 1 same-rack, 1 different-rack — survives rack-level failures)
- What is the re-replication trigger? (Block report from DataNode shows fewer replicas than RF; NameNode schedules copy)
- How does HDFS choose which DataNode to replicate to? (Available disk space + existing replica distribution — not implemented here)

**On fault tolerance:**
- What is a split-brain scenario and how does it apply to the NameNode? (Two NameNodes each believe they are primary — HDFS HA uses ZooKeeper quorum to prevent this)
- What happens to an in-flight write when a DataNode fails? (Pipeline recovery — client retries the failed block to a replacement node)

**On Kubernetes:**
- Why StatefulSet over Deployment for DataNodes? (Stable network identity — the NameNode's chunk map keys on node name)
- What does PodDisruptionBudget buy you? (Guarantees voluntary evictions never violate the minimum replica count)
- Why does the NameNode need a PVC? (metadata.json is the system's namespace — losing it means losing all file location data)



### 1. Known Bugs section — mark all 5 as resolved

Add a `✅ Fixed (Tier 1)` label to each one:

- 1.1 — `__name__` guard → `✅ Fixed (Tier 1.1)`
- 1.2 — Duplicate datanode files → `✅ Fixed (Tier 1.2)`
- 1.3 — Binary encoding → `✅ Fixed (Tier 1.3)`
- 1.4 — DataNode recv_all no timeout → `✅ Fixed (Tier 1.4)`
- 1.5 — Hardcoded IPs → `✅ Fixed (Tier 1.5)`

---

### 2. Project Structure section — update the file tree

Remove `datanode1.py` and `datanode2.py`, add `datanode.py`:

```
├── datanode.py          # Single env-var-driven DataNode (replaces datanode1/2)
```

---

### 3. How to Run section — update the startup commands

Replace the old per-file startup with the env var commands from Fix 1.5's verify step. This is now the canonical way to run the project.

---

### 4. What Works Right Now table — add one row

| Env-var driven config (no hardcoded IPs) | ✅ Working | All components configurable via environment |

---