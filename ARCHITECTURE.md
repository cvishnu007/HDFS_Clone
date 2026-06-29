# Mini HDFS — Architecture

> A complete guide to how this distributed file system works — from a file upload to fault recovery to metrics scraping.

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Component Roles](#component-roles)
3. [Port Map](#port-map)
4. [Upload Flow](#upload-flow)
5. [Download Flow](#download-flow)
6. [Heartbeat & Failure Detection](#heartbeat--failure-detection)
7. [Automatic Re-Replication](#automatic-re-replication)
8. [File Delete Flow](#file-delete-flow)
9. [Observability — Prometheus & Grafana](#observability--prometheus--grafana)
10. [Data Integrity](#data-integrity)
11. [Kubernetes Design Decisions](#kubernetes-design-decisions)

---

## System Overview

```
┌───────────────────────────────────────────────────────────────────────┐
│                     CLIENT  (Streamlit :8501)                          │
│                                                                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐             │
│  │  Upload  │  │ Download │  │  Delete  │  │  Status  │             │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘             │
└───────┼─────────────┼─────────────┼──────────────┼───────────────────┘
        │  TCP :5000  │             │              │
        ▼             │             │              │
┌───────────────────────────────────────────────────────────────────────┐
│                    NAMENODE  (:5000 RPC | :6000 UDP | :8080 metrics)  │
│                                                                        │
│  ┌──────────────────────────────────────────────────────────────┐     │
│  │  metadata.json — namespace, chunk map, replica locations     │     │
│  └──────────────────────────────────────────────────────────────┘     │
│                                                                        │
│  Threads:                                                              │
│  • client_handler()   — TCP accept loop, routes RPC actions           │
│  • heartbeat_listener() — UDP :6000, records last-seen per node       │
│  • monitor_nodes()    — every 5s, evicts nodes silent > 15s           │
│  • re_replicate()     — every 5s, restores under-replicated chunks    │
│  • metrics HTTP       — prometheus_client serves /metrics on :8080    │
│                                                                        │
└──────┬────────────────────────────────────────────────────────────────┘
       │  TCP (store / read / delete)
       │
       ├──────────────────┬──────────────────┐
       ▼                  ▼                  ▼
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│  DATANODE 1  │   │  DATANODE 2  │   │  DATANODE 3  │
│   :5001      │   │   :5001      │   │   :5001      │
│             │   │             │   │             │
│ data_blocks/│   │ data_blocks/│   │ data_blocks/│
│  file_part0 │   │  file_part0 │   │  file_part1 │
│  file_part1 │   │  file_part1 │   │  file_part2 │
│  ...        │   │  ...        │   │  ...        │
│             │   │             │   │             │
│ UDP hbeat → │   │ UDP hbeat → │   │ UDP hbeat → │
│ NameNode    │   │ NameNode    │   │ NameNode    │
└─────────────┘   └─────────────┘   └─────────────┘
       ▲
       │ scrape every 10s
┌─────────────┐        ┌─────────────┐
│ PROMETHEUS  │───────▶│   GRAFANA   │
│   :9090     │        │   :3000     │
└─────────────┘        └─────────────┘
```

---

## Component Roles

### NameNode (Central Coordinator)
- The **single source of truth** for the file namespace
- Stores `metadata.json` — maps every filename → chunk list → replica nodes → SHA-256 checksums
- Splits incoming files into **2 MB chunks** before distribution
- Decides which DataNodes receive each chunk (`min(RF, alive_nodes)`)
- Never stores file data itself — only metadata
- Runs 4 background threads concurrently with client handling

### DataNode (Storage Workers)
- Stores raw chunk bytes in `data_blocks/` as individual files
- Serves read and delete requests from NameNode
- Sends a UDP heartbeat to NameNode every **5 seconds** to signal liveness
- Config driven entirely by environment variables — a single `datanode.py` scales to any number of nodes

### Client (Streamlit Dashboard)
- Communicates exclusively with the NameNode for control-plane operations (upload, delete, list, status)
- **Fetches chunk data directly from DataNodes** — bypasses NameNode for data-plane reads to avoid bottleneck
- Reassembles chunks in order and verifies full-file SHA-256 on download

---

## Port Map

| Service | Port | Protocol | Purpose |
|---------|------|----------|---------|
| NameNode | 5000 | TCP | Client RPC (upload / download / delete / list / status) |
| NameNode | 6000 | UDP | Heartbeat receiver |
| NameNode | 8080 | HTTP | Prometheus `/metrics` endpoint |
| DataNode (each) | 5001 | TCP | Block store / read / delete |
| Client | 8501 | HTTP | Streamlit web dashboard |
| Prometheus | 9090 | HTTP | Metrics query / UI |
| Grafana | 3000 | HTTP | Dashboard (admin / hdfsclone) |

---

## Upload Flow

```
Client                         NameNode                    DataNode N
  │                               │                              │
  │── JSON {action:"upload",      │                              │
  │         filename, content_b64}│                              │
  │─────────────────────────────▶│                              │
  │                               │                              │
  │                         decode base64                        │
  │                         split into 2MB chunks                │
  │                               │                              │
  │                   ┌── for each chunk ──┐                    │
  │                   │           │         │                    │
  │                   │    for each target node (up to RF):      │
  │                   │           │── {action:"store",           │
  │                   │           │    filename: chunk_name,     │
  │                   │           │    content_b64: base64_chunk}│
  │                   │           │─────────────────────────────▶│
  │                   │           │                    write to  │
  │                   │           │                    data_blocks/
  │                   │           │◀── {status:"stored"} ───────│
  │                   │     record replica in metadata.json      │
  │                   └─────────────────────────────────────────┘
  │                               │                              │
  │◀── {status:"uploaded",        │                              │
  │     chunks: N} ───────────────│                              │
```

**Key design choices:**
- File is base64-encoded all the way from Client → NameNode → DataNode (no silent byte drops)
- SHA-256 is computed per-chunk and per-file before encoding; stored in metadata
- Replication factor is configurable (`REPLICATION_FACTOR` env var, default 2)

---

## Download Flow

```
Client                         NameNode                    DataNode N
  │                               │                              │
  │── {action:"download",         │                              │
  │    filename} ────────────────▶│                              │
  │                               │                              │
  │◀── {status:"ok",              │                              │
  │     chunks:[{chunk_name,      │                              │
  │              replicas:[...]}]}│                              │
  │                               │                              │
  │  ┌── for each chunk (in order) ────────────────────────────┐ │
  │  │   pick first alive replica                              │ │
  │  │── {action:"read",                                       │ │
  │  │    filename: chunk_name} ──────────────────────────────▶│ │
  │  │◀── {status:"ok", content_b64} ─────────────────────────│ │
  │  │   decode + append                                        │ │
  │  └──────────────────────────────────────────────────────────┘ │
  │                                                               │
  │  verify full-file SHA-256                                     │
  │  stream file to browser                                       │
```

**Key design choice:** The Client contacts DataNodes directly for data. This is exactly how real HDFS works — the NameNode is the bottleneck; data path must not go through it.

---

## Heartbeat & Failure Detection

```
DataNode                      NameNode
   │                              │
   │  every 5s:                   │
   │── UDP "datanode1" ──────────▶│
   │                         metadata["datanode_status"]["datanode1"] = now()
   │
   │  [DataNode goes silent]
   │
   │                         monitor_nodes() runs every 5s:
   │                           if now() - last_seen > 15s:
   │                             del metadata["datanode_status"][node]
   │                             → node is now "dead"
   │                             → update_gauges() refreshes hdfs_datanodes_alive
   │                             → re_replicate() will detect under-replicated chunks
```

A node is considered **dead** after 15 seconds of missed heartbeats (3 missed cycles). No explicit "deregister" call — failure is purely timeout-based, same as real HDFS.

---

## Automatic Re-Replication

```
NameNode (re_replicate thread — runs every 5s)
  │
  │  for each file → for each chunk:
  │    alive_replicas = [n for n in chunk.replicas if n in alive_nodes]
  │    need = REPLICATION_FACTOR - len(alive_replicas)
  │
  │    if need > 0:
  │      candidates = alive nodes NOT already holding this chunk
  │      source = alive_replicas[0]   ← read from surviving replica
  │
  │── {action:"read", filename:chunk_name} ──▶ source DataNode
  │◀── {content_b64} ────────────────────────
  │
  │    for target in candidates[:need]:
  │── {action:"store", ...} ──────────────▶ target DataNode
  │◀── {status:"stored"} ─────────────────
  │      update metadata: add target to chunk.replicas
  │      update hdfs_blocks_under_replicated gauge
```

This is what separates **fault-tolerant** from **fault-detecting**. The under-replicated block count in Grafana will drop back to 0 within 1–2 check cycles after a DataNode failure.

---

## File Delete Flow

```
Client                     NameNode                  DataNode N
  │                            │                          │
  │── {action:"delete",        │                          │
  │    filename} ─────────────▶│                          │
  │                      read chunk list from metadata    │
  │                            │                          │
  │               ┌── for each chunk, for each replica ──┐│
  │               │── {action:"delete",                  ││
  │               │    filename: chunk_name} ────────────▶││
  │               │◀── {status:"deleted"} ───────────────││
  │               └─────────────────────────────────────  ││
  │                      del metadata["files"][filename]  │
  │                      save_metadata()                  │
  │◀── {status:"deleted"} ─────│                          │
```

Best-effort delete: NameNode removes the metadata entry regardless of whether all DataNode deletes succeed (e.g. if a node is down). Partial failures are reported back to the client.

---

## Observability — Prometheus & Grafana

```
NameNode (:8080/metrics)
  │
  │  hdfs_files_total              Gauge   — files in namespace
  │  hdfs_chunks_total             Gauge   — total chunks
  │  hdfs_blocks_under_replicated  Gauge   — chunks below RF
  │  hdfs_datanodes_alive          Gauge   — live node count
  │  hdfs_bytes_stored_total       Gauge   — total bytes
  │  hdfs_upload_requests_total    Counter — cumulative uploads
  │  hdfs_download_requests_total  Counter — cumulative downloads
  │  hdfs_delete_requests_total    Counter — cumulative deletes
  │  hdfs_rpc_latency_seconds      Histogram {op=upload|download}
  │
  ▼  scraped every 10s
Prometheus (:9090)
  │
  ▼  PromQL queries
Grafana (:3000)
  │  Dashboard: "HDFS Clone — Cluster Overview"
  │  • 5 stat panels (nodes alive, files, chunks, under-replicated, bytes)
  │  • Request rate time-series (uploads/downloads/deletes per second)
  │  • RPC latency p50/p95 per operation type
  │  • DataNode alive history (node failure events visible as drops)
  │  • Under-replicated block history (re-replication recovery visible)
```

Gauges are refreshed after every mutation (upload, delete, re-replication) and every heartbeat cycle — not on a fixed poll interval. This means the metrics are always current.

---

## Data Integrity

Every chunk goes through the following integrity chain:

```
Raw bytes
  │
  ├── SHA-256(chunk bytes) → stored in metadata.json as chunk["checksum"]
  │
  ├── base64.b64encode(chunk bytes) → sent as JSON content_b64
  │       ↓
  │   DataNode: base64.b64decode → write raw bytes to disk
  │
SHA-256(full file raw bytes) → stored in metadata.json as file["file_checksum"]
  │
  └── verified by Client after reassembly on download
```

The checksum is computed **before** encoding and verified **after** decoding — so any corruption introduced during encoding, transmission, or storage is caught.

---

## Kubernetes Design Decisions

| Decision | Why |
|----------|-----|
| DataNodes as **StatefulSet** | Gives each pod a stable DNS name (`hdfs-datanode-0`, `1`, `2`). The NameNode keys its chunk map on node names — random Deployment names would break replica tracking on pod restart |
| **PodDisruptionBudget** on DataNodes | Kubernetes must never voluntarily evict more than 1 DataNode at once — this would drop below the replication factor and cause data loss |
| **PersistentVolumeClaim** on NameNode | `metadata.json` is the entire file namespace. Losing it means losing all knowledge of where blocks live, even if blocks still exist on DataNodes |
| **ConfigMap** for all config | Chunk size, RF, heartbeat timeout — all externalized. Changing cluster config doesn't require rebuilding images |
| **Liveness probe** on NameNode | HTTP GET `/health` — Kubernetes restarts the pod if the RPC server stops responding |
| **Readiness probe** on DataNode | Only marks ready after first heartbeat acknowledged — prevents traffic before the node is registered |
| **Headless Service** for DataNodes | Enables direct pod-to-pod DNS (`datanode-0.hdfs-datanode.default.svc.cluster.local`) without load balancing — the NameNode needs to address specific nodes |
