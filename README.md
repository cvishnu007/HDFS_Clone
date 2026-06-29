# Mini HDFS — Distributed File System Clone

A from-scratch implementation of the core HDFS (Hadoop Distributed File System) architecture in Python. Demonstrates distributed storage internals — block splitting, replication, fault detection, automatic re-replication, and end-to-end data integrity — with no Hadoop dependencies. Fully containerized with Docker Compose, deployable on Kubernetes, and production-observable via Prometheus and Grafana.

> **See [ARCHITECTURE.md](./ARCHITECTURE.md)** for detailed flow diagrams (upload, download, heartbeat, re-replication, metrics).

---

## Table of Contents

1. [What This Is](#what-this-is)
2. [Architecture Overview](#architecture-overview)
3. [What Works](#what-works)
4. [Project Structure](#project-structure)
5. [Quick Start — Docker Compose](#quick-start--docker-compose)
6. [Observability](#observability)
7. [Kubernetes Deployment](#kubernetes-deployment)
8. [Running Tests](#running-tests)
9. [CI/CD](#cicd)
10. [Interview Concepts](#interview-concepts)

---

## What This Is

HDFS splits files into fixed-size blocks, distributes and replicates those blocks across DataNodes, and uses a central NameNode to track where every block lives. When a machine dies, HDFS automatically re-replicates the lost blocks to maintain the configured replication factor.

This project reimplements that core architecture from scratch using raw Python sockets and threading — no Hadoop, no HDFS libraries. The goal is to demonstrate real understanding of distributed systems internals: the RPC protocol between client and NameNode, the block store protocol between NameNode and DataNodes, heartbeat-based failure detection, and end-to-end data integrity via checksums.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                   CLIENT (Streamlit :8501)                   │
│  Upload · Download · Delete · Status                         │
└────────────────────────┬────────────────────────────────────┘
                         │ TCP :5000
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              NAMENODE (:5000 RPC | :6000 UDP | :8080 metrics)│
│  • Chunks files into 2MB blocks                              │
│  • Replicates to min(RF, alive nodes) DataNodes              │
│  • Tracks chunk → node → checksum in metadata.json          │
│  • Detects dead nodes via 15s heartbeat timeout              │
│  • Re-replicates under-replicated chunks automatically       │
│  • Exposes Prometheus metrics at :8080/metrics               │
└──────┬──────────────────┬──────────────────┬───────────────┘
       │ TCP :5001        │ TCP :5001        │ TCP :5001
       ▼                  ▼                  ▼
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│  DATANODE 1  │   │  DATANODE 2  │   │  DATANODE 3  │
│ data_blocks/ │   │ data_blocks/ │   │ data_blocks/ │
│ UDP hbeat ↑ │   │ UDP hbeat ↑ │   │ UDP hbeat ↑ │
└─────────────┘   └─────────────┘   └─────────────┘
       ▲
       │ scrape :8080/metrics every 10s
┌─────────────┐        ┌─────────────┐
│ PROMETHEUS  │───────▶│   GRAFANA   │
│   :9090     │        │   :3000     │
└─────────────┘        └─────────────┘
```

---

## What Works

| Feature | Status | Notes |
|---------|--------|-------|
| File upload (text + binary) | ✅ Working | Base64 end-to-end, up to 50MB |
| File chunking | ✅ Working | 2MB fixed chunk size |
| Configurable replication factor | ✅ Working | `REPLICATION_FACTOR` env var, default 2 |
| File download + reassembly | ✅ Working | Fetches direct from DataNodes |
| End-to-end SHA-256 integrity | ✅ Working | Per-chunk and full-file, verified on download |
| File delete | ✅ Working | Removes from all replicas + metadata |
| File list | ✅ Working | Filename, size, chunks, upload timestamp |
| UDP heartbeat (DataNode → NameNode) | ✅ Working | Every 5 seconds |
| Liveness detection | ✅ Working | 15s timeout marks node dead |
| **Automatic block re-replication** | ✅ Working | Restores RF copies when a node dies |
| Under-replication detection | ✅ Working | Shown in dashboard and Prometheus |
| Metadata persistence | ✅ Working | metadata.json survives restarts |
| NameNode rotating logs | ✅ Working | logs/namenode.log |
| Streamlit dashboard | ✅ Working | Status, upload, download, delete, debug tabs |
| Env-var driven config (no hardcoded IPs) | ✅ Working | All components configurable via environment |
| Docker Compose cluster | ✅ Working | One command starts NameNode + 3 DataNodes + Client |
| Kubernetes manifests | ✅ Working | StatefulSet, PVC, PDB, ConfigMap, headless service |
| Unit + integration test suite | ✅ Working | pytest, 60%+ coverage |
| GitHub Actions CI | ✅ Working | Unit tests → integration tests → image publish |
| **Prometheus metrics** | ✅ Working | 9 metrics at :8080/metrics |
| **Grafana dashboard** | ✅ Working | Auto-provisioned, 9 panels, live data |

---

## Project Structure

```
hdfs-clone/
├── namenode/
│   ├── namenode.py          # NameNode — metadata, routing, heartbeats, metrics
│   └── Dockerfile
├── datanode/
│   ├── datanode.py          # Single env-var-driven DataNode (scales to any N)
│   └── Dockerfile
├── client/
│   ├── client.py            # Streamlit dashboard
│   └── Dockerfile
├── monitoring/
│   ├── prometheus/
│   │   └── prometheus.yml   # Scrape config (namenode:8080 every 10s)
│   └── grafana/
│       └── provisioning/
│           ├── datasources/prometheus.yml   # Auto-provision Prometheus datasource
│           └── dashboards/
│               ├── dashboards.yml           # Dashboard provider config
│               └── hdfs.json               # Pre-built cluster overview dashboard
├── k8s/
│   ├── configmap.yaml
│   ├── namenode/
│   │   ├── deployment.yaml
│   │   ├── service.yaml
│   │   └── pvc.yaml
│   └── datanode/
│       ├── statefulset.yaml
│       ├── service.yaml
│       └── pdb.yaml
├── tests/
│   ├── unit/                # test_chunking, test_checksum, test_metadata
│   └── integration/         # test_upload_download, test_node_failure, ...
├── docker-compose.yml
├── ARCHITECTURE.md          # Detailed flow diagrams
└── README.md
```

---

## Quick Start — Docker Compose

**Requirements:** Docker + Docker Compose (V2)

```bash
# 1. Build images
docker build -t hdfs-namenode ./namenode
docker build -t hdfs-datanode ./datanode
docker build -t hdfs-client   ./client

# 2. Start the full cluster (NameNode + 3 DataNodes + Client + Prometheus + Grafana)
docker compose up -d

# 3. Open the Streamlit dashboard
open http://localhost:8501
```

### Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Streamlit Dashboard | http://localhost:8501 | — |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / hdfsclone |
| Raw metrics | http://localhost:8080/metrics | — |

### Stop the cluster

```bash
docker compose down          # stop containers
docker compose down -v       # stop + wipe all volumes
```

---

## Observability

### Prometheus Metrics

All metrics exposed at `http://localhost:8080/metrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `hdfs_files_total` | Gauge | Files in namespace |
| `hdfs_chunks_total` | Gauge | Total chunks across all files |
| `hdfs_blocks_under_replicated` | Gauge | Chunks with fewer alive replicas than RF |
| `hdfs_datanodes_alive` | Gauge | Live DataNode count |
| `hdfs_bytes_stored_total` | Gauge | Total bytes stored |
| `hdfs_upload_requests_total` | Counter | Cumulative upload operations |
| `hdfs_download_requests_total` | Counter | Cumulative download operations |
| `hdfs_delete_requests_total` | Counter | Cumulative delete operations |
| `hdfs_rpc_latency_seconds{op}` | Histogram | Upload / download RPC latency |

### Grafana Dashboard

Open `http://localhost:3000` (admin / hdfsclone) → Dashboards → HDFS → **HDFS Clone — Cluster Overview**

The dashboard auto-provisions on first start. Panels:
- **DataNodes Alive** — turns red when a node dies
- **Files / Chunks / Bytes** — namespace stats
- **Under-Replicated Blocks** — orange/red alert; watch it drop to 0 after re-replication
- **Request Rate** — uploads/downloads/deletes per second
- **RPC Latency p50 / p95** — upload and download latency histograms

---

## Kubernetes Deployment

Manifests are in `k8s/`. Key design decisions:

| Resource | Reason |
|----------|--------|
| DataNodes as **StatefulSet** | Stable pod DNS names — NameNode keys chunk map on node identity |
| **PodDisruptionBudget** | Never voluntarily evict more than 1 DataNode — preserves replication factor |
| **PersistentVolumeClaim** on NameNode | metadata.json is the namespace — must survive pod restarts |
| **ConfigMap** for all config | Chunk size, RF, heartbeat timeout externalized from images |
| **Headless Service** for DataNodes | Pod-to-pod direct addressing without load balancing |

```bash
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/namenode/
kubectl apply -f k8s/datanode/
```

---

## Running Tests

```bash
pip install pytest pytest-cov

# Unit tests only (no Docker needed)
pytest tests/unit/ -v --cov=namenode --cov-report=term-missing

# Integration tests (requires docker compose up -d first)
pytest tests/integration/ -v
```

---

## CI/CD

GitHub Actions workflow (`.github/workflows/ci.yaml`):

```
push / PR to master
  │
  ├── unit-tests        ← pytest tests/unit/
  │
  ├── integration-tests ← docker compose up → pytest tests/integration/ → compose down
  │
  └── build-images      ← (master only) build + push to ghcr.io
                           ghcr.io/<owner>/hdfs-namenode:<sha>
                           ghcr.io/<owner>/hdfs-datanode:<sha>
                           ghcr.io/<owner>/hdfs-client:<sha>
```

---

## Interview Concepts

After building this project you can speak confidently to:

**Storage protocol:**
- Why does the client fetch chunks directly from DataNodes instead of going through the NameNode?  
  *(NameNode is the bottleneck — data path must not go through it; DataNodes scale horizontally)*
- Why base64 inside JSON instead of a raw binary TCP stream?  
  *(Tradeoff: simplicity vs ~33% overhead — production HDFS uses protobuf over raw TCP)*
- What happens if the NameNode crashes after storing metadata but before responding to the client?  
  *(At-least-once vs exactly-once semantics, write-ahead logging)*

**Replication:**
- Why is the replication factor 3 in real HDFS?  
  *(Rack awareness: 1 local, 1 same-rack, 1 different-rack — survives rack-level failures)*
- What is the re-replication trigger?  
  *(Block report shows fewer replicas than RF → NameNode schedules copy)*

**Fault tolerance:**
- What is a split-brain scenario and how does it apply to the NameNode?  
  *(Two NameNodes each believe they are primary — HDFS HA uses ZooKeeper quorum)*
- What happens to an in-flight write when a DataNode fails?  
  *(Pipeline recovery — client retries the failed block to a replacement node)*

**Kubernetes:**
- Why StatefulSet over Deployment for DataNodes?  
  *(Stable network identity — NameNode's chunk map keys on node name)*
- What does PodDisruptionBudget buy you?  
  *(Guarantees voluntary evictions never violate the minimum replica count)*
- Why does the NameNode need a PVC?  
  *(metadata.json is the system's namespace — losing it means losing all file location data)*
