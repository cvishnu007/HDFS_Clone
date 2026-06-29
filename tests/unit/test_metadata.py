import json, os, tempfile, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../namenode'))

def test_metadata_save_load():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        path = f.name
        json.dump({"files": {}, "datanode_status": {}}, f)

    with open(path) as f:
        loaded = json.load(f)

    assert "files" in loaded
    assert "datanode_status" in loaded
    os.unlink(path)

def test_under_replication_detection():
    files = {
        "test.txt": {
            "chunks": [
                {"chunk_name": "test.txt_part0", "replicas": ["dn1", "dn2"], "checksum": "abc"}
            ]
        }
    }
    alive = {"dn1": 1234567890.0}  # only dn1 alive

    under = []
    for fname, entry in files.items():
        for ch in entry["chunks"]:
            alive_replicas = [n for n in ch["replicas"] if n in alive]
            if len(alive_replicas) < len(ch["replicas"]):
                under.append(ch["chunk_name"])

    assert "test.txt_part0" in under

def test_no_under_replication_when_all_alive():
    files = {
        "test.txt": {
            "chunks": [
                {"chunk_name": "test.txt_part0", "replicas": ["dn1", "dn2"], "checksum": "abc"}
            ]
        }
    }
    alive = {"dn1": 1234567890.0, "dn2": 1234567890.0}

    under = []
    for fname, entry in files.items():
        for ch in entry["chunks"]:
            alive_replicas = [n for n in ch["replicas"] if n in alive]
            if len(alive_replicas) < len(ch["replicas"]):
                under.append(ch["chunk_name"])

    assert under == []