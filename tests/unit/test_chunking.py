import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../namenode'))

from namenode import CHUNK_SIZE

def test_chunk_count_single():
    raw = b"x" * CHUNK_SIZE
    parts = [raw[i:i+CHUNK_SIZE] for i in range(0, len(raw), CHUNK_SIZE)]
    assert len(parts) == 1

def test_chunk_count_multiple():
    raw = b"x" * (CHUNK_SIZE * 3)
    parts = [raw[i:i+CHUNK_SIZE] for i in range(0, len(raw), CHUNK_SIZE)]
    assert len(parts) == 3

def test_chunk_count_partial():
    raw = b"x" * (CHUNK_SIZE + 100)
    parts = [raw[i:i+CHUNK_SIZE] for i in range(0, len(raw), CHUNK_SIZE)]
    assert len(parts) == 2
    assert len(parts[1]) == 100

def test_chunk_reassembly():
    raw = b"hello" * 1000
    parts = [raw[i:i+CHUNK_SIZE] for i in range(0, len(raw), CHUNK_SIZE)]
    reassembled = b"".join(parts)
    assert reassembled == raw

def test_empty_file():
    raw = b""
    parts = [raw[i:i+CHUNK_SIZE] for i in range(0, len(raw), CHUNK_SIZE)]
    assert parts == []