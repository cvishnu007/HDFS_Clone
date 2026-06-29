import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../namenode'))

from namenode import sha256_bytes

def test_checksum_consistency():
    data = b"hello world"
    assert sha256_bytes(data) == sha256_bytes(data)

def test_checksum_differs_on_different_data():
    assert sha256_bytes(b"hello") != sha256_bytes(b"world")

def test_checksum_empty():
    result = sha256_bytes(b"")
    assert isinstance(result, str)
    assert len(result) == 64

def test_checksum_binary():
    data = bytes(range(256))
    result = sha256_bytes(data)
    assert len(result) == 64

def test_checksum_known_value():
    import hashlib
    data = b"hdfs-clone"
    expected = hashlib.sha256(data).hexdigest()
    assert sha256_bytes(data) == expected