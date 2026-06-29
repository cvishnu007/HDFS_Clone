import base64

def test_base64_roundtrip_text():
    raw = b"hello world"
    encoded = base64.b64encode(raw).decode("ascii")
    decoded = base64.b64decode(encoded)
    assert decoded == raw

def test_base64_roundtrip_binary():
    raw = bytes(range(256))
    encoded = base64.b64encode(raw).decode("ascii")
    decoded = base64.b64decode(encoded)
    assert decoded == raw

def test_base64_roundtrip_pdf_header():
    # PDF magic bytes
    raw = b"%PDF-1.4" + bytes(range(256))
    encoded = base64.b64encode(raw).decode("ascii")
    decoded = base64.b64decode(encoded)
    assert decoded == raw

def test_base64_large_chunk():
    raw = b"x" * (2 * 1024 * 1024)  # 2MB
    encoded = base64.b64encode(raw).decode("ascii")
    decoded = base64.b64decode(encoded)
    assert decoded == raw