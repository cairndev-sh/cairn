"""dataset_hash: pinned file:// refs pin the content hash, not the URI text (#225)."""
import hashlib

from cairn.core.operator.pins import dataset_hash


def _sha(b: bytes) -> str:
    return "sha256:" + hashlib.sha256(b).hexdigest()


def test_pinned_file_uri_uses_fragment_hash(tmp_path):
    f = tmp_path / "x.jsonl"
    f.write_bytes(b"hello\n")
    h = hashlib.sha256(b"hello\n").hexdigest()
    assert dataset_hash(f"file://{f}#sha256={h}") == f"sha256:{h}"


def test_fragment_is_taken_without_reading():
    h = "ab" * 32
    assert dataset_hash(f"file:///nope/x.jsonl#sha256={h}") == f"sha256:{h}"


def test_fragment_hex_is_lowercased():
    h = "AB" * 32
    assert dataset_hash(f"file:///nope/x.jsonl#sha256={h}") == "sha256:" + "ab" * 32


def test_bare_file_uri_hashes_bytes(tmp_path):
    f = tmp_path / "x.jsonl"
    f.write_bytes(b"hello\n")
    assert dataset_hash(f"file://{f}") == _sha(b"hello\n")


def test_file_uri_with_percent_encoded_path(tmp_path):
    f = tmp_path / "my data.jsonl"
    f.write_bytes(b"hello\n")
    assert dataset_hash(f.as_uri()) == _sha(b"hello\n")


def test_plain_path_unchanged(tmp_path):
    f = tmp_path / "x.jsonl"
    f.write_bytes(b"hello\n")
    assert dataset_hash(str(f)) == _sha(b"hello\n")


def test_remote_ref_keeps_string_hash():
    ref = "s3://bucket/x.jsonl"
    assert dataset_hash(ref) == _sha(ref.encode())


def test_malformed_fragment_falls_back(tmp_path):
    ref = "hf://org/ds#sha256=zzz"
    assert dataset_hash(ref) == _sha(ref.encode())


def test_same_bytes_two_paths_same_hash(tmp_path):
    a, b = tmp_path / "a.jsonl", tmp_path / "b.jsonl"
    a.write_bytes(b"x")
    b.write_bytes(b"x")
    assert dataset_hash(f"file://{a}") == dataset_hash(f"file://{b}")
