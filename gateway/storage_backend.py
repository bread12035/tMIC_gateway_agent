"""Pluggable storage backend so Gateway can run in tests without GCP.

`GCSStorageBackend` uses `google-cloud-storage`; `InMemoryStorageBackend`
is used by the unit tests and local dev mode. Both implement the same tiny
interface so GatewayServices doesn't need to know the difference.
"""
from __future__ import annotations

import os
from typing import Dict, List, Optional, Protocol


class StorageBackend(Protocol):
    def read(self, bucket: str, path: str) -> Optional[bytes]: ...
    def write(self, bucket: str, path: str, data: bytes) -> None: ...
    def exists(self, bucket: str, path: str) -> bool: ...
    def list_prefix(self, bucket: str, prefix: str) -> List[str]: ...
    def append_line(self, bucket: str, path: str, line: str) -> None: ...


class InMemoryStorageBackend:
    """Dict-based backend for tests and local development."""

    def __init__(self) -> None:
        # {(bucket, path): bytes}
        self._store: Dict[tuple, bytes] = {}

    def read(self, bucket: str, path: str) -> Optional[bytes]:
        return self._store.get((bucket, path))

    def write(self, bucket: str, path: str, data: bytes) -> None:
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._store[(bucket, path)] = data

    def exists(self, bucket: str, path: str) -> bool:
        return (bucket, path) in self._store

    def list_prefix(self, bucket: str, prefix: str) -> List[str]:
        return sorted(
            p for (b, p) in self._store if b == bucket and p.startswith(prefix)
        )

    def append_line(self, bucket: str, path: str, line: str) -> None:
        existing = self._store.get((bucket, path), b"")
        if existing and not existing.endswith(b"\n"):
            existing += b"\n"
        self._store[(bucket, path)] = existing + line.encode("utf-8") + b"\n"

    # Test helper
    def seed(self, bucket: str, path: str, data: bytes | str) -> None:
        self.write(bucket, path, data if isinstance(data, bytes) else data.encode("utf-8"))


class GCSStorageBackend:
    """Thin wrapper over google-cloud-storage.

    Imported lazily so tests and local dev don't need the package installed.
    """

    def __init__(self) -> None:
        from google.cloud import storage  # type: ignore

        self._client = storage.Client()

    def _blob(self, bucket: str, path: str):
        return self._client.bucket(bucket).blob(path)

    def read(self, bucket: str, path: str) -> Optional[bytes]:
        blob = self._blob(bucket, path)
        if not blob.exists():
            return None
        return blob.download_as_bytes()

    def write(self, bucket: str, path: str, data: bytes) -> None:
        self._blob(bucket, path).upload_from_string(data)

    def exists(self, bucket: str, path: str) -> bool:
        return self._blob(bucket, path).exists()

    def list_prefix(self, bucket: str, prefix: str) -> List[str]:
        return [b.name for b in self._client.list_blobs(bucket, prefix=prefix)]

    def append_line(self, bucket: str, path: str, line: str) -> None:
        # GCS has no true append; read-modify-write is acceptable at the
        # Task State volume described in the SDD (one write per task).
        existing = self.read(bucket, path) or b""
        if existing and not existing.endswith(b"\n"):
            existing += b"\n"
        self.write(bucket, path, existing + line.encode("utf-8") + b"\n")


def default_backend() -> StorageBackend:
    """Return GCS when credentials are available, otherwise in-memory."""
    if os.environ.get("GATEWAY_STORAGE_BACKEND") == "memory":
        return InMemoryStorageBackend()
    try:
        return GCSStorageBackend()
    except Exception:  # pragma: no cover — only hit when SDK missing
        return InMemoryStorageBackend()
