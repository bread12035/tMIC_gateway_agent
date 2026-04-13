"""Test configuration — make project root importable."""
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Force the in-memory storage backend for tests by default.
os.environ.setdefault("GATEWAY_STORAGE_BACKEND", "memory")
