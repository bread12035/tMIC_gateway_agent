"""Tests for DataPrefetcher — v0.4.0 processor injection (SDD §6).

Covers the processor callable contract, domain_fields propagation, exception
isolation, and backwards-compatibility with the v0.3.0 behaviour.
"""
from __future__ import annotations

import json
import os

from gateway.data_prefetcher import DataPrefetcher, PrefetchedFile
from gateway.storage_backend import InMemoryStorageBackend


def _read_local(path: str) -> bytes:
    with open(path, "rb") as fh:
        return fh.read()


def test_processor_is_called_with_raw_and_domain_fields(tmp_path):
    storage = InMemoryStorageBackend()
    storage.seed("bkt", "financials/TSMC/2026/Q1.json", b'{"v":1}')

    calls = []

    def spy_processor(raw, fields):
        calls.append({"raw": raw, "fields": fields})
        return raw  # pass-through

    prefetcher = DataPrefetcher(storage)
    prefetcher.prefetch(
        data_sources=[{
            "name": "financials",
            "bucket": "bkt",
            "gcs_path": "financials/TSMC/2026/Q1.json",
            "processor": spy_processor,
        }],
        workspace_dir=str(tmp_path),
        domain_fields={"company": "TSMC", "fiscal_year": 2026},
    )

    assert len(calls) == 1
    assert calls[0]["raw"] == b'{"v":1}'
    assert calls[0]["fields"]["company"] == "TSMC"
    assert calls[0]["fields"]["fiscal_year"] == 2026


def test_processor_transforms_content_before_landing(tmp_path):
    storage = InMemoryStorageBackend()
    storage.seed("bkt", "financials/Q1.json", b'{"raw": true}')

    def upper_processor(raw, fields):
        return b'{"transformed": true}'

    prefetcher = DataPrefetcher(storage)
    results = prefetcher.prefetch(
        data_sources=[{
            "name": "financials",
            "bucket": "bkt",
            "gcs_path": "financials/Q1.json",
            "processor": upper_processor,
        }],
        workspace_dir=str(tmp_path),
        domain_fields={},
    )

    assert len(results) == 1
    landed = _read_local(results[0].absolute_path)
    assert landed == b'{"transformed": true}'
    assert results[0].size_bytes == len(b'{"transformed": true}')


def test_processed_flag_set_on_prefetched_file(tmp_path):
    storage = InMemoryStorageBackend()
    storage.seed("bkt", "a.json", b"{}")

    prefetcher = DataPrefetcher(storage)
    results = prefetcher.prefetch(
        data_sources=[{
            "name": "a",
            "bucket": "bkt",
            "gcs_path": "a.json",
            "processor": lambda raw, fields: raw,
        }],
        workspace_dir=str(tmp_path),
    )

    assert results[0].processed is True
    assert results[0].to_task_state()["processed"] is True


def test_processor_exception_skips_source_continues_others(tmp_path):
    storage = InMemoryStorageBackend()
    storage.seed("bkt", "good.json", b'{"ok": true}')
    storage.seed("bkt", "bad.json", b'{"ok": false}')

    def boom(raw, fields):
        raise ValueError("intentional failure")

    prefetcher = DataPrefetcher(storage)
    results = prefetcher.prefetch(
        data_sources=[
            {
                "name": "bad",
                "bucket": "bkt",
                "gcs_path": "bad.json",
                "processor": boom,
            },
            {
                "name": "good",
                "bucket": "bkt",
                "gcs_path": "good.json",
                "processor": lambda raw, fields: raw,
            },
        ],
        workspace_dir=str(tmp_path),
    )

    assert len(results) == 1
    assert results[0].name == "good"
    # Failed source must NOT leave any file behind in inputs/
    inputs_dir = os.path.join(str(tmp_path), "inputs")
    landed = os.listdir(inputs_dir)
    assert "bad.json" not in landed
    assert "good.json" in landed


def test_no_processor_preserves_v030_behaviour(tmp_path):
    storage = InMemoryStorageBackend()
    storage.seed("bkt", "plain.txt", b"hello world")

    prefetcher = DataPrefetcher(storage)
    results = prefetcher.prefetch(
        data_sources=[{
            "name": "plain",
            "bucket": "bkt",
            "gcs_path": "plain.txt",
            # no "processor" key at all — simulates v0.3.0 call site
        }],
        workspace_dir=str(tmp_path),
    )

    assert len(results) == 1
    assert results[0].processed is False
    assert _read_local(results[0].absolute_path) == b"hello world"
    assert results[0].to_task_state()["processed"] is False


def test_domain_fields_used_in_conditional_processor(tmp_path):
    storage = InMemoryStorageBackend()
    storage.seed("bkt", "tsmc.json", b'{"net_revenue": 100, "basic_eps": 1.1}')
    storage.seed("bkt", "samsung.json", b'{"total_revenue": 200, "diluted_eps": 2.2}')

    schema_map = {
        "TSMC": {"revenue": "net_revenue", "eps": "basic_eps"},
        "SAMSUNG": {"revenue": "total_revenue", "eps": "diluted_eps"},
    }

    def adaptive(raw, fields):
        data = json.loads(raw)
        schema = schema_map[fields["company"]]
        return json.dumps({
            "company": fields["company"],
            "revenue": data[schema["revenue"]],
            "eps": data[schema["eps"]],
        }).encode()

    prefetcher = DataPrefetcher(storage)

    tsmc_results = prefetcher.prefetch(
        data_sources=[{
            "name": "fin", "bucket": "bkt",
            "gcs_path": "tsmc.json", "processor": adaptive,
        }],
        workspace_dir=str(tmp_path / "tsmc"),
        domain_fields={"company": "TSMC"},
    )
    samsung_results = prefetcher.prefetch(
        data_sources=[{
            "name": "fin", "bucket": "bkt",
            "gcs_path": "samsung.json", "processor": adaptive,
        }],
        workspace_dir=str(tmp_path / "samsung"),
        domain_fields={"company": "SAMSUNG"},
    )

    tsmc_flat = json.loads(_read_local(tsmc_results[0].absolute_path))
    samsung_flat = json.loads(_read_local(samsung_results[0].absolute_path))
    assert tsmc_flat == {"company": "TSMC", "revenue": 100, "eps": 1.1}
    assert samsung_flat == {"company": "SAMSUNG", "revenue": 200, "eps": 2.2}


def test_processor_empty_bytes_lands_empty_file(tmp_path):
    storage = InMemoryStorageBackend()
    storage.seed("bkt", "anything.txt", b"non-empty original")

    prefetcher = DataPrefetcher(storage)
    results = prefetcher.prefetch(
        data_sources=[{
            "name": "empty",
            "bucket": "bkt",
            "gcs_path": "anything.txt",
            "processor": lambda raw, fields: b"",
        }],
        workspace_dir=str(tmp_path),
    )

    assert len(results) == 1
    assert results[0].processed is True
    assert results[0].size_bytes == 0
    assert _read_local(results[0].absolute_path) == b""
