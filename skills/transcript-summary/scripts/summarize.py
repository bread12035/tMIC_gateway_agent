#!/usr/bin/env python3
"""Sample skill: transcript_summary.

Reads a transcript file (passed either via --input-file or via
params["input_path"]), produces a short summary, and writes a structured
JSON result to stdout plus a `summary.json` file into the workspace's
`outputs/` directory.

The skill deliberately uses only the stdlib so the subprocess has no
network side effects — matching SDD §8.3.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def summarise(text: str, max_sentences: int = 5) -> str:
    sentences = [s.strip() for s in text.replace("\n", " ").split(".") if s.strip()]
    return ". ".join(sentences[:max_sentences]) + ("." if sentences else "")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--params", default="{}")
    parser.add_argument("--input-file", default=None)
    args = parser.parse_args()

    params = json.loads(args.params)
    workspace = Path(args.workspace)
    outputs = workspace / "outputs"
    outputs.mkdir(parents=True, exist_ok=True)

    input_path = args.input_file or params.get("input_path")
    if not input_path:
        print(json.dumps({"error": "no input_path provided"}))
        return 2

    abs_input = Path(input_path)
    if not abs_input.is_absolute():
        abs_input = workspace / abs_input
    if not abs_input.exists():
        print(json.dumps({"error": f"input not found: {abs_input}"}))
        return 2

    text = abs_input.read_text(encoding="utf-8", errors="replace")
    summary = summarise(text, max_sentences=int(params.get("max_sentences", 5)))

    result = {
        "input": str(input_path),
        "summary": summary,
        "word_count": len(text.split()),
    }
    (outputs / "summary.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
