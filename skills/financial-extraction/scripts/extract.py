#!/usr/bin/env python3
"""Sample skill: financial_extraction.

Extracts numeric tokens adjacent to currency markers from a text file and
writes them as `key_metrics.csv` in the workspace outputs directory.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path

CURRENCY_RE = re.compile(r"(USD|NT\$|\$)\s*([0-9][0-9,\.]*)")


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

    text = abs_input.read_text(encoding="utf-8", errors="replace")
    matches = CURRENCY_RE.findall(text)

    csv_path = outputs / "key_metrics.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["currency", "amount"])
        for currency, amount in matches:
            writer.writerow([currency, amount])

    print(json.dumps({
        "input": str(input_path),
        "metrics_found": len(matches),
        "output_file": "key_metrics.csv",
    }))
    return 0


if __name__ == "__main__":
    sys.exit(main())
