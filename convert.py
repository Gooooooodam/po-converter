#!/usr/bin/env python3
# coding: utf-8
"""
CLI wrapper for gpstopo converter.

Usage:
    python convert.py INPUT.xlsx ERP.csv [--type po|so] [--output OUTPUT.csv]

This script lets you call the same conversion logic that powers the Flask
web app directly from the command line (or a GitHub Action runner) without
starting a server.

Arguments
---------
INPUT.xlsx   The customer GPS report (Excel) you want to convert.
ERP.csv      The Xoro ERP product master file (CSV) that provides unit cost &
             title mapping.
--type/-t    Target document type: "po" (purchase order, default) or
             "so" (sales order).
--output/-o  Optional path for the converted CSV. If omitted, the script
             writes «INPUT_PO.csv» or «INPUT_SO.csv» next to the source file.

Examples
--------
Convert to PO CSV in default location:
    python convert.py sample.xlsx xoro.csv

Convert to SO and save under a specific name:
    python convert.py sample.xlsx xoro.csv --type so --output out/sales.csv

Why another file?
-----------------
Your existing *gpstopo.py* focuses on the Flask UI. GitHub Action runners (or
any automated system) don't need a web server—they just need a function call.
This thin wrapper imports the proven `convert_po` / `convert_so` functions from
*gpstopo.py* and makes them available as a one‑shot CLI tool.

You can now trigger it from a GitHub Action like:
    - name: Run converter
      run: python convert.py input.xlsx erp.csv --type ${{ env.DOC_TYPE }}
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

# Re‑use the business logic from your existing web app
try:
    from gpstopo import convert_po, convert_so
except ImportError as err:
    sys.exit(
        "❌ Cannot import convert functions from gpstopo.py — "\
        "make sure convert.py sits in the same directory or gpstopo "\
        "is on PYTHONPATH. Original error: " + str(err)
    )


def _read_binary(path: Path) -> bytes:
    """Return file content or exit with an explicit error message."""
    try:
        return path.read_bytes()
    except Exception as exc:  # pragma: no cover – we exit on purpose
        sys.exit(f"❌ Failed to read '{path}': {exc}")


def _read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception as exc:  # pragma: no cover
        sys.exit(f"❌ Failed to read CSV '{path}': {exc}")


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(
        prog="convert.py",
        description="Convert GPS order Excel into ERP‑ready CSV (PO or SO).",
    )
    parser.add_argument("input_file", type=Path, help="GPS report .xlsx file")
    parser.add_argument("erp_file", type=Path, help="Xoro data .csv file")
    parser.add_argument(
        "--type",
        "-t",
        choices=["po", "so"],
        default="po",
        help="Target document type (default: po)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Where to write the converted CSV (defaults to INPUT_*O.csv)",
    )

    args = parser.parse_args()

    po_bytes = _read_binary(args.input_file)
    erp_df = _read_csv(args.erp_file)

    # Run conversion
    if args.type == "po":
        csv_io = convert_po(po_bytes, erp_df)
        suffix = "_PO.csv"
    else:
        csv_io = convert_so(po_bytes, erp_df)
        suffix = "_SO.csv"

    # Decide output location
    out_path: Path = (
        args.output
        if args.output is not None
        else args.input_file.with_suffix("").with_name(
            args.input_file.stem + suffix
        )
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(csv_io.getvalue())

    print(f"✅ Converted file written to: {out_path.resolve()}")


if __name__ == "__main__":
    main()
