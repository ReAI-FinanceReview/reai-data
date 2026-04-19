"""Load Stage Entrypoint - Parquet 배치 → DB 적재

Usage:
    PYTHONPATH=. python scripts/load_reviews.py
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.pipeline.steps import run_load


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Load pending parquet batches into ReviewMasterIndex.")
    parser.add_argument("--date", help="Load only batches created on YYYY-MM-DD.")
    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()
    result = run_load(target_date=args.date)
    print(f"Load result: {result.as_dict()}")
    sys.exit(0 if result.status == "success" else 1)
