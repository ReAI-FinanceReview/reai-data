"""Load Stage Entrypoint - Parquet 배치 → DB 적재

Usage:
    PYTHONPATH=src python scripts/load_reviews.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.pipeline.steps import run_load

if __name__ == "__main__":
    result = run_load()
    print(f"Load result: {result.as_dict()}")
    sys.exit(0 if result.status == "success" else 1)
