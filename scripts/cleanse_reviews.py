#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Bronze-to-Silver Review Cleansing Pipeline CLI.

Usage:
    python scripts/cleanse_reviews.py
    python scripts/cleanse_reviews.py --date 2026-03-04
"""
import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

# Ensure project root is on sys.path so src.* imports work regardless of CWD
_PROJECT_ROOT = Path(__file__).parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv()

from src.utils.minio_client import MinIOClient
from src.utils.db_connector import DatabaseConnector
from src.utils.logger import get_logger
from src.processing.cleanse import ReviewCleaningPipeline

logger = get_logger(__name__)

_CONFIG_DIR = _PROJECT_ROOT / 'config' / 'dictionaries'
SYNONYMS_PATH = str(_CONFIG_DIR / 'synonyms.json')
PROFANITY_PATH = str(_CONFIG_DIR / 'profanity.json')


def main():
    parser = argparse.ArgumentParser(description='Bronze → Silver Review Cleansing Pipeline')
    parser.add_argument('--date', type=str, default=None, help='처리 날짜 YYYY-MM-DD (기본값: 어제)')
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date) if args.date else date.today() - timedelta(days=1)
    logger.info(f"Starting cleansing pipeline for date: {target_date}")

    pipeline = ReviewCleaningPipeline(
        minio_client=MinIOClient(),
        db_connector=DatabaseConnector(),
        synonyms_path=SYNONYMS_PATH,
        profanity_path=PROFANITY_PATH,
    )
    try:
        result = pipeline.run(target_date=target_date)
        logger.info(
            f"Pipeline complete: processed={result['processed']}, "
            f"skipped={result['skipped']}, elapsed={result['elapsed_sec']}s"
        )
    except Exception:
        logger.exception("Cleanse pipeline failed")
        sys.exit(1)


if __name__ == '__main__':
    main()
