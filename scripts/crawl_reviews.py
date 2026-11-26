#!/usr/bin/env python3
"""Run crawl step only.

    python scripts/crawl_reviews.py                   # 두 스토어 (App Store, Play 순서)
    python scripts/crawl_reviews.py --store appstore  # Airflow crawl_appstore 태스크
"""
import argparse
import json
import sys
import os
from dotenv import load_dotenv
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.crawlers.unified_crawler import CRAWL_STORE_CHOICES  # noqa: E402
from src.pipeline.steps import run_crawl  # noqa: E402


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the crawl step for one store or both.")
    parser.add_argument(
        "--store",
        choices=list(CRAWL_STORE_CHOICES),
        default="all",
        help="크롤할 스토어 (기본 all: 두 스토어를 모두 돌리고 하나라도 실패하면 exit 1)",
    )
    parser.add_argument(
        "--config", default="config/crawler_config.yml", help="Path to crawler/config file."
    )
    return parser


def main(argv=None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = run_crawl(config_path=args.config, store=args.store)
    print(json.dumps(result.as_dict(), ensure_ascii=False))
    if result.status != "success":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
