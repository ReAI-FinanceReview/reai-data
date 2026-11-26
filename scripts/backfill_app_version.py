#!/usr/bin/env python3
"""Backfill app_reviews.app_version for existing reviews by re-fetching them (Issue #125).

기본은 dry-run(쓰기 없음)이다. ``--apply`` 일 때만 app_version IS NULL 인 행을 갱신한다.
App Store 를 먼저 처리한다. 오류가 난 앱이 하나라도 있으면 종료 코드는 1 이다.

    python scripts/backfill_app_version.py                          # dry-run, 두 스토어
    python scripts/backfill_app_version.py --platform appstore --apply
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.crawlers.app_version_backfill import (  # noqa: E402
    PLATFORM_CHOICES,
    backfill_app_versions,
    format_report,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fill app_reviews.app_version from a store re-fetch (Issue #125)."
    )
    parser.add_argument(
        "--apply", action="store_true", help="실제로 갱신한다. 생략하면 dry-run 으로 건수만 보고한다"
    )
    parser.add_argument(
        "--platform", choices=list(PLATFORM_CHOICES), default="all", help="대상 스토어 (기본 all)"
    )
    parser.add_argument(
        "--config", default="config/crawler_config.yml", help="Path to crawler/config file."
    )
    return parser


def main(argv=None) -> int:
    args = build_arg_parser().parse_args(argv)
    report = backfill_app_versions(args.config, PLATFORM_CHOICES[args.platform], apply=args.apply)
    print(format_report(report))
    return 1 if report.has_errors else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
