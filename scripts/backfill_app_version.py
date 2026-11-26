#!/usr/bin/env python3
"""Backfill app_reviews.app_version for existing reviews by re-fetching them (Issue #125).

기본은 dry-run(쓰기 없음)이다. ``--apply`` 일 때만 app_version IS NULL 인 행을 갱신한다.
App Store 를 먼저 처리한다.

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

EXIT_CONFIRMED = 0
EXIT_ERROR = 1
EXIT_USAGE = 2  # argparse's own exit code for a bad argument; the backfill does not run
EXIT_UNVERIFIED = 3

EXIT_CODES_HELP = f"""\
exit codes:
  {EXIT_CONFIRMED}  모든 앱을 끝까지 읽었다. 못 맞춘 리뷰는 모두 unmatched_confirmed 다
  {EXIT_ERROR}  스토어 요청이나 DB 쓰기가 실패한 앱이 있다 (error 칸). {EXIT_UNVERIFIED} 보다 우선한다
  {EXIT_USAGE}  인자가 잘못됐다 (argparse). 백필을 실행하지 않는다
  {EXIT_UNVERIFIED}  오류는 없지만 unverified 가 남았다 (빈 페이지, 빈 피드, Play 상한, 두 번 같지
     않았던 Play 끝, App Store 조회 중단). 나중에 다시 실행하면 더 채워질 수 있다
"""


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fill app_reviews.app_version from a store re-fetch (Issue #125).",
        epilog=EXIT_CODES_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
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
    if report.has_errors:
        return EXIT_ERROR
    if report.has_unverified:
        return EXIT_UNVERIFIED
    return EXIT_CONFIRMED


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
