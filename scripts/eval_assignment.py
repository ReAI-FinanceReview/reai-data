#!/usr/bin/env python3
"""Evaluate stored department assignments against human labels.

reviews_assigned 에 이미 적재된 배정 결과를 라벨 CSV 와 대조한다. 배정 자체는
하지 않으므로, 규칙/LLM 중 무엇을 돌렸든 직후에 그대로 실행하면 된다.

    uv run python scripts/eval_assignment.py
    uv run python scripts/eval_assignment.py --labels path/to/labels.csv --json out.json
"""
import argparse
import json
import os
import sys
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from sqlalchemy import text  # noqa: E402

from src.gold.dept_assigner import UNCLASSIFIED, fetch_assignments  # noqa: E402
from src.gold.dept_eval import (  # noqa: E402
    DEFAULT_LABELS_PATH,
    evaluate,
    format_report,
    load_labels,
)
from src.utils.db_connector import DatabaseConnector  # noqa: E402

DEFAULT_CONFIG_PATH = "config/crawler_config.yml"
LABELS_PATH_ENV = "DEPT_LABELS_PATH"

_COVERAGE_SQL = text(
    """
    SELECT
        COUNT(*)                                                        AS total,
        COUNT(*) FILTER (WHERE is_failed)                               AS failed,
        COUNT(*) FILTER (WHERE NOT is_failed
                           AND assigned_dept = ARRAY[:unclassified])    AS unclassified
    FROM reviews_assigned
    """
)


def resolve_labels_path(cli_value: Optional[str]) -> Path:
    """--labels > 환경변수 > 기본 경로 순으로 라벨 위치를 정한다."""
    if cli_value:
        return Path(cli_value)
    env_value = os.getenv(LABELS_PATH_ENV)
    if env_value:
        return Path(env_value)
    return ROOT / DEFAULT_LABELS_PATH


def format_coverage(total: int, failed: int, unclassified: int) -> str:
    """전체 적재분의 기권 비율. 라벨 평가와 별개의 참고 지표다."""
    assigned = total - failed - unclassified
    rate = (assigned / total) if total else 0.0
    return (
        "reviews_assigned 적재 현황\n"
        f"  total        {total:>6}\n"
        f"  assigned     {assigned:>6}  ({rate:.1%})\n"
        f"  unclassified {unclassified:>6}\n"
        f"  failed       {failed:>6}"
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Evaluate stored department assignments against human labels."
    )
    parser.add_argument("--labels", help=f"라벨 CSV 경로 (기본 {DEFAULT_LABELS_PATH})")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="DB 설정 파일 경로")
    parser.add_argument("--json", dest="json_path", help="집계 결과를 JSON 으로 저장할 경로")
    args = parser.parse_args(argv)

    labels_path = resolve_labels_path(args.labels)
    if not labels_path.exists():
        print(f"라벨 CSV 가 없다: {labels_path}", file=sys.stderr)
        print("라벨링을 마친 뒤 해당 경로에 두거나 --labels 로 지정하라.", file=sys.stderr)
        return 1

    labels = load_labels(labels_path)
    if not labels:
        print(f"라벨이 비어 있다 (assigned_dept 열이 모두 공란): {labels_path}", file=sys.stderr)
        return 1

    session = DatabaseConnector(args.config).get_session()
    try:
        assignments = fetch_assignments(session, [label.review_id for label in labels])
        coverage = session.execute(_COVERAGE_SQL, {"unclassified": UNCLASSIFIED}).one()
    finally:
        session.close()

    result = evaluate(labels, assignments)

    print(format_coverage(coverage[0], coverage[1], coverage[2]))
    print()
    print(format_report(result, title=f"부서 배정 평가 ({labels_path.name})"))

    if args.json_path:
        payload = {
            "labels_path": str(labels_path),
            "coverage": {
                "total": coverage[0],
                "failed": coverage[1],
                "unclassified": coverage[2],
            },
            "summary": result.as_dict(),
            "per_review": result.per_review,
        }
        Path(args.json_path).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"\nJSON 저장: {args.json_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
