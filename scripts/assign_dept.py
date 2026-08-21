#!/usr/bin/env python3
"""Run department assignment step only.

DAG 는 날짜를 주고 하루치만 배정한다. 전량 처리는 ``--backfill`` 로 명시해야
도달한다 — LLM 배정기의 과거 누적분은 anti-join 이 중복을 막으므로 DAG 밖에서
한 번 수동 실행한다.

    python scripts/assign_dept.py --date 2026-08-20            # 하루치, 규칙+LLM
    python scripts/assign_dept.py --backfill --assigner llm    # 전량 백필 (유료)
    python scripts/assign_dept.py --backfill --assigner llm --limit 10   # 소량 검증
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.gold.assigner_ids import KNOWN_ASSIGNERS  # noqa: E402
from src.pipeline.steps import run_dept_assign  # noqa: E402


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Assign departments to ANALYZED reviews (Issue #40)."
    )
    # 둘 중 하나를 반드시 골라야 한다. 날짜 생략이 곧 전량 처리이면, DAG 정의에서
    # 명령을 복사해 수동 재실행할 때 플래그 하나가 빠지는 것만으로 전 이력에 대한
    # 유료 LLM 스윕이 된다. 비싼 쪽은 타이핑을 요구한다.
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--date", default=None, help="대상 날짜 YYYY-MM-DD")
    scope.add_argument(
        "--backfill",
        action="store_true",
        help="ANALYZED 전량을 대상으로 한다. LLM 배정기와 함께 쓰면 전 이력에 유료 호출이 걸린다",
    )
    parser.add_argument(
        "--assigner",
        action="append",
        choices=list(KNOWN_ASSIGNERS),
        help="실행할 배정기. 반복 지정 가능하며 기본은 둘 다",
    )
    parser.add_argument("--batch-size", type=int, default=100, help="커밋 단위 배치 크기")
    parser.add_argument("--limit", type=int, default=None, help="처리할 리뷰 수 상한")
    parser.add_argument(
        "--reassign", action="store_true", help="이미 배정된 건도 다시 처리한다"
    )
    return parser


def main(argv=None) -> int:
    args = build_arg_parser().parse_args(argv)

    result = run_dept_assign(
        batch_size=args.batch_size,
        limit=args.limit,
        target_date=args.date,
        reassign=args.reassign,
        assigners=args.assigner,
    )
    print(result.as_dict())
    return 0 if result.status == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
