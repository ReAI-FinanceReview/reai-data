#!/usr/bin/env python3
"""Run department assignment step only.

DAG 는 날짜를 주고 하루치만 배정한다. 날짜를 생략하면 ANALYZED 전량이 대상이므로
백필 경로가 된다 — LLM 배정기의 과거 누적분은 anti-join 이 중복을 막으므로
DAG 밖에서 한 번 수동 실행한다.

    python scripts/assign_dept.py --date 2026-08-20     # 하루치, 규칙+LLM
    python scripts/assign_dept.py --assigner llm        # 백필: ANALYZED 전량
    python scripts/assign_dept.py --assigner llm --reassign   # 이미 배정된 건도 재처리
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.gold.dept_assigner import ASSIGNER_LLM, ASSIGNER_RULE  # noqa: E402
from src.pipeline.steps import run_dept_assign  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Assign departments to ANALYZED reviews (Issue #40)."
    )
    parser.add_argument(
        "--date", default=None, help="대상 날짜 YYYY-MM-DD (생략 시 ANALYZED 전량 = 백필)"
    )
    parser.add_argument(
        "--assigner",
        action="append",
        choices=[ASSIGNER_RULE, ASSIGNER_LLM],
        help="실행할 배정기. 반복 지정 가능하며 기본은 둘 다",
    )
    parser.add_argument("--batch-size", type=int, default=100, help="커밋 단위 배치 크기")
    parser.add_argument("--limit", type=int, default=None, help="처리할 리뷰 수 상한")
    parser.add_argument(
        "--reassign", action="store_true", help="이미 배정된 건도 다시 처리한다"
    )
    args = parser.parse_args(argv)

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
