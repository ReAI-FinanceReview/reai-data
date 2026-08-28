"""Command-line interface for running pipeline steps."""
import argparse
from datetime import date
import json
from pathlib import Path

try:
    from dotenv import load_dotenv as _load_dotenv
except ImportError:
    _load_dotenv = None

from src.pipeline.steps import run_crawl, run_extract_features, run_generate_embeddings, run_preprocess, run_action_analysis  # noqa: F401
from src.pipeline.steps import run_steps
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _load_dotenv_if_present() -> None:
    if _load_dotenv is None:
        return
    env_path = Path(__file__).parent.parent.parent / ".env"
    if env_path.exists():
        _ = _load_dotenv(env_path)


def _parse_steps(steps_str: str) -> list[str]:
    steps = [step.strip().lower() for step in steps_str.split(",") if step.strip()]
    return steps


def _parse_target_date(value: str) -> str:
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid --target-date {value!r}: expected YYYY-MM-DD"
        ) from exc
    return value


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run pipeline steps sequentially.")
    parser.add_argument(
        "--steps",
        # DAG(dags/financial_review_pipeline.py) 순서를 따른다. gold 는
        # embedding/ABSA/action 을 오케스트레이션하므로 그 개별 단계를 겸한다.
        # 날짜를 요구하는 단계(dept_assign, aggregate, post_aggregate_validate)는
        # --target-date 없이는 의미가 없어 기본값에서 뺀다.
        default="crawl,load,cleanse,gold",
        help=(
            "Comma-separated steps to run "
            "(options: crawl, load, cleanse, features, action, embed, gold, "
            "dept_assign, aggregate, post_aggregate_validate, validate; "
            "preprocess is deprecated and always fails - use cleanse)"
        ),
    )
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing steps.")
    parser.add_argument("--limit", type=int, default=None, help="Optional limit for records processed.")
    parser.add_argument("--model-name", type=str, default="text-embedding-3-small", help="Embedding model name.")
    parser.add_argument("--config", type=str, default="config/crawler_config.yml", help="Path to crawler/config file.")
    parser.add_argument(
        "--target-date",
        default=None,
        type=_parse_target_date,
        help="Optional target date for date-scoped steps (YYYY-MM-DD).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    _load_dotenv_if_present()
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    steps = _parse_steps(args.steps)
    results = run_steps(
        steps=steps,
        batch_size=args.batch_size,
        limit=args.limit,
        model_name=args.model_name,
        config_path=args.config,
        target_date=args.target_date,
    )

    for result in results:
        logger.info("Step %s -> %s", result.step, result.status)
        logger.info("Result: %s", json.dumps(result.as_dict(), ensure_ascii=False))
        if result.status != "success":
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
