"""Command-line interface for running pipeline steps."""
import argparse
import json
from pathlib import Path
from typing import List, Optional

try:
    from dotenv import load_dotenv

    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False

from src.pipeline.steps import run_steps
from src.pipeline.steps import run_crawl, run_extract_features, run_generate_embeddings, run_preprocess, run_action_analysis  # noqa: F401
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _load_dotenv_if_present() -> None:
    if not DOTENV_AVAILABLE:
        return
    env_path = Path(__file__).parent.parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)


def _parse_steps(steps_str: str) -> List[str]:
    steps = [step.strip().lower() for step in steps_str.split(",") if step.strip()]
    return steps


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run pipeline steps sequentially.")
    parser.add_argument(
        "--steps",
        default="crawl,preprocess,features,action,embed",
        help="Comma-separated steps to run (options: crawl, preprocess, features, action, embed)",
    )
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing steps.")
    parser.add_argument("--limit", type=int, default=None, help="Optional limit for records processed.")
    parser.add_argument("--model-name", type=str, default="text-embedding-3-small", help="Embedding model name.")
    parser.add_argument("--config", type=str, default="config/crawler_config.yml", help="Path to crawler/config file.")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
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
    )

    for result in results:
        logger.info("Step %s -> %s", result.step, result.status)
        logger.info("Result: %s", json.dumps(result.as_dict(), ensure_ascii=False))
        if result.status != "success":
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
