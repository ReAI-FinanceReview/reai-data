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
from src.pipeline.steps import run_crawl, run_extract_features, run_generate_embeddings, run_preprocess  # noqa: F401
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _load_dotenv_if_present() -> None:
    """
    Load environment variables from a .env file located three directories above this file, if python-dotenv is installed and the file exists.
    
    If the dotenv package is not available or the .env file is absent, this function does nothing.
    """
    if not DOTENV_AVAILABLE:
        return
    env_path = Path(__file__).parent.parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)


def _parse_steps(steps_str: str) -> List[str]:
    """
    Normalize a comma-separated steps string into a list of step names.
    
    Parameters:
        steps_str (str): Comma-separated step names; may contain whitespace and mixed casing.
    
    Returns:
        List[str]: Step names trimmed of whitespace, converted to lowercase, and excluding empty entries.
    """
    steps = [step.strip().lower() for step in steps_str.split(",") if step.strip()]
    return steps


def build_arg_parser() -> argparse.ArgumentParser:
    """
    Create an ArgumentParser configured with command-line options for running pipeline steps.
    
    The parser defines the following options with their defaults:
    - --steps: Comma-separated steps to run (default: "crawl,preprocess,features,embed").
    - --batch-size: Batch size for processing steps (default: 100).
    - --limit: Optional limit for records processed (default: None).
    - --model-name: Embedding model name (default: "text-embedding-3-small").
    - --config: Path to crawler/config file (default: "config/crawler_config.yml").
    
    Returns:
        argparse.ArgumentParser: A parser configured with the CLI options described above.
    """
    parser = argparse.ArgumentParser(description="Run pipeline steps sequentially.")
    parser.add_argument(
        "--steps",
        default="crawl,preprocess,features,embed",
        help="Comma-separated steps to run (options: crawl, preprocess, features, embed)",
    )
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing steps.")
    parser.add_argument("--limit", type=int, default=None, help="Optional limit for records processed.")
    parser.add_argument("--model-name", type=str, default="text-embedding-3-small", help="Embedding model name.")
    parser.add_argument("--config", type=str, default="config/crawler_config.yml", help="Path to crawler/config file.")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """
    Run the configured pipeline steps from command-line arguments and return an exit code.
    
    Loads environment variables if a .env file is present, parses CLI arguments (or the provided argv list), executes the selected pipeline steps, logs each step's status and result, and returns a non-zero exit code if any step failed.
    
    Parameters:
        argv (Optional[List[str]]): Command-line arguments to parse. If None, the process's command-line arguments are used.
    
    Returns:
        int: Exit code — 0 if all steps succeeded, 1 if any step's status is not "success".
    """
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