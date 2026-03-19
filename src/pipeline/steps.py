"""Pipeline step wrappers used by CLI and Airflow."""
import warnings
from dataclasses import asdict, dataclass
from typing import Callable, Dict, List, Optional

from src.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class RunResult:
    step: str
    status: str
    input_count: Optional[int] = None
    output_count: Optional[int] = None
    output_path: Optional[str] = None
    validations: Optional[Dict] = None
    message: Optional[str] = None

    def as_dict(self) -> Dict:
        return asdict(self)


def _handle_step(step: str, func: Callable[[], None]) -> RunResult:
    try:
        func()
        return RunResult(step=step, status="success")
    except Exception as exc:  # noqa: BLE001
        logger.exception("%s step failed", step)
        return RunResult(step=step, status="failed", message=str(exc))


def run_crawl(config_path: Optional[str] = None) -> RunResult:
    """Run unified crawl step."""
    from src.crawlers.unified_crawler import UnifiedCrawler
    return _handle_step("crawl", lambda: UnifiedCrawler(config_path).run())


def run_preprocess(batch_size: int = 100, limit: Optional[int] = None, config_path: Optional[str] = None) -> RunResult:
    """Run preprocessing step (deprecated: replaced by Bronze-to-Silver cleansing pipeline)."""
    msg = (
        "run_preprocess is deprecated and has no effect. "
        "Use scripts/cleanse_reviews.py for Bronze-to-Silver cleansing."
    )
    warnings.warn(msg, DeprecationWarning, stacklevel=2)
    logger.warning(msg)
    return RunResult(step="preprocess", status="failed", message=msg)


def run_extract_features(batch_size: int = 100, limit: Optional[int] = None, config_path: Optional[str] = None) -> RunResult:
    """Run ABSA feature extraction step (Gold Layer)."""
    from src.gold.absa_analyzer import GoldABSAAnalyzer
    return _handle_step("features", lambda: GoldABSAAnalyzer(config_path).process_batch(batch_size=batch_size, limit=limit))


def run_action_analysis(batch_size: int = 100, limit: Optional[int] = None, config_path: Optional[str] = None) -> RunResult:
    """Run actionability & LLM summary step (Gold Layer)."""
    from src.gold.action_analyzer import GoldActionAnalyzer
    return _handle_step("action", lambda: GoldActionAnalyzer(config_path).process_batch(batch_size=batch_size, limit=limit))


def run_generate_embeddings(
    batch_size: int = 100, limit: Optional[int] = None, model_name: str = "text-embedding-3-small", config_path: Optional[str] = None
) -> RunResult:
    """Run embedding generation step (Gold Layer)."""
    from src.gold.embedding_generator import GoldEmbeddingGenerator
    return _handle_step(
        "embed",
        lambda: GoldEmbeddingGenerator(model_name=model_name, config_path=config_path).process_batch(batch_size=batch_size, limit=limit),
    )


def run_gold(batch_size: int = 100, limit: Optional[int] = None, config_path: Optional[str] = None) -> RunResult:
    """Run Gold Layer orchestration step (embedding → ABSA → action analysis)."""
    from src.gold.orchestrator import GoldOrchestrator

    def _run():
        result = GoldOrchestrator(config_path).run(batch_size=batch_size, limit=limit)
        if result["total"] > 0 and result["analyzed"] == 0:
            raise RuntimeError(f"Gold: 0/{result['total']} succeeded")

    return _handle_step("gold", _run)


def run_aggregate(target_date: Optional[str] = None, config_path: Optional[str] = None) -> RunResult:
    """Run Gold Layer aggregation step (fact tables + serving mart)."""
    from src.gold.aggregator import GoldAggregator
    from datetime import date as _date

    parsed_date = None
    if target_date:
        from datetime import datetime
        parsed_date = datetime.strptime(target_date, "%Y-%m-%d").date()

    return _handle_step(
        "aggregate",
        lambda: GoldAggregator(config_path).run(target_date=parsed_date or _date.today()),
    )


def run_load(batch_size: int = 100, config_path: Optional[str] = None) -> RunResult:
    """Run Parquet batch → DB load step."""
    from src.loaders.batch_loader import BatchLoader
    return _handle_step("load", lambda: BatchLoader(config_path).load_pending_batches(limit=batch_size))


def run_steps(
    steps: List[str],
    batch_size: int = 100,
    limit: Optional[int] = None,
    model_name: str = "text-embedding-3-small",
    config_path: Optional[str] = None,
) -> List[RunResult]:
    """Run multiple steps in sequence; stop on first failure."""
    step_funcs: Dict[str, Callable[[], RunResult]] = {
        "crawl": lambda: run_crawl(config_path),
        "load": lambda: run_load(batch_size=batch_size, config_path=config_path),
        "preprocess": lambda: run_preprocess(batch_size=batch_size, limit=limit, config_path=config_path),
        "features": lambda: run_extract_features(batch_size=batch_size, limit=limit, config_path=config_path),
        "action": lambda: run_action_analysis(batch_size=batch_size, limit=limit, config_path=config_path),
        "embed": lambda: run_generate_embeddings(batch_size=batch_size, limit=limit, model_name=model_name, config_path=config_path),
        "gold": lambda: run_gold(batch_size=batch_size, limit=limit, config_path=config_path),
        "aggregate": lambda: run_aggregate(config_path=config_path),
    }

    results: List[RunResult] = []
    for step in steps:
        if step not in step_funcs:
            results.append(RunResult(step=step, status="failed", message="unknown step"))
            break
        result = step_funcs[step]()
        results.append(result)
        if result.status != "success":
            break
    return results
