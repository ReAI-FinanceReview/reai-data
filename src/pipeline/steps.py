"""Pipeline step wrappers used by CLI and Airflow."""
from dataclasses import asdict, dataclass
from typing import Callable, Dict, List, Optional

from src.crawlers.unified_crawler import UnifiedCrawler
from src.processing.embedding import EmbeddingGenerator
from src.processing.feature_extraction import FeatureExtractor
from src.processing.preprocess import TextPreprocessor
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
    return _handle_step("crawl", lambda: UnifiedCrawler(config_path).run())


def run_preprocess(batch_size: int = 100, limit: Optional[int] = None, config_path: Optional[str] = None) -> RunResult:
    """Run preprocessing step."""
    return _handle_step("preprocess", lambda: TextPreprocessor(config_path).process_batch(batch_size=batch_size, limit=limit))


def run_extract_features(batch_size: int = 100, limit: Optional[int] = None, config_path: Optional[str] = None) -> RunResult:
    """Run feature extraction step."""
    return _handle_step("features", lambda: FeatureExtractor(config_path).process_batch(batch_size=batch_size, limit=limit))


def run_generate_embeddings(
    batch_size: int = 100, limit: Optional[int] = None, model_name: str = "text-embedding-3-small", config_path: Optional[str] = None
) -> RunResult:
    """Run embedding generation step."""
    return _handle_step(
        "embed",
        lambda: EmbeddingGenerator(model_name=model_name, config_path=config_path).process_batch(batch_size=batch_size, limit=limit),
    )


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
        "preprocess": lambda: run_preprocess(batch_size=batch_size, limit=limit, config_path=config_path),
        "features": lambda: run_extract_features(batch_size=batch_size, limit=limit, config_path=config_path),
        "embed": lambda: run_generate_embeddings(batch_size=batch_size, limit=limit, model_name=model_name, config_path=config_path),
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
