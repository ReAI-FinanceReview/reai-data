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
        """
        Produce a dictionary representation of the RunResult.
        
        Returns:
            result (Dict): A mapping of field names to their corresponding values.
        """
        return asdict(self)


def _handle_step(step: str, func: Callable[[], None]) -> RunResult:
    """
    Execute a single pipeline step function and return a RunResult indicating success or failure.
    
    Parameters:
        step (str): Name of the pipeline step being executed.
        func (Callable[[], None]): Zero-argument callable that performs the step; exceptions raised by this callable are caught.
    
    Returns:
        RunResult: A result with `status` set to "success" if `func` completes without raising; if an exception is raised, `status` is "failed" and `message` contains the exception string.
    """
    try:
        func()
        return RunResult(step=step, status="success")
    except Exception as exc:  # noqa: BLE001
        logger.exception("%s step failed", step)
        return RunResult(step=step, status="failed", message=str(exc))


def run_crawl(config_path: Optional[str] = None) -> RunResult:
    """
    Execute the unified crawling pipeline step.
    
    Parameters:
        config_path (Optional[str]): Path to a configuration file for the crawler, if different from the default.
    
    Returns:
        RunResult: Result object describing the "crawl" step outcome, including status and any error message.
    """
    return _handle_step("crawl", lambda: UnifiedCrawler(config_path).run())


def run_preprocess(batch_size: int = 100, limit: Optional[int] = None, config_path: Optional[str] = None) -> RunResult:
    """
    Execute the preprocessing pipeline over data in batches.
    
    Parameters:
    	batch_size (int): Number of items to process per batch.
    	limit (Optional[int]): Maximum total number of items to process; `None` processes all available items.
    	config_path (Optional[str]): Filesystem path to a configuration file for the preprocessor; `None` uses default configuration.
    
    Returns:
    	RunResult: Object describing the step outcome. `status` is `"success"` on success or `"failed"` on error; `message` contains error details when failed.
    """
    return _handle_step("preprocess", lambda: TextPreprocessor(config_path).process_batch(batch_size=batch_size, limit=limit))


def run_extract_features(batch_size: int = 100, limit: Optional[int] = None, config_path: Optional[str] = None) -> RunResult:
    """
    Run the feature extraction step for the dataset.
    
    Parameters:
        batch_size (int): Number of items to process per batch.
        limit (Optional[int]): Maximum total items to process; processes all available items when None.
        config_path (Optional[str]): Path to a configuration file for the feature extractor; uses the default configuration when None.
    
    Returns:
        RunResult: Outcome of the "features" step — `status` is `"success"` on completion or `"failed"` on error; may include `input_count`, `output_count`, `output_path`, `validations`, and `message`.
    """
    return _handle_step("features", lambda: FeatureExtractor(config_path).process_batch(batch_size=batch_size, limit=limit))


def run_generate_embeddings(
    batch_size: int = 100, limit: Optional[int] = None, model_name: str = "text-embedding-3-small", config_path: Optional[str] = None
) -> RunResult:
    """
    Run the embedding generation step using the configured EmbeddingGenerator.
    
    Parameters:
        batch_size (int): Number of items to process per batch.
        limit (Optional[int]): Maximum total items to process; process all items when None.
        model_name (str): Name of the embedding model to use.
        config_path (Optional[str]): Optional path to a configuration file for the generator.
    
    Returns:
        RunResult: Summary of the embedding generation step; `status` is `"success"` on success or `"failed"` with an error message on failure.
    """
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
    """
    Run a sequence of pipeline steps in order, stopping when a step fails or an unknown step is encountered.
    
    Parameters:
        steps (List[str]): Ordered list of step names to execute. Supported names: "crawl", "preprocess", "features", "embed".
        batch_size (int): Number of items to process per batch for batchable steps.
        limit (Optional[int]): Optional maximum number of items to process; pass None for no limit.
        model_name (str): Embedding model identifier used by the "embed" step.
        config_path (Optional[str]): Optional path to a configuration file passed to each step.
    
    Returns:
        List[RunResult]: A list of RunResult objects representing each executed step in order. Execution stops after the first non-"success" result; an unknown step yields a RunResult with status "failed" and message "unknown step".
    """
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