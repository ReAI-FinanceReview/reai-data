"""Pipeline step wrappers used by CLI and Airflow."""
import warnings
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Callable

from src.utils.logger import get_logger

logger = get_logger(__name__)


def _parse_date_arg(arg_name: str, value: str):
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"Invalid {arg_name}: {value!r}. Expected YYYY-MM-DD.") from None


@dataclass
class RunResult:
    step: str
    status: str
    input_count: int | None = None
    output_count: int | None = None
    output_path: str | None = None
    validations: dict[str, Any] | None = None
    message: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _handle_step(step: str, func: Callable[[], object]) -> RunResult:
    try:
        func()
        return RunResult(step=step, status="success")
    except Exception as exc:  # noqa: BLE001
        logger.exception("%s step failed", step)
        return RunResult(step=step, status="failed", message=str(exc))


def run_crawl(config_path: str | None = None) -> RunResult:
    """Run unified crawl step."""
    from src.crawlers.unified_crawler import UnifiedCrawler
    crawler = UnifiedCrawler(config_path) if config_path is not None else UnifiedCrawler()
    return _handle_step("crawl", lambda: crawler.run())


def run_preprocess(batch_size: int = 100, limit: int | None = None, config_path: str | None = None) -> RunResult:
    """Run preprocessing step (deprecated: replaced by Bronze-to-Silver cleansing pipeline)."""
    msg = (
        "run_preprocess is deprecated and has no effect. "
        "Use scripts/cleanse_reviews.py for Bronze-to-Silver cleansing."
    )
    warnings.warn(msg, DeprecationWarning, stacklevel=2)
    logger.warning(msg)
    return RunResult(step="preprocess", status="failed", message=msg)


def run_extract_features(batch_size: int = 100, limit: int | None = None, config_path: str | None = None) -> RunResult:
    """Run ABSA feature extraction step (Gold Layer)."""
    from src.gold.absa_analyzer import GoldABSAAnalyzer
    return _handle_step("features", lambda: GoldABSAAnalyzer(config_path).process_batch(batch_size=batch_size, limit=limit))


def run_action_analysis(batch_size: int = 100, limit: int | None = None, config_path: str | None = None) -> RunResult:
    """Run actionability & LLM summary step (Gold Layer)."""
    from src.gold.action_analyzer import GoldActionAnalyzer
    analyzer = GoldActionAnalyzer(config_path) if config_path is not None else GoldActionAnalyzer()
    return _handle_step("action", lambda: analyzer.process_batch(batch_size=batch_size, limit=limit))


def run_generate_embeddings(
    batch_size: int = 100, limit: int | None = None, model_name: str = "text-embedding-3-small", config_path: str | None = None
) -> RunResult:
    """Run embedding generation step (Gold Layer)."""
    from src.gold.embedding_generator import GoldEmbeddingGenerator
    return _handle_step(
        "embed",
        lambda: (
            GoldEmbeddingGenerator(model_name=model_name, config_path=config_path)
            if config_path is not None
            else GoldEmbeddingGenerator(model_name=model_name)
        ).process_batch(batch_size=batch_size, limit=limit),
    )


def run_gold(
    batch_size: int = 100,
    limit: int | None = None,
    target_date: str | None = None,
    config_path: str | None = None,
) -> RunResult:
    """Run Gold Layer orchestration step (embedding → ABSA → action analysis)."""
    from src.gold.orchestrator import GoldOrchestrator

    if target_date is not None:
        try:
            parsed_date = _parse_date_arg("target_date", target_date)
        except ValueError as exc:
            return RunResult(step="gold", status="failed", message=str(exc))
    else:
        parsed_date = None

    def _run():
        orchestrator = GoldOrchestrator(config_path) if config_path is not None else GoldOrchestrator()
        result = orchestrator.run(
            batch_size=batch_size,
            limit=limit,
            target_date=parsed_date,
        )
        if result["total"] > 0 and result["analyzed"] == 0:
            raise RuntimeError(f"Gold: 0/{result['total']} succeeded")

    return _handle_step("gold", _run)


def run_dept_assign(
    batch_size: int = 100,
    limit: int | None = None,
    target_date: str | None = None,
    config_path: str | None = None,
    reassign: bool = False,
    assigners: list[str] | None = None,
) -> RunResult:
    """Run department assignment step (Gold Layer).

    두 배정기를 한 스텝에서 순차 실행한다 (기본: rule → llm). 규칙 배정은 DB 질의만
    하므로 빠르고, LLM 배정은 하루치가 수십 건이라 둘의 합이 DAG 태스크의
    execution_timeout 안에 들어온다.

    Args:
        target_date: 지정하면 그 날짜의 ANALYZED 리뷰만 처리한다 (DAG 규약).
            생략하면 ANALYZED 전량이 대상이 되므로 백필 경로가 된다.
        reassign: True 면 이미 배정된 건도 다시 처리한다.
        assigners: 실행할 배정기 목록. None 이면 둘 다.
    """
    from src.gold import dept_assigner
    from src.gold.aggregator import resolve_production_assigner
    from src.gold.assigner_ids import KNOWN_ASSIGNERS

    selected = list(assigners) if assigners is not None else list(KNOWN_ASSIGNERS)

    unknown = [name for name in selected if name not in dept_assigner.ASSIGNERS]
    if unknown:
        return RunResult(
            step="dept_assign",
            status="failed",
            message=f"unknown assigner(s): {', '.join(unknown)}",
        )

    if target_date is not None:
        try:
            parsed_date = _parse_date_arg("target_date", target_date)
        except ValueError as exc:
            return RunResult(step="dept_assign", status="failed", message=str(exc))
    else:
        parsed_date = None

    # 마트가 실제로 소비하는 배정기. 이것만 스텝의 성패를 좌우한다.
    production = resolve_production_assigner()
    summaries: dict[str, Any] = {}

    def _run():
        for name in selected:
            # 모듈에서 호출 시점에 꺼낸다 — 레지스트리가 클래스 객체를 붙들면
            # 테스트의 패치가 닿지 않아 실제 유료 호출로 새어나간다.
            assigner_cls = getattr(dept_assigner, dept_assigner.ASSIGNERS[name])
            assigner = assigner_cls(config_path) if config_path is not None else assigner_cls()
            summary = assigner.assign_batch(
                batch_size=batch_size,
                limit=limit,
                target_date=parsed_date,
                reassign=reassign,
            )
            summaries[name] = summary
            # 미분류는 근거 부족이라는 판정이지 실패가 아니다 (규칙 배정기는 대부분을
            # 미분류로 남긴다). 전량이 오류로 떨어졌을 때만 실패로 본다.
            if summary["total"] == 0 or summary["failed"] != summary["total"]:
                continue

            detail = f"dept_assign({name}): {summary['failed']}/{summary['total']} failed"
            # 마트가 읽지 않는 배정기가 죽었다고 스텝을 실패시키면 dept_assign 이
            # gold_aggregate 의 상류라서 그날 팩트 테이블 4개와 집계 후 검증까지
            # 전부 못 돌게 된다. 배정과 무관한 산출물을 배정 장애로 지우는 셈이다.
            # 예컨대 워커에 OPENAI_API_KEY 가 없으면 llm 은 전량 실패하지만
            # 기본 설정에서 마트는 rule 행만 읽는다.
            if name != production:
                logger.warning("%s — 마트 노출 배정기(%s)가 아니므로 스텝은 계속한다", detail, production)
                continue
            raise RuntimeError(detail)

    result = _handle_step("dept_assign", _run)
    if summaries:
        result.validations = summaries
        # 두 배정기는 같은 리뷰를 각각 처리하므로 합산하면 같은 건을 두 번 센다.
        # 대표값은 마트가 소비하는 배정기의 숫자로 낸다. 배정기별 숫자는
        # validations 에 그대로 남는다.
        counted = summaries.get(production) or next(iter(summaries.values()))
        result.input_count = counted["total"]
        result.output_count = counted["assigned"]
    return result


def run_aggregate(
    target_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    config_path: str | None = None,
) -> RunResult:
    """Run Gold Layer aggregation step (fact tables + serving mart)."""
    from src.gold.aggregator import GoldAggregator
    from datetime import date as _date

    if target_date and (start_date or end_date):
        return RunResult(
            step="aggregate",
            status="failed",
            message="target_date cannot be combined with start_date/end_date",
        )

    if bool(start_date) ^ bool(end_date):
        return RunResult(
            step="aggregate",
            status="failed",
            message="start_date and end_date must be provided together",
        )

    if target_date:
        try:
            parsed_date = _parse_date_arg("target_date", target_date)
        except ValueError as exc:
            return RunResult(step="aggregate", status="failed", message=str(exc))
        aggregator = GoldAggregator(config_path) if config_path is not None else GoldAggregator()
        return _handle_step("aggregate", lambda: aggregator.run(target_date=parsed_date))

    if start_date and end_date:
        try:
            parsed_start = _parse_date_arg("start_date", start_date)
            parsed_end = _parse_date_arg("end_date", end_date)
        except ValueError as exc:
            return RunResult(step="aggregate", status="failed", message=str(exc))
        aggregator = GoldAggregator(config_path) if config_path is not None else GoldAggregator()
        return _handle_step(
            "aggregate",
            lambda: aggregator.run_range(
                start_date=parsed_start,
                end_date=parsed_end,
            ),
        )

    aggregator = GoldAggregator(config_path) if config_path is not None else GoldAggregator()
    return _handle_step("aggregate", lambda: aggregator.run(target_date=_date.today()))


def run_post_aggregate_validation(
    target_date: str,
    config_path: str | None = None,
) -> RunResult:
    """Run post-aggregate DB readiness validation for a target date."""
    from src.pipeline.post_aggregate_validation import PostAggregateValidator

    try:
        parsed_date = _parse_date_arg("target_date", target_date)
    except ValueError as exc:
        return RunResult(step="post_aggregate_validate", status="failed", message=str(exc))

    try:
        report = PostAggregateValidator(config_path).validate(parsed_date)
    except Exception as exc:  # noqa: BLE001
        logger.exception("post_aggregate_validate step failed")
        return RunResult(step="post_aggregate_validate", status="failed", message=str(exc))

    validations = report.as_dict()
    if report.status != "success":
        failed_checks = [
            check.name
            for check in report.checks
            if check.severity == "failure" and not check.passed
        ]
        return RunResult(
            step="post_aggregate_validate",
            status="failed",
            validations=validations,
            message=f"Post-aggregate validation failed: {', '.join(failed_checks)}",
        )

    return RunResult(
        step="post_aggregate_validate",
        status="success",
        validations=validations,
    )


def run_load(batch_size: int = 100, config_path: str | None = None) -> RunResult:
    """Run Parquet batch → DB load step."""
    from src.loaders.batch_loader import BatchLoader
    loader = BatchLoader(config_path) if config_path is not None else BatchLoader()
    return _handle_step("load", lambda: loader.load_pending_batches(limit=batch_size))


def run_steps(
    steps: list[str],
    batch_size: int = 100,
    limit: int | None = None,
    model_name: str = "text-embedding-3-small",
    config_path: str | None = None,
    target_date: str | None = None,
) -> list[RunResult]:
    """Run multiple steps in sequence; stop on first failure."""
    step_funcs: dict[str, Callable[[], RunResult]] = {
        "crawl": lambda: run_crawl(config_path),
        "load": lambda: run_load(batch_size=batch_size, config_path=config_path),
        "preprocess": lambda: run_preprocess(batch_size=batch_size, limit=limit, config_path=config_path),
        "features": lambda: run_extract_features(batch_size=batch_size, limit=limit, config_path=config_path),
        "action": lambda: run_action_analysis(batch_size=batch_size, limit=limit, config_path=config_path),
        "embed": lambda: run_generate_embeddings(batch_size=batch_size, limit=limit, model_name=model_name, config_path=config_path),
        "gold": lambda: run_gold(
            batch_size=batch_size,
            limit=limit,
            config_path=config_path,
            target_date=target_date,
        ),
        # 날짜를 요구한다. 생략을 허용하면 --target-date 를 빠뜨린 CLI 한 줄이
        # ANALYZED 전량에 대한 유료 LLM 스윕이 된다. 전량 처리는 의도를 명시해야
        # 도달하도록 scripts/assign_dept.py --backfill 로만 남긴다.
        "dept_assign": lambda: run_dept_assign(
            batch_size=batch_size,
            limit=limit,
            config_path=config_path,
            target_date=target_date or "",
        ),
        "aggregate": lambda: run_aggregate(config_path=config_path, target_date=target_date),
        "post_aggregate_validate": lambda: run_post_aggregate_validation(
            target_date=target_date or "",
            config_path=config_path,
        ),
        "validate": lambda: run_post_aggregate_validation(
            target_date=target_date or "",
            config_path=config_path,
        ),
    }

    results: list[RunResult] = []
    for step in steps:
        if step not in step_funcs:
            results.append(RunResult(step=step, status="failed", message="unknown step"))
            break
        result = step_funcs[step]()
        results.append(result)
        if result.status != "success":
            break
    return results
