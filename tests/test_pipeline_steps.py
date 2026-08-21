from unittest.mock import MagicMock, patch

from src.gold.dept_assigner import ASSIGNER_LLM, ASSIGNER_RULE
from src.pipeline.steps import (
    run_aggregate,
    run_dept_assign,
    run_gold,
    run_post_aggregate_validation,
    run_steps,
)


def _summary(total=1, assigned=1, unclassified=0, failed=0):
    return {
        "total": total,
        "assigned": assigned,
        "unclassified": unclassified,
        "failed": failed,
    }


@patch("src.gold.aggregator.GoldAggregator")
def test_run_aggregate_uses_target_date_by_default(mock_aggregator):
    instance = mock_aggregator.return_value
    instance.run = MagicMock()

    result = run_aggregate(target_date="2025-01-15")

    instance.run.assert_called_once()
    assert str(instance.run.call_args.kwargs["target_date"]) == "2025-01-15"
    assert result.status == "success"


@patch("src.gold.aggregator.GoldAggregator")
def test_run_aggregate_uses_date_range_when_provided(mock_aggregator):
    instance = mock_aggregator.return_value
    instance.run_range = MagicMock()

    result = run_aggregate(start_date="2025-01-10", end_date="2025-01-15")

    instance.run_range.assert_called_once()
    kwargs = instance.run_range.call_args.kwargs
    assert str(kwargs["start_date"]) == "2025-01-10"
    assert str(kwargs["end_date"]) == "2025-01-15"
    assert result.status == "success"


def test_run_aggregate_rejects_mixing_target_date_and_range():
    result = run_aggregate(
        target_date="2025-01-15",
        start_date="2025-01-10",
        end_date="2025-01-15",
    )

    assert result.status == "failed"
    assert "target_date cannot be combined" in result.message


def test_run_aggregate_requires_complete_range():
    result = run_aggregate(start_date="2025-01-10")

    assert result.status == "failed"
    assert "start_date and end_date must be provided together" in result.message


def test_run_aggregate_returns_failed_result_for_invalid_target_date():
    result = run_aggregate(target_date="2025/01/15")

    assert result.status == "failed"
    assert "YYYY-MM-DD" in result.message


def test_run_aggregate_returns_failed_result_for_invalid_range_date():
    result = run_aggregate(start_date="2025-01-10", end_date="bad-date")

    assert result.status == "failed"
    assert "YYYY-MM-DD" in result.message


@patch("src.gold.orchestrator.GoldOrchestrator")
def test_run_gold_passes_target_date_to_orchestrator(mock_orchestrator):
    instance = mock_orchestrator.return_value
    instance.run.return_value = {"total": 1, "analyzed": 1, "failed": 0}

    result = run_gold(batch_size=100, target_date="2025-01-15")

    instance.run.assert_called_once()
    assert str(instance.run.call_args.kwargs["target_date"]) == "2025-01-15"
    assert result.status == "success"


@patch("src.gold.orchestrator.GoldOrchestrator")
def test_run_gold_uses_default_orchestrator_config_when_config_path_omitted(mock_orchestrator):
    instance = mock_orchestrator.return_value
    instance.run.return_value = {"total": 1, "analyzed": 1, "failed": 0}

    result = run_gold(batch_size=100, target_date="2025-01-15")

    mock_orchestrator.assert_called_once_with()
    assert result.status == "success"


def test_run_gold_returns_failed_result_for_invalid_target_date():
    result = run_gold(target_date="2025/01/15")

    assert result.status == "failed"
    assert "YYYY-MM-DD" in result.message


def test_run_gold_returns_failed_result_for_empty_target_date():
    result = run_gold(target_date="")

    assert result.status == "failed"
    assert "YYYY-MM-DD" in result.message


def test_run_post_aggregate_validation_returns_failed_result_for_invalid_target_date():
    result = run_post_aggregate_validation(target_date="2025/01/15")

    assert result.status == "failed"
    assert "YYYY-MM-DD" in result.message


@patch("src.gold.dept_assigner.LLMAssigner")
@patch("src.gold.dept_assigner.RuleBasedAssigner")
def test_run_dept_assign_runs_both_assigners_by_default(mock_rule, mock_llm):
    mock_rule.return_value.assign_batch.return_value = _summary(total=3, assigned=1, unclassified=2)
    mock_llm.return_value.assign_batch.return_value = _summary(total=3, assigned=3)

    result = run_dept_assign(target_date="2025-01-15")

    for mock_assigner in (mock_rule, mock_llm):
        kwargs = mock_assigner.return_value.assign_batch.call_args.kwargs
        assert str(kwargs["target_date"]) == "2025-01-15"
        assert kwargs["reassign"] is False
    assert result.status == "success"
    assert result.validations == {
        ASSIGNER_RULE: _summary(total=3, assigned=1, unclassified=2),
        ASSIGNER_LLM: _summary(total=3, assigned=3),
    }
    # 두 배정기가 같은 리뷰 3건을 각각 처리하므로 합산은 같은 건을 두 번 센다.
    # 대표값은 마트가 소비하는 배정기(기본 rule)의 숫자다.
    assert result.input_count == 3
    assert result.output_count == 1


@patch("src.gold.dept_assigner.LLMAssigner")
@patch("src.gold.dept_assigner.RuleBasedAssigner")
def test_run_dept_assign_backfills_full_backlog_when_target_date_omitted(mock_rule, mock_llm):
    """날짜를 생략하면 ANALYZED 전량이 대상이다 (LLM 누적분 수동 백필 경로)."""
    mock_llm.return_value.assign_batch.return_value = _summary(total=621, assigned=600, unclassified=21)

    result = run_dept_assign(assigners=[ASSIGNER_LLM], reassign=True)

    kwargs = mock_llm.return_value.assign_batch.call_args.kwargs
    assert kwargs["target_date"] is None
    assert kwargs["reassign"] is True
    mock_rule.assert_not_called()
    assert result.status == "success"
    assert result.input_count == 621


@patch("src.gold.dept_assigner.LLMAssigner")
@patch("src.gold.dept_assigner.RuleBasedAssigner")
def test_run_dept_assign_treats_unclassified_as_success(mock_rule, mock_llm):
    """미분류는 근거 부족이라는 판정이지 실패가 아니다.

    규칙 배정기는 실측에서 621건 중 533건을 미분류로 남겼다. 이를 실패로 세면
    DAG 가 정상 실행마다 빨간불이 된다.
    """
    all_unclassified = _summary(total=10, assigned=0, unclassified=10)
    mock_rule.return_value.assign_batch.return_value = all_unclassified
    mock_llm.return_value.assign_batch.return_value = all_unclassified

    result = run_dept_assign(target_date="2025-01-15")

    assert result.status == "success"
    assert result.output_count == 0


@patch("src.gold.dept_assigner.LLMAssigner")
@patch("src.gold.dept_assigner.RuleBasedAssigner")
def test_run_dept_assign_fails_when_every_review_errored(mock_rule, mock_llm):
    """마트가 소비하는 배정기(기본 rule)가 전량 실패하면 스텝이 실패한다."""
    mock_rule.return_value.assign_batch.return_value = _summary(total=5, assigned=0, failed=5)

    result = run_dept_assign(target_date="2025-01-15")

    assert result.status == "failed"
    assert "5/5 failed" in result.message
    # 규칙 배정에서 이미 실패했으므로 LLM 유료 호출로 넘어가지 않는다.
    mock_llm.return_value.assign_batch.assert_not_called()


@patch("src.gold.dept_assigner.LLMAssigner")
@patch("src.gold.dept_assigner.RuleBasedAssigner")
def test_run_dept_assign_tolerates_total_failure_of_a_non_production_assigner(mock_rule, mock_llm):
    """마트가 읽지 않는 배정기의 장애로 그날 집계를 통째로 막으면 안 된다.

    dept_assign 은 gold_aggregate 의 상류다. 워커에 OPENAI_API_KEY 가 없기만 해도
    llm 은 전량 실패하는데, 기본 설정에서 마트는 rule 행만 읽는다. 이때 스텝을
    실패시키면 배정과 무관한 팩트 테이블 4개와 집계 후 검증까지 못 돌게 된다.
    """
    mock_rule.return_value.assign_batch.return_value = _summary(total=5, assigned=5)
    mock_llm.return_value.assign_batch.return_value = _summary(total=5, assigned=0, failed=5)

    result = run_dept_assign(target_date="2025-01-15")

    assert result.status == "success"
    assert result.validations[ASSIGNER_LLM]["failed"] == 5


@patch("src.gold.dept_assigner.LLMAssigner")
@patch("src.gold.dept_assigner.RuleBasedAssigner")
def test_run_dept_assign_continues_past_partial_failure(mock_rule, mock_llm):
    """일부만 실패한 것은 실패가 아니다 — 실패 행은 is_failed 로 남고 재시도 대상이 된다.

    경계가 `==` 라 `>=` 로 바뀌거나 배정기 간 failed 를 합산하면 여기서 깨진다.
    """
    mock_rule.return_value.assign_batch.return_value = _summary(
        total=10, assigned=5, unclassified=2, failed=3
    )
    mock_llm.return_value.assign_batch.return_value = _summary(total=10, assigned=10)

    result = run_dept_assign(target_date="2025-01-15")

    assert result.status == "success"
    # 전량 실패가 아니므로 다음 배정기로 넘어간다.
    mock_llm.return_value.assign_batch.assert_called_once()
    assert result.input_count == 10
    assert result.output_count == 5


@patch("src.gold.dept_assigner.LLMAssigner")
@patch("src.gold.dept_assigner.RuleBasedAssigner")
def test_run_dept_assign_succeeds_when_there_is_nothing_to_assign(mock_rule, mock_llm):
    """대상 0건(0/0)이 공허하게 '전량 실패' 로 읽히면 한가한 날마다 DAG 가 실패한다."""
    empty = _summary(total=0, assigned=0)
    mock_rule.return_value.assign_batch.return_value = empty
    mock_llm.return_value.assign_batch.return_value = empty

    result = run_dept_assign(target_date="2025-01-15")

    assert result.status == "success"
    assert result.input_count == 0


def test_run_dept_assign_rejects_unknown_assigner():
    result = run_dept_assign(target_date="2025-01-15", assigners=["gpt"])

    assert result.status == "failed"
    assert "unknown assigner(s): gpt" in result.message


def test_run_dept_assign_returns_failed_result_for_invalid_target_date():
    result = run_dept_assign(target_date="2025/01/15")

    assert result.status == "failed"
    assert "YYYY-MM-DD" in result.message


@patch("src.pipeline.steps.run_dept_assign")
def test_run_steps_supports_dept_assign_step_name(mock_run_dept_assign):
    mock_run_dept_assign.return_value = MagicMock(status="success")

    results = run_steps(["dept_assign"], target_date="2025-01-15")

    assert len(results) == 1
    assert mock_run_dept_assign.call_args.kwargs["target_date"] == "2025-01-15"


def test_run_steps_requires_a_target_date_for_dept_assign():
    """날짜를 빠뜨린 CLI 한 줄이 ANALYZED 전량에 대한 유료 LLM 스윕이 되면 안 된다.

    전량 처리는 scripts/assign_dept.py --backfill 로만 도달한다.
    """
    results = run_steps(["dept_assign"])

    assert results[0].step == "dept_assign"
    assert results[0].status == "failed"
    assert "YYYY-MM-DD" in results[0].message


def test_run_steps_supports_post_aggregate_validate_step_name():
    results = run_steps(["post_aggregate_validate"], target_date="")

    assert len(results) == 1
    assert results[0].step == "post_aggregate_validate"
    assert results[0].status == "failed"
    assert "YYYY-MM-DD" in results[0].message
