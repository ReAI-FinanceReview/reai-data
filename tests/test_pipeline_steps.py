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
    assert result.input_count == 6
    assert result.output_count == 4


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
    mock_rule.return_value.assign_batch.return_value = _summary(total=5, assigned=0, failed=5)

    result = run_dept_assign(target_date="2025-01-15")

    assert result.status == "failed"
    assert "5/5 failed" in result.message
    # 규칙 배정에서 이미 실패했으므로 LLM 유료 호출로 넘어가지 않는다.
    mock_llm.return_value.assign_batch.assert_not_called()


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


def test_run_steps_supports_post_aggregate_validate_step_name():
    results = run_steps(["post_aggregate_validate"], target_date="")

    assert len(results) == 1
    assert results[0].step == "post_aggregate_validate"
    assert results[0].status == "failed"
    assert "YYYY-MM-DD" in results[0].message
