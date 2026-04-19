from unittest.mock import MagicMock, patch

from src.pipeline.steps import run_aggregate, run_gold, run_load


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


@patch("src.loaders.batch_loader.BatchLoader")
def test_run_load_passes_target_date_to_batch_loader(mock_loader):
    instance = mock_loader.return_value
    instance.load_pending_batches = MagicMock()

    result = run_load(batch_size=100, target_date="2025-01-15")

    instance.load_pending_batches.assert_called_once()
    assert str(instance.load_pending_batches.call_args.kwargs["target_date"]) == "2025-01-15"
    assert result.status == "success"


def test_run_gold_returns_failed_result_for_invalid_target_date():
    result = run_gold(target_date="2025/01/15")

    assert result.status == "failed"
    assert "YYYY-MM-DD" in result.message


def test_run_load_returns_failed_result_for_invalid_target_date():
    result = run_load(target_date="2025/01/15")

    assert result.status == "failed"
    assert "YYYY-MM-DD" in result.message
