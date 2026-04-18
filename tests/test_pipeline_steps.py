from unittest.mock import MagicMock, patch

from src.pipeline.steps import run_aggregate


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
