# Aggregate Range Backfill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore single-date scheduled aggregation and add an explicit date-range backfill utility for manual repair.

**Architecture:** Keep `GoldAggregator.run()` as the daily path, introduce `run_range()` for bounded multi-date backfills, and route `run_aggregate()` based on explicit arguments. Preserve the old `run_all()` only as an intentional full backfill helper.

**Tech Stack:** Python, pytest, Airflow BashOperator, SQLAlchemy

---

### Task 1: Lock the new aggregate interface in tests

**Files:**
- Modify: `tests/test_gold_aggregator.py`
- Create: `tests/test_pipeline_steps.py`

- [ ] Add failing tests for bounded range aggregation and aggregate argument validation.
- [ ] Run `PYTHONPATH=. uv run pytest tests/test_gold_aggregator.py tests/test_pipeline_steps.py -q` and confirm the new tests fail for missing `run_range` and missing argument handling.

### Task 2: Implement bounded range aggregation

**Files:**
- Modify: `src/gold/aggregator.py`

- [ ] Add `run_range(start_date, end_date)` and extend `_fetch_analyzed_dates()` with optional range bounds.
- [ ] Keep `run_all()` as an explicit full backfill wrapper.
- [ ] Re-run `PYTHONPATH=. uv run pytest tests/test_gold_aggregator.py tests/test_pipeline_steps.py -q`.

### Task 3: Restore scheduled single-date aggregation

**Files:**
- Modify: `src/pipeline/steps.py`
- Modify: `dags/financial_review_pipeline.py`

- [ ] Update `run_aggregate()` to dispatch to `run`, `run_range`, or fail on invalid combinations.
- [ ] Restore the DAG command to `run_aggregate(target_date='{{ ds }}')`.
- [ ] Re-run `PYTHONPATH=. uv run pytest tests/test_gold_aggregator.py tests/test_pipeline_steps.py -q`.
