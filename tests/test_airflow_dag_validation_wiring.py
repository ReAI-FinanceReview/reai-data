from pathlib import Path


DAG_PATH = Path(__file__).resolve().parents[1] / "dags" / "financial_review_pipeline.py"


def test_airflow_dag_wires_post_aggregate_validation_after_gold_aggregate():
    dag_source = DAG_PATH.read_text(encoding="utf-8")

    assert 'task_id="post_aggregate_validate"' in dag_source
    assert "--steps post_aggregate_validate --target-date {{ ds }}" in dag_source
    assert "gold_aggregate\n    >> post_aggregate_validate" in dag_source
    assert dag_source.index('task_id="gold_aggregate"') < dag_source.index(
        'task_id="post_aggregate_validate"'
    )
