import re
from pathlib import Path


DAG_PATH = Path(__file__).resolve().parents[1] / "dags" / "financial_review_pipeline.py"


def test_airflow_dag_wires_post_aggregate_validation_after_gold_aggregate():
    dag_source = DAG_PATH.read_text(encoding="utf-8")

    assert 'task_id="post_aggregate_validate"' in dag_source
    assert "--steps post_aggregate_validate --target-date {{ ds }}" in dag_source
    assert re.search(r"gold_aggregate\s*>>\s*post_aggregate_validate", dag_source)
    assert dag_source.index('task_id="gold_aggregate"') < dag_source.index(
        'task_id="post_aggregate_validate"'
    )


def test_airflow_dag_wires_dept_assign_between_gold_analyze_and_gold_aggregate():
    """배정은 분석 뒤·집계 앞이어야 한다.

    앞으로 당기면 ANALYZED 가 아직 아니라 대상이 비고, 뒤로 밀면 그날 집계가
    전날 배정 결과로 만들어진다 (서빙 마트가 reviews_assigned 를 조인한다).
    """
    dag_source = DAG_PATH.read_text(encoding="utf-8")

    assert 'task_id="dept_assign"' in dag_source
    assert "scripts/assign_dept.py --date {{ ds }}" in dag_source
    assert re.search(r"gold_analyze\s*>>\s*dept_assign\s*>>\s*gold_aggregate", dag_source)
    assert dag_source.index('task_id="gold_analyze"') < dag_source.index(
        'task_id="dept_assign"'
    ) < dag_source.index('task_id="gold_aggregate"')


def test_dept_assign_task_timeout_covers_both_assigners():
    """규칙과 LLM 을 한 태스크에서 순차 실행하므로 타임아웃이 둘의 합을 덮어야 한다."""
    dag_source = DAG_PATH.read_text(encoding="utf-8")

    dept_block = dag_source[
        dag_source.index('task_id="dept_assign"') : dag_source.index('task_id="gold_aggregate"')
    ]
    assert "execution_timeout=timedelta(hours=1)" in dept_block
