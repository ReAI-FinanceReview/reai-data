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


def _dept_assign_block(dag_source: str) -> str:
    return dag_source[
        dag_source.index('task_id="dept_assign"') : dag_source.index('task_id="gold_aggregate"')
    ]


def test_dept_assign_task_timeout_covers_both_assigners():
    """규칙과 LLM 을 한 태스크에서 순차 실행하므로 타임아웃이 둘의 합을 덮어야 한다."""
    dag_source = DAG_PATH.read_text(encoding="utf-8")

    assert "execution_timeout=timedelta(hours=1)" in _dept_assign_block(dag_source)


def test_dept_assign_task_overrides_the_default_retry_budget():
    """default_args 의 retries=3 은 배정기의 MAX_TRIES=3 과 같은 숫자다.

    그대로 두면 Airflow 재시도가 try_number 상한을 소진해 4번째 실행이 대상 0건을
    보고 성공으로 끝난다 — 지속적인 실패가 초록불이 된다.
    """
    from src.gold.dept_assigner import _BatchAssignerBase

    dag_source = DAG_PATH.read_text(encoding="utf-8")

    assert "retries=1," in _dept_assign_block(dag_source)
    assert _BatchAssignerBase.MAX_TRIES > 1, "재시도 예산이 상한과 같으면 실패가 성공으로 보인다"


def test_dept_assign_task_commits_in_small_batches():
    """기본 batch_size 는 하루치를 한 청크로 묶어, 타임아웃 kill 시 지불한 호출을 통째로 버린다."""
    dag_source = DAG_PATH.read_text(encoding="utf-8")

    assert "--batch-size" in _dept_assign_block(dag_source)
