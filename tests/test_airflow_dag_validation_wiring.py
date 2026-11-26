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


def _task_block(dag_source: str, task_id: str) -> str:
    """The BashOperator call that defines ``task_id``."""
    start = dag_source.rindex("= BashOperator(", 0, dag_source.index(f'task_id="{task_id}"'))
    return dag_source[start : dag_source.index("\n)\n", start)]


def test_crawl_runs_as_one_task_per_store():
    """스토어마다 따로 실패하고 따로 재시도하도록 크롤을 스토어별 태스크로 나눈다 (#127)."""
    dag_source = DAG_PATH.read_text(encoding="utf-8")

    assert 'task_id="crawl_reviews"' not in dag_source
    for store in ("appstore", "playstore"):
        block = _task_block(dag_source, f"crawl_{store}")
        assert f"scripts/crawl_reviews.py --store {store}" in block
        assert "execution_timeout=timedelta(hours=2)" in block
        assert "trigger_rule" not in block and "retries" not in block  # default_args: 3 retries / 5 min


def test_load_reviews_waits_for_both_crawls_and_runs_after_one_failed():
    """all_done: 두 크롤이 재시도까지 끝난 뒤, 하나가 실패해도 다른 스토어의 배치를 적재한다.

    none_failed_min_one_success 는 크롤 하나가 실패하면 load_reviews 를 upstream_failed 로
    건너뛰고, one_success 는 다른 크롤을 기다리지 않는다.
    """
    dag_source = DAG_PATH.read_text(encoding="utf-8")

    assert re.search(r"\[crawl_appstore, crawl_playstore\]\s*>>\s*load_reviews\s*>>\s*cleanse_reviews", dag_source)
    assert 'trigger_rule="all_done"' in _task_block(dag_source, "load_reviews")


def test_crawl_watcher_keeps_a_failed_crawl_in_the_dag_run_state():
    """DAG 실행 상태는 leaf 태스크로 정해진다. load_reviews 가 all_done 이라 크롤 실패 뒤에도
    leaf 가 성공할 수 있으므로, 크롤 실패를 받아 실패하는 leaf 가 있어야 DAG 실행이 실패로 남는다."""
    dag_source = DAG_PATH.read_text(encoding="utf-8")
    block = _task_block(dag_source, "crawl_watcher")

    assert re.search(r"\[crawl_appstore, crawl_playstore\]\s*>>\s*crawl_watcher\s*\n", dag_source)
    assert 'trigger_rule="one_failed"' in block
    assert "retries=0" in block
    assert "exit 1" in block
    assert not re.search(r"crawl_watcher\s*>>", dag_source), "crawl_watcher must stay a leaf"
