#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pyright: reportMissingImports=false
"""
Financial Review ETL Pipeline DAG

금융 앱 리뷰 분석을 위한 Airflow DAG.

실행 순서:
  1. crawl_appstore, crawl_playstore — 스토어별 리뷰 크롤링 → MinIO Parquet + IngestionBatch(PENDING)
     (crawl_watcher — 크롤이 하나라도 끝내 실패하면 실패해 DAG 실행을 실패로 남김)
  2. load_reviews    — IngestionBatch Parquet → ReviewMasterIndex(RAW)
  3. cleanse_reviews — Bronze Parquet → Silver(reviews_preprocessed), ReviewMasterIndex(CLEANED)
  4. gold_analyze    — GoldOrchestrator: embedding → ABSA → action, ReviewMasterIndex(ANALYZED)
  5. dept_assign     — 부서 배정 (규칙 + LLM), reviews_assigned UPSERT
  6. gold_aggregate  — 팩트 테이블 UPSERT (fact_service_review_daily 등 4개)
  7. post_aggregate_validate — target date DB 검증 및 DAG 성공/실패 판정
"""
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parents[1]))
PYTHON_BIN = os.environ.get("PYTHON_BIN", f"{PROJECT_ROOT}/.venv/bin/python")

default_args = {
    "owner": "finance-review-team",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "financial_review_etl_pipeline",
    default_args=default_args,
    description="금융 앱 리뷰 크롤링 및 분석 파이프라인",
    schedule_interval="@daily",
    start_date=datetime(2025, 11, 19),
    catchup=False,
    tags=["finance", "etl", "reviews", "nlp"],
)

# Step 1: 리뷰 크롤링 (스토어별 태스크)
# AppStoreCrawler / PlayStoreCrawler → MinIO Parquet + IngestionBatch PENDING
# 스토어마다 따로 실패하고 따로 재시도한다(default_args 의 3회, 5분 간격). 스토어 단계가 실패하는
# 경우는 크롤러 예외와 피드 장애 판정이다(App Store: 리뷰를 못 읽은 앱이 연달아
# appstore.max_consecutive_empty_feeds 개, Play: 모든 앱 0건). 앱 하나가 빈 것은 경고다.
crawl_appstore = BashOperator(
    task_id="crawl_appstore",
    bash_command=f"cd {PROJECT_ROOT} && PYTHONPATH=. {PYTHON_BIN} scripts/crawl_reviews.py --store appstore",
    dag=dag,
    execution_timeout=timedelta(hours=2),
)

crawl_playstore = BashOperator(
    task_id="crawl_playstore",
    bash_command=f"cd {PROJECT_ROOT} && PYTHONPATH=. {PYTHON_BIN} scripts/crawl_reviews.py --store playstore",
    dag=dag,
    execution_timeout=timedelta(hours=2),
)

# DAG 실행의 성패는 leaf 태스크로 정해진다. 아래 load_reviews 는 크롤 하나가 실패해도 돌기 때문에
# 뒤 단계가 성공하면 DAG 실행은 성공이 되어 크롤 실패가 묻힌다. 이 leaf 태스크가 크롤 실패를
# 받아 실패해 DAG 실행을 실패로 남긴다(Airflow 문서의 watcher 패턴). 크롤이 모두 성공하면 skipped 다.
crawl_watcher = BashOperator(
    task_id="crawl_watcher",
    bash_command='echo "crawl_appstore 또는 crawl_playstore 가 재시도 끝에 실패했다" >&2; exit 1',
    dag=dag,
    trigger_rule="one_failed",
    retries=0,
)

# Step 2: DB 로드
# IngestionBatch(PENDING) → ReviewMasterIndex(RAW)
# trigger_rule="all_done": 두 크롤 태스크가 재시도까지 모두 끝난 뒤, 하나가 실패했어도 적재한다.
# none_failed_min_one_success 는 크롤 하나만 실패해도 이 태스크를 upstream_failed 로 건너뛰고,
# one_success 는 다른 스토어의 크롤(과 재시도)을 기다리지 않는다. 둘 다 실패하면 이전 실행이나
# 장애로 멈춘 크롤이 남긴 PENDING 배치만 적재한다.
load_reviews = BashOperator(
    task_id="load_reviews",
    bash_command=f"cd {PROJECT_ROOT} && PYTHONPATH=. {PYTHON_BIN} scripts/load_reviews.py",
    dag=dag,
    trigger_rule="all_done",
    execution_timeout=timedelta(minutes=30),
)

# Step 3: Bronze → Silver 클렌징
# Bronze Parquet → Silver(reviews_preprocessed), ReviewMasterIndex 상태 RAW → CLEANED
cleanse_reviews = BashOperator(
    task_id="cleanse_reviews",
    bash_command=f"cd {PROJECT_ROOT} && PYTHONPATH=. {PYTHON_BIN} scripts/cleanse_reviews.py --date {{{{ ds }}}}",
    dag=dag,
    execution_timeout=timedelta(hours=1),
)

# Step 4: Gold 분석
# GoldOrchestrator: embedding → ABSA → action (순차), ReviewMasterIndex(ANALYZED)
gold_analyze = BashOperator(
    task_id="gold_analyze",
    bash_command=(
        f"cd {PROJECT_ROOT} && PYTHONPATH=. {PYTHON_BIN} -c "
        '"from src.pipeline.steps import run_gold; import sys; '
        "r = run_gold(batch_size=100, target_date='{{ ds }}'); "
        "print(r.as_dict()); "
        "sys.exit(0 if r.status == 'success' else 1)\""
    ),
    dag=dag,
    execution_timeout=timedelta(hours=3),
)

# Step 5: 부서 배정
# 규칙 배정기와 LLM 배정기를 한 태스크에서 순차 실행한다. 규칙은 DB 질의뿐이고
# LLM 은 하루치가 수십 건이라 둘의 합이 아래 타임아웃 안에 들어온다.
#
# processing_status 는 ANALYZED 로 둔다. 배정이 끝났다고 다음 상태로 전이시키면
# gold_aggregate 의 서빙 마트 질의가 ANALYZED 만 적재하므로 배정된 리뷰가
# 마트에서 사라진다.
#
# 과거 누적분(LLM 미배정 잔량)은 이 DAG 가 아니라 scripts/assign_dept.py 를
# 날짜 없이 한 번 수동 실행해 채운다 — anti-join 이 중복 배정을 막는다.
#
# retries 와 batch_size 는 기본값을 쓰면 안 된다.
#   - default_args 의 retries=3 은 배정기의 MAX_TRIES=3 과 같은 숫자다. Airflow 가
#     3번 재시도하면 try_number 가 상한에 닿아 4번째 실행은 대상 0건을 보고 성공으로
#     끝난다 — 지속적인 실패가 초록불이 된다. 그래서 이 태스크만 retries=1 이다.
#   - assign_batch 는 batch_size 마다 커밋한다. 기본 100 이면 하루치가 한 청크라
#     타임아웃 kill 시 그 시간에 지불한 LLM 호출이 통째로 사라진다. 작게 잘라
#     중단되어도 직전 청크까지는 남게 한다.
dept_assign = BashOperator(
    task_id="dept_assign",
    bash_command=(
        f"cd {PROJECT_ROOT} && PYTHONPATH=. {PYTHON_BIN} "
        "scripts/assign_dept.py --date {{ ds }} --batch-size 10"
    ),
    dag=dag,
    retries=1,
    execution_timeout=timedelta(hours=1),
)

# Step 6: Gold 집계
# 기본은 DAG 실행일 기준 단일 날짜 집계.
# 과거 날짜 복구가 필요할 때만 start_date/end_date 범위 백필을 수동 실행한다.
# UPSERT: fact_service_review_daily, fact_service_aspect_daily,
#          fact_category_radar_scores, srv_daily_review_list
gold_aggregate = BashOperator(
    task_id="gold_aggregate",
    bash_command=(
        f"cd {PROJECT_ROOT} && PYTHONPATH=. {PYTHON_BIN} -c "
        '"from src.pipeline.steps import run_aggregate; import sys; '
        "r = run_aggregate(target_date='{{ ds }}'); "
        "print(r.as_dict()); "
        "sys.exit(0 if r.status == 'success' else 1)\""
    ),
    dag=dag,
    execution_timeout=timedelta(hours=1),
)

# Step 7: 집계 후 DB 검증
# 신규 유입 0건은 warning/report로 남기되, 상태 전이·서빙 mart·무결성 실패는 DAG 실패로 전파한다.
post_aggregate_validate = BashOperator(
    task_id="post_aggregate_validate",
    bash_command=(
        f"cd {PROJECT_ROOT} && PYTHONPATH=. {PYTHON_BIN} -m src.pipeline.cli "
        "--steps post_aggregate_validate --target-date {{ ds }}"
    ),
    dag=dag,
    execution_timeout=timedelta(minutes=15),
)

[crawl_appstore, crawl_playstore] >> crawl_watcher

(
    [crawl_appstore, crawl_playstore]
    >> load_reviews
    >> cleanse_reviews
    >> gold_analyze
    >> dept_assign
    >> gold_aggregate
    >> post_aggregate_validate
)
