#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Financial Review ETL Pipeline DAG

금융 앱 리뷰 분석을 위한 Airflow DAG.

실행 순서:
  1. crawl_reviews   — App Store / Play Store 리뷰 크롤링 → MinIO Parquet + IngestionBatch(PENDING)
  2. load_reviews    — IngestionBatch Parquet → ReviewMasterIndex(RAW)
  3. cleanse_reviews — Bronze Parquet → Silver(reviews_preprocessed), ReviewMasterIndex(CLEANED)
  4. gold_analyze    — GoldOrchestrator: embedding → ABSA → action, ReviewMasterIndex(ANALYZED)
  5. gold_aggregate  — 팩트 테이블 UPSERT (fact_service_review_daily 등 4개)
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

# Step 1: 리뷰 크롤링
# AppStoreCrawler + PlayStoreCrawler → MinIO Parquet + IngestionBatch PENDING
crawl_reviews = BashOperator(
    task_id="crawl_reviews",
    bash_command=f"cd {PROJECT_ROOT} && PYTHONPATH=. {PYTHON_BIN} scripts/crawl_reviews.py",
    dag=dag,
    execution_timeout=timedelta(hours=2),
)

# Step 2: DB 로드
# IngestionBatch(PENDING) → ReviewMasterIndex(RAW)
load_reviews = BashOperator(
    task_id="load_reviews",
    bash_command=f"cd {PROJECT_ROOT} && PYTHONPATH=. {PYTHON_BIN} scripts/load_reviews.py",
    dag=dag,
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
        "r = run_gold(batch_size=100); "
        "print(r.as_dict()); "
        "sys.exit(0 if r.status == 'success' else 1)\""
    ),
    dag=dag,
    execution_timeout=timedelta(hours=3),
)

# Step 5: Gold 집계
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

crawl_reviews >> load_reviews >> cleanse_reviews >> gold_analyze >> gold_aggregate
