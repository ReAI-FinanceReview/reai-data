#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Financial Review ETL Pipeline DAG

금융 앱 리뷰 분석을 위한 Airflow DAG 정의.

Steps:
  1. Crawl reviews → app_reviews
  2. Preprocess → reviews_preprocessed
  3. Extract features → review_aspects (Gold ABSA)
  4. Generate embeddings → review_embeddings (Gold)
  5. Gold orchestration → review_embeddings + review_aspects + review_action_analysis
"""
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# 프로젝트 경로/파이썬 실행기: 환경변수 우선, 없으면 DAG 파일 기준 상대경로 사용
PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parents[1]))
PYTHON_PATH = os.environ.get("PYTHON_BIN", f"{PROJECT_ROOT}/venv/bin/python")

# 기본 설정
default_args = {
    'owner': 'finance-review-team',
    'depends_on_past': False,
    'email': ['your-email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=6),  # 전체 파이프라인 실행 제한
}

# DAG 정의
dag = DAG(
    'financial_review_etl_pipeline',
    default_args=default_args,
    description='금융 앱 리뷰 크롤링 및 분석 파이프라인',
    schedule_interval='@daily',  # 매일 실행
    start_date=datetime(2025, 11, 19),
    catchup=False,  # 과거 날짜에 대해 실행 안함
    tags=['finance', 'etl', 'reviews', 'nlp'],
)


# Step 1: 리뷰 크롤링
crawl_reviews = BashOperator(
    task_id='step1_crawl_reviews',
    bash_command=f'cd {PROJECT_ROOT} && PYTHONPATH=src {PYTHON_PATH} {PROJECT_ROOT}/scripts/crawl_reviews.py',
    dag=dag,
    execution_timeout=timedelta(hours=2),  # 크롤링 실행 제한 2시간
)

# Step 2: 텍스트 전처리
preprocess_reviews = BashOperator(
    task_id='step2_preprocess_reviews',
    bash_command=f'cd {PROJECT_ROOT} && PYTHONPATH=src {PYTHON_PATH} {PROJECT_ROOT}/scripts/preprocess_reviews.py',
    dag=dag,
    execution_timeout=timedelta(hours=1),
)

# Step 3 & 4: 병렬 처리 가능
with TaskGroup('step3_4_parallel_processing', dag=dag) as parallel_processing:
    # Step 3: 특성 추출 (감정분석, 키워드, 토픽)
    extract_features = BashOperator(
        task_id='step3_extract_features',
        bash_command=f'cd {PROJECT_ROOT} && PYTHONPATH=src {PYTHON_PATH} {PROJECT_ROOT}/scripts/extract_features.py',
        execution_timeout=timedelta(hours=2),
    )

    # Step 4: 임베딩 생성
    generate_embeddings = BashOperator(
        task_id='step4_generate_embeddings',
        bash_command=f'cd {PROJECT_ROOT} && PYTHONPATH=src {PYTHON_PATH} {PROJECT_ROOT}/scripts/generate_embeddings.py',
        execution_timeout=timedelta(hours=2),
    )


# Step 5: Gold Layer 분석 (embedding → ABSA → action)
gold_analyze = BashOperator(
    task_id='step5_gold_analyze',
    bash_command=(
        f'cd {PROJECT_ROOT} && PYTHONPATH=. {PYTHON_PATH} -c '
        '"from src.pipeline.steps import run_gold; '
        'r = run_gold(batch_size=100); '
        'print(r.as_dict())"'
    ),
    dag=dag,
    execution_timeout=timedelta(hours=3),
)

# Step 6: Gold Layer 집계 (fact tables + serving mart)
gold_aggregate = BashOperator(
    task_id='step6_gold_aggregate',
    bash_command=(
        f'cd {PROJECT_ROOT} && PYTHONPATH=. {PYTHON_PATH} -c '
        '"from src.pipeline.steps import run_aggregate; '
        'r = run_aggregate(); '
        'print(r.as_dict())"'
    ),
    dag=dag,
    execution_timeout=timedelta(hours=1),
)

# 의존성 설정
# Step 1 → Step 2 → [Step 3 & Step 4 병렬] → Step 5 → Step 6
crawl_reviews >> preprocess_reviews >> parallel_processing >> gold_analyze >> gold_aggregate


# ============================================================================
#  추가 옵션: Python Operator를 사용한 데이터 품질 검증
# ============================================================================

def validate_data_quality(**context):
    """
    데이터 품질 검증 함수

    - 각 단계별 레코드 개수 확인
    - 성공률 계산
    - 임계값 기준 검증
    """
    import sys

    # 프로젝트 루트를 Python 경로에 추가
    sys.path.insert(0, str(PROJECT_ROOT))

    from src.utils.db_connector import DatabaseConnector
    from sqlalchemy import text

    db = DatabaseConnector()

    with db.get_session() as session:
        # Bronze Layer: 크롤링된 데이터 개수
        bronze_count = session.execute(
            text("SELECT COUNT(*) FROM app_reviews")
        ).scalar()

        # Silver Layer: 처리된 데이터 개수
        preprocessed_count = session.execute(
            text("SELECT COUNT(*) FROM reviews_preprocessed")
        ).scalar()

        features_count = session.execute(
            text("SELECT COUNT(*) FROM reviews_features")
        ).scalar()

        embeddings_count = session.execute(
            text("SELECT COUNT(*) FROM review_embeddings")
        ).scalar()

        print(f"Data Quality Report:")
        print(f"  - Bronze (app_reviews): {bronze_count:,}")
        print(f"  - Silver (preprocessed): {preprocessed_count:,}")
        print(f"  - Silver (features): {features_count:,}")
        print(f"  - Silver (embeddings): {embeddings_count:,}")

        # 성공률 계산
        if bronze_count > 0:
            success_rate = (features_count / bronze_count) * 100
            print(f"  - Success Rate: {success_rate:.2f}%")

            # 임계값 검증 (80% 미만이면 경고)
            if success_rate < 80:
                raise ValueError(f"Data processing success rate is below 80%: {success_rate:.2f}%")

        return {
            'bronze': bronze_count,
            'preprocessed': preprocessed_count,
            'features': features_count,
            'embeddings': embeddings_count,
        }


# 데이터 품질 검증 Task (옵션)
# validate_quality = PythonOperator(
#     task_id='validate_data_quality',
#     python_callable=validate_data_quality,
#     provide_context=True,
#     dag=dag,
# )

# 검증을 파이프라인 마지막에 추가하려면:
# parallel_processing >> validate_quality


# ============================================================================
# 알림 및 모니터링 설정 예시
# ============================================================================

# Slack 알림 예시 (airflow-providers-slack 패키지 필요)
# from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
#
# def task_failure_alert(context):
#     """Task 실패 시 Slack 알림"""
#     slack_msg = f"""
#     :red_circle: Task Failed
#     *Task*: {context.get('task_instance').task_id}
#     *Dag*: {context.get('task_instance').dag_id}
#     *Execution Time*: {context.get('execution_date')}
#     *Log Url*: {context.get('task_instance').log_url}
#     """
#     return SlackWebhookOperator(
#         task_id='slack_notification',
#         http_conn_id='slack_webhook',
#         message=slack_msg,
#         username='airflow'
#     ).execute(context=context)
#
# # DAG default_args에 추가:
# # 'on_failure_callback': task_failure_alert,
