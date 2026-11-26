# Airflow Continuous Load Readiness

## Contract boundary

`financial_review_etl_pipeline`의 운영 성공 기준은 Airflow task 실행 성공에 더해
`gold_aggregate` 이후 `post_aggregate_validate` DB 검증 task 성공까지 포함한다.

크롤은 스토어별 task(`crawl_appstore`, `crawl_playstore`)로 나뉘어 따로 실패하고 따로 재시도한다
(#127). `load_reviews`는 `trigger_rule="all_done"`이라 한 스토어가 재시도 끝에 실패해도 다른 스토어의
배치를 적재하고, leaf task `crawl_watcher`(`one_failed`)가 실패해 DAG 실행은 실패로 남는다. Airflow는
DAG 실행 상태를 leaf task로만 정하므로 이 task가 없으면 뒤 단계가 성공했을 때 크롤 실패가 묻힌다.

검증 task는 target date(`{{ ds }}`) 기준으로 기존 DB 테이블을 조회한다. 1차 범위에서는
신규 모니터링 테이블, Slack/Email/PagerDuty, live App Store/Play Store API gate,
Metabase/Grafana dashboard 구현을 추가하지 않는다.

## Warning vs failure policy

| 판정 | 의미 | Airflow 결과 |
|---|---|---|
| 크롤 앱 warning (`feed_empty`, `gap`, `error`) | 앱 하나가 리뷰 0건이거나, 빈 쪽 뒤에서 리뷰를 읽었거나, 요청이 실패함 | 그 스토어 task 성공, task 로그의 WARNING과 `RunResult.validations[store].warnings`로 확인 |
| App Store 크롤 failure | 크롤러 예외, 또는 리뷰를 하나도 못 읽은 앱이 `appstore.max_consecutive_empty_feeds`개 연속(남은 앱은 조회 안 함, 읽은 리뷰는 업로드) | `crawl_appstore` 실패(재시도 후), `crawl_watcher` 실패, DAG 실패. 적재는 진행 |
| Play 크롤 failure | 크롤러 예외, 또는 모든 앱이 리뷰 0건 | `crawl_playstore` 실패(재시도 후), `crawl_watcher` 실패, DAG 실패. 적재는 진행 |
| `fresh_ingestion` warning | target date 신규 batch/review count가 0건 | 성공 가능, report 확인 |
| `batch_state` failure | `PENDING`, `FAILED`, `RETRYING`, `DEAD_LETTER` batch가 남음 | task 실패 |
| `review_state` failure | `RAW`, `CLEANED`, retry-exhausted `FAILED` review가 남음 (`SKIPPED` 는 종결 상태라 `skipped_count` 로 보고만 함) | task 실패 |
| `metadata_mapping` failure | `service_id` 누락, 또는 리뷰 날짜가 `valid_from`~`valid_to` 안에 드는 `app_metadata` mapping 누락(`is_active`는 보지 않음) | task 실패 |
| `orphan_integrity` failure | analyzed row의 action/app review/preprocessed/aspect 연결 누락 | task 실패 |
| `mart_freshness` failure | analyzed upstream이 있는데 target-date mart row가 없음 | task 실패 |
| `mart_count_consistency` failure | `fact_service_review_daily.total_review_cnt`와 upstream count 불일치 | task 실패 |
| `mart_quality` failure | mart count/rating/ratio/sentiment/confidence 범위 위반 | task 실패 |

## Operator commands

Airflow task와 동일한 검증은 아래 명령으로 재현한다.

```bash
PYTHONPATH=. uv run python -m src.pipeline.cli \
  --steps post_aggregate_validate \
  --target-date YYYY-MM-DD
```

스토어 하나의 크롤 task는 아래 명령으로 재현한다. 마지막 줄에 `RunResult` JSON을 출력하고, 실패면
exit 1이다.

```bash
PYTHONPATH=. uv run python scripts/crawl_reviews.py --store appstore
```

집계까지 포함한 로컬 검증은 아래 순서로 실행한다.

```bash
PYTHONPATH=. uv run python -m src.pipeline.cli \
  --steps aggregate,post_aggregate_validate \
  --target-date YYYY-MM-DD
```

## Interpreting validation output

CLI 로그의 `Result` JSON에서 다음 필드를 확인한다.

| 필드 | 해석 |
|---|---|
| `status` | `success`면 필수 DB 검증 통과, `failed`면 Airflow task non-zero 대상 |
| `warnings` | 실패는 아니지만 운영자가 확인해야 할 report성 신호 |
| `checks[].severity` | `failure`는 task 실패 기준, `warning`은 report 기준 |
| `checks[].metrics` | SQL count 기반 원인 파악용 수치 |

## Verification commands

```bash
PYTHONPATH=. uv run pytest \
  tests/test_post_aggregate_validation.py \
  tests/test_pipeline_cli_validation.py \
  tests/test_airflow_dag_validation_wiring.py \
  tests/test_airflow_readiness_docs.py \
  tests/test_crawl_outcome.py \
  tests/test_crawl_reviews_cli.py

PYTHONPATH=. uv run pytest \
  tests/test_backend_datamart_contract.py \
  tests/test_backend_datamart_serving_readiness.py \
  tests/test_pipeline_steps.py
```

DB 테스트는 실제 PostgreSQL test DB가 필요하다. 기본 연결은 `TEST_DATABASE_URL` 또는
`TEST_POSTGRES_PORT`/`docker-compose.test.yml` 정책을 따른다.

## Follow-up: Metabase/Grafana

Metabase/Grafana readiness metrics dashboard는 1차 구현 범위가 아니다. 별도 이슈로
다음 항목을 등록한다.

- daily ingestion count trend
- warning/failure check trend
- mart freshness and row-count trend
- service/platform breakdown for failed checks
