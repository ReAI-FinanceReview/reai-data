"""Store-level crawl outcomes and the crawl step (Issue #127).

The App Store runs over real HTTP against ``appstore_rss_server``. Play's decision, every app
empty, is tested here as the pure ``playstore_failure`` over results built from Play review dicts
(``PlayStoreCrawler.run`` over ``play_reviews_server`` is in ``tests/test_playstore_reviews.py``),
and in the step tests its app-id file is left empty so ``PlayStoreCrawler.run`` raises before any
request. The database is the real test database;
the re-run test also writes to a real MinIO bucket (``minio_bucket``), loads with the real
``BatchLoader`` and cleanses with the real ``ReviewCleaningPipeline``.
"""
from datetime import datetime
from pathlib import Path

import pytest
import yaml
from sqlalchemy import text

from src.crawlers.crawl_outcome import (
    OUTCOME_ERROR,
    OUTCOME_FEED_EMPTY,
    OUTCOME_OK,
    AppCrawlResult,
    StoreCrawlSummary,
    playstore_failure,
)
from src.pipeline import cli
from src.pipeline.steps import run_crawl, run_steps

CONFIG = yaml.safe_load(
    (Path(__file__).resolve().parents[1] / 'config' / 'crawler_config.yml').read_text(encoding='utf-8')
)
PAGE_SIZE = CONFIG['appstore']['rss_page_size']
MAX_REVIEWS = CONFIG['appstore']['max_reviews_per_app']
CUTOFF = CONFIG['appstore']['max_consecutive_empty_feeds']
NO_APP_IDS = 'ValueError: 앱 ID 파일에서 유효한 ID를 찾을 수 없습니다'


def _play_review(review_id):
    """One review dict as playstore_reviews.review_from_item returns it."""
    return {
        'reviewId': review_id,
        'userName': '사용자',
        'score': 5,
        'content': '업데이트 후 로그인이 빨라졌어요',
        'at': datetime(2026, 11, 25, 9, 0),
        'thumbsUpCount': 0,
        'replyContent': None,
        'repliedAt': None,
        'reviewCreatedVersion': '2.3.1',
    }


def _play_result(app_id, reviews):
    """The per-app result PlayStoreCrawler.run records for the reviews the Play client returned."""
    return AppCrawlResult(app_id, OUTCOME_OK if reviews else OUTCOME_FEED_EMPTY, fetched=len(reviews))


def _page(rss_review_entry, prefix, count=PAGE_SIZE):
    return [rss_review_entry(f'{prefix}-{i}', '7.0.0') for i in range(count)]


# ========================================
# A. PLAY: every app empty fails the store, one empty app does not
# ========================================

def test_play_fails_only_when_every_app_returned_zero_reviews():
    reviews = [_play_review(f'0a1b-{i}') for i in range(3)]

    assert playstore_failure([_play_result('com.kbstar.kbbank', []), _play_result('com.shinhan.sbanking', [])]) == (
        'Play Store: 앱 2개 모두 리뷰 0건'
    )
    assert playstore_failure([_play_result('com.kbstar.kbbank', []), _play_result('com.shinhan.sbanking', reviews)]) is None
    assert playstore_failure([]) is None


def test_play_app_that_failed_before_any_review_counts_as_zero():
    failed = AppCrawlResult('com.kbstar.kbbank', OUTCOME_ERROR, detail="NotFoundError('App not found(404).')")
    failed_after_reading = AppCrawlResult('com.shinhan.sbanking', OUTCOME_ERROR, fetched=3, detail='OperationalError()')

    assert playstore_failure([failed, _play_result('com.wooribank.smart.npib', [])]) is not None
    assert playstore_failure([failed, failed_after_reading]) is None


def test_summary_lists_every_app_and_warns_about_the_others():
    summary = StoreCrawlSummary(
        store='playstore',
        apps=[
            AppCrawlResult('com.kbstar.kbbank', OUTCOME_OK, fetched=10, new=4),
            AppCrawlResult('com.kebhana.hanapush', OUTCOME_FEED_EMPTY),
        ],
    )

    report = summary.as_dict()

    assert (report['status'], report['fetched'], report['new']) == ('success', 10, 4)
    assert report['warnings'] == ['com.kebhana.hanapush: feed_empty']
    assert [app['app_id'] for app in report['apps']] == ['com.kbstar.kbbank', 'com.kebhana.hanapush']


# ========================================
# B. THE CRAWL STEP: store selection and per-store failure
# ========================================

@pytest.mark.requires_db
def test_a_store_whose_crawler_raises_fails_its_step(crawl_config):
    result = run_crawl(crawl_config.path, store='appstore')

    assert result.status == 'failed'
    assert result.message.startswith(f'crawl failed: appstore: {NO_APP_IDS}')
    assert list(result.validations) == ['appstore']


@pytest.mark.requires_db
def test_all_fails_naming_the_failed_store_after_running_the_other(
    crawl_config, appstore_rss_server, rss_review_entry
):
    appstore_rss_server.set_pages('901', _page(rss_review_entry, 'r901'))
    crawl_config.set_app_ids('appstore', '901')

    result = run_crawl(crawl_config.path, store='all')

    assert result.status == 'failed'
    assert result.message.startswith(f'crawl failed: playstore: {NO_APP_IDS}')
    assert 'appstore' not in result.message
    assert result.validations['appstore']['status'] == 'success'
    assert result.validations['playstore']['status'] == 'failed'
    assert appstore_rss_server.requested_pages('901') == [1]


@pytest.mark.requires_db
def test_all_runs_play_after_the_app_store_crawler_raised(crawl_config):
    result = run_crawl(crawl_config.path, store='all')

    assert result.status == 'failed'
    assert list(result.validations) == ['appstore', 'playstore']
    appstore_failure, playstore_failure_ = result.message.removeprefix('crawl failed: ').split('; ')
    assert appstore_failure.startswith(f'appstore: {NO_APP_IDS}: ')
    assert playstore_failure_.startswith(f'playstore: {NO_APP_IDS}: ')
    assert playstore_failure_.endswith(str(crawl_config.app_id_files['playstore']))


def test_unknown_store_fails_without_crawling():
    result = run_crawl(store='unified')

    assert (result.status, result.validations) == ('failed', None)
    assert result.message == "unknown store: 'unified' (choose from appstore, playstore, all)"


@pytest.mark.requires_db
def test_pipeline_cli_crawl_step_crawls_both_stores(crawl_config, appstore_rss_server, rss_review_entry):
    appstore_rss_server.set_pages('911', _page(rss_review_entry, 'r911'))
    crawl_config.set_app_ids('appstore', '911')

    [result] = run_steps(['crawl'], config_path=crawl_config.path)

    assert (result.step, result.status) == ('crawl', 'failed')
    assert list(result.validations) == ['appstore', 'playstore']
    assert result.message.startswith(f'crawl failed: playstore: {NO_APP_IDS}')
    assert cli.main(['--steps', 'crawl', '--config', crawl_config.path]) == 1
    assert appstore_rss_server.requested_pages('911') == [1, 1]


# ========================================
# C. SAME-DAY RE-RUN: an Airflow retry after the empty-feed cutoff (MinIO + BatchLoader)
# ========================================

def _delete_crawled_rows(engine, platform_app_ids, storage_paths):
    with engine.begin() as conn:
        app_ids = list(conn.execute(
            text("SELECT app_id FROM apps WHERE platform_type = 'APPSTORE' AND platform_app_id = ANY(:ids)"),
            {'ids': platform_app_ids},
        ).scalars())
        conn.execute(text('DELETE FROM app_reviews WHERE app_id = ANY(:ids)'), {'ids': app_ids})
        conn.execute(
            text('DELETE FROM reviews_preprocessed WHERE platform_review_id IN '
                 '(SELECT platform_review_id FROM review_master_index WHERE app_id = ANY(:ids))'),
            {'ids': app_ids},
        )
        conn.execute(text('DELETE FROM review_master_index WHERE app_id = ANY(:ids)'), {'ids': app_ids})
        conn.execute(text('DELETE FROM ingestion_batch WHERE storage_path = ANY(:paths)'), {'paths': storage_paths})
        conn.execute(text('DELETE FROM apps WHERE app_id = ANY(:ids)'), {'ids': app_ids})


@pytest.mark.requires_db
def test_retried_crawl_after_the_cutoff_persists_both_runs_and_loads_each_review_once(
    crawl_config, appstore_rss_server, rss_review_entry, minio_bucket, test_db_engine, monkeypatch
):
    """Run 1 hits the cutoff but still uploads what it read; the retry, the same day and before any
    load, re-reads those reviews into a second object under new review_ids; the loader stores each
    review once, and the cleanse of that day's Bronze partition keeps only the loaded copy."""
    from src.loaders.batch_loader import BatchLoader
    from src.processing.cleanse import PROFANITY_PATH, SYNONYMS_PATH, ReviewCleaningPipeline
    from src.utils.db_connector import DatabaseConnector
    from src.utils.minio_client import MinIOClient

    s3, bucket = minio_bucket
    monkeypatch.setenv('ENABLE_PARQUET_WRITE', 'true')
    first, last = '8100001', '8100099'
    empty = [f'810001{i}' for i in range(CUTOFF)]
    store_ids = [first, *empty, last]
    appstore_rss_server.set_pages(first, _page(rss_review_entry, f'rerun-{first}'))
    crawl_config.set_app_ids('appstore', *store_ids)
    storage_paths = []
    try:
        run_1 = run_crawl(crawl_config.path, store='appstore')
        storage_paths.append(run_1.validations['appstore']['storage_path'])
        assert run_1.status == 'failed' and 'App Store RSS' in run_1.message
        assert (run_1.output_count, storage_paths[0] is not None) == (MAX_REVIEWS, True)
        assert appstore_rss_server.requested_pages(last) == []

        for store_id in [*empty, last]:  # the RSS has recovered by the retry
            appstore_rss_server.set_pages(store_id, _page(rss_review_entry, f'rerun-{store_id}'))
        run_2 = run_crawl(crawl_config.path, store='appstore')
        storage_paths.append(run_2.validations['appstore']['storage_path'])
        assert run_2.status == 'success', run_2.message
        assert run_2.output_count == MAX_REVIEWS * len(store_ids)  # the first app's reviews again

        # Two objects in the same day partition, told apart by the timestamp and microseconds.
        assert storage_paths[0] != storage_paths[1]
        assert Path(storage_paths[0]).parent == Path(storage_paths[1]).parent
        keys = {obj['Key'] for obj in s3.list_objects_v2(Bucket=bucket)['Contents']}
        assert keys == set(storage_paths)
        with test_db_engine.connect() as conn:
            batches = conn.execute(
                text('SELECT storage_path, status::text, record_count FROM ingestion_batch '
                     'WHERE storage_path = ANY(:paths) ORDER BY created_at'),
                {'paths': storage_paths},
            ).all()
        assert [tuple(b) for b in batches] == [
            (storage_paths[0], 'PENDING', MAX_REVIEWS),
            (storage_paths[1], 'PENDING', MAX_REVIEWS * len(store_ids)),
        ]

        assert BatchLoader(crawl_config.path).load_pending_batches() == 2

        with test_db_engine.connect() as conn:
            loaded = conn.execute(
                text("SELECT r.platform_review_id FROM review_master_index r JOIN apps a ON a.app_id = r.app_id "
                     "WHERE a.platform_type = 'APPSTORE' AND a.platform_app_id = ANY(:ids)"),
                {'ids': store_ids},
            ).scalars().all()
            app_reviews = conn.execute(
                text("SELECT count(*) FROM app_reviews r JOIN apps a ON a.app_id = r.app_id "
                     "WHERE a.platform_type = 'APPSTORE' AND a.platform_app_id = ANY(:ids)"),
                {'ids': store_ids},
            ).scalar_one()
            statuses = conn.execute(
                text('SELECT status::text FROM ingestion_batch WHERE storage_path = ANY(:paths)'),
                {'paths': storage_paths},
            ).scalars().all()
        assert len(loaded) == len(set(loaded)) == MAX_REVIEWS * len(store_ids) == app_reviews
        assert statuses == ['LOADED', 'LOADED']

        run_3 = run_crawl(crawl_config.path, store='appstore')  # after the load nothing is new
        assert (run_3.status, run_3.output_count) == ('success', 0)
        assert run_3.validations['appstore']['storage_path'] is None

        # The cleanse of that Bronze partition reads both objects: the first app's reviews twice.
        bronze_date = datetime.strptime(  # bronze/app_reviews/year=YYYY/month=MM/day=DD/<object>
            '/'.join(Path(storage_paths[0]).parts[2:5]), 'year=%Y/month=%m/day=%d'
        ).date()
        minio = MinIOClient()
        result = ReviewCleaningPipeline(
            minio_client=minio, db_connector=DatabaseConnector(crawl_config.path),
            synonyms_path=SYNONYMS_PATH, profanity_path=PROFANITY_PATH,
        ).run(target_date=bronze_date)

        assert (result['processed'], result['skipped'], result['duplicates_dropped']) == (
            MAX_REVIEWS * len(store_ids), 0, MAX_REVIEWS,
        )
        with test_db_engine.connect() as conn:
            master = conn.execute(
                text("SELECT r.platform_review_id, r.review_id::text, r.processing_status::text "
                     "FROM review_master_index r JOIN apps a ON a.app_id = r.app_id "
                     "WHERE a.platform_type = 'APPSTORE' AND a.platform_app_id = ANY(:ids)"),
                {'ids': store_ids},
            ).all()
            preprocessed = conn.execute(
                text('SELECT platform_review_id, review_id::text FROM reviews_preprocessed '
                     'WHERE platform_review_id = ANY(:pids)'),
                {'pids': [row[0] for row in master]},
            ).all()
        loaded_review_ids = {platform_review_id: review_id for platform_review_id, review_id, _ in master}
        assert {status for _, _, status in master} == {'CLEANED'}
        assert len(preprocessed) == len(loaded_review_ids) == MAX_REVIEWS * len(store_ids)
        assert dict(preprocessed) == loaded_review_ids
        silver_keys = {obj['Key'] for obj in s3.list_objects_v2(Bucket=bucket)['Contents']} - set(storage_paths)
        silver = [
            (row['platform_review_id'], row['review_id'])
            for key in silver_keys
            for row in minio.get_parquet(key).to_pylist()
        ]
        assert sorted(silver) == sorted(loaded_review_ids.items())
    finally:
        _delete_crawled_rows(test_db_engine, store_ids, [p for p in storage_paths if p])
