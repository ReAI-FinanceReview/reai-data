"""Play review client (Issue #133) and PlayStoreCrawler over real HTTP.

``play_reviews_server`` (conftest) is a real HTTP server on 127.0.0.1 that answers the
batchexecute POST in the shape captured from the live store on 2026-11-28: the ``)]}'`` prefix,
a ``wrb.fr`` entry whose payload holds the review arrays at [0] and the next-page token at
[-2][-1], and no token on the last page. Review text and names are synthetic. No request reaches
the live store and nothing is mocked; the crawler runs against the real test database, and the
Bronze test writes to a real MinIO bucket.
"""
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml
from sqlalchemy import text

from src.crawlers.crawl_outcome import OUTCOME_ERROR, OUTCOME_FEED_EMPTY, OUTCOME_OK
from src.crawlers.playstore_crawler import PlayStoreCrawler
from src.crawlers.playstore_reviews import (
    PlayPage,
    PlayReviewsError,
    fetch_reviews_page,
    parse_reviews_response,
    play_reviews_settings,
)
from tests.conftest import PLAY_REVIEW_AT, PlayAnswer, PlayReviewsServer

ROOT = Path(__file__).resolve().parents[1]
CONFIG = yaml.safe_load((ROOT / 'config' / 'crawler_config.yml').read_text(encoding='utf-8'))
REVIEWS_PER_APP = CONFIG['playstore']['reviews_per_app']
APP = 'com.example.bank'
RPC_ID = CONFIG['playstore']['reviews_rpc_id']


@pytest.fixture
def settings(play_reviews_server):
    """The configured client settings, pointed at play_reviews_server and retrying without waiting."""
    return replace(
        play_reviews_settings(CONFIG), url_template=play_reviews_server.url_template, retry_backoff_seconds=0
    )


def _items(play_review_item, prefix, count):
    return [play_review_item(f'{prefix}-{i}', '7.0.0', at=PLAY_REVIEW_AT - i) for i in range(count)]


# ========================================
# A. PAGES: first, next, last, empty
# ========================================

def test_first_page_reads_the_newest_reviews_and_a_next_page_token(play_reviews_server, settings, play_review_item):
    play_reviews_server.set_reviews(APP, _items(play_review_item, 'r', 3))

    page = fetch_reviews_page(APP, 2, settings)

    assert [r['reviewId'] for r in page.reviews] == ['r-0', 'r-1']
    assert page.next_token is not None
    [request] = play_reviews_server.requests
    assert (request['app_id'], request['count'], request['token']) == (APP, 2, None)
    assert (request['rpc_id'], request['sort']) == (settings.rpc_id, 2)  # 2: newest first
    assert request['query'] == {'hl': [settings.language], 'gl': [settings.country]}


def test_next_page_follows_the_token_to_a_last_page_without_one(play_reviews_server, settings, play_review_item):
    play_reviews_server.set_reviews(APP, _items(play_review_item, 'r', 5))

    first = fetch_reviews_page(APP, 2, settings)
    second = fetch_reviews_page(APP, 2, settings, first.next_token)
    last = fetch_reviews_page(APP, 2, settings, second.next_token)

    assert [r['reviewId'] for r in second.reviews] == ['r-2', 'r-3']
    assert [r['reviewId'] for r in last.reviews] == ['r-4']
    assert last.next_token is None
    assert play_reviews_server.requests_for(APP) == [(2, None), (2, first.next_token), (2, second.next_token)]


def test_an_app_without_reviews_is_an_empty_page_without_a_token(play_reviews_server, settings):
    """The live store answers an app id it does not know the same way."""
    assert fetch_reviews_page(APP, 10, settings) == PlayPage([], None)


# ========================================
# B. FAILURES: raised, never an empty page
# ========================================

def test_a_client_error_status_raises_at_once(play_reviews_server, settings, play_review_item):
    play_reviews_server.set_reviews(APP, _items(play_review_item, 'r', 3))
    play_reviews_server.push(APP, PlayAnswer(status=404))

    with pytest.raises(PlayReviewsError, match='HTTP 404'):
        fetch_reviews_page(APP, 10, settings)
    assert len(play_reviews_server.requests_for(APP)) == 1


def test_a_server_error_is_retried_and_raised_once_retries_run_out(play_reviews_server, settings, play_review_item):
    play_reviews_server.set_reviews(APP, _items(play_review_item, 'r', 3))
    attempts = settings.max_retries + 1
    play_reviews_server.push(APP, *[PlayAnswer(status=500)] * attempts)

    with pytest.raises(PlayReviewsError, match=f'{attempts} attempts failed, last: HTTP 500'):
        fetch_reviews_page(APP, 10, settings)
    assert len(play_reviews_server.requests_for(APP)) == attempts

    play_reviews_server.push(APP, PlayAnswer(status=503))  # one failure, then the page
    assert len(fetch_reviews_page(APP, 10, settings).reviews) == 3


def test_a_timeout_raises(play_reviews_server, settings, play_review_item):
    play_reviews_server.set_reviews(APP, _items(play_review_item, 'r', 3))
    quick = replace(settings, timeout_seconds=0.2, max_retries=1)
    play_reviews_server.push(APP, PlayAnswer(delay=5), PlayAnswer(delay=5))

    started = time.monotonic()
    with pytest.raises(PlayReviewsError, match='Timeout'):
        fetch_reviews_page(APP, 10, quick)

    assert time.monotonic() - started < 5
    assert len(play_reviews_server.requests_for(APP)) == 2


def test_play_gateway_error_is_retried_then_the_page_is_read(play_reviews_server, settings, play_review_item):
    play_reviews_server.set_reviews(APP, _items(play_review_item, 'r', 3))
    play_reviews_server.push(APP, PlayAnswer(body=PlayReviewsServer.gateway_error_body(RPC_ID)))

    page = fetch_reviews_page(APP, 10, settings)

    assert len(page.reviews) == 3
    assert len(play_reviews_server.requests_for(APP)) == 2


def test_play_gateway_error_on_every_attempt_raises(play_reviews_server, settings):
    attempts = settings.max_retries + 1
    play_reviews_server.push(APP, *[PlayAnswer(body=PlayReviewsServer.gateway_error_body(RPC_ID))] * attempts)

    with pytest.raises(PlayReviewsError, match='last: PlayGatewayError'):
        fetch_reviews_page(APP, 10, settings)
    assert len(play_reviews_server.requests_for(APP)) == attempts


@pytest.mark.parametrize('body', [
    'not a batchexecute answer',
    PlayReviewsServer.PREFIX + '[["wrb.fr", "oCPfdb", ',                                        # cut short
    PlayReviewsServer.PREFIX + '[["di",48],["af.httprm",47,"-7539252846892424692",28]]',         # no wrb.fr entry
    PlayReviewsServer.envelope('someOtherRpc', PlayReviewsServer.page_payload([], None)),        # another RPC
    PlayReviewsServer.envelope(RPC_ID, None, [3]),                       # what the live store sends for a bad token
    PlayReviewsServer.envelope(RPC_ID, 'not json'),
    PlayReviewsServer.envelope(RPC_ID, '[]'),
    PlayReviewsServer.envelope(RPC_ID, '[{"reviews": 1}, [null, ["token"]], null]'),
])
def test_a_malformed_response_raises(play_reviews_server, settings, body):
    play_reviews_server.push(APP, PlayAnswer(body=body))

    with pytest.raises(PlayReviewsError):
        fetch_reviews_page(APP, 10, settings)
    assert len(play_reviews_server.requests_for(APP)) == 1  # not retried


def test_a_bad_continuation_token_raises_instead_of_ending_the_feed(play_reviews_server, settings, play_review_item):
    play_reviews_server.set_reviews(APP, _items(play_review_item, 'r', 3))

    with pytest.raises(PlayReviewsError, match='without a payload'):
        fetch_reviews_page(APP, 10, settings, 'no-such-token')


# ========================================
# C. FIELDS
# ========================================

def test_review_fields_with_reply_thumbs_and_version(play_review_item):
    item = play_review_item(
        'b4c0ffee-0000-4000-8000-000000000001', '7.12.0', at=PLAY_REVIEW_AT, score=2,
        content='이체 화면이 멈춰요', user_name='리뷰어', thumbs_up=7,
        reply='불편을 드려 죄송합니다', replied_at=PLAY_REVIEW_AT + 3600,
    )

    page = parse_reviews_response(
        PlayReviewsServer.envelope(RPC_ID, PlayReviewsServer.page_payload([item], None)), RPC_ID
    )

    assert page.reviews == [{
        'reviewId': 'b4c0ffee-0000-4000-8000-000000000001',
        'userName': '리뷰어',
        'score': 2,
        'content': '이체 화면이 멈춰요',
        'at': datetime.fromtimestamp(PLAY_REVIEW_AT),
        'thumbsUpCount': 7,
        'replyContent': '불편을 드려 죄송합니다',
        'repliedAt': datetime.fromtimestamp(PLAY_REVIEW_AT + 3600),
        'reviewCreatedVersion': '7.12.0',
    }]
    assert page.reviews[0]['at'].tzinfo is None  # naive local time, as google-play-scraper built it


def test_review_fields_without_reply_or_version(play_review_item):
    item = play_review_item('b4c0ffee-0000-4000-8000-000000000002', None)

    [review] = parse_reviews_response(
        PlayReviewsServer.envelope(RPC_ID, PlayReviewsServer.page_payload([item], None)), RPC_ID
    ).reviews

    assert (review['reviewCreatedVersion'], review['replyContent'], review['repliedAt']) == (None, None, None)
    assert review['thumbsUpCount'] == 0
    assert PlayStoreCrawler._extract_platform_review_id(review) == 'b4c0ffee-0000-4000-8000-000000000002'
    assert PlayStoreCrawler._extract_app_version(review) is None


def test_a_short_review_array_reads_missing_positions_as_none():
    [review] = parse_reviews_response(
        PlayReviewsServer.envelope(RPC_ID, PlayReviewsServer.page_payload([['only-an-id']], None)), RPC_ID
    ).reviews

    assert review['reviewId'] == 'only-an-id'
    assert {key for key, value in review.items() if value is not None} == {'reviewId'}


# ========================================
# E. PlayStoreCrawler.run OVER HTTP
# ========================================

def _set_play_config(crawl_config, **values):
    crawl_config.config['playstore'].update(values)
    Path(crawl_config.path).write_text(yaml.safe_dump(crawl_config.config, allow_unicode=True), encoding='utf-8')


@pytest.mark.requires_db
def test_every_app_with_zero_reviews_fails_the_store(crawl_config, play_reviews_server):
    crawl_config.set_app_ids('playstore', 'com.example.empty1', 'com.example.empty2')

    summary = PlayStoreCrawler(crawl_config.path).run()

    assert summary.failure == 'Play Store: 앱 2개 모두 리뷰 0건'
    assert [(app.outcome, app.fetched) for app in summary.apps] == [(OUTCOME_FEED_EMPTY, 0)] * 2


@pytest.mark.requires_db
def test_one_failed_app_is_a_warning(crawl_config, play_reviews_server, play_review_item):
    play_reviews_server.set_reviews('com.example.ok', _items(play_review_item, 'ok', REVIEWS_PER_APP))
    play_reviews_server.push('com.example.down', PlayAnswer(status=404))
    crawl_config.set_app_ids('playstore', 'com.example.ok', 'com.example.down')

    summary = PlayStoreCrawler(crawl_config.path).run()

    assert summary.failure is None
    ok, down = summary.apps
    assert (ok.outcome, ok.fetched) == (OUTCOME_OK, REVIEWS_PER_APP)
    assert (down.outcome, down.fetched) == (OUTCOME_ERROR, 0)
    assert down.detail.startswith('PlayReviewsError(') and 'HTTP 404' in down.detail
    assert summary.warnings == [down]


@pytest.mark.requires_db
def test_every_app_failing_fails_the_store(crawl_config, play_reviews_server):
    attempts = crawl_config.config['playstore']['max_retries'] + 1
    for app_id in ('com.example.down1', 'com.example.down2'):
        play_reviews_server.push(app_id, *[PlayAnswer(status=500)] * attempts)
    crawl_config.set_app_ids('playstore', 'com.example.down1', 'com.example.down2')

    summary = PlayStoreCrawler(crawl_config.path).run()

    assert summary.failure == 'Play Store: 앱 2개 모두 리뷰 0건'
    assert [(app.outcome, app.fetched) for app in summary.apps] == [(OUTCOME_ERROR, 0)] * 2


@pytest.mark.requires_db
def test_crawler_pages_with_tokens_up_to_reviews_per_app(crawl_config, play_reviews_server, play_review_item):
    page_size = 4
    _set_play_config(crawl_config, page_size=page_size)
    play_reviews_server.set_reviews('com.example.big', _items(play_review_item, 'big', REVIEWS_PER_APP + 5))
    play_reviews_server.set_reviews('com.example.small', _items(play_review_item, 'small', page_size + 2))
    crawl_config.set_app_ids('playstore', 'com.example.big', 'com.example.small')

    big, small = PlayStoreCrawler(crawl_config.path).run().apps

    assert (big.fetched, small.fetched) == (REVIEWS_PER_APP, page_size + 2)
    counts = [count for count, _ in play_reviews_server.requests_for('com.example.big')]
    assert counts == [min(page_size, REVIEWS_PER_APP - got) for got in range(0, REVIEWS_PER_APP, page_size)]
    tokens = [token for _, token in play_reviews_server.requests_for('com.example.small')]
    assert tokens[0] is None and tokens[1] is not None and len(tokens) == 2  # the second page had no token


@pytest.mark.requires_db
def test_crawled_reviews_reach_bronze_and_apps_keep_their_catalog_name(
    crawl_config, play_reviews_server, play_review_item, minio_bucket, test_db_engine, monkeypatch
):
    from uuid6 import uuid7

    from src.utils.minio_client import MinIOClient

    monkeypatch.setenv('ENABLE_PARQUET_WRITE', 'true')
    known, new = 'com.example.known', 'com.example.new'
    with test_db_engine.begin() as conn:
        conn.execute(
            text("INSERT INTO apps (app_id, platform_app_id, platform_type, name) "
                 "VALUES (:app_id, :platform_app_id, 'PLAYSTORE', '테스트 뱅킹')"),
            {'app_id': uuid7(), 'platform_app_id': known},
        )
    play_reviews_server.set_reviews(known, [
        play_review_item('known-1', '7.12.0', score=5, content='빨라졌어요', user_name='리뷰어1',
                         reply='감사합니다', replied_at=PLAY_REVIEW_AT + 60),
        play_review_item('known-2', None, score=1, content='버전 정보가 없는 리뷰'),
        play_review_item('known-3', '7.12.0', content=''),  # no text: not written
    ])
    play_reviews_server.set_reviews(new, [play_review_item('new-1', '1.0.0')])
    crawl_config.set_app_ids('playstore', known, new)
    storage_path = None
    try:
        summary = PlayStoreCrawler(crawl_config.path).run()
        storage_path = summary.storage_path

        assert summary.failure is None and storage_path is not None
        rows = {row['platform_review_id']: row for row in MinIOClient().get_parquet(storage_path).to_pylist()}
        assert set(rows) == {'known-1', 'known-2', 'new-1'}
        assert {key: rows['known-1'][key] for key in (
            'platform_type', 'reviewer_name', 'review_text', 'rating', 'reply_comment', 'app_version'
        )} == {
            'platform_type': 'PLAYSTORE', 'reviewer_name': '리뷰어1', 'review_text': '빨라졌어요',
            'rating': 5, 'reply_comment': '감사합니다', 'app_version': '7.12.0',
        }
        assert (rows['known-2']['rating'], rows['known-2']['app_version']) == (1, None)
        # PlayStoreCrawler._parse_reviewed_at labels the naive local `at` as UTC.
        expected_at = datetime.fromtimestamp(PLAY_REVIEW_AT).replace(tzinfo=timezone.utc)
        assert rows['known-1']['reviewed_at'].astimezone(timezone.utc) == expected_at
        with test_db_engine.connect() as conn:
            names = dict(conn.execute(
                text("SELECT platform_app_id, name FROM apps "
                     "WHERE platform_type = 'PLAYSTORE' AND platform_app_id = ANY(:ids)"),
                {'ids': [known, new]},
            ).all())
        assert names == {known: '테스트 뱅킹', new: f'app_{new}'}
    finally:
        with test_db_engine.begin() as conn:
            conn.execute(text('DELETE FROM ingestion_batch WHERE storage_path = :path'), {'path': storage_path})
            conn.execute(
                text("DELETE FROM apps WHERE platform_type = 'PLAYSTORE' AND platform_app_id = ANY(:ids)"),
                {'ids': [known, new]},
            )
