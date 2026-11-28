"""Play review client (Issue #133) and PlayStoreCrawler over real HTTP.

``play_reviews_server`` (conftest) is a real HTTP server on 127.0.0.1 that answers the
batchexecute POST in the shape captured from the live store on 2026-11-28: the ``)]}'`` prefix,
a ``wrb.fr`` entry whose payload holds the review arrays at [0] and the next-page token at
[-2][-1], and no token on the last page. Review text and names are synthetic. No request reaches
the live store and nothing is mocked; the crawler runs against the real test database, and the
Bronze test writes to a real MinIO bucket.
"""
import json
import subprocess
import sys
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
    GATEWAY_ERROR_MARKER,
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
REQUIRED_ONLY_ITEM = ['only-required', None, 4, None, None, [PLAY_REVIEW_AT]]  # reviewId [0], score [2], at [5][0]


@pytest.fixture
def settings(play_reviews_server):
    """The configured client settings, pointed at play_reviews_server and retrying without waiting."""
    return replace(
        play_reviews_settings(CONFIG), url_template=play_reviews_server.url_template, retry_backoff_seconds=0
    )


def _items(play_review_item, prefix, count):
    return [play_review_item(f'{prefix}-{i}', '7.0.0', at=PLAY_REVIEW_AT - i) for i in range(count)]


def _page_body(items, tail):
    """A response body whose payload holds ``items`` at [0] and ``tail`` in the token slot [-2]."""
    return PlayReviewsServer.envelope(RPC_ID, json.dumps([items, tail, None]))


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


@pytest.mark.parametrize('drop, error', [
    (PlayAnswer.BEFORE_ANSWER, 'ConnectionError'),
    (PlayAnswer.MID_BODY, 'ChunkedEncodingError'),
])
def test_a_dropped_connection_is_retried_and_raised_once_retries_run_out(
    play_reviews_server, settings, play_review_item, drop, error
):
    play_reviews_server.set_reviews(APP, _items(play_review_item, 'r', 3))
    attempts = settings.max_retries + 1
    play_reviews_server.push(APP, *[PlayAnswer(drop=drop)] * attempts)

    with pytest.raises(PlayReviewsError, match=f'{attempts} attempts failed, last: {error}'):
        fetch_reviews_page(APP, 10, settings)
    assert len(play_reviews_server.requests_for(APP)) == attempts

    play_reviews_server.push(APP, PlayAnswer(drop=drop))  # one drop, then the page
    assert len(fetch_reviews_page(APP, 10, settings).reviews) == 3


def test_a_tls_failure_raises_at_once(play_reviews_server, settings):
    """https:// to the plain-HTTP server fails the handshake, so no certificate is needed."""
    https = replace(settings, url_template=settings.url_template.replace('http://', 'https://', 1))

    with pytest.raises(PlayReviewsError, match='^TLS handshake or certificate verification failed'):
        fetch_reviews_page(APP, 10, https)
    assert play_reviews_server.connections == 1  # not retried


def test_play_gateway_error_is_retried_then_the_page_is_read(play_reviews_server, settings, play_review_item):
    play_reviews_server.set_reviews(APP, _items(play_review_item, 'r', 3))
    play_reviews_server.push(APP, PlayAnswer(body=PlayReviewsServer.gateway_error_body(RPC_ID)))

    page = fetch_reviews_page(APP, 10, settings)

    assert len(page.reviews) == 3
    assert len(play_reviews_server.requests_for(APP)) == 2


@pytest.mark.parametrize('body', [
    pytest.param(PlayReviewsServer.gateway_error_body(RPC_ID), id='entry_without_payload'),
    pytest.param(PlayReviewsServer.PREFIX + '[' * 100000 + GATEWAY_ERROR_MARKER, id='too_deep_to_parse'),
])
def test_play_gateway_error_on_every_attempt_raises(play_reviews_server, settings, body):
    attempts = settings.max_retries + 1
    play_reviews_server.push(APP, *[PlayAnswer(body=body)] * attempts)

    with pytest.raises(PlayReviewsError, match='last: PlayGatewayError'):
        fetch_reviews_page(APP, 10, settings)
    assert len(play_reviews_server.requests_for(APP)) == attempts


def test_a_review_quoting_the_play_gateway_error_name_is_read_at_once(
    play_reviews_server, settings, play_review_item
):
    content = f'앱을 열면 {GATEWAY_ERROR_MARKER} 라고 떠요'
    play_reviews_server.set_reviews(APP, [play_review_item('r-0', content=content)])

    [review] = fetch_reviews_page(APP, 10, settings).reviews

    assert review['content'] == content
    assert len(play_reviews_server.requests_for(APP)) == 1


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


@pytest.mark.parametrize('body, match', [
    pytest.param(
        PlayReviewsServer.PREFIX + json.dumps([['wrb.fr', RPC_ID]]), 'without a payload', id='entry_too_short'
    ),
    pytest.param(PlayReviewsServer.PREFIX + '[' * 100000, 'RecursionError', id='envelope_too_deep'),
    pytest.param(PlayReviewsServer.envelope(RPC_ID, '[' * 100000), 'RecursionError', id='payload_too_deep'),
    pytest.param(_page_body({}, None), 'reviews dict', id='reviews_empty_object'),
    pytest.param(_page_body(0, None), 'reviews int', id='reviews_zero'),
    pytest.param(_page_body([REQUIRED_ONLY_ITEM], 'abc'), 'token slot str', id='token_slot_string'),
])
def test_a_malformed_response_raises_naming_what_is_wrong(play_reviews_server, settings, body, match):
    play_reviews_server.push(APP, PlayAnswer(body=body))

    with pytest.raises(PlayReviewsError, match=match):
        fetch_reviews_page(APP, 10, settings)
    assert len(play_reviews_server.requests_for(APP)) == 1  # not retried


@pytest.mark.parametrize('prepended', [None, 'extra'])
def test_a_review_shifted_by_one_position_raises_without_its_text(
    play_reviews_server, settings, play_review_item, prepended
):
    """Every position moves, as if Play added a field in front; the error names types, never values."""
    item = play_review_item('r-0', content='오류 메시지에 나오면 안 되는 본문', user_name='나오면 안 되는 이름')
    body = PlayReviewsServer.envelope(RPC_ID, PlayReviewsServer.page_payload([[prepended] + item], None))
    play_reviews_server.push(APP, PlayAnswer(body=body))

    with pytest.raises(PlayReviewsError, match='item 0 (reviewId|score) is ') as raised:
        fetch_reviews_page(APP, 10, settings)
    assert '안 되는' not in str(raised.value)
    assert len(play_reviews_server.requests_for(APP)) == 1  # not retried


@pytest.mark.parametrize('position, value, message', [
    (0, '', 'reviewId is str'),
    (0, 7, 'reviewId is int'),
    (2, 0, 'score is int'),
    (2, 6, 'score is int'),
    (2, True, 'score is bool'),
    (2, '4', 'score is str'),
    (5, None, 'at is NoneType'),
    (5, [str(PLAY_REVIEW_AT), 0], 'at is str'),
])
def test_a_review_without_a_required_field_raises_naming_the_item_and_field(
    play_review_item, position, value, message
):
    items = [play_review_item('r-0'), play_review_item('r-1')]
    items[1][position] = value
    body = PlayReviewsServer.envelope(RPC_ID, PlayReviewsServer.page_payload(items, None))

    with pytest.raises(PlayReviewsError, match=f'item 1 {message}, expected '):
        parse_reviews_response(body, RPC_ID)


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


def test_a_short_review_array_reads_missing_optional_positions_as_none():
    [review] = parse_reviews_response(
        PlayReviewsServer.envelope(RPC_ID, PlayReviewsServer.page_payload([REQUIRED_ONLY_ITEM], None)), RPC_ID
    ).reviews

    assert (review['reviewId'], review['score']) == ('only-required', 4)
    assert {key for key, value in review.items() if value is not None} == {'reviewId', 'score', 'at'}


# ========================================
# D. NO PROCESS-WIDE TLS CHANGE
# ========================================

def test_importing_the_crawlers_keeps_the_default_tls_context():
    """google-play-scraper 1.2.7 switched it to an unverified context on import (#128)."""
    code = (
        'import ssl, sys\n'
        'before = ssl._create_default_https_context\n'
        'import src.crawlers, src.crawlers.playstore_crawler, src.crawlers.playstore_reviews\n'
        'import src.crawlers.app_version_backfill, src.crawlers.unified_crawler\n'
        'context = ssl._create_default_https_context()\n'
        'print(ssl._create_default_https_context is before, context.verify_mode == ssl.CERT_REQUIRED,\n'
        '      context.check_hostname, "google_play_scraper" in sys.modules)\n'
    )
    result = subprocess.run(
        [sys.executable, '-c', code], cwd=ROOT, capture_output=True, text=True, check=True,
        env={'PYTHONPATH': str(ROOT), 'PATH': ''},
    )

    assert result.stdout.split() == ['True', 'True', 'True', 'False']


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
def test_a_review_whose_positions_moved_makes_the_app_an_error(crawl_config, play_reviews_server, play_review_item):
    play_reviews_server.set_reviews('com.example.ok', _items(play_review_item, 'ok', 2))
    play_reviews_server.set_reviews(
        'com.example.moved', [[None] + item for item in _items(play_review_item, 'moved', 2)]
    )
    crawl_config.set_app_ids('playstore', 'com.example.ok', 'com.example.moved')

    summary = PlayStoreCrawler(crawl_config.path).run()

    ok, moved = summary.apps
    assert (ok.outcome, ok.fetched) == (OUTCOME_OK, 2)
    assert (moved.outcome, moved.fetched) == (OUTCOME_ERROR, 0)
    assert 'item 0 reviewId is NoneType' in moved.detail


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
def test_a_page_without_reviews_but_with_a_token_ends_the_app_as_an_error(
    crawl_config, play_reviews_server, play_review_item
):
    page_size, app_id = 4, 'com.example.looping'
    _set_play_config(crawl_config, page_size=page_size, reviews_per_app=10)
    play_reviews_server.set_reviews(app_id, _items(play_review_item, 'looping', 10))
    empty_with_token = PlayAnswer(
        body=PlayReviewsServer.envelope(RPC_ID, PlayReviewsServer.page_payload([], 'tok-again'))
    )
    # Each such answer asks for one more page; five are queued so a crawler that follows them ends.
    play_reviews_server.push(app_id, PlayAnswer(), *[empty_with_token] * 5)
    crawl_config.set_app_ids('playstore', app_id)

    [app] = PlayStoreCrawler(crawl_config.path).run().apps

    assert (app.outcome, app.fetched) == (OUTCOME_ERROR, page_size)
    assert 'a page without reviews came with a next-page token' in app.detail
    assert len(play_reviews_server.requests_for(app_id)) == 2


@pytest.mark.requires_db
def test_a_later_page_failing_keeps_the_reviews_read_before_it(
    crawl_config, play_reviews_server, play_review_item, minio_bucket, test_db_engine, monkeypatch
):
    from src.utils.minio_client import MinIOClient

    monkeypatch.setenv('ENABLE_PARQUET_WRITE', 'true')
    page_size, app_id = 4, 'com.example.partial'
    _set_play_config(crawl_config, page_size=page_size, reviews_per_app=10)
    play_reviews_server.set_reviews(app_id, _items(play_review_item, 'partial', 10))
    play_reviews_server.push(app_id, PlayAnswer(), PlayAnswer(status=404))  # the second page fails
    crawl_config.set_app_ids('playstore', app_id)
    storage_path = None
    try:
        summary = PlayStoreCrawler(crawl_config.path).run()
        storage_path = summary.storage_path

        [app] = summary.apps
        assert (app.outcome, app.fetched, app.new) == (OUTCOME_ERROR, page_size, page_size)
        assert app.detail.startswith('PlayReviewsError(') and 'HTTP 404' in app.detail
        assert summary.failure is None and storage_path is not None
        rows = MinIOClient().get_parquet(storage_path).to_pylist()
        assert sorted(row['platform_review_id'] for row in rows) == [f'partial-{i}' for i in range(page_size)]
        assert len(play_reviews_server.requests_for(app_id)) == 2
    finally:
        with test_db_engine.begin() as conn:
            conn.execute(text('DELETE FROM ingestion_batch WHERE storage_path = :path'), {'path': storage_path})
            conn.execute(
                text("DELETE FROM apps WHERE platform_type = 'PLAYSTORE' AND platform_app_id = :id"), {'id': app_id}
            )


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
