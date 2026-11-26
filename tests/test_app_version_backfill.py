"""app_version backfill from a store re-fetch (Issue #125).

Store payloads are shaped like real responses: App Store entries copy the keys of the
customerreviews RSS JSON (``rss_review_entry`` in conftest), Play reviews copy the dict
google-play-scraper returns (naive ``at``). The App Store fetcher is driven over real HTTP against
``appstore_rss_server`` on 127.0.0.1, and the database is the real test database. Play paging is
covered through ``scan_play_page``, the decision step of its loop, because the loop's only other
line is the live google-play-scraper call.
"""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text
from uuid6 import uuid7

from src.crawlers.app_version_backfill import (
    PLATFORM_ORDER,
    REVIEWED_AT_SLACK,
    STOP_ALL_MATCHED,
    STOP_CAP_REACHED,
    STOP_EMPTY_PAGE,
    STOP_ERROR,
    STOP_EXHAUSTED,
    STOP_FEED_EMPTY,
    STOP_PAGE_LIMIT,
    STOP_PASSED_OLDEST_TARGET,
    STOP_SKIPPED_AFTER_EMPTY_FEEDS,
    AppTargets,
    FetchResult,
    appstore_feed_complete,
    appstore_review_entries,
    backfill_app_versions,
    count_targets_without_app,
    fetch_appstore_versions,
    load_settings,
    load_targets,
    match_versions,
    play_stop_reason,
    scan_play_page,
    settle_app,
)
from src.crawlers.appstore_crawler import AppStoreCrawler
from src.crawlers.playstore_crawler import PlayStoreCrawler
from src.models.apps import App
from src.models.enums import PlatformType

APPSTORE = PlatformType.APPSTORE
PLAYSTORE = PlatformType.PLAYSTORE
REVIEWED_AT = datetime(2026, 8, 10, 9, 0, tzinfo=timezone.utc)
CONFIG_PATH = str(Path(__file__).resolve().parents[1] / 'config' / 'crawler_config.yml')
SETTINGS = load_settings(CONFIG_PATH)
PAGE_SIZE = SETTINGS.appstore_rss_page_size

# App-metadata entry the RSS has put first on page 1 (AppStoreCrawler skips it by `im:name`).
RSS_APP_METADATA_ENTRY = {
    'im:name': {'label': '테스트 뱅킹'},
    'title': {'label': '테스트 뱅킹 - 테스트은행'},
    'id': {
        'label': 'https://apps.apple.com/kr/app/id1067748687?uo=2',
        'attributes': {'im:id': '1067748687', 'im:bundleId': 'com.example.bank'},
    },
    'im:contentType': {'attributes': {'term': 'Application', 'label': '앱'}},
    'link': {'attributes': {
        'rel': 'alternate', 'type': 'text/html',
        'href': 'https://apps.apple.com/kr/app/id1067748687?uo=2',
    }},
}


def _play_review(review_id, version, at=datetime(2026, 8, 10, 9, 0)):
    """One review dict as google-play-scraper's reviews() returns it."""
    return {
        'reviewId': review_id,
        'userName': '사용자',
        'userImage': 'https://play-lh.googleusercontent.com/a/example',
        'content': '업데이트 후 로그인이 빨라졌어요',
        'score': 5,
        'thumbsUpCount': 0,
        'reviewCreatedVersion': version,
        'at': at,
        'replyContent': None,
        'repliedAt': None,
        'appVersion': version,
    }


def _full_page(rss_review_entry, prefix, *targets):
    """A full RSS page: the given (review_id, version) targets padded with other reviews."""
    entries = [rss_review_entry(rid, version) for rid, version in targets]
    entries += [rss_review_entry(f'{prefix}-{i}', '7.0.0') for i in range(PAGE_SIZE - len(entries))]
    return entries


def _appstore_target(platform_app_id, *review_ids):
    return AppTargets(uuid7(), APPSTORE, platform_app_id, {rid: REVIEWED_AT for rid in review_ids})


# ========================================
# A. FIELD EXTRACTION (shared with the crawlers)
# ========================================

def test_appstore_extractors_read_rss_entry(rss_review_entry):
    entry = rss_review_entry('14807491528', '7.3.16')

    assert AppStoreCrawler._extract_platform_review_id(entry) == '14807491528'
    assert AppStoreCrawler._extract_app_version(entry) == '7.3.16'
    assert AppStoreCrawler._extract_app_version(rss_review_entry('14807491528')) is None


def test_playstore_extractors_read_scraper_review():
    review = _play_review('65e580b6-c58a-41d6-9216-57445e548778', '2.3.1')

    assert PlayStoreCrawler._extract_platform_review_id(review) == '65e580b6-c58a-41d6-9216-57445e548778'
    assert PlayStoreCrawler._extract_app_version(review) == '2.3.1'
    assert PlayStoreCrawler._extract_app_version(_play_review('r', None)) is None


def test_appstore_review_entries_drops_app_metadata_and_wraps_single_entry(rss_review_entry):
    reviews = [rss_review_entry('1', '7.3.16'), rss_review_entry('2', '7.3.15')]

    assert appstore_review_entries({'feed': {'entry': [RSS_APP_METADATA_ENTRY, *reviews]}}) == reviews
    assert appstore_review_entries({'feed': {'entry': reviews[0]}}) == [reviews[0]]
    assert appstore_review_entries({'feed': {'entry': RSS_APP_METADATA_ENTRY}}) == []
    assert appstore_review_entries({'feed': {'author': {'name': {'label': 'iTunes Store'}}}}) == []


def test_match_versions_keeps_target_ids_only_with_or_without_version(rss_review_entry):
    appstore_found = match_versions(
        APPSTORE,
        [RSS_APP_METADATA_ENTRY, rss_review_entry('1', '7.3.16'), rss_review_entry('2'),
         rss_review_entry('9', '7.0.0')],
        {'1', '2', '3'},
    )
    play_found = match_versions(
        PLAYSTORE,
        [_play_review('a', '2.3.1'), _play_review('b', None), _play_review('z', '2.0.0')],
        {'a', 'b', 'c'},
    )

    assert appstore_found == {'1': '7.3.16', '2': None}
    assert play_found == {'a': '2.3.1', 'b': None}


# ========================================
# B. APP STORE: is the read complete?
# ========================================

@pytest.mark.parametrize('page_counts, complete', [
    ([PAGE_SIZE] * 10, True),                        # the whole window, every page full
    ([6] + [0] * 9, True),                            # small app (1187184693 has 6 reviews)
    ([42] + [0] * 9, True),                           # small app (718821674 has 42)
    ([PAGE_SIZE, PAGE_SIZE, 20] + [0] * 7, True),     # the feed ends on a partial page
    ([PAGE_SIZE, PAGE_SIZE, 20], True),
    ([0] * 10, False),                                # every page empty
    ([0, 0, PAGE_SIZE, PAGE_SIZE] + [0] * 6, False),  # observed outage: 1-2 and 5-10 empty
    ([PAGE_SIZE, PAGE_SIZE] + [0] * 8, False),        # empty pages after a full page
    ([PAGE_SIZE, 30, PAGE_SIZE], False),              # a partial page in the middle
])
def test_appstore_feed_complete(page_counts, complete):
    assert appstore_feed_complete(page_counts, PAGE_SIZE) is complete


# ========================================
# C. APP STORE FETCH over real HTTP
# ========================================

@pytest.fixture
def rss_settings(appstore_rss_server):
    return replace(SETTINGS, appstore_reviews_rss_url=appstore_rss_server.url_template, delay_seconds=0)


def test_fetch_appstore_reads_past_gap_pages(appstore_rss_server, rss_settings, rss_review_entry):
    """Pages 1-2 and 5-10 empty, 3-4 full: the 2026-11-26 outage pattern."""
    appstore_rss_server.set_pages(
        '111', [], [],
        _full_page(rss_review_entry, 'p3', ('t3', '7.3.16')),
        _full_page(rss_review_entry, 'p4'),
    )

    result = fetch_appstore_versions(_appstore_target('111', 't3', 'gone'), rss_settings)

    assert result.found == {'t3': '7.3.16'}
    assert appstore_rss_server.requested_pages('111') == list(range(1, SETTINGS.appstore_pages + 1))
    assert (result.fetched, result.empty_pages) == (2 * PAGE_SIZE, SETTINGS.appstore_pages - 2)
    assert (result.stop_reason, result.complete, result.error) == (STOP_PAGE_LIMIT, False, None)


def test_fetch_appstore_stops_once_every_target_matched(appstore_rss_server, rss_settings, rss_review_entry):
    appstore_rss_server.set_pages(
        '222', _full_page(rss_review_entry, 'p1', ('a', '7.3.16'), ('b', None)),
        _full_page(rss_review_entry, 'p2'),
    )

    result = fetch_appstore_versions(_appstore_target('222', 'a', 'b'), rss_settings)

    assert appstore_rss_server.requested_pages('222') == [1]
    assert result.found == {'a': '7.3.16', 'b': None}
    assert (result.stop_reason, result.complete) == (STOP_ALL_MATCHED, True)


def test_fetch_appstore_http_error_names_the_page(appstore_rss_server, rss_settings, rss_review_entry):
    appstore_rss_server.set_pages('333', _full_page(rss_review_entry, 'p1', ('a', '7.3.16')))
    appstore_rss_server.fail('333', 2, status=500)

    result = fetch_appstore_versions(_appstore_target('333', 'a', 'b'), rss_settings)

    assert appstore_rss_server.requested_pages('333') == [1, 2]
    assert result.found == {'a': '7.3.16'}
    assert (result.stop_reason, result.complete) == (STOP_ERROR, False)
    assert result.error is not None
    assert result.error.startswith('page 2: HTTPError(') and '500' in result.error


def test_fetch_appstore_all_empty_is_feed_empty(appstore_rss_server, rss_settings):
    result = fetch_appstore_versions(_appstore_target('444', 'a'), rss_settings)

    assert (result.fetched, result.empty_pages) == (0, SETTINGS.appstore_pages)
    assert (result.stop_reason, result.complete, result.error) == (STOP_FEED_EMPTY, False, None)


def test_fetch_appstore_partial_last_page_confirms_missing(appstore_rss_server, rss_settings, rss_review_entry):
    """A small app: one partial page, then the feed's own end."""
    appstore_rss_server.set_pages('555', [rss_review_entry(f'r{i}', '1.0.0') for i in range(6)])

    result = fetch_appstore_versions(_appstore_target('555', 'r1', 'deleted'), rss_settings)

    assert result.found == {'r1': '1.0.0'}
    assert (result.stop_reason, result.complete) == (STOP_PAGE_LIMIT, True)


def test_fetch_appstore_empty_pages_after_full_page_stay_unverified(
    appstore_rss_server, rss_settings, rss_review_entry
):
    appstore_rss_server.set_pages('666', _full_page(rss_review_entry, 'p1', ('a', '7.3.16')))

    result = fetch_appstore_versions(_appstore_target('666', 'a', 'maybe-on-page-2'), rss_settings)

    assert (result.stop_reason, result.complete) == (STOP_PAGE_LIMIT, False)


# ========================================
# D. PLAY PAGING DECISIONS
# ========================================

def _stop(**overrides):
    """A mid-paging state that continues: 2 of 5 targets matched, 400 of 3000 fetched."""
    args: dict[str, Any] = dict(
        target_times=[REVIEWED_AT] * 5, matched=2, fetched=400, max_reviews=3000,
        last_page_size=200, has_next_page=True, oldest_fetched=REVIEWED_AT,
    )
    args.update(overrides)
    return play_stop_reason(**args)


@pytest.mark.parametrize('overrides, expected', [
    ({}, None),
    ({'matched': 5}, STOP_ALL_MATCHED),
    # The page that matched the last target may also be the last page.
    ({'matched': 5, 'has_next_page': False}, STOP_ALL_MATCHED),
    ({'oldest_fetched': REVIEWED_AT - REVIEWED_AT_SLACK - timedelta(seconds=1)}, STOP_PASSED_OLDEST_TARGET),
    # Within the slack a shifted reviewed_at may still be ahead, so paging goes on.
    ({'oldest_fetched': REVIEWED_AT - timedelta(hours=9)}, None),
    ({'oldest_fetched': None, 'last_page_size': 0, 'fetched': 0, 'has_next_page': False}, STOP_FEED_EMPTY),
    ({'last_page_size': 0, 'has_next_page': False}, STOP_EMPTY_PAGE),
    ({'last_page_size': 120, 'has_next_page': False}, STOP_EXHAUSTED),
    ({'fetched': 3000}, STOP_CAP_REACHED),
    ({'fetched': 2800}, None),
])
def test_play_stop_reason(overrides, expected):
    assert _stop(**overrides) == expected


def test_play_stop_reason_waits_for_the_oldest_target():
    """Paging stops only past the oldest target, not past the newest one."""
    newest, oldest = REVIEWED_AT, REVIEWED_AT - timedelta(days=200)

    assert _stop(target_times=[newest, oldest], matched=1, oldest_fetched=newest - timedelta(days=30)) is None
    assert _stop(target_times=[newest, oldest], matched=1, oldest_fetched=oldest - timedelta(days=2)) == (
        STOP_PASSED_OLDEST_TARGET
    )


def _play_target(**reviewed_at):
    return AppTargets(uuid7(), PLAYSTORE, 'com.example.bank', reviewed_at)


def test_scan_play_first_page_empty_is_feed_empty_and_unverified():
    """google-play-scraper answers a failed first request with an empty page and no token."""
    result = FetchResult()

    stop = scan_play_page(result, [], False, _play_target(a=REVIEWED_AT), max_reviews=3000)

    assert (stop, result.complete, result.fetched, result.empty_pages) == (STOP_FEED_EMPTY, False, 0, 1)


def test_scan_play_empty_page_after_reviews_is_unverified():
    target = _play_target(a=REVIEWED_AT, b=REVIEWED_AT)
    result = FetchResult()

    assert scan_play_page(result, [_play_review('a', '2.3.1')], True, target, max_reviews=3000) is None
    assert scan_play_page(result, [], False, target, max_reviews=3000) == STOP_EMPTY_PAGE
    assert (result.found, result.complete) == ({'a': '2.3.1'}, False)


def test_scan_play_last_page_with_reviews_confirms_missing():
    """The store's own end: a page with reviews and no next-page token."""
    result = FetchResult()

    stop = scan_play_page(
        result, [_play_review('a', '2.3.1'), _play_review('x', None)], False,
        _play_target(a=REVIEWED_AT, deleted=REVIEWED_AT), max_reviews=3000,
    )

    assert (stop, result.complete, result.found) == (STOP_EXHAUSTED, True, {'a': '2.3.1'})


def test_scan_play_cap_leaves_targets_unverified():
    result = FetchResult()
    page = [_play_review(f'x{i}', '1.0.0') for i in range(200)]

    stop = scan_play_page(result, page, True, _play_target(a=REVIEWED_AT), max_reviews=200)

    assert (stop, result.complete) == (STOP_CAP_REACHED, False)


def test_scan_play_pages_on_until_past_the_oldest_target():
    target = _play_target(new=REVIEWED_AT, old=REVIEWED_AT - timedelta(days=200))
    result = FetchResult()
    # Naive local times, as google-play-scraper builds them; the test host runs in UTC.
    between = [_play_review('new', '3.0.0', at=datetime(2026, 5, 1, 9, 0))]
    older = [_play_review('x', '1.0.0', at=datetime(2025, 12, 1, 9, 0))]

    assert scan_play_page(result, between, True, target, max_reviews=3000) is None
    assert scan_play_page(result, older, True, target, max_reviews=3000) == STOP_PASSED_OLDEST_TARGET
    assert (result.complete, result.found, result.fetched) == (True, {'new': '3.0.0'}, 2)


def test_scan_play_stops_once_every_target_matched():
    result = FetchResult()

    stop = scan_play_page(
        result, [_play_review('a', '2.3.1'), _play_review('b', None)], True,
        _play_target(a=REVIEWED_AT, b=REVIEWED_AT), max_reviews=3000,
    )

    assert (stop, result.complete) == (STOP_ALL_MATCHED, True)


# ========================================
# E. SETTINGS
# ========================================

def test_load_settings_reads_crawler_config():
    assert SETTINGS.appstore_pages > 0
    assert SETTINGS.appstore_rss_page_size > 0
    assert SETTINGS.appstore_max_consecutive_empty_feeds > 0
    assert 0 < SETTINGS.playstore_page_size <= SETTINGS.playstore_max_reviews_per_app
    assert SETTINGS.delay_seconds >= 0
    assert '{page}' in SETTINGS.appstore_reviews_rss_url


def test_load_settings_fails_on_missing_backfill_key(tmp_path):
    config = tmp_path / 'crawler_config.yml'
    config.write_text(
        'global: {delay_between_requests: 2, timeout: 30}\n'
        'appstore: {country: kr, pages_to_crawl: 10, reviews_rss_url: "http://127.0.0.1/{page}",\n'
        '           rss_page_size: 50, backfill_max_consecutive_empty_feeds: 3}\n'
        'playstore: {language: ko, country: kr, backfill_page_size: 200}\n',
        encoding='utf-8',
    )

    with pytest.raises(KeyError, match='backfill_max_reviews_per_app'):
        load_settings(str(config))


# ========================================
# F. DATABASE: targets and updates (rollback-only session)
# ========================================

def _seed_app(session, platform_type):
    app = App(
        app_id=uuid7(),
        platform_app_id=f'backfill-{uuid7()}',
        platform_type=platform_type,
        name='Backfill App',
    )
    session.add(app)
    session.flush()
    return app.app_id


OLD_UPDATED_AT = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _seed_review(session, app_id, platform_type, platform_review_id, app_version=None,
                 reviewed_at=REVIEWED_AT):
    session.execute(
        text("""
            INSERT INTO app_reviews (
                review_id, app_id, platform_type, platform_review_id,
                review_text, rating, app_version, reviewed_at, updated_at
            ) VALUES (
                :review_id, :app_id, :platform_type, :platform_review_id,
                '리뷰 본문', 4, :app_version, :reviewed_at, :updated_at
            )
        """),
        {
            'review_id': uuid7(), 'app_id': app_id, 'platform_type': platform_type.value,
            'platform_review_id': platform_review_id, 'app_version': app_version,
            'reviewed_at': reviewed_at, 'updated_at': OLD_UPDATED_AT,
        },
    )


def _stored(session, app_id, column='app_version'):
    rows = session.execute(
        text(f"SELECT platform_review_id, {column} FROM app_reviews WHERE app_id = :app_id"),
        {'app_id': app_id},
    ).all()
    return dict(rows)


def _target(session, app_id, platforms=PLATFORM_ORDER):
    return next(t for t in load_targets(session, platforms) if t.app_id == app_id)


def _seed_appstore_app(session):
    """r1/r2 lack a version the RSS has, r3 already has one, r4 is no longer in the RSS."""
    app_id = _seed_app(session, APPSTORE)
    _seed_review(session, app_id, APPSTORE, 'r1')
    _seed_review(session, app_id, APPSTORE, 'r2')
    _seed_review(session, app_id, APPSTORE, 'r3', app_version='7.0.0')
    _seed_review(session, app_id, APPSTORE, 'r4')
    return app_id


def _appstore_fetch(rss_review_entry, target, complete):
    """One RSS page with r1/r2/r3 and a stranger; ``complete`` as fetch_appstore_versions decided."""
    entries = appstore_review_entries({'feed': {'entry': [
        RSS_APP_METADATA_ENTRY,
        rss_review_entry('r1', '7.3.16'),
        rss_review_entry('r2', '7.3.15'),
        rss_review_entry('r3', '7.3.16'),
        rss_review_entry('other', '7.3.16'),
    ]}})
    return FetchResult(
        found=match_versions(APPSTORE, entries, target.reviewed_at),
        fetched=len(entries),
        empty_pages=0 if complete else 1,
        stop_reason=STOP_PAGE_LIMIT,
        complete=complete,
    )


@pytest.mark.requires_db
def test_dry_run_reports_would_update_and_writes_nothing(test_db_session, rss_review_entry):
    app_id = _seed_appstore_app(test_db_session)
    target = _target(test_db_session, app_id)

    result = settle_app(test_db_session, target, _appstore_fetch(rss_review_entry, target, True), apply=False)

    assert set(target.reviewed_at) == {'r1', 'r2', 'r4'}
    assert (result.targets, result.fetched, result.matched) == (3, 4, 2)
    assert (result.matched_with_version, result.updated) == (2, 2)
    assert (result.unmatched_confirmed, result.unverified) == (1, 0)
    assert _stored(test_db_session, app_id) == {'r1': None, 'r2': None, 'r3': '7.0.0', 'r4': None}


@pytest.mark.requires_db
def test_incomplete_read_counts_unmatched_as_unverified(test_db_session, rss_review_entry):
    app_id = _seed_appstore_app(test_db_session)
    target = _target(test_db_session, app_id)

    result = settle_app(test_db_session, target, _appstore_fetch(rss_review_entry, target, False), apply=False)

    assert (result.matched, result.unmatched_confirmed, result.unverified) == (2, 0, 1)


@pytest.mark.requires_db
def test_dry_run_count_skips_rows_filled_after_targets_were_loaded(test_db_session, rss_review_entry):
    """would_update uses the same `app_version IS NULL` guard as the UPDATE."""
    app_id = _seed_appstore_app(test_db_session)
    target = _target(test_db_session, app_id)
    test_db_session.execute(
        text("UPDATE app_reviews SET app_version = '7.3.0' WHERE app_id = :app_id AND platform_review_id = 'r1'"),
        {'app_id': app_id},
    )

    result = settle_app(test_db_session, target, _appstore_fetch(rss_review_entry, target, True), apply=False)

    assert (result.matched_with_version, result.updated) == (2, 1)


@pytest.mark.requires_db
def test_apply_fills_only_null_rows_and_rerun_is_idempotent(test_db_session, rss_review_entry):
    app_id = _seed_appstore_app(test_db_session)
    target = _target(test_db_session, app_id)
    fetch = _appstore_fetch(rss_review_entry, target, True)

    first = settle_app(test_db_session, target, fetch, apply=True)
    after_first = _stored(test_db_session, app_id)

    assert first.updated == 2
    assert after_first == {'r1': '7.3.16', 'r2': '7.3.15', 'r3': '7.0.0', 'r4': None}

    # A re-run only sees the row still NULL, and the store still does not return it.
    rerun_target = _target(test_db_session, app_id)
    rerun = settle_app(test_db_session, rerun_target, _appstore_fetch(rss_review_entry, rerun_target, True), apply=True)
    # Replaying the first run's matches cannot overwrite what it wrote.
    replay = settle_app(test_db_session, target, fetch, apply=True)

    assert set(rerun_target.reviewed_at) == {'r4'}
    assert (rerun.targets, rerun.matched, rerun.updated, rerun.unmatched_confirmed) == (1, 0, 0, 1)
    assert replay.updated == 0
    assert _stored(test_db_session, app_id) == after_first


@pytest.mark.requires_db
def test_apply_stamps_updated_at_on_filled_rows_only(test_db_session, rss_review_entry):
    app_id = _seed_appstore_app(test_db_session)
    target = _target(test_db_session, app_id)
    transaction_start = test_db_session.execute(text("SELECT NOW()")).scalar_one()

    settle_app(test_db_session, target, _appstore_fetch(rss_review_entry, target, True), apply=True)

    updated_at = _stored(test_db_session, app_id, column='updated_at')
    assert updated_at['r1'] == updated_at['r2'] == transaction_start > OLD_UPDATED_AT
    assert updated_at['r3'] == updated_at['r4'] == OLD_UPDATED_AT


@pytest.mark.requires_db
def test_play_matches_without_version_and_unmatched_are_counted_not_written(test_db_session):
    app_id = _seed_app(test_db_session, PLAYSTORE)
    for review_id in ('p1', 'p2', 'p3'):
        _seed_review(test_db_session, app_id, PLAYSTORE, review_id)
    target = _target(test_db_session, app_id)
    fetch = FetchResult()
    scan_play_page(
        fetch, [_play_review('p1', '2.3.1'), _play_review('p2', None), _play_review('other', '2.3.1')],
        False, target, max_reviews=3000,
    )

    result = settle_app(test_db_session, target, fetch, apply=True)

    assert (result.targets, result.matched, result.matched_with_version) == (3, 2, 1)
    assert (result.updated, result.unmatched_confirmed, result.unverified) == (1, 1, 0)
    assert result.stop_reason == STOP_EXHAUSTED
    assert _stored(test_db_session, app_id) == {'p1': '2.3.1', 'p2': None, 'p3': None}


@pytest.mark.requires_db
def test_load_targets_orders_appstore_first_and_filters_platform(test_db_session):
    play_app = _seed_app(test_db_session, PLAYSTORE)
    appstore_app = _seed_app(test_db_session, APPSTORE)
    older = REVIEWED_AT - timedelta(days=400)
    _seed_review(test_db_session, play_app, PLAYSTORE, 'play-null', reviewed_at=older)
    _seed_review(test_db_session, play_app, PLAYSTORE, 'play-set', app_version='1.0.0')
    _seed_review(test_db_session, appstore_app, APPSTORE, 'as-null')
    ours = {play_app, appstore_app}

    all_targets = [t for t in load_targets(test_db_session, PLATFORM_ORDER) if t.app_id in ours]
    play_only = [t for t in load_targets(test_db_session, (PLAYSTORE,)) if t.app_id in ours]

    assert [t.app_id for t in all_targets] == [appstore_app, play_app]
    assert [t.platform_type for t in all_targets] == [APPSTORE, PLAYSTORE]
    assert all_targets[1].reviewed_at == {'play-null': older}
    assert [t.app_id for t in play_only] == [play_app]


@pytest.mark.requires_db
def test_rows_without_an_app_are_counted_not_targeted(test_db_session):
    before_all = count_targets_without_app(test_db_session, PLATFORM_ORDER)
    before_play = count_targets_without_app(test_db_session, (PLAYSTORE,))
    missing_app_id = uuid7()
    _seed_review(test_db_session, missing_app_id, APPSTORE, 'orphan')

    assert count_targets_without_app(test_db_session, PLATFORM_ORDER) == before_all + 1
    assert count_targets_without_app(test_db_session, (PLAYSTORE,)) == before_play
    assert all(t.app_id != missing_app_id for t in load_targets(test_db_session, PLATFORM_ORDER))


# ========================================
# G. THE WHOLE BACKFILL: own connection, local RSS server, committed rows
# ========================================

@pytest.mark.requires_db
def test_backfill_dry_run_changes_nothing_seen_from_another_connection(
    backfill_db, backfill_config, appstore_rss_server, rss_review_entry
):
    app_id = backfill_db.seed_app(APPSTORE, '1000001', [('a', None), ('b', None), ('c', '6.0.0')])
    appstore_rss_server.set_pages('1000001', _full_page(rss_review_entry, 'p1', ('a', '7.3.16'), ('b', '7.3.15')))

    report = backfill_app_versions(backfill_config, (APPSTORE,), apply=False)

    (app,) = report.apps
    assert (app.platform_app_id, app.updated, app.stop_reason) == ('1000001', 2, STOP_ALL_MATCHED)
    assert backfill_db.versions(app_id) == {'a': None, 'b': None, 'c': '6.0.0'}


@pytest.mark.requires_db
def test_backfill_apply_commits_each_app_before_a_later_app_fails(
    backfill_db, backfill_config, appstore_rss_server, rss_review_entry
):
    first = backfill_db.seed_app(APPSTORE, '1000001', [('a', None)])
    later = backfill_db.seed_app(APPSTORE, '1000002', [('z', None)])
    appstore_rss_server.set_pages('1000001', _full_page(rss_review_entry, 'p1', ('a', '7.3.16')))
    appstore_rss_server.fail('1000002', 1, status=500)

    report = backfill_app_versions(backfill_config, (APPSTORE,), apply=True)

    assert [(a.platform_app_id, a.updated, a.stop_reason) for a in report.apps] == [
        ('1000001', 1, STOP_ALL_MATCHED), ('1000002', 0, STOP_ERROR),
    ]
    assert report.has_errors
    assert (report.apps[1].error or '').startswith('page 1:')
    assert backfill_db.versions(first) == {'a': '7.3.16'}
    assert backfill_db.versions(later) == {'z': None}


@pytest.mark.requires_db
def test_backfill_stops_asking_the_app_store_after_consecutive_empty_feeds(
    backfill_db, backfill_config, appstore_rss_server
):
    cutoff = SETTINGS.appstore_max_consecutive_empty_feeds
    store_ids = [f'200000{i}' for i in range(cutoff + 2)]
    for store_id in store_ids:
        backfill_db.seed_app(APPSTORE, store_id, [('a', None), ('b', None)])

    report = backfill_app_versions(backfill_config, (APPSTORE,), apply=False)

    assert [a.stop_reason for a in report.apps] == (
        [STOP_FEED_EMPTY] * cutoff + [STOP_SKIPPED_AFTER_EMPTY_FEEDS] * 2
    )
    for store_id in store_ids[cutoff:]:
        assert appstore_rss_server.requested_pages(store_id) == []
    assert all((a.unverified, a.unmatched_confirmed) == (2, 0) for a in report.apps)
    assert report.has_unverified and not report.has_errors
