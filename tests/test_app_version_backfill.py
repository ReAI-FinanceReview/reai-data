"""app_version backfill from a store re-fetch (Issue #125).

Store payloads below are shaped like real responses: App Store entries copy the keys of the
customerreviews RSS JSON, Play reviews copy the dict google-play-scraper returns (naive `at`).
They are the input data of the match/update layer; no network client or database is replaced.
"""
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
    STOP_EXHAUSTED,
    STOP_PAGE_LIMIT,
    STOP_PASSED_OLDEST_TARGET,
    FetchResult,
    appstore_review_entries,
    count_targets_without_app,
    load_settings,
    load_targets,
    match_versions,
    play_stop_reason,
    settle_app,
)
from src.crawlers.appstore_crawler import AppStoreCrawler
from src.crawlers.playstore_crawler import PlayStoreCrawler
from src.models.apps import App
from src.models.enums import PlatformType

APPSTORE = PlatformType.APPSTORE
PLAYSTORE = PlatformType.PLAYSTORE
REVIEWED_AT = datetime(2026, 8, 10, 9, 0, tzinfo=timezone.utc)


def _rss_entry(review_id, version=None):
    """One review entry of the App Store customerreviews RSS JSON."""
    entry = {
        'author': {
            'uri': {'label': 'https://itunes.apple.com/kr/reviews/id1335187572'},
            'name': {'label': '리뷰어'},
            'label': '',
        },
        'updated': {'label': '2026-11-18T21:37:26-07:00'},
        'im:rating': {'label': '1'},
        'id': {'label': review_id},
        'title': {'label': '로그인 오류'},
        'content': {'label': '로그인이 자꾸 풀려요', 'attributes': {'type': 'text'}},
        'link': {'attributes': {
            'rel': 'related',
            'href': 'https://itunes.apple.com/kr/review?id=1067748687&type=Purple%20Software',
        }},
        'im:voteSum': {'label': '0'},
        'im:contentType': {'attributes': {'term': 'Application', 'label': '앱'}},
        'im:voteCount': {'label': '0'},
    }
    if version is not None:
        entry['im:version'] = {'label': version}
    return entry


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


# ========================================
# A. FIELD EXTRACTION (shared with the crawlers)
# ========================================

def test_appstore_extractors_read_rss_entry():
    assert AppStoreCrawler._extract_platform_review_id(_rss_entry('14807491528', '7.3.16')) == '14807491528'
    assert AppStoreCrawler._extract_app_version(_rss_entry('14807491528', '7.3.16')) == '7.3.16'
    assert AppStoreCrawler._extract_app_version(_rss_entry('14807491528')) is None


def test_playstore_extractors_read_scraper_review():
    review = _play_review('65e580b6-c58a-41d6-9216-57445e548778', '2.3.1')

    assert PlayStoreCrawler._extract_platform_review_id(review) == '65e580b6-c58a-41d6-9216-57445e548778'
    assert PlayStoreCrawler._extract_app_version(review) == '2.3.1'
    assert PlayStoreCrawler._extract_app_version(_play_review('r', None)) is None


def test_appstore_review_entries_drops_app_metadata_and_wraps_single_entry():
    reviews = [_rss_entry('1', '7.3.16'), _rss_entry('2', '7.3.15')]

    assert appstore_review_entries({'feed': {'entry': [RSS_APP_METADATA_ENTRY, *reviews]}}) == reviews
    assert appstore_review_entries({'feed': {'entry': reviews[0]}}) == [reviews[0]]
    assert appstore_review_entries({'feed': {'entry': RSS_APP_METADATA_ENTRY}}) == []
    assert appstore_review_entries({'feed': {'author': {'name': {'label': 'iTunes Store'}}}}) == []


def test_match_versions_keeps_target_ids_only_with_or_without_version():
    appstore_found = match_versions(
        APPSTORE,
        [RSS_APP_METADATA_ENTRY, _rss_entry('1', '7.3.16'), _rss_entry('2'), _rss_entry('9', '7.0.0')],
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
# B. PLAY STOP RULES
# ========================================

def _stop(**overrides):
    """A mid-paging state that continues: 2 of 5 targets matched, 400 of 3000 fetched."""
    args: dict[str, Any] = dict(
        targets=5, matched=2, fetched=400, max_reviews=3000, last_page_size=200,
        has_next_page=True, oldest_fetched=REVIEWED_AT, oldest_target=REVIEWED_AT,
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
    ({'oldest_fetched': None, 'last_page_size': 0, 'fetched': 0}, STOP_EXHAUSTED),
    ({'has_next_page': False}, STOP_EXHAUSTED),
    ({'fetched': 3000}, STOP_CAP_REACHED),
    ({'fetched': 2800}, None),
])
def test_play_stop_reason(overrides, expected):
    assert _stop(**overrides) == expected


# ========================================
# C. SETTINGS
# ========================================

def test_load_settings_reads_crawler_config():
    settings = load_settings(str(Path(__file__).resolve().parents[1] / 'config' / 'crawler_config.yml'))

    assert settings.appstore_pages > 0
    assert 0 < settings.playstore_page_size <= settings.playstore_max_reviews_per_app
    assert settings.delay_seconds >= 0


def test_load_settings_fails_on_missing_backfill_key(tmp_path):
    config = tmp_path / 'crawler_config.yml'
    config.write_text(
        'global: {delay_between_requests: 2, timeout: 30}\n'
        'appstore: {country: kr, pages_to_crawl: 10}\n'
        'playstore: {language: ko, country: kr, backfill_page_size: 200}\n',
        encoding='utf-8',
    )

    with pytest.raises(KeyError, match='backfill_max_reviews_per_app'):
        load_settings(str(config))


# ========================================
# D. DATABASE: targets and updates
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


def _seed_review(session, app_id, platform_type, platform_review_id, app_version=None,
                 reviewed_at=REVIEWED_AT):
    session.execute(
        text("""
            INSERT INTO app_reviews (
                review_id, app_id, platform_type, platform_review_id,
                review_text, rating, app_version, reviewed_at
            ) VALUES (
                :review_id, :app_id, :platform_type, :platform_review_id,
                '리뷰 본문', 4, :app_version, :reviewed_at
            )
        """),
        {
            'review_id': uuid7(), 'app_id': app_id, 'platform_type': platform_type.value,
            'platform_review_id': platform_review_id, 'app_version': app_version,
            'reviewed_at': reviewed_at,
        },
    )


def _stored_versions(session, app_id):
    rows = session.execute(
        text("SELECT platform_review_id, app_version FROM app_reviews WHERE app_id = :app_id"),
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


APPSTORE_PAGE = {'feed': {'entry': [
    RSS_APP_METADATA_ENTRY,
    _rss_entry('r1', '7.3.16'),
    _rss_entry('r2', '7.3.15'),
    _rss_entry('r3', '7.3.16'),
    _rss_entry('other', '7.3.16'),
]}}


def _appstore_fetch(target):
    """APPSTORE_PAGE was the only page with entries; one more page in range came back empty."""
    entries = appstore_review_entries(APPSTORE_PAGE)
    return FetchResult(
        found=match_versions(APPSTORE, entries, target.reviewed_at),
        fetched=len(entries),
        empty_pages=1,
        stop_reason=STOP_PAGE_LIMIT,
    )


@pytest.mark.requires_db
def test_dry_run_reports_would_update_and_writes_nothing(test_db_session):
    app_id = _seed_appstore_app(test_db_session)
    target = _target(test_db_session, app_id)

    result = settle_app(test_db_session, target, _appstore_fetch(target), apply=False)

    assert set(target.reviewed_at) == {'r1', 'r2', 'r4'}
    assert (result.targets, result.fetched, result.empty_pages, result.matched) == (3, 4, 1, 2)
    assert (result.matched_with_version, result.updated, result.unmatched) == (2, 2, 1)
    assert result.stop_reason == STOP_PAGE_LIMIT
    assert _stored_versions(test_db_session, app_id) == {
        'r1': None, 'r2': None, 'r3': '7.0.0', 'r4': None,
    }


@pytest.mark.requires_db
def test_apply_fills_only_null_rows_and_rerun_is_idempotent(test_db_session):
    app_id = _seed_appstore_app(test_db_session)
    target = _target(test_db_session, app_id)
    fetch = _appstore_fetch(target)

    first = settle_app(test_db_session, target, fetch, apply=True)
    after_first = _stored_versions(test_db_session, app_id)

    assert first.updated == 2
    assert after_first == {'r1': '7.3.16', 'r2': '7.3.15', 'r3': '7.0.0', 'r4': None}

    # A re-run only sees the row still NULL, and the store still does not return it.
    rerun_target = _target(test_db_session, app_id)
    rerun = settle_app(test_db_session, rerun_target, _appstore_fetch(rerun_target), apply=True)
    # Replaying the first run's matches cannot overwrite what it wrote.
    replay = settle_app(test_db_session, target, fetch, apply=True)

    assert set(rerun_target.reviewed_at) == {'r4'}
    assert (rerun.targets, rerun.matched, rerun.updated, rerun.unmatched) == (1, 0, 0, 1)
    assert replay.updated == 0
    assert _stored_versions(test_db_session, app_id) == after_first


@pytest.mark.requires_db
def test_play_matches_without_version_and_unmatched_are_counted_not_written(test_db_session):
    app_id = _seed_app(test_db_session, PLAYSTORE)
    for review_id in ('p1', 'p2', 'p3'):
        _seed_review(test_db_session, app_id, PLAYSTORE, review_id)
    target = _target(test_db_session, app_id)
    page = [_play_review('p1', '2.3.1'), _play_review('p2', None), _play_review('other', '2.3.1')]
    fetch = FetchResult(
        found=match_versions(PLAYSTORE, page, target.reviewed_at),
        fetched=len(page),
        stop_reason=STOP_EXHAUSTED,
    )

    result = settle_app(test_db_session, target, fetch, apply=True)

    assert (result.targets, result.matched, result.matched_with_version) == (3, 2, 1)
    assert (result.updated, result.unmatched, result.stop_reason) == (1, 1, STOP_EXHAUSTED)
    assert _stored_versions(test_db_session, app_id) == {'p1': '2.3.1', 'p2': None, 'p3': None}


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
