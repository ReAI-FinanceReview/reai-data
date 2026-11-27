"""App Store RSS reading shared by the routine crawl and the app_version backfill (Issue #127).

The reader and AppStoreCrawler run over real HTTP against ``appstore_rss_server`` on 127.0.0.1,
whose entries copy the keys of the live customerreviews RSS JSON (``rss_review_entry``). The
crawler tests use the real test database with Parquet writes off (``crawl_config``), so a crawl
reads the store and writes nothing.
"""
import logging
from pathlib import Path

import pytest
import yaml

from src.crawlers.appstore_crawler import AppStoreCrawler
from src.crawlers.appstore_rss import FeedRead, read_feed, read_outcome
from src.crawlers.crawl_outcome import (
    OUTCOME_ERROR,
    OUTCOME_FEED_EMPTY,
    OUTCOME_GAP,
    OUTCOME_OK,
    OUTCOME_SKIPPED_AFTER_EMPTY_FEEDS,
)
from src.pipeline.steps import run_crawl

CONFIG = yaml.safe_load(
    (Path(__file__).resolve().parents[1] / 'config' / 'crawler_config.yml').read_text(encoding='utf-8')
)
PAGE_SIZE = CONFIG['appstore']['rss_page_size']
PAGES = CONFIG['appstore']['pages_to_crawl']
MAX_REVIEWS = CONFIG['appstore']['max_reviews_per_app']
CUTOFF = CONFIG['appstore']['max_consecutive_empty_feeds']


def _app_metadata_entry(name):
    """The app-metadata entry the RSS puts first on page 1: ``im:name``, no ``im:rating``."""
    return {
        'im:name': {'label': name},
        'title': {'label': f'{name} - 테스트은행'},
        'id': {'label': 'https://apps.apple.com/kr/app/id1067748687?uo=2'},
        'im:contentType': {'attributes': {'term': 'Application', 'label': '앱'}},
    }


def _page(rss_review_entry, prefix, count=PAGE_SIZE):
    return [rss_review_entry(f'{prefix}-{i}', '7.0.0') for i in range(count)]


class _WarningRecords(logging.Handler):
    """Collects the WARNING-and-above records of one logger while attached.

    Not ``caplog``: it needs pytest's logging plugin, which a rootdir config with
    ``-p no:logging`` (as an ancestor pyproject.toml of a scratch worktree can have) switches off.
    """

    def __init__(self):
        super().__init__(logging.WARNING)
        self.records = []

    def emit(self, record):
        self.records.append(record)


def _read(server, app_id, **overrides):
    args = dict(
        url_template=server.url_template, country=CONFIG['appstore']['country'], pages=PAGES,
        timeout=CONFIG['global']['timeout'], delay=0,
    )
    args.update(overrides)
    return read_feed(app_id, **args)


# ========================================
# A. read_feed over real HTTP
# ========================================

def test_read_feed_reads_past_empty_pages(appstore_rss_server, rss_review_entry):
    """Pages 1-2 empty and 3-4 full: the 2026-11-26 pattern no longer ends the read at page 1."""
    appstore_rss_server.set_pages('101', [], [], _page(rss_review_entry, 'p3'), _page(rss_review_entry, 'p4'))

    read = _read(appstore_rss_server, '101')

    assert appstore_rss_server.requested_pages('101') == list(range(1, PAGES + 1))
    assert read.page_counts == [0, 0, PAGE_SIZE, PAGE_SIZE] + [0] * (PAGES - 4)
    assert (read.fetched, read.empty_pages, read.error, read.stopped) == (2 * PAGE_SIZE, PAGES - 2, None, False)
    assert read.entries[0]['id']['label'] == 'p3-0'


def test_read_feed_stops_when_on_page_says_so(appstore_rss_server, rss_review_entry):
    appstore_rss_server.set_pages('102', _page(rss_review_entry, 'p1'), _page(rss_review_entry, 'p2'))
    seen = []

    def on_page(read, entries):
        seen.append(len(entries))
        return read.fetched >= PAGE_SIZE

    read = _read(appstore_rss_server, '102', on_page=on_page)

    assert appstore_rss_server.requested_pages('102') == [1]
    assert (seen, read.stopped, read.fetched) == ([PAGE_SIZE], True, PAGE_SIZE)


def test_read_feed_failed_request_ends_the_read_and_names_the_page(appstore_rss_server, rss_review_entry):
    appstore_rss_server.set_pages('103', _page(rss_review_entry, 'p1', 7))
    appstore_rss_server.fail('103', 2, status=503)

    read = _read(appstore_rss_server, '103')

    assert appstore_rss_server.requested_pages('103') == [1, 2]
    assert (read.fetched, read.page_counts, read.feed_empty) == (7, [7], False)
    assert read.error.startswith('page 2: HTTPError(') and '503' in read.error


def test_read_feed_takes_the_app_name_from_the_metadata_entry_and_does_not_count_it(
    appstore_rss_server, rss_review_entry
):
    appstore_rss_server.set_pages('104', [_app_metadata_entry('테스트 뱅킹'), *_page(rss_review_entry, 'p1', 3)])

    read = _read(appstore_rss_server, '104')

    assert (read.app_name, read.fetched, read.page_counts[0]) == ('테스트 뱅킹', 3, 3)
    assert all('im:rating' in entry for entry in read.entries)


@pytest.mark.parametrize('read, outcome', [
    (FeedRead(page_counts=[PAGE_SIZE], stopped=True), OUTCOME_OK),            # max_reviews_per_app stop
    (FeedRead(page_counts=[6] + [0] * (PAGES - 1)), OUTCOME_OK),               # small app: the feed's own end
    (FeedRead(page_counts=[0] * PAGES), OUTCOME_FEED_EMPTY),                   # Hana EZ, or the RSS failing
    (FeedRead(page_counts=[0, PAGE_SIZE], stopped=True), OUTCOME_GAP),        # page 1 empty, page 2 full
    (FeedRead(page_counts=[0, 0, 6] + [0] * (PAGES - 3)), OUTCOME_GAP),        # reviews behind empty pages
    (FeedRead(page_counts=[3, PAGE_SIZE], stopped=True), OUTCOME_GAP),        # a partial page before the last
    (FeedRead(page_counts=[], error='page 1: ConnectionError()'), OUTCOME_ERROR),
    (FeedRead(page_counts=[4], error='page 2: HTTPError()'), OUTCOME_ERROR),
])
def test_read_outcome(read, outcome):
    read.entries = [{}] * sum(read.page_counts)
    assert read_outcome(read, PAGE_SIZE) == outcome


# ========================================
# B. AppStoreCrawler over real HTTP (test DB, Parquet writes off)
# ========================================

@pytest.mark.requires_db
def test_crawler_stops_at_max_reviews_per_app_and_keeps_the_app_name(
    crawl_config, appstore_rss_server, rss_review_entry
):
    appstore_rss_server.set_pages(
        '201', [_app_metadata_entry('테스트 뱅킹'), *_page(rss_review_entry, 'p1')], _page(rss_review_entry, 'p2'),
    )
    appstore_rss_server.set_pages('202', _page(rss_review_entry, 'q1'))
    crawler = AppStoreCrawler(crawl_config.path)

    name, reviews = crawler.get_app_store_reviews_and_appname('201')
    fallback_name, _ = crawler.get_app_store_reviews_and_appname('202')
    empty_name, empty = crawler.get_app_store_reviews_and_appname('203')

    assert appstore_rss_server.requested_pages('201') == [1]
    assert (name, len(reviews), reviews[0]['id']['label']) == ('테스트 뱅킹', MAX_REVIEWS, 'p1-0')
    assert fallback_name == 'app_202'
    assert (empty_name, empty) == (None, [])


@pytest.mark.requires_db
def test_crawler_getter_logs_a_failed_request_and_returns_what_it_read(
    crawl_config, appstore_rss_server, rss_review_entry
):
    appstore_rss_server.set_pages('204', _page(rss_review_entry, 'p1', MAX_REVIEWS - 1))
    appstore_rss_server.fail('204', 2, status=503)
    crawler = AppStoreCrawler(crawl_config.path)
    crawler_logger = logging.getLogger(AppStoreCrawler.__name__.lower())  # BaseCrawler's logger name
    warnings = _WarningRecords()
    crawler_logger.addHandler(warnings)
    try:
        name, reviews = crawler.get_app_store_reviews_and_appname('204')
    finally:
        crawler_logger.removeHandler(warnings)

    assert appstore_rss_server.requested_pages('204') == [1, 2]
    assert (name, [review['id']['label'] for review in reviews]) == (
        'app_204', [f'p1-{i}' for i in range(MAX_REVIEWS - 1)],
    )
    [record] = warnings.records
    assert record.levelno == logging.ERROR
    assert record.getMessage().startswith('앱 ID 204: HTTP 요청 중 에러 발생 - page 2: HTTPError(')
    assert '503' in record.getMessage()


@pytest.mark.requires_db
def test_one_empty_app_is_a_warning_and_the_store_succeeds(crawl_config, appstore_rss_server, rss_review_entry):
    """A Hana EZ-like app whose every page is empty, between apps with reviews."""
    appstore_rss_server.set_pages('301', _page(rss_review_entry, 'a1'))
    appstore_rss_server.set_pages('303', _page(rss_review_entry, 'c1'))
    crawl_config.set_app_ids('appstore', '301', '302', '303')
    crawler_logger = logging.getLogger(AppStoreCrawler.__name__.lower())  # BaseCrawler's logger name
    warnings = _WarningRecords()
    crawler_logger.addHandler(warnings)
    try:
        result = run_crawl(crawl_config.path, store='appstore')
    finally:
        crawler_logger.removeHandler(warnings)

    assert result.status == 'success', result.message
    appstore = result.validations['appstore']
    assert [(a['app_id'], a['outcome'], a['fetched']) for a in appstore['apps']] == [
        ('301', OUTCOME_OK, MAX_REVIEWS), ('302', OUTCOME_FEED_EMPTY, 0), ('303', OUTCOME_OK, MAX_REVIEWS),
    ]
    assert appstore['warnings'] == [f'302: feed_empty (쪽별 리뷰 수 {[0] * PAGES})']
    assert appstore_rss_server.requested_pages('302') == list(range(1, PAGES + 1))
    assert [(r.levelno, r.getMessage()) for r in warnings.records] == [
        (logging.WARNING, f'앱 ID 302: feed_empty (쪽별 리뷰 수 {[0] * PAGES})'),
    ]


@pytest.mark.requires_db
def test_reviews_behind_gap_pages_are_read_and_reported_as_a_gap(
    crawl_config, appstore_rss_server, rss_review_entry
):
    appstore_rss_server.set_pages('401', [], [], _page(rss_review_entry, 'p3'))
    crawl_config.set_app_ids('appstore', '401')

    summary = AppStoreCrawler(crawl_config.path).run()

    assert appstore_rss_server.requested_pages('401') == [1, 2, 3]
    [app] = summary.apps
    assert (app.outcome, app.fetched, app.detail) == (OUTCOME_GAP, MAX_REVIEWS, f'쪽별 리뷰 수 {[0, 0, PAGE_SIZE]}')
    assert summary.failure is None


@pytest.mark.requires_db
def test_consecutive_empty_feeds_fail_the_store_and_skip_the_rest(
    crawl_config, appstore_rss_server, rss_review_entry
):
    empty = [f'51{i}' for i in range(CUTOFF)]
    appstore_rss_server.set_pages('500', _page(rss_review_entry, 'first'))
    appstore_rss_server.set_pages('599', _page(rss_review_entry, 'after'))
    crawl_config.set_app_ids('appstore', '500', *empty, '599')

    result = run_crawl(crawl_config.path, store='appstore')

    assert result.status == 'failed'
    assert result.message.startswith('crawl failed: appstore: App Store RSS:')
    outcomes = [(a['app_id'], a['outcome']) for a in result.validations['appstore']['apps']]
    assert outcomes == (
        [('500', OUTCOME_OK)] + [(i, OUTCOME_FEED_EMPTY) for i in empty]
        + [('599', OUTCOME_SKIPPED_AFTER_EMPTY_FEEDS)]
    )
    assert appstore_rss_server.requested_pages('599') == []
    assert result.input_count == MAX_REVIEWS  # what was read before the cutoff is still reported


@pytest.mark.requires_db
def test_failed_requests_before_any_review_count_toward_the_cutoff(crawl_config, appstore_rss_server):
    failing = [f'60{i}' for i in range(CUTOFF)]
    for app_id in failing:
        appstore_rss_server.fail(app_id, 1, status=429)
    crawl_config.set_app_ids('appstore', *failing, '699')

    summary = AppStoreCrawler(crawl_config.path).run()

    assert [a.outcome for a in summary.apps] == [OUTCOME_ERROR] * CUTOFF + [OUTCOME_SKIPPED_AFTER_EMPTY_FEEDS]
    assert '429' in summary.apps[0].detail
    assert summary.failure is not None


@pytest.mark.requires_db
def test_an_app_with_reviews_resets_the_empty_feed_count(crawl_config, appstore_rss_server, rss_review_entry):
    """CUTOFF - 1 empty apps, an app with reviews, CUTOFF - 1 empty apps again: no outage."""
    first = [f'70{i}' for i in range(CUTOFF - 1)]
    second = [f'71{i}' for i in range(CUTOFF - 1)]
    appstore_rss_server.set_pages('750', _page(rss_review_entry, 'mid'))
    crawl_config.set_app_ids('appstore', *first, '750', *second)

    summary = AppStoreCrawler(crawl_config.path).run()

    assert summary.failure is None
    assert [a.outcome for a in summary.apps] == (
        [OUTCOME_FEED_EMPTY] * (CUTOFF - 1) + [OUTCOME_OK] + [OUTCOME_FEED_EMPTY] * (CUTOFF - 1)
    )
