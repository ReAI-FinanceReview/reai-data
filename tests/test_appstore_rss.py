"""App Store RSS reading shared by the routine crawl and the app_version backfill (Issue #127).

The reader and AppStoreCrawler run over real HTTP against ``appstore_rss_server`` on 127.0.0.1,
whose entries copy the keys of the live customerreviews RSS JSON (``rss_review_entry``). The
crawler tests use the real test database with Parquet writes off (``crawl_config``), so a crawl
reads the store and writes nothing.
"""
from pathlib import Path

import pytest
import yaml

from src.crawlers.appstore_crawler import AppStoreCrawler
from src.crawlers.appstore_rss import read_feed

CONFIG = yaml.safe_load(
    (Path(__file__).resolve().parents[1] / 'config' / 'crawler_config.yml').read_text(encoding='utf-8')
)
PAGE_SIZE = CONFIG['appstore']['rss_page_size']
PAGES = CONFIG['appstore']['pages_to_crawl']
MAX_REVIEWS = CONFIG['appstore']['max_reviews_per_app']


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
