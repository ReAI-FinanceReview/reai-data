"""App Store 고객 리뷰 RSS를 앱 하나씩 쪽 단위로 읽는 한 곳 (Issue #127)

평소 크롤(AppStoreCrawler.read_reviews)과 app_version 백필(app_version_backfill.fetch_appstore_versions)이
같은 코드로 읽는다.

- URL은 ``appstore.reviews_rss_url`` 템플릿을 country, page, app_id로 채워 만든다.
- 리뷰 항목은 ``im:rating``이 있는 항목만이다. RSS가 1쪽 맨 앞에 붙이기도 하는 앱 메타 항목은
  ``im:rating``이 없고 ``im:name``(앱 이름)을 가진다.
- RSS는 리뷰가 있는데도 HTTP 200에 항목 없는 쪽을 돌려줄 때가 있다(2026-11-26: 모든 쪽이 빈 앱,
  1-2쪽과 5-10쪽이 비고 3-4쪽만 찬 앱). 그래서 빈 쪽에서 멈추지 않고 요청한 쪽까지 읽고, 읽은 뒤에
  ``FeedRead.feed_empty``(모든 쪽이 빔)와 ``appstore_feed_complete``(피드를 끝까지 봤는가)로 판정한다.
- 요청이 실패하면(HTTP 상태, 네트워크, JSON) 그 쪽에서 읽기를 멈추고 ``error``에 쪽 번호와 예외를 남긴다.
"""

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence

import requests

from .crawl_outcome import OUTCOME_ERROR, OUTCOME_FEED_EMPTY, OUTCOME_GAP, OUTCOME_OK


def _feed_entries(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    entries = payload.get('feed', {}).get('entry') or []
    return [entries] if isinstance(entries, dict) else entries


def appstore_review_entries(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Review entries of one RSS page; an app-metadata entry (no ``im:rating``) is dropped."""
    return [entry for entry in _feed_entries(payload) if 'im:rating' in entry]


def appstore_app_name(payload: Dict[str, Any]) -> Optional[str]:
    """``im:name`` of the page's app-metadata entry, or ``None`` when the page carries none."""
    for entry in _feed_entries(payload):
        if 'im:rating' not in entry and 'im:name' in entry:
            return entry['im:name'].get('label')
    return None


def appstore_feed_complete(page_counts: Sequence[int], page_size: int) -> bool:
    """Whether RSS pages read in order, ``page_counts`` review entries each, show the whole feed.

    The feed is complete when every page before the last non-empty one is full and the pages
    after it, if any, are empty while it is partial: that is how the feed itself ends, so small
    apps with a few dozen reviews come out complete. An empty page before a non-empty one, or
    empty pages after a full page, is what the RSS outages observed on 2026-11-26 looked like
    (every page empty; pages 1-2 and 5-10 empty with 3-4 full), so such a read is not complete.
    """
    non_empty = [index for index, count in enumerate(page_counts) if count]
    if not non_empty:
        return False
    last = non_empty[-1]
    if any(count < page_size for count in page_counts[:last]):
        return False
    if last < len(page_counts) - 1:
        return page_counts[last] < page_size
    return True


@dataclass
class FeedRead:
    """One app's RSS pages, read in order from page 1.

    ``page_counts`` holds the number of review entries on each page read and ``entries`` the
    entries themselves. ``error`` names the page whose request failed and ended the read;
    ``stopped`` says ``on_page`` ended it.
    """

    page_counts: List[int] = field(default_factory=list)
    entries: List[Dict[str, Any]] = field(default_factory=list)
    app_name: Optional[str] = None
    error: Optional[str] = None
    stopped: bool = False

    @property
    def fetched(self) -> int:
        return len(self.entries)

    @property
    def empty_pages(self) -> int:
        return self.page_counts.count(0)

    @property
    def feed_empty(self) -> bool:
        """Every page requested answered without a review entry: no reviews, or the RSS failing."""
        return self.error is None and not self.entries


def read_feed(
    app_id: str,
    *,
    url_template: str,
    country: str,
    pages: int,
    timeout: float,
    delay: float,
    on_page: Optional[Callable[[FeedRead, List[Dict[str, Any]]], bool]] = None,
) -> FeedRead:
    """Read RSS pages 1..``pages`` of ``app_id``, waiting ``delay`` seconds between pages.

    An empty page does not end the read. ``on_page(read, page_entries)`` runs after every page,
    empty ones included; returning True ends the read there. A failed request (HTTP status,
    network, JSON decode) ends it with ``error`` set to the page and the exception.
    """
    read = FeedRead()
    for page in range(1, pages + 1):
        if page > 1:
            time.sleep(delay)
        url = url_template.format(country=country, page=page, app_id=app_id)
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:  # HTTP, network and JSON decode errors
            read.error = f'page {page}: {exc!r}'
            return read
        entries = appstore_review_entries(payload)
        if read.app_name is None:
            read.app_name = appstore_app_name(payload)
        read.page_counts.append(len(entries))
        read.entries.extend(entries)
        if on_page is not None and on_page(read, entries):
            read.stopped = True
            return read
    return read


def read_outcome(read: FeedRead, page_size: int) -> str:
    """The crawl outcome of one app's read: ``error``, ``feed_empty``, ``gap`` or ``ok``.

    ``gap`` is a read with reviews that appstore_feed_complete does not accept: an empty or
    partial page came before the last page read, so reviews the feed should have shown there
    may be missing. A read that ``on_page`` stopped ends on a page with reviews, so it is ``ok``
    exactly when every page before that one was full.
    """
    if read.error:
        return OUTCOME_ERROR
    if read.feed_empty:
        return OUTCOME_FEED_EMPTY
    if not appstore_feed_complete(read.page_counts, page_size):
        return OUTCOME_GAP
    return OUTCOME_OK
