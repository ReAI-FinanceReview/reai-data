"""기존 리뷰의 app_version을 스토어 재조회로 채우는 일회성 백필 (Issue #125)

#120 이전에 적재된 행은 평소 수집으로 버전이 채워지지 않는다. 크롤러는 이미 아는 리뷰 id를
레코드로 만들기 전에 거르고(BaseCrawler._filter_new_reviews), 로더는 app_reviews에 INSERT만
한다. 이 모듈은 app_version IS NULL인 행을 앱별로 모아 스토어를 다시 조회하고,
platform_review_id로 맞춘 행의 app_version과 updated_at만 UPDATE한다.

- 리뷰 id와 버전은 크롤러 레코드 빌더와 같은 정적 메서드로 읽는다
  (AppStoreCrawler / PlayStoreCrawler의 _extract_platform_review_id, _extract_app_version).
  크롤러 인스턴스는 만들지 않는다. 스토어 크롤러를 만들면 출력 디렉터리(BaseCrawler.__init__)와
  DatabaseConnector 가 생긴다.
- 기본은 dry-run이고 apply=True일 때만 쓴다. 이미 값이 있는 행은 건드리지 않으므로
  (AND app_version IS NULL) 다시 실행해도 결과가 같다. INSERT나 DELETE는 하지 않는다.
- 못 맞춘 리뷰는 둘로 나눈다. unmatched_confirmed는 스토어 응답을 끝까지 정상으로 읽었는데도
  없는 리뷰, unverified는 빈 응답이나 실패한 요청 때문에 없다고 단정할 수 없는 리뷰다.
- Play의 끝은 두 번 보고 정한다. google-play-scraper 1.2.7의 reviews()는 서버가 요청보다 적은
  리뷰와 다음 token을 주면 같은 호출 안에서 요청을 더 보내고, 그 뒤쪽 요청이 실패하면 예외를
  삼킨 채 그때까지 모은 리뷰를 token None과 함께 돌려준다. 이 모양은 진짜 끝과 같으므로, 리뷰가
  있고 token이 없는 쪽은 같은 continuation token으로 한 번 더 요청한다. 두 번째 응답이 같은 리뷰
  id들을 담고 또 token이 없을 때만 exhausted(확정)이고, 아니면 end_not_confirmed(미확인)다.
  남은 한계: 같은 실패가 두 요청에서 똑같이 되풀이되면(같은 부분 목록, token 없음) 여전히 진짜
  끝으로 보인다.
- 스토어를 기다리는 동안 DB 트랜잭션을 열어 두지 않는다. 대상을 읽은 뒤 바로 닫고, 앱마다
  apply는 commit, dry-run은 rollback으로 닫는다.
- 한 앱의 조회나 쓰기에서 예외가 나면 그 앱의 변경만 rollback하고 traceback을 로그에 남긴 뒤,
  그 앱을 error(대상 전부 unverified)로 기록하고 다음 앱으로 넘어간다. 앞서 commit한 앱은 남는다.
"""

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Collection, Dict, Iterable, List, Optional, Sequence
from uuid import UUID

import requests
import yaml
from google_play_scraper import Sort, reviews as play_reviews
from sqlalchemy import text

from src.models.enums import PlatformType
from src.utils.db_connector import DatabaseConnector
from src.utils.logger import get_logger
from .appstore_crawler import AppStoreCrawler
from .playstore_crawler import PlayStoreCrawler

logger = get_logger('app_version_backfill')

# App Store first: its RSS only reaches the newest pages_to_crawl pages, so every new review
# pushes an old one out of reach for good. Play pages back much further.
PLATFORM_ORDER = (PlatformType.APPSTORE, PlatformType.PLAYSTORE)
PLATFORM_CHOICES = {
    'appstore': (PlatformType.APPSTORE,),
    'playstore': (PlatformType.PLAYSTORE,),
    'all': PLATFORM_ORDER,
}

STOP_ALL_MATCHED = 'all_matched'
# App Store
STOP_PAGE_LIMIT = 'page_limit'  # read every page up to pages_to_crawl
STOP_FEED_EMPTY = 'feed_empty'  # every page empty (Play: the first page empty, no next page)
STOP_ERROR = 'error'
STOP_SKIPPED_AFTER_EMPTY_FEEDS = 'skipped_after_empty_feeds'  # not requested, see cutoff
# Play
STOP_PASSED_OLDEST_TARGET = 'passed_oldest_target'
STOP_EXHAUSTED = 'exhausted'  # a page with reviews and no next-page token, twice alike: the end
STOP_END_NOT_CONFIRMED = 'end_not_confirmed'  # that page answered differently the second time
STOP_EMPTY_PAGE = 'empty_page'  # an empty page after pages with reviews
STOP_CAP_REACHED = 'cap_reached'

# Play stops after which the unmatched targets are known to be absent from the store. An empty
# page can be a request google-play-scraper failed and swallowed, and the cap stops early.
# ``exhausted`` is complete only once confirm_play_end has seen the last page twice alike.
PLAY_COMPLETE_STOPS = frozenset({STOP_ALL_MATCHED, STOP_PASSED_OLDEST_TARGET})

# PlayStoreCrawler._parse_reviewed_at labels google-play-scraper's local-time `at` as UTC, so a
# stored reviewed_at can be later than the store's own timestamp by the crawl host's UTC offset.
# No UTC offset reaches a full day, so this much slack keeps that shift from ending paging early.
REVIEWED_AT_SLACK = timedelta(days=1)

_CRAWLERS = {PlatformType.APPSTORE: AppStoreCrawler, PlatformType.PLAYSTORE: PlayStoreCrawler}

_TARGETS_SQL = text("""
    SELECT a.app_id, a.platform_type::text AS platform_type, a.platform_app_id,
           r.platform_review_id, r.reviewed_at
    FROM app_reviews r
    JOIN apps a ON a.app_id = r.app_id
    WHERE r.app_version IS NULL
      AND a.platform_type::text = ANY(:platforms)
""")

_TARGETS_WITHOUT_APP_SQL = text("""
    SELECT count(*)
    FROM app_reviews r
    LEFT JOIN apps a ON a.app_id = r.app_id
    WHERE r.app_version IS NULL
      AND r.platform_type::text = ANY(:platforms)
      AND (a.app_id IS NULL OR a.platform_type IS NULL)
""")

_UPDATE_SQL = text("""
    UPDATE app_reviews SET app_version = :app_version, updated_at = NOW()
    WHERE app_id = :app_id AND platform_review_id = :platform_review_id AND app_version IS NULL
""")

# Same predicate as _UPDATE_SQL, so a dry-run reports exactly the rows --apply would change.
_WOULD_UPDATE_SQL = text("""
    SELECT count(*) FROM app_reviews
    WHERE app_id = :app_id AND platform_review_id = ANY(:platform_review_ids) AND app_version IS NULL
""")


@dataclass(frozen=True)
class BackfillSettings:
    """Store request settings, all read from crawler_config.yml with no coded fallbacks."""

    delay_seconds: float
    timeout_seconds: float
    appstore_reviews_rss_url: str
    appstore_country: str
    appstore_pages: int
    appstore_rss_page_size: int
    appstore_max_consecutive_empty_feeds: int
    playstore_language: str
    playstore_country: str
    playstore_page_size: int
    playstore_max_reviews_per_app: int


def load_settings(config_path: str) -> BackfillSettings:
    """Read the backfill settings from ``config_path``; a missing key raises ``KeyError``.

    ``appstore.max_reviews_per_app`` is deliberately not applied: it caps a routine crawl, while
    the backfill has to read every page the RSS still serves.
    """
    with open(config_path, encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return BackfillSettings(
        delay_seconds=config['global']['delay_between_requests'],
        timeout_seconds=config['global']['timeout'],
        appstore_reviews_rss_url=config['appstore']['reviews_rss_url'],
        appstore_country=config['appstore']['country'],
        appstore_pages=config['appstore']['pages_to_crawl'],
        appstore_rss_page_size=config['appstore']['rss_page_size'],
        appstore_max_consecutive_empty_feeds=config['appstore']['backfill_max_consecutive_empty_feeds'],
        playstore_language=config['playstore']['language'],
        playstore_country=config['playstore']['country'],
        playstore_page_size=config['playstore']['backfill_page_size'],
        playstore_max_reviews_per_app=config['playstore']['backfill_max_reviews_per_app'],
    )


@dataclass
class AppTargets:
    """One app's rows that still lack app_version, as {platform_review_id: reviewed_at}."""

    app_id: UUID
    platform_type: PlatformType
    platform_app_id: str
    reviewed_at: Dict[str, datetime] = field(default_factory=dict)


@dataclass
class FetchResult:
    """One app's re-fetch: {platform_review_id: store version or None} for the target ids seen.

    ``complete`` says the store's answer was read in full, so a target that was not seen is
    really absent; otherwise the unseen targets are unverified.
    """

    found: Dict[str, Optional[str]] = field(default_factory=dict)
    fetched: int = 0
    empty_pages: int = 0
    stop_reason: Optional[str] = None
    complete: bool = False
    error: Optional[str] = None
    oldest_fetched: Optional[datetime] = None


@dataclass
class AppResult:
    """Per-app report row. ``updated`` counts rows written, or rows a dry-run would write.

    Targets that were not matched are either ``unmatched_confirmed`` (the store's answer was read
    in full and they are not in it) or ``unverified`` (empty pages, a failed request, the Play cap
    or the App Store cutoff may have hidden them; a later re-run may still fill them).
    """

    platform_type: PlatformType
    platform_app_id: str
    targets: int
    fetched: int
    empty_pages: int
    matched: int
    matched_with_version: int
    updated: int
    unmatched_confirmed: int
    unverified: int
    stop_reason: Optional[str]
    error: Optional[str]


@dataclass
class BackfillReport:
    apply: bool
    apps: List[AppResult]
    targets_without_app: int

    @property
    def has_errors(self) -> bool:
        return any(app.error for app in self.apps)

    @property
    def has_unverified(self) -> bool:
        return any(app.unverified for app in self.apps)


def load_targets(session, platforms: Sequence[PlatformType]) -> List[AppTargets]:
    """Group the rows with ``app_version IS NULL`` by app; App Store apps come first."""
    rows = session.execute(_TARGETS_SQL, {'platforms': [p.value for p in platforms]}).all()
    by_app: Dict[UUID, AppTargets] = {}
    for row in rows:
        target = by_app.get(row.app_id)
        if target is None:
            target = by_app[row.app_id] = AppTargets(
                row.app_id, PlatformType(row.platform_type), row.platform_app_id
            )
        target.reviewed_at[row.platform_review_id] = row.reviewed_at
    return sorted(
        by_app.values(),
        key=lambda t: (PLATFORM_ORDER.index(t.platform_type), t.platform_app_id, str(t.app_id)),
    )


def count_targets_without_app(session, platforms: Sequence[PlatformType]) -> int:
    """Rows with ``app_version IS NULL`` that cannot be re-fetched: no apps row, or no platform."""
    return session.execute(
        _TARGETS_WITHOUT_APP_SQL, {'platforms': [p.value for p in platforms]}
    ).scalar_one()


def match_versions(
    platform_type: PlatformType,
    reviews: Iterable[Dict[str, Any]],
    target_ids: Collection[str],
) -> Dict[str, Optional[str]]:
    """Map each target id found in ``reviews`` to the version the store reports for it.

    Uses the crawler's own id/version extraction, so the backfill reads the same fields as the
    records the crawler writes.
    """
    crawler = _CRAWLERS[platform_type]
    found = {}
    for review in reviews:
        platform_review_id = crawler._extract_platform_review_id(review)
        if platform_review_id in target_ids:
            found[platform_review_id] = crawler._extract_app_version(review)
    return found


def appstore_review_entries(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Review entries of one RSS page; an app-metadata entry (no ``im:rating``) is dropped."""
    entries = payload.get('feed', {}).get('entry') or []
    if isinstance(entries, dict):
        entries = [entries]
    return [entry for entry in entries if 'im:rating' in entry]


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


def play_stop_reason(
    *,
    target_times: Collection[datetime],
    matched: int,
    fetched: int,
    max_reviews: int,
    last_page_size: int,
    has_next_page: bool,
    oldest_fetched: Optional[datetime],
) -> Optional[str]:
    """Whether to stop paging one Play app after a page; ``None`` means fetch the next page.

    ``target_times`` are the targets' stored reviewed_at. Pages come newest first, so once they
    reach further back than the oldest target (minus REVIEWED_AT_SLACK) the next page would only
    hold reviews older than every target. google-play-scraper turns a failed request into an
    empty page with no next-page token, so an empty page is ``feed_empty`` when it is the first
    one and ``empty_page`` otherwise; neither proves the remaining targets are gone.
    """
    if matched >= len(target_times):
        return STOP_ALL_MATCHED
    if oldest_fetched is not None and oldest_fetched < min(target_times) - REVIEWED_AT_SLACK:
        return STOP_PASSED_OLDEST_TARGET
    if last_page_size == 0:
        return STOP_FEED_EMPTY if fetched == 0 else STOP_EMPTY_PAGE
    if not has_next_page:
        return STOP_EXHAUSTED
    if fetched >= max_reviews:
        return STOP_CAP_REACHED
    return None


def scan_play_page(
    result: FetchResult,
    page: List[Dict[str, Any]],
    has_next_page: bool,
    target: AppTargets,
    max_reviews: int,
) -> Optional[str]:
    """Fold one google-play-scraper page into ``result``; return the stop reason, or ``None``.

    ``exhausted`` returned here is provisional and leaves ``result`` incomplete until
    confirm_play_end has seen the page a second time.
    """
    result.fetched += len(page)
    result.empty_pages += not page
    result.found.update(match_versions(PlatformType.PLAYSTORE, page, target.reviewed_at))
    # `at` is naive local time (datetime.fromtimestamp); astimezone() reads it as such.
    page_times = [review['at'].astimezone(timezone.utc) for review in page if review.get('at')]
    if result.oldest_fetched is not None:
        page_times.append(result.oldest_fetched)
    if page_times:
        result.oldest_fetched = min(page_times)
    result.stop_reason = play_stop_reason(
        target_times=target.reviewed_at.values(),
        matched=len(result.found),
        fetched=result.fetched,
        max_reviews=max_reviews,
        last_page_size=len(page),
        has_next_page=has_next_page,
        oldest_fetched=result.oldest_fetched,
    )
    result.complete = result.stop_reason in PLAY_COMPLETE_STOPS
    return result.stop_reason


def play_end_stop_reason(
    first_ids: Sequence[str],
    first_has_next_page: bool,
    second_ids: Sequence[str],
    second_has_next_page: bool,
) -> str:
    """Whether a Play page that came with no next-page token is the store's real end.

    The page is requested twice with the same continuation token. google-play-scraper returns
    that same shape (reviews, no token) when a later request inside one reviews() call fails,
    so only two answers with the same review ids and no token confirm the end.
    """
    if (
        first_ids
        and not first_has_next_page
        and not second_has_next_page
        and sorted(first_ids) == sorted(second_ids)
    ):
        return STOP_EXHAUSTED
    return STOP_END_NOT_CONFIRMED


def confirm_play_end(
    result: FetchResult,
    first_page: List[Dict[str, Any]],
    second_page: List[Dict[str, Any]],
    second_has_next_page: bool,
    target: AppTargets,
) -> str:
    """Fold the second answer for a would-be last Play page into ``result`` and settle the stop.

    Targets the second answer holds are merged first; the second answer's reviews are not added
    to ``fetched``, which counts each page's first answer.
    """
    result.found.update(match_versions(PlatformType.PLAYSTORE, second_page, target.reviewed_at))
    if len(result.found) >= len(target.reviewed_at):
        result.stop_reason = STOP_ALL_MATCHED
    else:
        result.stop_reason = play_end_stop_reason(
            [PlayStoreCrawler._extract_platform_review_id(review) for review in first_page],
            False,  # scan_play_page returns exhausted only for a page without a next-page token
            [PlayStoreCrawler._extract_platform_review_id(review) for review in second_page],
            second_has_next_page,
        )
    result.complete = result.stop_reason in (STOP_ALL_MATCHED, STOP_EXHAUSTED)
    return result.stop_reason


def fetch_appstore_versions(target: AppTargets, settings: BackfillSettings) -> FetchResult:
    """Read every RSS page up to ``appstore.pages_to_crawl``, stopping early once all match.

    An empty page does not end the read, because the RSS also answers 200 with no entries when
    it is failing; appstore_feed_complete decides afterwards whether the read was complete.
    """
    result = FetchResult()
    page_counts = []
    for page in range(1, settings.appstore_pages + 1):
        if page > 1:
            time.sleep(settings.delay_seconds)
        url = settings.appstore_reviews_rss_url.format(
            country=settings.appstore_country, page=page, app_id=target.platform_app_id
        )
        try:
            response = requests.get(url, timeout=settings.timeout_seconds)
            response.raise_for_status()
            entries = appstore_review_entries(response.json())
        except requests.RequestException as exc:  # HTTP, network and JSON decode errors
            result.stop_reason = STOP_ERROR
            result.error = f'page {page}: {exc!r}'
            return result
        page_counts.append(len(entries))
        result.empty_pages += not entries
        result.fetched += len(entries)
        result.found.update(match_versions(PlatformType.APPSTORE, entries, target.reviewed_at))
        if len(result.found) == len(target.reviewed_at):
            result.stop_reason = STOP_ALL_MATCHED
            result.complete = True
            return result
    if result.fetched == 0:
        result.stop_reason = STOP_FEED_EMPTY
    else:
        result.stop_reason = STOP_PAGE_LIMIT
        result.complete = appstore_feed_complete(page_counts, settings.appstore_rss_page_size)
    return result


def fetch_playstore_versions(target: AppTargets, settings: BackfillSettings) -> FetchResult:
    """Page newest first with continuation tokens until scan_play_page says stop.

    google-play-scraper takes no timeout and does not raise on a failed request: reviews()
    catches it and returns an empty page with no next-page token, which scan_play_page reports
    as ``feed_empty`` or ``empty_page`` (unverified) rather than as the end of the reviews.

    A page with reviews and no next-page token is not taken as the end on its own. When a
    response holds fewer reviews than requested together with a token, reviews() sends further
    requests within the same call, and if one of those fails it returns the reviews collected so
    far with no token, which looks exactly like the end. So that page is requested once more with
    the same continuation token, and confirm_play_end keeps ``exhausted`` only if both answers
    match; otherwise the stop is ``end_not_confirmed``. A failure that repeats identically on both
    requests still passes as the end.
    """

    def request(continuation_token):
        return play_reviews(
            target.platform_app_id,
            lang=settings.playstore_language,
            country=settings.playstore_country,
            sort=Sort.NEWEST,
            count=settings.playstore_page_size,
            continuation_token=continuation_token,
        )

    result = FetchResult()
    token = None
    while True:
        if token is not None:
            time.sleep(settings.delay_seconds)
        requested_with = token
        page, token = request(requested_with)
        stop = scan_play_page(
            result, page, token.token is not None, target, settings.playstore_max_reviews_per_app
        )
        if stop == STOP_EXHAUSTED:
            time.sleep(settings.delay_seconds)
            again, again_token = request(requested_with)
            stop = confirm_play_end(result, page, again, again_token.token is not None, target)
        if stop:
            return result


_FETCHERS = {
    PlatformType.APPSTORE: fetch_appstore_versions,
    PlatformType.PLAYSTORE: fetch_playstore_versions,
}


def settle_app(session, target: AppTargets, fetch: FetchResult, apply: bool) -> AppResult:
    """Write (``apply``) or count (dry-run) the versions ``fetch`` found for ``target``.

    Only rows whose app_version is still NULL are touched, and their updated_at is set to the
    time of the write. Matched reviews the store gave no version for, and unmatched reviews, are
    counted but not written. Does not commit.
    """
    versions = {rid: version for rid, version in fetch.found.items() if version}
    if not versions:
        updated = 0
    elif apply:
        updated = sum(
            session.execute(
                _UPDATE_SQL,
                {'app_version': version, 'app_id': target.app_id, 'platform_review_id': rid},
            ).rowcount
            for rid, version in versions.items()
        )
    else:
        updated = session.execute(
            _WOULD_UPDATE_SQL,
            {'app_id': target.app_id, 'platform_review_ids': list(versions)},
        ).scalar_one()
    unmatched = len(target.reviewed_at) - len(fetch.found)
    return AppResult(
        platform_type=target.platform_type,
        platform_app_id=target.platform_app_id,
        targets=len(target.reviewed_at),
        fetched=fetch.fetched,
        empty_pages=fetch.empty_pages,
        matched=len(fetch.found),
        matched_with_version=len(versions),
        updated=updated,
        unmatched_confirmed=unmatched if fetch.complete else 0,
        unverified=0 if fetch.complete else unmatched,
        stop_reason=fetch.stop_reason,
        error=fetch.error,
    )


def backfill_app_versions(
    config_path: str, platforms: Sequence[PlatformType], apply: bool
) -> BackfillReport:
    """Re-fetch every app that has rows without app_version and fill what the store still serves.

    No transaction stays open while a store is being asked: the target query is closed before
    the first request, and each app ends with a commit (``apply``) or a rollback (dry-run).

    Once ``appstore.backfill_max_consecutive_empty_feeds`` App Store apps in a row come back
    ``feed_empty`` the RSS is treated as down: the remaining App Store apps are not requested
    and end as ``skipped_after_empty_feeds`` with every target unverified. Play still runs.

    An exception while one app is fetched or written rolls back that app only, is logged with
    its traceback, and makes that app an ``error`` row with every target unverified; the run
    goes on with the next app, and apps committed before it stay committed.
    """
    settings = load_settings(config_path)
    session = DatabaseConnector(config_path).get_session()
    try:
        targets = load_targets(session, platforms)
        targets_without_app = count_targets_without_app(session, platforms)
        session.rollback()
        results = []
        empty_feeds_in_a_row = 0
        for index, target in enumerate(targets):
            label = f"[{index + 1}/{len(targets)}] {target.platform_type.value} {target.platform_app_id}"
            fetch = None
            try:
                appstore = target.platform_type is PlatformType.APPSTORE
                if appstore and empty_feeds_in_a_row >= settings.appstore_max_consecutive_empty_feeds:
                    fetch = FetchResult(stop_reason=STOP_SKIPPED_AFTER_EMPTY_FEEDS)
                else:
                    if index:
                        time.sleep(settings.delay_seconds)
                    fetch = _FETCHERS[target.platform_type](target, settings)
                    if appstore:
                        empty_feeds_in_a_row = (
                            empty_feeds_in_a_row + 1 if fetch.stop_reason == STOP_FEED_EMPTY else 0
                        )
                result = settle_app(session, target, fetch, apply)
                if apply:
                    session.commit()
                else:
                    session.rollback()
            except Exception as exc:  # noqa: BLE001 - one app's failure is recorded, not fatal
                session.rollback()
                logger.exception(f"{label}: 실패, 이 앱의 변경은 되돌림")
                result = AppResult(
                    platform_type=target.platform_type,
                    platform_app_id=target.platform_app_id,
                    targets=len(target.reviewed_at),
                    fetched=fetch.fetched if fetch else 0,
                    empty_pages=fetch.empty_pages if fetch else 0,
                    matched=0,
                    matched_with_version=0,
                    updated=0,
                    unmatched_confirmed=0,
                    unverified=len(target.reviewed_at),
                    stop_reason=STOP_ERROR,
                    error=repr(exc),
                )
            logger.info(
                f"{label}: "
                f"대상 {result.targets}, 조회 {result.fetched}, 빈 페이지 {result.empty_pages}, "
                f"맞춤 {result.matched}, {'갱신' if apply else '갱신 예정'} {result.updated}, "
                f"확정 누락 {result.unmatched_confirmed}, 미확인 {result.unverified}, "
                f"중단 {result.stop_reason}"
                + (f", 오류 {result.error}" if result.error else "")
            )
            results.append(result)
        return BackfillReport(apply=apply, apps=results, targets_without_app=targets_without_app)
    finally:
        session.close()


_COUNT_COLUMNS = (
    'targets', 'fetched', 'empty_pages', 'matched', 'matched_with_version', 'updated',
    'unmatched_confirmed', 'unverified',
)


def format_report(report: BackfillReport) -> str:
    """Plain-text table: one row per app, then totals per platform."""
    updated_label = 'updated' if report.apply else 'would_update'
    labels = [updated_label if c == 'updated' else c for c in _COUNT_COLUMNS]
    id_width = max([len('platform_app_id')] + [len(app.platform_app_id) for app in report.apps])
    lines = [
        f"mode: {'apply' if report.apply else 'dry-run (no writes)'}",
        '  '.join([f"{'platform':<9}", f"{'platform_app_id':<{id_width}}", *labels, 'stop', 'error']),
    ]
    for app in report.apps:
        counts = [f'{getattr(app, c):>{len(label)}}' for c, label in zip(_COUNT_COLUMNS, labels, strict=True)]
        lines.append('  '.join([
            f'{app.platform_type.value:<9}', f'{app.platform_app_id:<{id_width}}', *counts,
            app.stop_reason or '', app.error or '',
        ]).rstrip())
    lines.append('totals:')
    for platform in PLATFORM_ORDER:
        apps = [app for app in report.apps if app.platform_type == platform]
        if not apps:
            continue
        sums = ', '.join(
            f'{label}={sum(getattr(app, c) for app in apps)}'
            for c, label in zip(_COUNT_COLUMNS, labels, strict=True)
        )
        errors = sum(1 for app in apps if app.error)
        lines.append(f'  {platform.value:<10} apps={len(apps)}, {sums}, apps_with_error={errors}')
    lines.append(
        f'targets without a re-fetchable app (no apps row or no platform_type): '
        f'{report.targets_without_app}'
    )
    return '\n'.join(lines)
