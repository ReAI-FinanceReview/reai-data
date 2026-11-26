"""기존 리뷰의 app_version을 스토어 재조회로 채우는 일회성 백필 (Issue #125)

#120 이전에 적재된 행은 평소 수집으로 버전이 채워지지 않는다. 크롤러는 이미 아는 리뷰 id를
레코드로 만들기 전에 거르고(BaseCrawler._filter_new_reviews), 로더는 app_reviews에 INSERT만
한다. 이 모듈은 app_version IS NULL인 행을 앱별로 모아 스토어를 다시 조회하고,
platform_review_id로 맞춘 행의 app_version만 UPDATE한다.

- 리뷰 id와 버전은 크롤러 레코드 빌더와 같은 정적 메서드로 읽는다
  (AppStoreCrawler / PlayStoreCrawler의 _extract_platform_review_id, _extract_app_version).
  크롤러 인스턴스는 만들지 않는다. 스토어 크롤러를 만들면 출력 디렉터리(BaseCrawler.__init__)와
  DatabaseConnector 가 생긴다.
- 기본은 dry-run이고 apply=True일 때만 쓴다. 이미 값이 있는 행은 건드리지 않으므로
  (AND app_version IS NULL) 다시 실행해도 결과가 같다. INSERT나 DELETE는 하지 않는다.
- App Store 요청 실패는 그 앱의 결과(AppResult.error)에 남기고 다음 앱으로 넘어간다.
  Play 는 google-play-scraper 가 요청 실패를 스스로 삼키므로 exhausted 로 끝난다
  (fetch_playstore_versions 참고).
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
STOP_PASSED_OLDEST_TARGET = 'passed_oldest_target'
STOP_EXHAUSTED = 'exhausted'
STOP_CAP_REACHED = 'cap_reached'
STOP_PAGE_LIMIT = 'page_limit'
STOP_ERROR = 'error'

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
    UPDATE app_reviews SET app_version = :app_version
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
    """One app's re-fetch: {platform_review_id: store version or None} for the target ids seen."""

    found: Dict[str, Optional[str]] = field(default_factory=dict)
    fetched: int = 0
    empty_pages: int = 0
    stop_reason: Optional[str] = None
    error: Optional[str] = None


@dataclass
class AppResult:
    """Per-app report row. ``updated`` counts rows written, or rows a dry-run would write.

    ``empty_pages`` counts App Store RSS pages inside ``pages_to_crawl`` that came back empty;
    reviews on such a page show up as unmatched, and a later re-run may still fill them.
    """

    platform_type: PlatformType
    platform_app_id: str
    targets: int
    fetched: int
    empty_pages: int
    matched: int
    matched_with_version: int
    updated: int
    stop_reason: Optional[str]
    error: Optional[str]

    @property
    def unmatched(self) -> int:
        return self.targets - self.matched


@dataclass
class BackfillReport:
    apply: bool
    apps: List[AppResult]
    targets_without_app: int

    @property
    def has_errors(self) -> bool:
        return any(app.error for app in self.apps)


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


def play_stop_reason(
    *,
    targets: int,
    matched: int,
    fetched: int,
    max_reviews: int,
    last_page_size: int,
    has_next_page: bool,
    oldest_fetched: Optional[datetime],
    oldest_target: datetime,
) -> Optional[str]:
    """Whether to stop paging one Play app after a page; ``None`` means fetch the next page.

    Pages come newest first, so once they reach further back than the oldest target (minus
    REVIEWED_AT_SLACK) the next page would only hold reviews older than every target.
    """
    if matched >= targets:
        return STOP_ALL_MATCHED
    if oldest_fetched is not None and oldest_fetched < oldest_target - REVIEWED_AT_SLACK:
        return STOP_PASSED_OLDEST_TARGET
    if last_page_size == 0 or not has_next_page:
        return STOP_EXHAUSTED
    if fetched >= max_reviews:
        return STOP_CAP_REACHED
    return None


def fetch_appstore_versions(target: AppTargets, settings: BackfillSettings) -> FetchResult:
    """Read every RSS page up to ``appstore.pages_to_crawl``, stopping early once all match.

    An empty page does not end the read: the RSS intermittently answers 200 with no entries for
    pages in the middle of the range (on 2026-11-26 one app returned pages 1-2 and 5-10 empty
    and 3-4 full), so empty pages are counted instead.
    """
    result = FetchResult()
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
        if not entries:
            result.empty_pages += 1
            continue
        result.fetched += len(entries)
        result.found.update(match_versions(PlatformType.APPSTORE, entries, target.reviewed_at))
        if len(result.found) == len(target.reviewed_at):
            result.stop_reason = STOP_ALL_MATCHED
            return result
    result.stop_reason = STOP_PAGE_LIMIT
    return result


def fetch_playstore_versions(target: AppTargets, settings: BackfillSettings) -> FetchResult:
    """Page newest first with continuation tokens until play_stop_reason says stop.

    google-play-scraper takes no timeout and does not raise on a failed request: reviews()
    catches it and returns what it has with no next-page token. Such a failure therefore ends as
    ``exhausted``, and it is not told apart from the real end of the reviews.
    """
    result = FetchResult()
    oldest_target = min(target.reviewed_at.values())
    oldest_fetched: Optional[datetime] = None
    token = None
    while True:
        if token is not None:
            time.sleep(settings.delay_seconds)
        page, token = play_reviews(
            target.platform_app_id,
            lang=settings.playstore_language,
            country=settings.playstore_country,
            sort=Sort.NEWEST,
            count=settings.playstore_page_size,
            continuation_token=token,
        )
        result.fetched += len(page)
        result.found.update(match_versions(PlatformType.PLAYSTORE, page, target.reviewed_at))
        # `at` is naive local time (datetime.fromtimestamp); astimezone() reads it as such.
        page_times = [review['at'].astimezone(timezone.utc) for review in page if review.get('at')]
        if page_times:
            oldest_fetched = min(page_times + ([oldest_fetched] if oldest_fetched else []))
        result.stop_reason = play_stop_reason(
            targets=len(target.reviewed_at),
            matched=len(result.found),
            fetched=result.fetched,
            max_reviews=settings.playstore_max_reviews_per_app,
            last_page_size=len(page),
            has_next_page=token.token is not None,
            oldest_fetched=oldest_fetched,
            oldest_target=oldest_target,
        )
        if result.stop_reason:
            return result


_FETCHERS = {
    PlatformType.APPSTORE: fetch_appstore_versions,
    PlatformType.PLAYSTORE: fetch_playstore_versions,
}


def settle_app(session, target: AppTargets, fetch: FetchResult, apply: bool) -> AppResult:
    """Write (``apply``) or count (dry-run) the versions ``fetch`` found for ``target``.

    Only rows whose app_version is still NULL are touched. Matched reviews the store gave no
    version for, and unmatched reviews, are counted but not written. Does not commit.
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
    return AppResult(
        platform_type=target.platform_type,
        platform_app_id=target.platform_app_id,
        targets=len(target.reviewed_at),
        fetched=fetch.fetched,
        empty_pages=fetch.empty_pages,
        matched=len(fetch.found),
        matched_with_version=len(versions),
        updated=updated,
        stop_reason=fetch.stop_reason,
        error=fetch.error,
    )


def backfill_app_versions(
    config_path: str, platforms: Sequence[PlatformType], apply: bool
) -> BackfillReport:
    """Re-fetch every app that has rows without app_version and fill what the store still serves.

    With ``apply`` each app's updates are committed before the next app is fetched; a dry-run
    never commits.
    """
    settings = load_settings(config_path)
    session = DatabaseConnector(config_path).get_session()
    try:
        targets = load_targets(session, platforms)
        targets_without_app = count_targets_without_app(session, platforms)
        results = []
        for index, target in enumerate(targets):
            if index:
                time.sleep(settings.delay_seconds)
            fetch = _FETCHERS[target.platform_type](target, settings)
            result = settle_app(session, target, fetch, apply)
            if apply:
                session.commit()
            logger.info(
                f"[{index + 1}/{len(targets)}] {target.platform_type.value} {target.platform_app_id}: "
                f"대상 {result.targets}, 조회 {result.fetched}, 빈 페이지 {result.empty_pages}, "
                f"맞춤 {result.matched}, "
                f"{'갱신' if apply else '갱신 예정'} {result.updated}, 중단 {result.stop_reason}"
                + (f", 오류 {result.error}" if result.error else "")
            )
            results.append(result)
        return BackfillReport(apply=apply, apps=results, targets_without_app=targets_without_app)
    finally:
        session.close()


_COUNT_COLUMNS = (
    'targets', 'fetched', 'empty_pages', 'matched', 'matched_with_version', 'updated', 'unmatched',
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
