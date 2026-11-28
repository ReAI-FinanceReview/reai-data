"""크롤 결과: 앱별 결과와 스토어 단위 성패 (Issue #127)

스토어 크롤러의 run()은 StoreCrawlSummary를 돌려준다. 앱 하나가 비거나(feed_empty) 중간 쪽이
빠진(gap) 것은 경고로 남기고, 스토어 전체를 실패로 보는 판정은 ``failure``에 적는다.
pipeline 단계(run_crawl)는 ``failure``가 있는 스토어를 실패로 돌려준다.
"""

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Sequence

OUTCOME_OK = 'ok'
OUTCOME_FEED_EMPTY = 'feed_empty'  # no review at all: the store had none, or answered empty while failing
OUTCOME_GAP = 'gap'  # App Store: reviews read, but an empty or partial page came before the last one read
OUTCOME_ERROR = 'error'  # a request or the app's processing failed; reviews read before it are kept
OUTCOME_SKIPPED_AFTER_EMPTY_FEEDS = 'skipped_after_empty_feeds'  # App Store: not requested, see the cutoff


@dataclass
class AppCrawlResult:
    """One app's crawl: reviews taken from the store (``fetched``) and those not yet loaded (``new``)."""

    app_id: str
    outcome: str
    fetched: int = 0
    new: int = 0
    detail: Optional[str] = None


@dataclass
class StoreCrawlSummary:
    """One store's crawl. ``failure`` is set when the store as a whole failed; app outcomes other
    than ``ok`` are warnings."""

    store: str
    apps: List[AppCrawlResult] = field(default_factory=list)
    storage_path: Optional[str] = None
    failure: Optional[str] = None

    @property
    def warnings(self) -> List[AppCrawlResult]:
        return [app for app in self.apps if app.outcome != OUTCOME_OK]

    def as_dict(self) -> Dict[str, Any]:
        return {
            'status': 'failed' if self.failure else 'success',
            'failure': self.failure,
            'fetched': sum(app.fetched for app in self.apps),
            'new': sum(app.new for app in self.apps),
            'storage_path': self.storage_path,
            'warnings': [
                f'{app.app_id}: {app.outcome}' + (f' ({app.detail})' if app.detail else '')
                for app in self.warnings
            ],
            'apps': [asdict(app) for app in self.apps],
        }


def playstore_failure(apps: Sequence[AppCrawlResult]) -> Optional[str]:
    """The Play store fails when every app came back with zero reviews.

    An app whose first request failed is ``error`` with zero reviews (a later page failing keeps
    the reviews read before it), and Play answers an app id it does not know with an empty page,
    so one such app is a warning, while every app empty or failing at once is the store or the
    client failing.
    """
    if apps and all(app.fetched == 0 for app in apps):
        return f'Play Store: 앱 {len(apps)}개 모두 리뷰 0건'
    return None
