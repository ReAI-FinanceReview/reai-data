"""
통합 크롤러 클래스
"""
from datetime import datetime
from typing import List, Optional, Sequence

from ..utils.logger import get_logger
from .appstore_crawler import AppStoreCrawler
from .crawl_outcome import StoreCrawlSummary
from .playstore_crawler import PlayStoreCrawler

STORE_CRAWLERS = {'appstore': AppStoreCrawler, 'playstore': PlayStoreCrawler}
# scripts/crawl_reviews.py --store 와 run_crawl(store=...) 가 받는 값. all 은 App Store, Play 순서
CRAWL_STORE_CHOICES = {
    'appstore': ('appstore',),
    'playstore': ('playstore',),
    'all': ('appstore', 'playstore'),
}
_STORE_LABELS = {'appstore': '🍎 App Store', 'playstore': '🤖 Play Store'}


class UnifiedCrawler:
    """고른 스토어의 크롤러를 순서대로 실행하는 오케스트레이터.

    크롤러가 아니므로 BaseCrawler를 상속하지 않습니다.
    Parquet 포맷 일관성은 각 플랫폼 크롤러가 BaseCrawler를 통해 보장합니다.
    한 스토어의 예외는 그 스토어의 실패(StoreCrawlSummary.failure)로 남기고 다음 스토어를 실행하므로,
    한 스토어가 다른 스토어를 막지 않으면서도 실패가 묻히지 않습니다.
    """

    def __init__(self, config_path: Optional[str] = None, stores: Sequence[str] = CRAWL_STORE_CHOICES['all']):
        self.logger = get_logger(__name__)
        self.config_path = config_path
        self.stores = tuple(stores)
        self.logger.info(f"통합 크롤러 초기화 완료: {', '.join(self.stores)}")

    def run(self) -> List[StoreCrawlSummary]:
        """고른 스토어를 순서대로 크롤하고 스토어마다 결과를 돌려준다."""
        self.logger.info("=" * 60)
        self.logger.info("통합 크롤러 실행 시작")
        self.logger.info("=" * 60)

        start_time = datetime.now()
        summaries = []

        for store in self.stores:
            label = _STORE_LABELS[store]
            self.logger.info(f"{label} 크롤링 시작...")
            try:
                summary = STORE_CRAWLERS[store](self.config_path).run()
            except Exception as e:
                self.logger.exception(f"❌ {label} 크롤링 실패: {e}")
                summary = StoreCrawlSummary(store=store, failure=f"{type(e).__name__}: {e}")
            else:
                if summary.failure:
                    self.logger.error(f"❌ {label} 크롤링 실패: {summary.failure}")
                else:
                    self.logger.info(f"✅ {label} 크롤링 완료 (경고 {len(summary.warnings)}개)")
            summaries.append(summary)

        duration = (datetime.now() - start_time).total_seconds()
        self.logger.info("=" * 60)
        self.logger.info("📊 통합 크롤링 완료")
        self.logger.info(f"   - 소요 시간: {duration:.1f}초")
        self.logger.info("=" * 60)
        return summaries
