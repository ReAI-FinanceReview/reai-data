"""
통합 크롤러 클래스
"""
from datetime import datetime
from typing import List, Dict, Any

from .base_crawler import BaseCrawler
from .appstore_crawler import AppStoreCrawler
from .playstore_crawler import PlayStoreCrawler


class UnifiedCrawler(BaseCrawler):
    """App Store와 Play Store를 통합으로 크롤링하는 클래스"""

    def __init__(self, config_path: str = None):
        """
        Initialize the UnifiedCrawler and instantiate App Store and Play Store crawler components.
        
        Parameters:
            config_path (str | None): Optional path to a configuration file used to initialize the base crawler and to pass to the AppStoreCrawler and PlayStoreCrawler instances. If None, defaults from the base class are used.
        """
        super().__init__(config_path)

        # 개별 크롤러 인스턴스 생성
        self.appstore_crawler = AppStoreCrawler(config_path)
        self.playstore_crawler = PlayStoreCrawler(config_path)

        self.logger.info("통합 크롤러 초기화 완료")

    def crawl_reviews(self, app_id: str) -> List[Dict[str, Any]]:
        """
        Placeholder method for crawling reviews for a single app; UnifiedCrawler does not implement per-app crawling.
        
        Parameters:
            app_id (str): Identifier of the app whose reviews would be crawled.
        
        Raises:
            NotImplementedError: Always raised. Use UnifiedCrawler.run() to perform crawling across stores.
        """
        raise NotImplementedError("UnifiedCrawler는 crawl_reviews를 직접 호출하지 않습니다. run()을 사용하세요.")

    def run(self) -> None:
        """
        Run the unified crawler by executing App Store and Play Store crawls and reporting results.
        
        Triggers each internal crawler's run() method in sequence, logs start and completion messages, records the total elapsed time, and logs any failures encountered during each store's crawl.
        """
        self.logger.info("=" * 60)
        self.logger.info("통합 크롤러 실행 시작")
        self.logger.info("=" * 60)
        
        start_time = datetime.now()
        
        # App Store 크롤링
        try:
            self.logger.info("🍎 App Store 크롤링 시작...")
            self.appstore_crawler.run()
            self.logger.info("✅ App Store 크롤링 완료")
        except Exception as e:
            self.logger.error(f"❌ App Store 크롤링 실패: {e}")
        
        # Play Store 크롤링
        try:
            self.logger.info("🤖 Play Store 크롤링 시작...")
            self.playstore_crawler.run()
            self.logger.info("✅ Play Store 크롤링 완료")
        except Exception as e:
            self.logger.error(f"❌ Play Store 크롤링 실패: {e}")

        duration = (datetime.now() - start_time).total_seconds()
        self.logger.info("=" * 60)
        self.logger.info("📊 통합 크롤링 완료")
        self.logger.info(f"   - 소요 시간: {duration:.1f}초")
        self.logger.info("=" * 60)