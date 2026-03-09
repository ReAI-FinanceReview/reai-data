"""
통합 크롤러 클래스
"""
from datetime import datetime

from ..utils.logger import get_logger
from .appstore_crawler import AppStoreCrawler
from .playstore_crawler import PlayStoreCrawler


class UnifiedCrawler:
    """App Store와 Play Store를 순차적으로 실행하는 오케스트레이터.

    크롤러가 아니므로 BaseCrawler를 상속하지 않습니다.
    Parquet 포맷 일관성은 각 플랫폼 크롤러가 BaseCrawler를 통해 보장합니다.
    """

    def __init__(self, config_path: str = None):
        self.logger = get_logger(__name__)
        self.appstore_crawler = AppStoreCrawler(config_path)
        self.playstore_crawler = PlayStoreCrawler(config_path)
        self.logger.info("통합 크롤러 초기화 완료")

    def run(self) -> None:
        """통합 크롤러 실행"""
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
