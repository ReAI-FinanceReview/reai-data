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
        super().__init__(config_path)

        # 개별 크롤러 인스턴스 생성
        self.appstore_crawler = AppStoreCrawler(config_path)
        self.playstore_crawler = PlayStoreCrawler(config_path)

        self.logger.info("통합 크롤러 초기화 완료")

    def crawl_reviews(self, app_id: str) -> List[Dict[str, Any]]:
        """
        리뷰 크롤링 (통합 크롤러에서는 사용하지 않음)
        UnifiedCrawler는 개별 크롤러들을 오케스트레이션만 함
        """
        raise NotImplementedError("UnifiedCrawler는 crawl_reviews를 직접 호출하지 않습니다. run()을 사용하세요.")

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
