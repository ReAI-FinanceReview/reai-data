"""
Play Store 크롤러 클래스
"""
from google_play_scraper import reviews, Sort, app as gp_app
from datetime import datetime
from typing import List, Dict, Any

from .base_crawler import BaseCrawler
from src.utils.db_connector import DatabaseConnector
from src.models.base import Base
from src.models.app import App
from src.models.review import Review


class PlayStoreCrawler(BaseCrawler):
    """Play Store 리뷰 크롤러"""

    def __init__(self, config_path: str = None):
        """
        Initialize the PlayStoreCrawler and load Play Store–specific configuration and resources.
        
        Parameters:
            config_path (str|None): Path to the crawler configuration file. If omitted, the crawler uses the default configuration resolution from the base class.
        
        Detailed behavior:
            - Sets `language` (default 'ko'), `country` (default 'kr'), and `reviews_per_app` (default 100) from the 'playstore' section of the configuration.
            - Sets `app_ids_file` from the 'app_ids.playstore' configuration entry (default 'config/app_ids/playstore_app_ids.txt').
            - Initializes `db_connector` with `config_path` or the fallback 'config/crawler_config.yml'.
        """
        super().__init__(config_path)

        # Play Store 특화 설정
        self.language = self.config.get('playstore', {}).get('language', 'ko')
        self.country = self.config.get('playstore', {}).get('country', 'kr')
        self.reviews_per_app = self.config.get('playstore', {}).get('reviews_per_app', 100)

        # 앱 ID 파일 경로
        self.app_ids_file = self.config.get('app_ids', {}).get('playstore', 'config/app_ids/playstore_app_ids.txt')

        # 데이터베이스 커넥터 초기화
        self.db_connector = DatabaseConnector(config_path or 'config/crawler_config.yml')

    def crawl_reviews(self, app_id: str) -> List[Dict[str, Any]]:
        """
        Fetches reviews for the given Play Store app identifier.
        
        Parameters:
            app_id (str): Play Store package name (e.g., "com.example.app") identifying the app to crawl.
        
        Returns:
            reviews (List[Dict[str, Any]]): A list of review dictionaries returned from the Play Store; empty list if none or on error.
        """
        return self.get_playstore_reviews(app_id)

    def get_playstore_reviews(self, app_id: str, lang: str = None, country: str = None, count: int = None) -> List[Dict[str, Any]]:
        """
        Fetches reviews for a Google Play app.
        
        Parameters:
            app_id (str): The Play Store application ID to fetch reviews for.
            lang (str, optional): Language code to request reviews in; uses the instance default if None.
            country (str, optional): Country code to request reviews from; uses the instance default if None.
            count (int, optional): Maximum number of reviews to retrieve; uses the instance default if None.
        
        Returns:
            List[dict]: A list of review dictionaries as returned by the Play Store scraper; returns an empty list on error.
        """
        if lang is None:
            lang = self.language
        if country is None:
            country = self.country
        if count is None:
            count = self.reviews_per_app
        
        try:
            self.logger.info(f"Play Store 리뷰 수집 시작 - 앱 ID: {app_id}, 개수: {count}")
            
            result, _ = reviews(
                app_id,
                lang=lang,
                country=country,
                sort=Sort.NEWEST,
                count=count
            )
            
            self.logger.info(f"앱 ID {app_id}: {len(result)}개 리뷰 수집 완료")
            return result
            
        except Exception as e:
            self.logger.error(f"앱 ID {app_id}의 리뷰 데이터를 가져오지 못했습니다: {e}")
            return []

    def run(self) -> None:
        """
        Run the Play Store crawling process for configured app IDs.
        
        Reads app IDs from the configured file, ensures database tables exist, and iterates over each app to fetch current app details and recent Play Store reviews. For each app found in the local database, updates its updated_at timestamp, inserts any new Review records (deduplicating by platform review ID), commits per-app changes, and rolls back on per-app errors. Waits between requests as configured and logs progress and a final summary. Raises ValueError if no valid app IDs are found.
        """
        self.logger.info("Play Store 크롤러 실행 시작")

        self.db_connector.create_tables(Base)
        session = self.db_connector.get_session()

        app_ids = self.read_app_ids(self.app_ids_file)
        if not app_ids:
            raise ValueError(f"앱 ID 파일에서 유효한 ID를 찾을 수 없습니다: {self.app_ids_file}")

        self.logger.info(f"총 {len(app_ids)}개 앱의 리뷰를 크롤링합니다.")

        successful_apps = 0
        total_reviews_added = 0

        for i, app_id in enumerate(app_ids, 1):
            self.logger.info(f"[{i}/{len(app_ids)}] 앱 ID: {app_id} 크롤링 시작...")

            try:
                app_details = gp_app(app_id, lang=self.language, country=self.country)
                app_name = app_details.get('title')

                # App 정보 조회 (playstore_id로 조회)
                app = session.query(App).filter_by(playstore_id=app_id).first()
                if not app:
                    self.logger.warning(f"앱 ID {app_id} ({app_name})가 apps 테이블에 없습니다. 스킵합니다.")
                    continue
                else:
                    app.updated_at = datetime.now()

                reviews_data = self.get_playstore_reviews(app_id)

                if not reviews_data:
                    self.logger.warning(f"앱 ID {app_id}: 수집된 리뷰 없음")
                    session.commit()
                    continue

                reviews_added_for_app = 0
                for review_data in reviews_data:
                    platform_review_id = review_data.get('reviewId')
                    if not platform_review_id:
                        continue

                    # 중복 리뷰 확인
                    existing_review = session.query(Review).filter_by(
                        platform_review_id=platform_review_id,
                        app_id=app.id,
                        platform='PLAYSTORE'
                    ).first()
                    if existing_review:
                        continue

                    # reviewed_at 처리
                    reviewed_at = review_data.get('at')
                    if not isinstance(reviewed_at, datetime):
                        reviewed_at = datetime.now()

                    review = Review(
                        app_id=app.id,
                        platform='PLAYSTORE',
                        country_code='kr',
                        platform_review_id=platform_review_id,
                        reviewer_name=review_data.get('userName'),
                        review_text=review_data.get('content', ''),
                        rating=review_data.get('score', 0),
                        app_version=review_data.get('reviewCreatedVersion'),
                        reviewed_at=reviewed_at
                    )
                    session.add(review)
                    reviews_added_for_app += 1

                if reviews_added_for_app > 0:
                    self.logger.info(f"앱 ID {app_id}: {reviews_added_for_app}개의 새로운 리뷰 추가")
                    total_reviews_added += reviews_added_for_app
                else:
                    self.logger.info(f"앱 ID {app_id}: 새로운 리뷰 없음")

                successful_apps += 1
                session.commit()

            except Exception as e:
                self.logger.error(f"앱 ID {app_id} 크롤링 실패: {e}")
                session.rollback()
                continue

            if i < len(app_ids):
                self.wait_between_requests()

        session.close()
        self.logger.info(f"크롤링 완료 - 성공: {successful_apps}/{len(app_ids)}개 앱")
        self.logger.info(f"총 {total_reviews_added}개 리뷰가 데이터베이스에 추가되었습니다.")