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
        리뷰 크롤링 (추상 메서드 구현)
        """
        return self.get_playstore_reviews(app_id)

    def get_playstore_reviews(self, app_id: str, lang: str = None, country: str = None, count: int = None) -> List[Dict[str, Any]]:
        """
        지정된 앱 ID로 Google Play Store 리뷰를 가져옵니다.
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
        """크롤러 실행"""
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
