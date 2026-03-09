"""Play Store 크롤러 클래스 (Issue #19: Batch DLQ)

Crawl 단계: API → Parquet 쓰기 + ingestion_batch PENDING 등록
Load 단계(BatchLoader)에서 Parquet → ReviewMasterIndex 적재
"""

import os
from google_play_scraper import reviews, Sort, app as gp_app
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional
from uuid import UUID

from .base_crawler import BaseCrawler
from .exceptions import ParquetWriteError
from src.utils.db_connector import DatabaseConnector
from src.utils.parquet_writer import ParquetWriter
from src.utils.path_resolver import get_medallion_paths
from src.models.base import Base
from src.models.enums import PlatformType
from src.schemas.parquet.app_review import AppReviewSchema


class PlayStoreCrawler(BaseCrawler):
    """Play Store 리뷰 크롤러 (Batch DLQ Architecture)

    crawl 단계에서 Parquet 파일을 생성하고 ingestion_batch PENDING 레코드를 등록합니다.
    ReviewMasterIndex 생성은 load 단계(BatchLoader)에서 처리합니다.
    """

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

        # Parquet Writer 초기화
        self.enable_parquet = os.getenv('ENABLE_PARQUET_WRITE', 'true').lower() == 'true'

        if self.enable_parquet:
            paths = get_medallion_paths(create_if_missing=True)
            bronze_path = paths['bronze_dir'] / 'app_reviews'

            self.parquet_writer = ParquetWriter(
                base_path=str(bronze_path),
                partition_by='year_month_day'
            )
            self.logger.info(f"Parquet writer initialized: {bronze_path}")
        else:
            self.logger.warning("Parquet write disabled (ENABLE_PARQUET_WRITE=false)")
            self.parquet_writer = None

    def crawl_reviews(self, app_id: str) -> List[Dict[str, Any]]:
        """리뷰 크롤링 (추상 메서드 구현)"""
        return self.get_playstore_reviews(app_id)

    def _get_platform_type(self) -> PlatformType:
        """Get platform type for PlayStore."""
        return PlatformType.PLAYSTORE

    def _extract_platform_review_id(self, review_data: Dict[str, Any]) -> str:
        """Extract reviewId from Play Store review data."""
        return review_data.get('reviewId', '')

    def _parse_reviewed_at(self, review_data: Dict[str, Any]) -> datetime:
        """Parse reviewed_at timestamp from Play Store review data."""
        reviewed_at = review_data.get('at')
        if not isinstance(reviewed_at, datetime):
            reviewed_at = datetime.now(timezone.utc)
        elif reviewed_at.tzinfo is None:
            reviewed_at = reviewed_at.replace(tzinfo=timezone.utc)
        return reviewed_at

    def get_playstore_reviews(
        self,
        app_id: str,
        lang: str = None,
        country: str = None,
        count: int = None
    ) -> List[Dict[str, Any]]:
        """지정된 앱 ID로 Google Play Store 리뷰를 가져옵니다."""
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

    def get_app_details(self, app_id: str) -> Optional[Dict[str, Any]]:
        """Play Store 앱 상세 정보를 가져옵니다."""
        try:
            return gp_app(app_id, lang=self.language, country=self.country)
        except Exception as e:
            self.logger.error(f"앱 상세 정보 가져오기 실패 - {app_id}: {e}")
            return None

    def _build_parquet_records(
        self,
        reviews_data: List[Dict[str, Any]],
        review_id_map: Dict[str, UUID],
        reviewed_at_cache: Dict[str, datetime],
        app
    ) -> List[AppReviewSchema]:
        """Play Store 전용 Parquet 레코드 빌더."""
        parquet_records = []
        for review_data in reviews_data:
            platform_review_id = self._extract_platform_review_id(review_data)
            if not platform_review_id or platform_review_id not in review_id_map:
                continue

            review_text = review_data.get('content', '')
            if not review_text or not review_text.strip():
                continue

            try:
                rating = int(review_data.get('score') or 1)
            except (TypeError, ValueError):
                rating = 1
            if rating < 1:
                rating = 1
            elif rating > 5:
                rating = 5

            parquet_records.append(AppReviewSchema(
                review_id=str(review_id_map[platform_review_id]),
                app_id=str(app.app_id),
                platform_type='PLAYSTORE',
                platform_review_id=platform_review_id,
                reviewer_name=review_data.get('userName'),
                review_text=review_text,
                rating=rating,
                reviewed_at=reviewed_at_cache[platform_review_id],
                is_reply=False,
                reply_comment=review_data.get('replyContent')
            ))
        return parquet_records

    def run(self) -> None:
        """크롤러 실행 (Batch DLQ Architecture)"""
        self.logger.info("Play Store 크롤러 실행 시작 (Batch DLQ mode)")

        self.db_connector.create_tables(Base)

        app_ids = self.read_app_ids(self.app_ids_file)
        if not app_ids:
            raise ValueError(f"앱 ID 파일에서 유효한 ID를 찾을 수 없습니다: {self.app_ids_file}")

        self.logger.info(f"총 {len(app_ids)}개 앱의 리뷰를 크롤링합니다.")

        successful_apps = 0
        total_records = 0

        for i, app_id in enumerate(app_ids, 1):
            self.logger.info(f"[{i}/{len(app_ids)}] 앱 ID: {app_id} 크롤링 시작...")

            try:
                app_details = self.get_app_details(app_id)
                app_name = app_details.get('title') if app_details else f'app_{app_id}'

                reviews_data = self.get_playstore_reviews(app_id)

                if not reviews_data:
                    self.logger.warning(f"앱 ID {app_id}: 수집된 리뷰 없음")
                    continue

                _, count, _ = self.save_crawl_batch(
                    app_id, app_name, reviews_data, self._build_parquet_records
                )

                if count > 0:
                    self.logger.info(f"앱 ID {app_id}: {count}개 배치 등록 완료")
                    total_records += count
                    successful_apps += 1
                else:
                    self.logger.info(f"앱 ID {app_id}: 새로운 리뷰 없음")

            except ParquetWriteError as e:
                self.logger.error(f"앱 ID {app_id} Parquet 쓰기 실패: {e}")
                continue

            except Exception as e:
                self.logger.error(f"앱 ID {app_id} 크롤링 실패: {e}")
                continue

            if i < len(app_ids):
                self.wait_between_requests()

        self.logger.info(f"크롤링 완료 - 성공: {successful_apps}/{len(app_ids)}개 앱")
        self.logger.info(f"총 {total_records}개 리뷰 배치 등록 완료")
        self.logger.info(f"Parquet: {self.parquet_writer.base_path if self.parquet_writer else 'Disabled'}")
