"""Play Store 크롤러 클래스 (Phase 3: NAS-first Architecture)

This module implements the Play Store crawler with NAS-first dual-write pattern
to ensure distributed consistency between PostgreSQL and Parquet storage.

Key Features:
- 2-Phase Commit: Parquet write → DB commit
- Idempotency via platform_review_id
- State machine tracking via processing_status
- Lightweight retry mechanism
"""

import os
from google_play_scraper import reviews, Sort, app as gp_app
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional
from uuid6 import uuid7

from .base_crawler import BaseCrawler
from src.utils.db_connector import DatabaseConnector
from src.utils.parquet_writer import ParquetWriter
from src.utils.path_resolver import get_medallion_paths
from src.models.base import Base
from src.models.apps import App
from src.models.review_master_index import ReviewMasterIndex
from src.models.enums import PlatformType, ProcessingStatusType
from src.schemas.parquet.app_review import AppReviewSchema


class ParquetWriteError(Exception):
    """Raised when Parquet write fails (Phase 1)."""
    pass


class DBCommitError(Exception):
    """Raised when DB commit fails after Parquet success (Phase 2)."""
    pass


class PlayStoreCrawler(BaseCrawler):
    """Play Store 리뷰 크롤러 (NAS-first Architecture)

    Features:
    - NAS-first dual-write pattern
    - Distributed consistency guarantees
    - Retry mechanism for failed writes
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
                partition_by='year_month'
            )
            self.logger.info(f"Parquet writer initialized: {bronze_path}")
        else:
            self.logger.warning("Parquet write disabled (ENABLE_PARQUET_WRITE=false)")
            self.parquet_writer = None

    def crawl_reviews(self, app_id: str) -> List[Dict[str, Any]]:
        """리뷰 크롤링 (추상 메서드 구현)"""
        return self.get_playstore_reviews(app_id)

    def get_playstore_reviews(
        self,
        app_id: str,
        lang: str = None,
        country: str = None,
        count: int = None
    ) -> List[Dict[str, Any]]:
        """지정된 앱 ID로 Google Play Store 리뷰를 가져옵니다.

        Args:
            app_id: Play Store app package name
            lang: Language code (default: self.language)
            country: Country code (default: self.country)
            count: Number of reviews to fetch (default: self.reviews_per_app)

        Returns:
            List of review dictionaries from the API
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

    def get_app_details(self, app_id: str) -> Optional[Dict[str, Any]]:
        """Play Store 앱 상세 정보를 가져옵니다.

        Args:
            app_id: Play Store app package name

        Returns:
            App details dictionary or None if failed
        """
        try:
            return gp_app(app_id, lang=self.language, country=self.country)
        except Exception as e:
            self.logger.error(f"앱 상세 정보 가져오기 실패 - {app_id}: {e}")
            return None

    def save_to_parquet_and_database(
        self,
        app_id: str,
        app_name: str,
        reviews_data: List[Dict[str, Any]]
    ) -> int:
        """NAS-first dual-write: Parquet → DB (2-phase commit)

        Phase 1: Write to Parquet (NAS)
        Phase 2: Commit to DB (only if Phase 1 succeeds)

        This ensures no Ghost Records (DB without Parquet data).

        Args:
            app_id: Play Store app package name
            app_name: App display name
            reviews_data: List of review dictionaries from API

        Returns:
            Number of new reviews added

        Raises:
            ParquetWriteError: If Parquet write fails
            DBCommitError: If DB commit fails (Parquet already written)
        """
        session = self.db_connector.get_session()

        try:
            # ========================================
            # 0. App 확인/생성
            # ========================================
            app = session.query(App).filter_by(
                platform_app_id=app_id,
                platform_type=PlatformType.PLAYSTORE
            ).first()

            if not app:
                app = App(
                    app_id=uuid7(),
                    platform_app_id=app_id,
                    name=app_name or f'app_{app_id}',
                    platform_type=PlatformType.PLAYSTORE
                )
                session.add(app)
                session.flush()
                self.logger.info(f"Created new app: {app.name} ({app_id})")

            # ========================================
            # 1. Idempotency Check (중복 방지)
            # ========================================
            existing_platform_ids = set(
                row.platform_review_id for row in
                session.query(ReviewMasterIndex.platform_review_id).filter_by(
                    app_id=app.app_id,
                    platform_type=PlatformType.PLAYSTORE
                ).all()
            )

            new_reviews_data = []
            for review in reviews_data:
                platform_review_id = review.get('reviewId')
                if platform_review_id and platform_review_id not in existing_platform_ids:
                    new_reviews_data.append(review)

            if not new_reviews_data:
                self.logger.info(f"No new reviews for app {app_id} (all duplicates)")
                session.commit()  # Commit app if new
                return 0

            self.logger.info(f"Found {len(new_reviews_data)} new reviews for {app_id}")

            # ========================================
            # 2. PHASE 1: Write to Parquet (NAS-first)
            # ========================================
            parquet_records = []
            review_id_map = {}  # platform_review_id → review_id

            for review_data in new_reviews_data:
                platform_review_id = review_data.get('reviewId')
                if not platform_review_id:
                    continue

                # Generate UUID v7 (time-sortable)
                review_id = str(uuid7())
                review_id_map[platform_review_id] = review_id

                # Parse reviewed_at
                reviewed_at = review_data.get('at')
                if not isinstance(reviewed_at, datetime):
                    reviewed_at = datetime.now(timezone.utc)
                elif reviewed_at.tzinfo is None:
                    # Make timezone-aware if naive
                    reviewed_at = reviewed_at.replace(tzinfo=timezone.utc)

                # Parse review_text
                review_text = review_data.get('content', '')
                if not review_text or not review_text.strip():
                    continue  # Skip empty reviews

                # Parse rating (Play Store uses 'score')
                rating = review_data.get('score', 0)
                if rating < 1:
                    rating = 1
                elif rating > 5:
                    rating = 5

                # Create Parquet record
                parquet_record = AppReviewSchema(
                    review_id=review_id,
                    app_id=str(app.app_id),
                    platform_type='PLAYSTORE',
                    platform_review_id=platform_review_id,
                    reviewer_name=review_data.get('userName'),
                    review_text=review_text,
                    rating=rating,
                    reviewed_at=reviewed_at,
                    is_reply=False,
                    reply_comment=review_data.get('replyContent')
                )
                parquet_records.append(parquet_record)

            if not parquet_records:
                self.logger.info(f"No valid reviews to write for {app_id}")
                session.commit()
                return 0

            # Write to Parquet (MUST succeed before DB commit)
            if not self.enable_parquet:
                self.logger.warning("Parquet write disabled (ENABLE_PARQUET_WRITE=false)")
                # Skip Parquet, proceed to DB (legacy mode for dev)
            else:
                try:
                    parquet_file_path = self.parquet_writer.write_batch(parquet_records)
                    self.logger.info(
                        f"✅ PHASE 1 SUCCESS: Wrote {len(parquet_records)} reviews "
                        f"to Parquet: {parquet_file_path}"
                    )
                except Exception as e:
                    self.logger.error(f"❌ PHASE 1 FAILED: Parquet write error: {e}")
                    # DO NOT proceed to DB commit
                    raise ParquetWriteError(f"Parquet write failed: {e}") from e

            # ========================================
            # 3. PHASE 2: Write to DB (only if Phase 1 succeeded)
            # ========================================
            master_index_records = []
            now = datetime.now(timezone.utc)

            for review_data in new_reviews_data:
                platform_review_id = review_data.get('reviewId')
                if not platform_review_id or platform_review_id not in review_id_map:
                    continue

                review_id = review_id_map[platform_review_id]

                # Parse reviewed_at again (same as above)
                reviewed_at = review_data.get('at')
                if not isinstance(reviewed_at, datetime):
                    reviewed_at = datetime.now(timezone.utc)
                elif reviewed_at.tzinfo is None:
                    reviewed_at = reviewed_at.replace(tzinfo=timezone.utc)

                master_index = ReviewMasterIndex(
                    review_id=review_id,
                    app_id=app.app_id,
                    platform_review_id=platform_review_id,
                    platform_type=PlatformType.PLAYSTORE,
                    review_created_at=reviewed_at,
                    ingested_at=now,
                    processing_status=ProcessingStatusType.RAW,  # State Machine
                    parquet_written_at=now if self.enable_parquet else None,  # Track Parquet write
                    is_active=True,
                    is_reply=False,
                    error_message=None,  # No error
                    retry_count=0
                )
                master_index_records.append(master_index)

            try:
                session.add_all(master_index_records)
                session.commit()

                self.logger.info(
                    f"✅ PHASE 2 SUCCESS: Committed {len(master_index_records)} reviews "
                    f"to DB (status=RAW)"
                )
                return len(master_index_records)

            except Exception as e:
                session.rollback()
                self.logger.error(
                    f"❌ PHASE 2 FAILED: DB commit error: {e}\n"
                    f"WARNING: Parquet data already written! Manual cleanup may be needed."
                )
                # Parquet already written, but DB failed
                # This is acceptable - can retry DB commit later
                raise DBCommitError(f"DB commit failed (Parquet OK): {e}") from e

        except ParquetWriteError:
            # Phase 1 failed - nothing committed
            session.rollback()
            raise

        except DBCommitError:
            # Phase 2 failed - Parquet OK, DB failed
            # Can retry DB commit later using platform_review_id
            raise

        finally:
            session.close()

    def retry_failed_reviews(self, max_retries: int = 3) -> int:
        """Retry failed reviews (Parquet write failures).

        Query reviews with processing_status = FAILED and retry_count < max_retries.
        Attempt to re-write to Parquet and update DB status.

        Args:
            max_retries: Maximum retry attempts (default: 3)

        Returns:
            Number of reviews retried
        """
        session = self.db_connector.get_session()

        try:
            # Query failed reviews
            failed_reviews = session.query(ReviewMasterIndex).filter(
                ReviewMasterIndex.processing_status == ProcessingStatusType.FAILED,
                ReviewMasterIndex.retry_count < max_retries,
                ReviewMasterIndex.platform_type == PlatformType.PLAYSTORE
            ).all()

            if not failed_reviews:
                self.logger.info("No failed reviews to retry")
                return 0

            self.logger.info(f"Found {len(failed_reviews)} failed reviews to retry")

            retried_count = 0
            for failed_review in failed_reviews:
                try:
                    # For MVP, just increment retry count
                    # Full implementation would re-crawl or reconstruct data
                    self.logger.warning(
                        f"Retry not fully implemented for review {failed_review.review_id}. "
                        f"Manual intervention needed."
                    )

                    # Increment retry count
                    failed_review.retry_count += 1
                    failed_review.error_message = f"Retry {failed_review.retry_count}/{max_retries} pending"
                    retried_count += 1

                except Exception as e:
                    self.logger.error(f"Retry failed for {failed_review.review_id}: {e}")
                    failed_review.retry_count += 1
                    failed_review.error_message = str(e)

            session.commit()
            return retried_count

        finally:
            session.close()

    def run(self) -> None:
        """크롤러 실행 (NAS-first Architecture)"""
        self.logger.info("Play Store 크롤러 실행 시작 (NAS-first mode)")

        self.db_connector.create_tables(Base)

        app_ids = self.read_app_ids(self.app_ids_file)
        if not app_ids:
            raise ValueError(f"앱 ID 파일에서 유효한 ID를 찾을 수 없습니다: {self.app_ids_file}")

        self.logger.info(f"총 {len(app_ids)}개 앱의 리뷰를 크롤링합니다.")

        successful_apps = 0
        total_reviews_added = 0

        for i, app_id in enumerate(app_ids, 1):
            self.logger.info(f"[{i}/{len(app_ids)}] 앱 ID: {app_id} 크롤링 시작...")

            try:
                # Get app details for name
                app_details = self.get_app_details(app_id)
                app_name = app_details.get('title') if app_details else f'app_{app_id}'

                # Get reviews
                reviews_data = self.get_playstore_reviews(app_id)

                if not reviews_data:
                    self.logger.warning(f"앱 ID {app_id}: 수집된 리뷰 없음")
                    continue

                # NAS-first dual-write
                reviews_added = self.save_to_parquet_and_database(app_id, app_name, reviews_data)

                if reviews_added > 0:
                    self.logger.info(f"앱 ID {app_id}: {reviews_added}개의 새로운 리뷰 추가")
                    total_reviews_added += reviews_added
                    successful_apps += 1
                else:
                    self.logger.info(f"앱 ID {app_id}: 새로운 리뷰 없음")

            except ParquetWriteError as e:
                self.logger.error(f"앱 ID {app_id} Parquet 쓰기 실패: {e}")
                # Mark as failed in DB (if possible)
                continue

            except DBCommitError as e:
                self.logger.error(f"앱 ID {app_id} DB commit 실패: {e}")
                # Parquet OK, DB failed - can retry later
                continue

            except Exception as e:
                self.logger.error(f"앱 ID {app_id} 크롤링 실패: {e}")
                continue

            if i < len(app_ids):
                self.wait_between_requests()

        self.logger.info(f"크롤링 완료 - 성공: {successful_apps}/{len(app_ids)}개 앱")
        self.logger.info(f"총 {total_reviews_added}개 리뷰가 추가되었습니다.")
        self.logger.info(f"Parquet: {self.parquet_writer.base_path if self.parquet_writer else 'Disabled'}")
