"""App Store 크롤러 클래스 (Phase 3: NAS-first Architecture)

This module implements the App Store crawler with NAS-first dual-write pattern
to ensure distributed consistency between PostgreSQL and Parquet storage.

Key Features:
- 2-Phase Commit: Parquet write → DB commit
- Idempotency via platform_review_id
- State machine tracking via processing_status
- Lightweight retry mechanism
"""

import requests
import json
import os
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


class AppStoreCrawler(BaseCrawler):
    """App Store 리뷰 크롤러 (NAS-first Architecture)

    Features:
    - NAS-first dual-write pattern
    - Distributed consistency guarantees
    - Retry mechanism for failed writes
    """

    def __init__(self, config_path: str = None):
        super().__init__(config_path)

        # App Store 특화 설정
        self.country = self.config.get('appstore', {}).get('country', 'kr')
        self.pages_to_crawl = self.config.get('appstore', {}).get('pages_to_crawl', 10)
        self.max_reviews_per_app = self.config.get('appstore', {}).get('max_reviews_per_app', 500)

        # 앱 ID 파일 경로
        self.app_ids_file = self.config.get('app_ids', {}).get('appstore', 'config/app_ids/appstore_app_ids.txt')

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
        _, reviews = self.get_app_store_reviews_and_appname(app_id)
        return reviews

    def _get_platform_type(self) -> PlatformType:
        """Get platform type for AppStore."""
        return PlatformType.APPSTORE

    def _extract_platform_review_id(self, review_data: Dict[str, Any]) -> str:
        """Extract review ID from App Store review data."""
        return review_data.get('id', {}).get('label', '')

    def _parse_reviewed_at(self, review_data: Dict[str, Any]) -> datetime:
        """Parse reviewed_at timestamp from App Store review data.

        Extracts timestamp from 'updated' -> 'label' field and ensures
        timezone-aware datetime. Falls back to current UTC time if parsing fails.

        Args:
            review_data: Review dictionary from App Store API

        Returns:
            Timezone-aware datetime object
        """
        reviewed_at = None
        if 'updated' in review_data and 'label' in review_data['updated']:
            try:
                reviewed_at = datetime.fromisoformat(
                    review_data['updated']['label'].replace('Z', '+00:00')
                )
            except (ValueError, TypeError):
                # If timestamp is malformed, fall back to current UTC time
                pass
        if not reviewed_at:
            reviewed_at = datetime.now(timezone.utc)
        return reviewed_at

    def get_app_store_reviews_and_appname(
        self,
        app_id: str,
        country: str = None,
        pages: int = None
    ) -> Tuple[Optional[str], List[Dict[str, Any]]]:
        """지정된 앱 ID와 국가 코드로 App Store 리뷰와 앱 이름을 가져옵니다.

        Args:
            app_id: App Store app ID
            country: Country code (default: self.country)
            pages: Number of pages to crawl (default: self.pages_to_crawl)

        Returns:
            Tuple of (app_name, reviews_data)
        """
        if country is None:
            country = self.country
        if pages is None:
            pages = self.pages_to_crawl

        all_reviews = []
        app_name = None

        self.logger.info(f"앱 ID {app_id}의 리뷰 크롤링 시작 (최대 {pages}페이지)")

        for page in range(1, pages + 1):
            url = f"https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortby=mostRecent/json"

            try:
                response = requests.get(url, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()

                entries = data.get('feed', {}).get('entry')
                if not entries:
                    self.logger.info(f"{page} 페이지에서 더 이상 리뷰를 찾을 수 없어 중단합니다.")
                    break

                if isinstance(entries, dict):
                    entries = [entries]
                elif not isinstance(entries, list):
                    self.logger.warning(f"{page} 페이지에서 유효하지 않은 데이터 구조입니다.")
                    break

                if page == 1:
                    if len(entries) > 0 and 'im:name' in entries[0]:
                        app_name = entries[0].get('im:name', {}).get('label', f'app_{app_id}')
                        self.logger.info(f"앱 이름: {app_name}")
                    else:
                        app_name = f'app_{app_id}'
                        for entry in entries:
                            all_reviews.append(entry)
                        self.logger.info(f"{page} 페이지의 리뷰를 성공적으로 가져왔습니다. (리뷰 수: {len(entries)})")
                        continue

                for entry in entries[1:]:
                    all_reviews.append(entry)

                review_count = len(entries) - 1 if page == 1 else len(entries)
                self.logger.info(f"{page} 페이지의 리뷰를 성공적으로 가져왔습니다. (리뷰 수: {review_count})")

                if len(all_reviews) >= self.max_reviews_per_app:
                    self.logger.info(f"최대 리뷰 수({self.max_reviews_per_app})에 도달하여 중단합니다.")
                    all_reviews = all_reviews[:self.max_reviews_per_app]
                    break

            except requests.exceptions.RequestException as e:
                self.logger.error(f"HTTP 요청 중 에러 발생: {e}")
                break
            except json.JSONDecodeError:
                self.logger.error("JSON 파싱 중 에러 발생. 응답이 올바른 JSON 형식이 아닙니다.")
                break
            except Exception as e:
                self.logger.error(f"앱 ID {app_id}, 페이지 {page} 처리 중 예상치 못한 오류 발생: {e}")
                break

            if page < pages:
                self.wait_between_requests()

        self.logger.info(f"앱 ID {app_id}: 총 {len(all_reviews)}개 리뷰 수집 완료")
        return app_name, all_reviews

    def save_to_parquet_and_database(
        self,
        app_id: str,
        reviews_data: List[Dict[str, Any]]
    ) -> int:
        """NAS-first dual-write: Parquet → DB (2-phase commit)

        Phase 1: Write to Parquet (NAS)
        Phase 2: Commit to DB (only if Phase 1 succeeds)

        This ensures no Ghost Records (DB without Parquet data).

        Args:
            app_id: App Store app ID
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
            # 0. App 확인/생성 (using base class helper)
            # ========================================
            platform_type = self._get_platform_type()
            # Extract app name from reviews_data if available
            app_name = reviews_data[0].get('im:name', {}).get('label') if reviews_data else None
            app = self._get_or_create_app(session, app_id, app_name, platform_type)

            # ========================================
            # 1. Idempotency Check (using base class helpers)
            # ========================================
            existing_platform_ids = self._get_existing_platform_ids(session, app.app_id, platform_type)
            new_reviews_data = self._filter_new_reviews(reviews_data, existing_platform_ids)

            if not new_reviews_data:
                self.logger.info(f"No new reviews for app {app_id} (all duplicates)")
                session.close()  # No new data to commit
                return 0

            self.logger.info(f"Found {len(new_reviews_data)} new reviews for {app_id}")

            # ========================================
            # 2. PHASE 1: Write to Parquet (NAS-first)
            # ========================================
            # Create ID and timestamp caches (using base class helper)
            review_id_map, reviewed_at_cache = self._create_review_id_and_timestamp_caches(
                new_reviews_data,
                self._parse_reviewed_at
            )

            parquet_records = []
            for review_data in new_reviews_data:
                platform_review_id = self._extract_platform_review_id(review_data)
                if not platform_review_id or platform_review_id not in review_id_map:
                    continue

                review_id = review_id_map[platform_review_id]
                reviewed_at = reviewed_at_cache[platform_review_id]

                # Parse review_text
                review_text = review_data.get('content', {}).get('label', '')
                if not review_text:
                    continue  # Skip empty reviews

                # Create Parquet record
                parquet_record = AppReviewSchema(
                    review_id=str(review_id),
                    app_id=str(app.app_id),
                    platform_type='APPSTORE',
                    platform_review_id=platform_review_id,
                    reviewer_name=review_data.get('author', {}).get('name', {}).get('label'),
                    review_text=review_text,
                    rating=int(review_data.get('im:rating', {}).get('label', 0)),
                    reviewed_at=reviewed_at,
                    is_reply=False,
                    reply_comment=None
                )
                parquet_records.append(parquet_record)

            if not parquet_records:
                self.logger.info(f"No valid reviews to write for app {app_id}")
                session.close()  # No valid data to commit
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
            # Create master index records (using base class helper)
            master_index_records = self._create_master_index_records(
                new_reviews_data,
                app.app_id,
                platform_type,
                review_id_map,
                reviewed_at_cache,
                self.enable_parquet
            )

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

    def _mark_reviews_as_failed(
        self,
        app_id: str,
        reviews_data: List[Dict[str, Any]],
        error_message: str,
        failure_reason: str
    ) -> None:
        """Mark reviews as FAILED in DB when Parquet or DB commit fails.

        This enables the retry mechanism to find and retry failed reviews.

        Args:
            app_id: App Store app ID
            reviews_data: List of review dictionaries from API
            error_message: Error details from the exception
            failure_reason: Short description (e.g., "PARQUET_WRITE_FAILED")
        """
        session = self.db_connector.get_session()
        try:
            # Find or create the App record
            app = session.query(App).filter_by(
                platform_app_id=app_id,
                platform_type=PlatformType.APPSTORE
            ).first()

            if not app:
                # Create minimal app record for failure tracking
                app = App(
                    app_id=uuid7(),
                    platform_app_id=app_id,
                    name=f'app_{app_id}',
                    platform_type=PlatformType.APPSTORE
                )
                session.add(app)
                session.flush()

            now = datetime.now(timezone.utc)
            failed_records = []

            for review_data in reviews_data:
                platform_review_id = review_data.get('id', {}).get('label', '')
                if not platform_review_id:
                    continue

                # Check if already tracked
                existing = session.query(ReviewMasterIndex).filter_by(
                    app_id=app.app_id,
                    platform_review_id=platform_review_id
                ).first()

                if existing:
                    # Update existing record to FAILED
                    existing.processing_status = ProcessingStatusType.FAILED
                    existing.error_message = f"{failure_reason}: {error_message}"
                    existing.retry_count = 0
                else:
                    # Create new FAILED record
                    review_id = uuid7()

                    # Parse reviewed_at
                    reviewed_at = self._parse_reviewed_at(review_data)

                    failed_record = ReviewMasterIndex(
                        review_id=review_id,
                        app_id=app.app_id,
                        platform_review_id=platform_review_id,
                        platform_type=PlatformType.APPSTORE,
                        review_created_at=reviewed_at,
                        ingested_at=now,
                        processing_status=ProcessingStatusType.FAILED,
                        parquet_written_at=None,  # Failed to write
                        is_active=True,
                        is_reply=False,
                        error_message=f"{failure_reason}: {error_message}",
                        retry_count=0
                    )
                    failed_records.append(failed_record)

            if failed_records:
                session.add_all(failed_records)

            session.commit()
            self.logger.info(
                f"Marked {len(failed_records)} reviews as FAILED for app {app_id} ({failure_reason})"
            )

        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to mark reviews as FAILED: {e}")
        finally:
            session.close()

    def retry_failed_reviews(self, max_retries: int = 3) -> int:
        """Retry failed reviews by re-crawling from App Store RSS feed.

        Strategy:
        1. Find failed reviews grouped by app_id
        2. For each app with failed reviews, re-crawl all reviews
        3. Idempotency check will skip already-processed reviews
        4. Failed reviews will be re-attempted with 2-phase commit
        5. Update retry_count and status based on outcome

        Args:
            max_retries: Maximum retry attempts (default: 3)

        Returns:
            Number of apps re-crawled (not individual reviews)
        """
        session = self.db_connector.get_session()

        try:
            # Query failed reviews grouped by app
            failed_reviews = session.query(ReviewMasterIndex).filter(
                ReviewMasterIndex.processing_status == ProcessingStatusType.FAILED,
                ReviewMasterIndex.retry_count < max_retries,
                ReviewMasterIndex.platform_type == PlatformType.APPSTORE
            ).all()

            if not failed_reviews:
                self.logger.info("No failed reviews to retry")
                return 0

            # Group failed reviews by app_id
            from collections import defaultdict
            failed_by_app = defaultdict(list)
            for failed_review in failed_reviews:
                # Get the App record to find platform_app_id
                app = session.query(App).filter_by(app_id=failed_review.app_id).first()
                if app:
                    failed_by_app[app.platform_app_id].append(failed_review)

            self.logger.info(
                f"Found {len(failed_reviews)} failed reviews across {len(failed_by_app)} apps. "
                f"Will re-crawl these apps."
            )

            retried_apps = 0
            for platform_app_id, failed_reviews_for_app in failed_by_app.items():
                try:
                    self.logger.info(
                        f"Retrying app {platform_app_id} "
                        f"({len(failed_reviews_for_app)} failed reviews)"
                    )

                    # Re-crawl all reviews for this app
                    app_name, reviews_data = self.get_app_store_reviews_and_appname(platform_app_id)

                    if not reviews_data:
                        self.logger.warning(f"No reviews found for app {platform_app_id} during retry")
                        # Mark as failed with updated message
                        for failed_review in failed_reviews_for_app:
                            failed_review.retry_count += 1
                            failed_review.error_message = (
                                f"Retry {failed_review.retry_count}: No reviews found from API"
                            )
                        session.commit()
                        continue

                    # Re-attempt 2-phase commit
                    # Idempotency check will skip already-processed reviews
                    reviews_added = self.save_to_parquet_and_database(platform_app_id, reviews_data)

                    # If we got here, retry was successful
                    # The failed reviews should now be in RAW status (re-created by save_to_parquet_and_database)
                    # We need to clean up the old FAILED records
                    for failed_review in failed_reviews_for_app:
                        # Check if review was successfully re-processed
                        updated_review = session.query(ReviewMasterIndex).filter_by(
                            app_id=failed_review.app_id,
                            platform_review_id=failed_review.platform_review_id,
                            processing_status=ProcessingStatusType.RAW
                        ).first()

                        if updated_review:
                            # Success! Delete the old failed record
                            session.delete(failed_review)
                            self.logger.info(
                                f"Successfully retried review {failed_review.platform_review_id}"
                            )
                        else:
                            # Still failed, increment retry count
                            failed_review.retry_count += 1
                            failed_review.error_message = (
                                f"Retry {failed_review.retry_count}: Re-crawl completed but review not found"
                            )

                    session.commit()
                    retried_apps += 1
                    self.logger.info(f"Successfully retried app {platform_app_id}")

                except Exception as e:
                    session.rollback()
                    self.logger.error(f"Retry failed for app {platform_app_id}: {e}")
                    # Increment retry count for all failed reviews of this app
                    for failed_review in failed_reviews_for_app:
                        try:
                            failed_review.retry_count += 1
                            failed_review.error_message = f"Retry {failed_review.retry_count}: {str(e)}"
                        except Exception:
                            pass
                    try:
                        session.commit()
                    except Exception as commit_err:
                        session.rollback()
                        self.logger.error(f"Failed to update retry count: {commit_err}")

            return retried_apps

        finally:
            session.close()

    def run(self) -> None:
        """크롤러 실행 (NAS-first Architecture)"""
        self.logger.info("App Store 크롤러 실행 시작 (NAS-first mode)")

        self.db_connector.create_tables(Base)

        app_ids = self.read_app_ids(self.app_ids_file)
        if not app_ids:
            raise ValueError(f"앱 ID 파일에서 유효한 ID를 찾을 수 없습니다: {self.app_ids_file}")

        self.logger.info(f"총 {len(app_ids)}개 앱의 리뷰를 크롤링합니다.")

        successful_apps = 0
        total_reviews_added = 0

        for i, app_id in enumerate(app_ids, 1):
            self.logger.info(f"[{i}/{len(app_ids)}] 앱 ID: {app_id} 크롤링 시작...")

            # Initialize reviews_data before try block to avoid undefined reference in exception handlers
            reviews_data = []

            try:
                app_name, reviews_data = self.get_app_store_reviews_and_appname(app_id)

                if not app_name:
                    self.logger.warning(f"앱 ID {app_id}의 이름을 찾을 수 없습니다. 건너뜁니다.")
                    continue

                if not reviews_data:
                    self.logger.warning(f"앱 ID {app_id}: 수집된 리뷰 없음")
                    continue

                # NAS-first dual-write
                reviews_added = self.save_to_parquet_and_database(app_id, reviews_data)

                if reviews_added > 0:
                    self.logger.info(f"앱 ID {app_id}: {reviews_added}개의 새로운 리뷰 추가")
                    total_reviews_added += reviews_added
                    successful_apps += 1
                else:
                    self.logger.info(f"앱 ID {app_id}: 새로운 리뷰 없음")

            except ParquetWriteError as e:
                self.logger.error(f"앱 ID {app_id} Parquet 쓰기 실패: {e}")
                # Mark as failed in DB for retry mechanism
                # Pass only the root cause to avoid redundant error message
                self._mark_reviews_as_failed(app_id, reviews_data, str(e.__cause__ or e), "PARQUET_WRITE_FAILED")
                continue

            except DBCommitError as e:
                self.logger.error(f"앱 ID {app_id} DB commit 실패: {e}")
                # Parquet OK, DB failed - mark for retry
                # Pass only the root cause to avoid redundant error message
                self._mark_reviews_as_failed(app_id, reviews_data, str(e.__cause__ or e), "DB_COMMIT_FAILED")
                continue

            except Exception as e:
                self.logger.error(f"앱 ID {app_id} 크롤링 실패: {e}")
                continue

            if i < len(app_ids):
                self.wait_between_requests()

        self.logger.info(f"크롤링 완료 - 성공: {successful_apps}/{len(app_ids)}개 앱")
        self.logger.info(f"총 {total_reviews_added}개 리뷰가 추가되었습니다.")
        self.logger.info(f"Parquet: {self.parquet_writer.base_path if self.parquet_writer else 'Disabled'}")
