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
            # 0. App 확인/생성
            # ========================================
            app = session.query(App).filter_by(
                platform_app_id=app_id,
                platform_type=PlatformType.APPSTORE
            ).first()

            if not app:
                app = App(
                    app_id=uuid7(),
                    platform_app_id=app_id,
                    name=reviews_data[0].get('im:name', {}).get('label', f'app_{app_id}') if reviews_data else f'app_{app_id}',
                    platform_type=PlatformType.APPSTORE
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
                    platform_type=PlatformType.APPSTORE
                ).all()
            )

            new_reviews_data = []
            for review in reviews_data:
                platform_review_id = review.get('id', {}).get('label', '')
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
                platform_review_id = review_data.get('id', {}).get('label', '')
                if not platform_review_id:
                    continue

                # Generate UUID v7 (time-sortable)
                review_id = str(uuid7())
                review_id_map[platform_review_id] = review_id

                # Parse reviewed_at
                reviewed_at = None
                if 'updated' in review_data and 'label' in review_data['updated']:
                    try:
                        reviewed_at = datetime.fromisoformat(
                            review_data['updated']['label'].replace('Z', '+00:00')
                        )
                    except (ValueError, TypeError):
                        pass
                if not reviewed_at:
                    reviewed_at = datetime.now(timezone.utc)

                # Parse review_text
                review_text = review_data.get('content', {}).get('label', '')
                if not review_text:
                    continue  # Skip empty reviews

                # Create Parquet record
                parquet_record = AppReviewSchema(
                    review_id=review_id,
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
                platform_review_id = review_data.get('id', {}).get('label', '')
                if not platform_review_id or platform_review_id not in review_id_map:
                    continue

                review_id = review_id_map[platform_review_id]

                # Parse reviewed_at again (same as above)
                reviewed_at = None
                if 'updated' in review_data and 'label' in review_data['updated']:
                    try:
                        reviewed_at = datetime.fromisoformat(
                            review_data['updated']['label'].replace('Z', '+00:00')
                        )
                    except (ValueError, TypeError):
                        pass
                if not reviewed_at:
                    reviewed_at = datetime.now(timezone.utc)

                master_index = ReviewMasterIndex(
                    review_id=review_id,
                    app_id=app.app_id,
                    platform_review_id=platform_review_id,
                    platform_type=PlatformType.APPSTORE,
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
                ReviewMasterIndex.platform_type == PlatformType.APPSTORE
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
