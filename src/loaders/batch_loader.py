"""Batch Loader - Parquet 배치 → DB 적재 (Load Stage)

ingestion_batch 테이블에서 PENDING/FAILED 배치를 조회하여
Parquet 파일을 읽고 ReviewMasterIndex에 적재합니다.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Set
from uuid import UUID

from uuid6 import uuid7

from src.utils.db_connector import DatabaseConnector
from src.utils.logger import get_logger
from src.utils.parquet_writer import read_parquet_to_schemas
from src.models.ingestion_batch import IngestionBatch
from src.models.review_master_index import ReviewMasterIndex
from src.models.apps import App
from src.models.enums import IngestionBatchStatusType, PlatformType, ProcessingStatusType
from src.schemas.parquet.app_review import AppReviewSchema


class BatchLoader:
    """PENDING/FAILED ingestion_batch를 조회하여 DB에 적재합니다."""

    def __init__(self, config_path: str = None):
        self.db_connector = DatabaseConnector(config_path or 'config/crawler_config.yml')
        self.logger = get_logger('batch_loader')

    def load_pending_batches(self, limit: int = 100) -> int:
        """PENDING/FAILED 상태 배치를 순차 적재.

        Returns:
            총 적재된 배치 수
        """
        session = self.db_connector.get_session()
        try:
            pending_batches = (
                session.query(IngestionBatch)
                .filter(
                    IngestionBatch.status.in_([
                        IngestionBatchStatusType.PENDING,
                        IngestionBatchStatusType.FAILED
                    ])
                )
                .order_by(IngestionBatch.created_at.asc())
                .limit(limit)
                .all()
            )

            if not pending_batches:
                self.logger.info("No pending batches to load")
                return 0

            self.logger.info(f"Found {len(pending_batches)} pending batches")
            loaded = 0

            for batch in pending_batches:
                try:
                    self._load_single_batch(session, batch)
                    loaded += 1
                except Exception as e:
                    self.logger.error(f"Failed to load batch {batch.batch_id}: {e}")
                    self._mark_batch_failed(session, batch, str(e))

            self.logger.info(f"Loaded {loaded}/{len(pending_batches)} batches")
            return loaded

        finally:
            session.close()

    def _load_single_batch(self, session, batch: IngestionBatch) -> int:
        """단일 배치 처리: Parquet 읽기 → ReviewMasterIndex 적재.

        Returns:
            적재된 레코드 수
        """
        self.logger.info(
            f"Loading batch {batch.batch_id}: {batch.storage_path} "
            f"(retry={batch.retry_count})"
        )

        # 1. Parquet 파일 존재 확인
        parquet_path = Path(batch.storage_path)
        if not parquet_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

        # 2. Parquet 읽기
        records = read_parquet_to_schemas(parquet_path, AppReviewSchema)
        if not records:
            self.logger.warning(f"Batch {batch.batch_id}: empty Parquet file")
            batch.status = IngestionBatchStatusType.LOADED
            batch.loaded_at = datetime.now(timezone.utc)
            batch.updated_at = datetime.now(timezone.utc)
            session.commit()
            return 0

        # 3. App 레코드 확인/생성
        platform_type = PlatformType(batch.source_type.value)
        app = self._get_or_create_app(session, batch.platform_app_id, batch.app_name, platform_type)

        # 4. 중복 제거
        existing_ids = self._get_existing_platform_ids(session, app.app_id, platform_type)
        new_records = [r for r in records if r.platform_review_id not in existing_ids]

        if not new_records:
            self.logger.info(f"Batch {batch.batch_id}: all records already loaded (idempotent)")
            batch.status = IngestionBatchStatusType.LOADED
            batch.loaded_at = datetime.now(timezone.utc)
            batch.updated_at = datetime.now(timezone.utc)
            session.commit()
            return 0

        # 5. ReviewMasterIndex 레코드 생성
        now = datetime.now(timezone.utc)
        master_index_records = []
        for record in new_records:
            master_index = ReviewMasterIndex(
                review_id=UUID(record.review_id),
                app_id=app.app_id,
                platform_review_id=record.platform_review_id,
                platform_type=platform_type,
                review_created_at=record.reviewed_at,
                ingested_at=now,
                processing_status=ProcessingStatusType.RAW,
                parquet_written_at=batch.created_at,
                storage_path=batch.storage_path,
                is_active=True,
                is_reply=record.is_reply or False,
                error_message=None,
                retry_count=0
            )
            master_index_records.append(master_index)

        # 6. DB 적재 + 배치 상태 업데이트
        session.add_all(master_index_records)
        batch.status = IngestionBatchStatusType.LOADED
        batch.loaded_at = now
        batch.updated_at = now
        session.commit()

        self.logger.info(
            f"Batch {batch.batch_id}: loaded {len(master_index_records)} records (status=RAW)"
        )
        return len(master_index_records)

    def _mark_batch_failed(self, session, batch: IngestionBatch, error_msg: str) -> None:
        """배치 실패 처리: retry_count 증가, max_retries 도달 시 DEAD_LETTER."""
        batch.retry_count += 1
        batch.error_message = error_msg
        batch.updated_at = datetime.now(timezone.utc)

        if batch.retry_count >= batch.max_retries:
            batch.status = IngestionBatchStatusType.DEAD_LETTER
            self.logger.warning(
                f"Batch {batch.batch_id} reached max retries ({batch.max_retries}) → DEAD_LETTER"
            )
        else:
            batch.status = IngestionBatchStatusType.FAILED

        try:
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to update batch status: {e}")

    def _get_or_create_app(self, session, platform_app_id: str, app_name: str, platform_type: PlatformType):
        """App 레코드 확인 또는 생성."""
        app = session.query(App).filter_by(
            platform_app_id=platform_app_id,
            platform_type=platform_type
        ).first()

        if not app:
            app = App(
                app_id=uuid7(),
                platform_app_id=platform_app_id,
                name=app_name or f'app_{platform_app_id}',
                platform_type=platform_type
            )
            session.add(app)
            session.flush()
            self.logger.info(f"Created new app: {app.name} ({platform_app_id})")

        return app

    def _get_existing_platform_ids(self, session, app_uuid: UUID, platform_type: PlatformType) -> Set[str]:
        """중복 방지용 기존 platform_review_id 조회."""
        return set(
            row.platform_review_id for row in
            session.query(ReviewMasterIndex.platform_review_id).filter_by(
                app_id=app_uuid,
                platform_type=platform_type
            ).all()
        )
