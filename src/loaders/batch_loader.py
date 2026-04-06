"""Batch Loader - Parquet 배치 → DB 적재 (Load Stage)

ingestion_batch 테이블에서 PENDING/FAILED 배치를 조회하여
Parquet 파일을 읽고 ReviewMasterIndex에 적재합니다.
"""

from datetime import datetime, timezone
from typing import Dict, Optional, Set
from uuid import UUID

from src.utils.db_connector import DatabaseConnector
from src.utils.logger import get_logger
from src.utils.minio_client import MinIOClient
from src.models.app_metadata import AppMetadata
from src.models.ingestion_batch import IngestionBatch
from src.models.review_master_index import ReviewMasterIndex
from src.models.enums import IngestionBatchStatusType, PlatformType, ProcessingStatusType
from src.schemas.parquet.app_review import AppReviewSchema


class BatchLoader:
    """PENDING/FAILED ingestion_batch를 조회하여 DB에 적재합니다."""

    def __init__(self, config_path: str = None):
        self.db_connector = DatabaseConnector(config_path or 'config/crawler_config.yml')
        self.logger = get_logger('batch_loader')
        self._minio = None  # lazy initialization to avoid eager env var requirement

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
                .filter(IngestionBatch.retry_count < IngestionBatch.max_retries)
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
                    session.rollback()
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

        # 1. MinIO에서 Parquet 읽기
        if self._minio is None:
            self._minio = MinIOClient()
        table = self._minio.get_parquet(batch.storage_path)
        data_dicts = table.to_pylist()
        records = [AppReviewSchema(**d) for d in data_dicts]
        if not records:
            self.logger.warning(f"Batch {batch.batch_id}: empty Parquet file")
            batch.status = IngestionBatchStatusType.LOADED
            batch.loaded_at = datetime.now(timezone.utc)
            batch.updated_at = datetime.now(timezone.utc)
            session.commit()
            return 0

        # 2. 중복 제거: 배치 내 모든 app_id별로 기존 platform_review_id 수집
        platform_type = PlatformType(batch.source_type.value)
        valid_records = []
        app_uuids_in_batch: set = set()
        for r in records:
            try:
                app_uuids_in_batch.add(UUID(r.app_id))
                valid_records.append(r)
            except ValueError:
                self.logger.warning(f"Skipping record with invalid app_id UUID: {r.app_id}")
        records = valid_records

        existing_ids: Set[str] = set()
        for app_uuid in app_uuids_in_batch:
            existing_ids.update(self._get_existing_platform_ids(session, app_uuid, platform_type))

        new_records = [r for r in records if r.platform_review_id not in existing_ids]

        if not new_records:
            self.logger.info(f"Batch {batch.batch_id}: all records already loaded (idempotent)")
            batch.status = IngestionBatchStatusType.LOADED
            batch.loaded_at = datetime.now(timezone.utc)
            batch.updated_at = datetime.now(timezone.utc)
            session.commit()
            return 0

        # 3. ReviewMasterIndex 레코드 생성 (app_id는 레코드에서 직접 사용)
        now = datetime.now(timezone.utc)
        service_id_cache: Dict[UUID, Optional[UUID]] = {}
        master_index_records = []
        for record in new_records:
            app_uuid = UUID(record.app_id)
            if app_uuid not in service_id_cache:
                service_id_cache[app_uuid] = self._get_service_id(session, app_uuid)
            master_index = ReviewMasterIndex(
                review_id=UUID(record.review_id),
                app_id=app_uuid,
                service_id=service_id_cache[app_uuid],
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

        # 4. DB 적재 + 배치 상태 업데이트
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

    def _get_service_id(self, session, app_id: UUID) -> Optional[UUID]:
        """app_metadata에서 현재 유효한 service_id 조회."""
        row = (
            session.query(AppMetadata.service_id)
            .filter(AppMetadata.app_id == app_id, AppMetadata.is_active == True)
            .order_by(AppMetadata.valid_from.desc())
            .first()
        )
        if row is None:
            self.logger.warning(f"No active app_metadata found for app_id={app_id}")
        return row.service_id if row else None

    def _get_existing_platform_ids(self, session, app_uuid: UUID, platform_type: PlatformType) -> Set[str]:
        """중복 방지용 기존 platform_review_id 조회."""
        return set(
            row.platform_review_id for row in
            session.query(ReviewMasterIndex.platform_review_id).filter_by(
                app_id=app_uuid,
                platform_type=platform_type
            ).all()
        )
