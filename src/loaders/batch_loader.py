"""Batch Loader - Parquet 배치 → DB 적재 (Load Stage)

ingestion_batch 테이블에서 PENDING/FAILED 배치를 조회하여
Parquet 파일을 읽고 ReviewMasterIndex에 적재합니다.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Set
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.exc import PendingRollbackError

from src.utils.db_connector import DatabaseConnector
from src.utils.logger import get_logger
from src.utils.minio_client import MinIOClient
from src.models.app_metadata import AppMetadata
from src.models.apps import App
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
        should_close_session = bool(session.info.get("owned_by_db_connector"))
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
                batch_snapshot = {
                    "batch_id": batch.batch_id,
                    "source_type": batch.source_type,
                    "platform_app_id": batch.platform_app_id,
                    "app_name": batch.app_name,
                    "storage_path": batch.storage_path,
                    "file_format": batch.file_format,
                    "record_count": batch.record_count,
                    "retry_count": batch.retry_count,
                    "max_retries": batch.max_retries,
                    "created_at": batch.created_at,
                }
                try:
                    self._load_single_batch(session, batch)
                    loaded += 1
                except Exception as e:
                    self.logger.error(f"Failed to load batch {batch_snapshot['batch_id']}: {e}")
                    try:
                        batch = session.get(IngestionBatch, batch_snapshot["batch_id"])
                    except PendingRollbackError:
                        session.rollback()
                        batch = session.get(IngestionBatch, batch_snapshot["batch_id"])
                    if batch is None:
                        batch = IngestionBatch(
                            batch_id=batch_snapshot["batch_id"],
                            source_type=batch_snapshot["source_type"],
                            platform_app_id=batch_snapshot["platform_app_id"],
                            app_name=batch_snapshot["app_name"],
                            storage_path=batch_snapshot["storage_path"],
                            file_format=batch_snapshot["file_format"],
                            record_count=batch_snapshot["record_count"],
                            status=IngestionBatchStatusType.PENDING,
                            retry_count=batch_snapshot["retry_count"],
                            max_retries=batch_snapshot["max_retries"],
                            created_at=batch_snapshot["created_at"],
                            updated_at=datetime.now(timezone.utc),
                        )
                        session.add(batch)
                        session.flush()
                    self._mark_batch_failed(session, batch, str(e))

            self.logger.info(f"Loaded {loaded}/{len(pending_batches)} batches")
            return loaded

        finally:
            if should_close_session:
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

        # 1. Parquet 읽기: 로컬 파일, s3:// URI, MinIO object key를 모두 지원한다.
        table = self._read_batch_table(batch.storage_path)
        data_dicts = table.to_pylist()
        records = [AppReviewSchema(**d) for d in data_dicts]
        if not records:
            self.logger.warning(f"Batch {batch.batch_id}: empty Parquet file")
            batch.status = IngestionBatchStatusType.LOADED
            batch.error_message = None
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
        self._ensure_apps_exist(session, records, batch)

        existing_ids: Set[str] = set()
        for app_uuid in app_uuids_in_batch:
            existing_ids.update(self._get_existing_platform_ids(session, app_uuid, platform_type))

        new_records = [r for r in records if r.platform_review_id not in existing_ids]

        if not new_records:
            self.logger.info(f"Batch {batch.batch_id}: all records already loaded (idempotent)")
            batch.status = IngestionBatchStatusType.LOADED
            batch.error_message = None
            batch.loaded_at = datetime.now(timezone.utc)
            batch.updated_at = datetime.now(timezone.utc)
            session.commit()
            return 0

        # 3. ReviewMasterIndex 레코드 생성 (app_id는 레코드에서 직접 사용)
        now = datetime.now(timezone.utc)
        service_id_cache: Dict[UUID, Optional[UUID]] = {}
        master_index_records = []
        app_review_rows = []
        for record in new_records:
            app_uuid = UUID(record.app_id)
            self._ensure_app_exists(session, app_uuid, record, batch, platform_type)
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
            app_review_rows.append(
                {
                    "review_id": UUID(record.review_id),
                    "app_id": app_uuid,
                    "platform_type": platform_type.value,
                    "platform_review_id": record.platform_review_id,
                    "reviewer_name": record.reviewer_name,
                    "review_text": record.review_text,
                    "rating": record.rating,
                    "reviewed_at": record.reviewed_at,
                    "is_reply": record.is_reply or False,
                    "reply_comment": record.reply_comment,
                }
            )

        # 4. DB 적재 + 배치 상태 업데이트
        session.add_all(master_index_records)
        if app_review_rows:
            session.execute(
                text(
                    """
                    INSERT INTO app_reviews (
                        review_id, app_id, platform_type, country_code,
                        platform_review_id, reviewer_name, review_text, rating,
                        app_version, reviewed_at, is_reply, reply_comment
                    )
                    VALUES (
                        :review_id, :app_id, :platform_type, 'kr',
                        :platform_review_id, :reviewer_name, :review_text, :rating,
                        NULL, :reviewed_at, :is_reply, :reply_comment
                    )
                    """
                ),
                app_review_rows,
            )
        batch.status = IngestionBatchStatusType.LOADED
        batch.error_message = None
        batch.loaded_at = now
        batch.updated_at = now
        session.commit()

        self.logger.info(
            f"Batch {batch.batch_id}: loaded {len(master_index_records)} records (status=RAW)"
        )
        return len(master_index_records)

    def _read_batch_table(self, storage_path: str):
        """Read a Parquet batch from local path or MinIO/S3 storage."""
        if not hasattr(self, "_minio"):
            self._minio = None

        if storage_path.startswith(("s3://", "minio://")):
            key = storage_path.split("://", 1)[1]
            if "/" in key:
                key = key.split("/", 1)[1]
            if self._minio is None:
                self._minio = MinIOClient()
            return self._minio.get_parquet(key)

        from pathlib import Path
        import pyarrow.parquet as pq

        local_path = Path(storage_path)
        if local_path.exists():
            return pq.read_table(local_path)
        if self._minio is None:
            self._minio = MinIOClient()
        return self._minio.get_parquet(storage_path)

    def _ensure_app_exists(
        self,
        session,
        app_uuid: UUID,
        record: AppReviewSchema,
        batch: IngestionBatch,
        platform_type: PlatformType,
    ) -> None:
        """Ensure FK target exists for ReviewMasterIndex rows."""
        if session.get(App, app_uuid) is not None:
            return

        session.add(
            App(
                app_id=app_uuid,
                platform_app_id=batch.platform_app_id,
                platform_type=platform_type,
                name=batch.app_name or f"app_{record.app_id}",
            )
        )
        session.flush()

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
            .filter(AppMetadata.app_id == app_id, AppMetadata.is_active.is_(True))
            .order_by(AppMetadata.valid_from.desc())
            .first()
        )
        if row is None:
            self.logger.warning(f"No active app_metadata found for app_id={app_id}")
        return row.service_id if row else None

    def _ensure_apps_exist(self, session, records: list[AppReviewSchema], batch: IngestionBatch) -> None:
        """ReviewMasterIndex FK를 만족하도록 배치 레코드의 app row를 보장."""
        app_ids = {UUID(record.app_id) for record in records}
        if not app_ids:
            return

        existing_ids = {
            row.app_id for row in
            session.query(App.app_id).filter(App.app_id.in_(app_ids)).all()
        }
        missing_ids = app_ids - existing_ids
        if not missing_ids:
            return

        for app_id in missing_ids:
            session.add(
                App(
                    app_id=app_id,
                    platform_app_id=batch.platform_app_id,
                    platform_type=batch.source_type,
                    name=batch.app_name or batch.platform_app_id,
                )
            )
        session.flush()

    def _get_existing_platform_ids(self, session, app_uuid: UUID, platform_type: PlatformType) -> Set[str]:
        """중복 방지용 기존 platform_review_id 조회."""
        return set(
            row.platform_review_id for row in
            session.query(ReviewMasterIndex.platform_review_id).filter_by(
                app_id=app_uuid,
                platform_type=platform_type
            ).all()
        )


def _is_local_storage_path(storage_path: str) -> bool:
    return "://" not in storage_path or Path(storage_path).is_absolute()


def _object_key_from_storage_path(storage_path: str) -> str:
    if storage_path.startswith("s3://"):
        return storage_path.removeprefix("s3://").split("/", 1)[1]
    return storage_path
