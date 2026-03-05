"""
기본 크롤러 클래스
"""
import time
from abc import ABC, abstractmethod
from typing import Callable, List, Dict, Any, Optional, Tuple, Set
from pathlib import Path
from datetime import datetime, timezone
from uuid import UUID
from uuid6 import uuid7

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

from ..utils.logger import get_logger
from ..utils.file_manager import FileManager
from ..utils.data_processor import DataProcessor


class BaseCrawler(ABC):
    """기본 크롤러 추상 클래스"""
    
    def __init__(self, config_path: str = None):
        self.logger = get_logger(self.__class__.__name__.lower())

        # 설정 로드
        self.config = self._load_config(config_path)

        output_cfg = self.config.get("output", {}) if isinstance(self.config, dict) else {}
        output_enabled = output_cfg.get("enabled", True)
        output_base = output_cfg.get("base_directory", "data")

        self.file_manager = FileManager(base_path=output_base, enabled=output_enabled)
        self.data_processor = DataProcessor()
        
        # 공통 설정
        self.delay = self.config.get('global', {}).get('delay_between_requests', 2)
        self.max_retries = self.config.get('global', {}).get('max_retries', 3)
        self.timeout = self.config.get('global', {}).get('timeout', 30)
    
    def _load_config(self, config_path: str = None) -> Dict[str, Any]:
        """설정 파일 로드"""
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config" / "crawler_config.yml"
        
        if not YAML_AVAILABLE:
            self.logger.warning("PyYAML이 설치되지 않았습니다. 기본 설정을 사용합니다.")
            return self._get_default_config()
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger.error(f"설정 파일을 로드할 수 없습니다: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """기본 설정 반환"""
        return {
            'global': {
                'delay_between_requests': 2,
                'max_retries': 3,
                'timeout': 30
            },
            'output': {
                'base_directory': 'data/raw',
                'file_format': 'csv',
                'encoding': 'utf-8-sig'
            }
        }
    
    def read_app_ids(self, filename: str) -> List[str]:
        """앱 ID 파일 읽기"""
        app_ids = []
        try:
            with open(filename, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    # 주석이 붙은 경우 분리
                    app_id = line.split('#')[0].strip()
                    if app_id:
                        app_ids.append(app_id)
        except FileNotFoundError:
            self.logger.error(f"앱 ID 파일을 찾을 수 없습니다: {filename}")
        except Exception as e:
            self.logger.error(f"앱 ID 파일 읽기 오류: {e}")
        
        return app_ids
    
    def wait_between_requests(self):
        """요청 간 대기"""
        time.sleep(self.delay)
    
    @abstractmethod
    def crawl_reviews(self, app_id: str) -> List[Dict[str, Any]]:
        """리뷰 크롤링 (하위 클래스에서 구현)"""
        pass
    
    @abstractmethod
    def run(self) -> str:
        """크롤러 실행 (하위 클래스에서 구현)"""
        pass

    # ========================================
    # Common Helper Methods (Issue #6 Fix)
    # ========================================

    @abstractmethod
    def _get_platform_type(self):
        """Get platform type enum (APPSTORE or PLAYSTORE)."""
        pass

    @abstractmethod
    def _extract_platform_review_id(self, review_data: Dict[str, Any]) -> str:
        """Extract platform-specific review ID from review data."""
        pass

    def _get_or_create_app(self, session, app_id: str, app_name: Optional[str], platform_type) -> Any:
        """Get existing app or create new one.

        Args:
            session: Database session
            app_id: Platform-specific app ID
            app_name: App display name (optional)
            platform_type: PlatformType enum

        Returns:
            App record
        """
        from ..models.apps import App

        app = session.query(App).filter_by(
            platform_app_id=app_id,
            platform_type=platform_type
        ).first()

        if not app:
            app = App(
                app_id=uuid7(),
                platform_app_id=app_id,
                name=app_name or f'app_{app_id}',
                platform_type=platform_type
            )
            session.add(app)
            session.flush()
            self.logger.info(f"Created new app: {app.name} ({app_id})")

        return app

    def _get_existing_platform_ids(self, session, app_uuid: UUID, platform_type) -> Set[str]:
        """Get set of existing platform_review_ids for idempotency check.

        Args:
            session: Database session
            app_uuid: App UUID (not platform_app_id)
            platform_type: PlatformType enum

        Returns:
            Set of existing platform_review_ids
        """
        from ..models.review_master_index import ReviewMasterIndex

        existing_platform_ids = set(
            row.platform_review_id for row in
            session.query(ReviewMasterIndex.platform_review_id).filter_by(
                app_id=app_uuid,
                platform_type=platform_type
            ).all()
        )

        return existing_platform_ids

    def _filter_new_reviews(
        self,
        reviews_data: List[Dict[str, Any]],
        existing_platform_ids: Set[str]
    ) -> List[Dict[str, Any]]:
        """Filter out duplicate reviews based on platform_review_id.

        Args:
            reviews_data: List of review dictionaries from API
            existing_platform_ids: Set of existing platform_review_ids

        Returns:
            List of new (non-duplicate) reviews
        """
        new_reviews = []
        for review in reviews_data:
            platform_review_id = self._extract_platform_review_id(review)
            if platform_review_id and platform_review_id not in existing_platform_ids:
                new_reviews.append(review)

        return new_reviews

    def _create_review_id_and_timestamp_caches(
        self,
        new_reviews_data: List[Dict[str, Any]],
        parse_reviewed_at_func
    ) -> Tuple[Dict[str, UUID], Dict[str, datetime]]:
        """Create caches for review_ids and parsed timestamps.

        Args:
            new_reviews_data: List of new review dictionaries
            parse_reviewed_at_func: Function to parse reviewed_at timestamp

        Returns:
            Tuple of (review_id_map, reviewed_at_cache)
        """
        review_id_map = {}
        reviewed_at_cache = {}

        for review_data in new_reviews_data:
            platform_review_id = self._extract_platform_review_id(review_data)
            if not platform_review_id:
                continue

            # Generate UUID v7 (time-sortable)
            review_id = uuid7()
            review_id_map[platform_review_id] = review_id

            # Parse and cache reviewed_at timestamp
            reviewed_at = parse_reviewed_at_func(review_data)
            reviewed_at_cache[platform_review_id] = reviewed_at

        return review_id_map, reviewed_at_cache

    def save_crawl_batch(
        self,
        app_id: str,
        app_name: Optional[str],
        reviews_data: List[Dict[str, Any]],
        build_parquet_records_func: Callable
    ) -> Tuple[Optional[UUID], int, Optional[Path]]:
        """Crawl 단계: Parquet 쓰기 + ingestion_batch PENDING 등록.

        DB write는 App 레코드 확인/생성(경량)과 ingestion_batch INSERT만 수행.
        ReviewMasterIndex 생성은 load 단계(BatchLoader)에서 처리.

        Args:
            app_id: Platform-specific app ID
            app_name: App display name (optional)
            reviews_data: List of review dictionaries from API
            build_parquet_records_func: Platform-specific function to build Parquet records.
                Signature: (reviews_data, review_id_map, reviewed_at_cache, app) → List[AppReviewSchema]

        Returns:
            Tuple of (batch_id, record_count, parquet_path).
            Returns (None, 0, None) if no new reviews.

        Raises:
            ParquetWriteError: If Parquet write fails (ingestion_batch NOT created)
        """
        from ..crawlers.exceptions import ParquetWriteError
        from ..models.ingestion_batch import IngestionBatch
        from ..models.enums import IngestionBatchStatusType

        session = self.db_connector.get_session()
        try:
            platform_type = self._get_platform_type()
            app = self._get_or_create_app(session, app_id, app_name, platform_type)

            # Idempotency: skip already-indexed reviews
            existing_platform_ids = self._get_existing_platform_ids(session, app.app_id, platform_type)
            new_reviews_data = self._filter_new_reviews(reviews_data, existing_platform_ids)

            if not new_reviews_data:
                self.logger.info(f"No new reviews for app {app_id} (all duplicates)")
                session.close()
                return None, 0, None

            self.logger.info(f"Found {len(new_reviews_data)} new reviews for {app_id}")

            # Build review UUID and timestamp caches
            review_id_map, reviewed_at_cache = self._create_review_id_and_timestamp_caches(
                new_reviews_data,
                self._parse_reviewed_at
            )

            # Build platform-specific Parquet records
            parquet_records = build_parquet_records_func(
                new_reviews_data, review_id_map, reviewed_at_cache, app
            )

            if not parquet_records:
                self.logger.info(f"No valid reviews to write for app {app_id}")
                session.close()
                return None, 0, None

            # ENABLE_PARQUET_WRITE=false: skip Parquet and ingestion_batch (dev mode)
            if not self.enable_parquet:
                self.logger.warning("Parquet write disabled (ENABLE_PARQUET_WRITE=false) — skipping batch")
                session.close()
                return None, 0, None

            # Write to Parquet
            try:
                parquet_path = self.parquet_writer.write_batch(parquet_records)
                self.logger.info(
                    f"Parquet write OK: {len(parquet_records)} reviews → {parquet_path}"
                )
            except Exception as e:
                self.logger.error(f"Parquet write FAILED: {e}")
                raise ParquetWriteError(f"Parquet write failed: {e}") from e

            # Register ingestion_batch as PENDING
            now = datetime.now(timezone.utc)
            batch = IngestionBatch(
                batch_id=uuid7(),
                source_type=platform_type,
                platform_app_id=app_id,
                app_name=app.name,
                storage_path=str(parquet_path),
                file_format='parquet',
                record_count=len(parquet_records),
                status=IngestionBatchStatusType.PENDING,
                retry_count=0,
                max_retries=self.max_retries,
                created_at=now,
                updated_at=now
            )
            session.add(batch)
            session.commit()

            self.logger.info(
                f"ingestion_batch PENDING registered: batch_id={batch.batch_id}, "
                f"records={len(parquet_records)}"
            )
            return batch.batch_id, len(parquet_records), Path(parquet_path)

        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
