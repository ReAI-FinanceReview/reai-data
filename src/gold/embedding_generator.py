"""Gold Layer - Embedding Generator

reviews_preprocessed의 refined_text를 벡터화하여 review_embeddings에 적재.

Usage (standalone):
    generator = GoldEmbeddingGenerator()
    generator.process_batch(batch_size=100)

Usage (via orchestrator):
    success = generator.process(session, review_id)
"""

import os
import time
from pathlib import Path
from typing import List, Optional
from uuid import UUID

try:
    from openai import OpenAI, APIError, RateLimitError
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    OpenAI = None
    APIError = Exception  # type: ignore
    RateLimitError = Exception  # type: ignore

try:
    from dotenv import load_dotenv
    _DOTENV_AVAILABLE = True
except ImportError:
    _DOTENV_AVAILABLE = False

from src.utils.db_connector import DatabaseConnector
from src.utils.logger import get_logger
from src.models.review_preprocessed import ReviewPreprocessed
from src.models.review_embedding import ReviewEmbedding


_DIMENSION_MAP = {
    "text-embedding-3-small": 1536,
    "text-embedding-3-large": 3072,
}
_MAX_RETRIES = 3
_RETRY_BACKOFF = 2.0


class GoldEmbeddingGenerator:
    """reviews_preprocessed → review_embeddings 적재 (Gold Layer).

    Orchestrator 단일 건 처리와 standalone 배치 처리 모두 지원.
    """

    def __init__(
        self,
        model_name: str = "text-embedding-3-small",
        config_path: str = "config/crawler_config.yml",
    ):
        if _DIMENSION_MAP.get(model_name, 1536) != 1536:
            raise ValueError(
                f"model '{model_name}' produces {_DIMENSION_MAP[model_name]}-dim vectors; "
                "review_embeddings only supports 1536. Use text-embedding-3-small."
            )
        self.model_name = model_name
        self.logger = get_logger(__name__)
        self.db_connector = DatabaseConnector(config_path)
        self._client: Optional[OpenAI] = self._init_client()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process(self, session, review_id: UUID) -> bool:
        """단일 review_id에 대해 임베딩 생성 후 DB 적재.

        Orchestrator에서 호출. 세션 관리는 호출자 책임.

        Returns:
            True: 성공(신규 적재 또는 이미 존재)
            False: 실패(API 오류 등)
        """
        if self._is_already_embedded(session, review_id):
            return True

        preprocessed = session.get(ReviewPreprocessed, review_id)
        if preprocessed is None or not preprocessed.refined_text:
            self.logger.warning(f"[{review_id}] No refined_text in reviews_preprocessed — skip")
            return True  # 처리할 데이터 없음 = 오류 아님

        vector = self._generate_embedding(preprocessed.refined_text)
        if vector is None:
            return False

        session.add(ReviewEmbedding(
            review_id=review_id,
            source_content_type="preprocessed",
            model_name=self.model_name,
            vector=vector,
        ))
        return True

    def process_batch(self, batch_size: int = 100, limit: Optional[int] = None) -> int:
        """CLEANED 상태이면서 임베딩이 없는 리뷰를 배치 처리.

        Returns:
            process() 호출 성공 건수 (신규 적재 + skip 포함, 실패 제외)
        """
        session = self.db_connector.get_session()
        try:
            review_ids = self._fetch_pending_review_ids(session, limit)
            if not review_ids:
                self.logger.info("No reviews pending embedding generation")
                return 0

            self.logger.info(f"Generating embeddings for {len(review_ids)} reviews")
            success_count = 0

            for i in range(0, len(review_ids), batch_size):
                chunk = review_ids[i:i + batch_size]
                for review_id in chunk:
                    if self.process(session, review_id):
                        success_count += 1
                    else:
                        self.logger.warning(f"[{review_id}] Embedding failed — skipping")

                session.commit()
                self.logger.info(
                    f"Progress: {min(i + batch_size, len(review_ids))}/{len(review_ids)}"
                )

            self.logger.info(f"Embedding generation complete: {success_count}/{len(review_ids)} processed (new inserts + skips)")
            return success_count

        except Exception:
            session.rollback()
            self.logger.exception("Batch embedding generation failed")
            raise
        finally:
            session.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _init_client(self) -> Optional[OpenAI]:
        if not OPENAI_AVAILABLE:
            self.logger.error("openai package not installed")
            return None

        if _DOTENV_AVAILABLE:
            env_path = Path(__file__).resolve().parents[2] / ".env"
            if env_path.exists():
                load_dotenv(env_path)

        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            self.logger.error("OPENAI_API_KEY not set")
            return None

        try:
            client = OpenAI(
                api_key=api_key,
                base_url=os.getenv("OPENAI_BASE_URL"),
            )
            self.logger.info(f"OpenAI client initialized (model={self.model_name})")
            return client
        except Exception as e:
            self.logger.error(f"OpenAI client init failed: {e}")
            return None

    def _generate_embedding(self, text: str) -> Optional[List[float]]:
        if not self._client:
            return None

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                response = self._client.embeddings.create(
                    model=self.model_name,
                    input=text,
                )
                return response.data[0].embedding
            except RateLimitError as e:
                self.logger.warning(f"Rate limit (attempt {attempt}/{_MAX_RETRIES}): {e}")
            except APIError as e:
                self.logger.error(f"API error (attempt {attempt}/{_MAX_RETRIES}): {e}")
                if getattr(e, "status_code", None) and e.status_code < 500:
                    break
            except Exception as e:
                self.logger.error(f"Unexpected error (attempt {attempt}/{_MAX_RETRIES}): {e}")
                break

            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_BACKOFF * attempt)

        return None

    def _is_already_embedded(self, session, review_id: UUID) -> bool:
        return session.get(ReviewEmbedding, review_id) is not None

    def _fetch_pending_review_ids(
        self, session, limit: Optional[int]
    ) -> List[UUID]:
        """임베딩 미생성 review_id 조회 (reviews_preprocessed 기준)."""
        from sqlalchemy import not_, exists
        from src.models.enums import ProcessingStatusType
        from src.models.review_master_index import ReviewMasterIndex

        query = (
            session.query(ReviewPreprocessed.review_id)
            .join(
                ReviewMasterIndex,
                ReviewMasterIndex.review_id == ReviewPreprocessed.review_id,
            )
            .filter(ReviewMasterIndex.processing_status == ProcessingStatusType.CLEANED)
            .filter(
                not_(
                    exists().where(
                        ReviewEmbedding.review_id == ReviewPreprocessed.review_id
                    )
                )
            )
        )
        if limit:
            query = query.limit(limit)
        return [row.review_id for row in query.all()]
