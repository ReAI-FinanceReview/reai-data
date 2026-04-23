# -*- coding: utf-8 -*-
"""Bronze-to-Silver Review Cleansing Pipeline.

Issue #14: Implement Review Data Cleansing Pipeline (Bronze to Silver)
"""
import re
import unicodedata

import emoji


# =========================================================
# 순수 텍스트 정제 함수 (Pure functions)
# =========================================================

def normalize_unicode(text: str) -> str:
    """NFKC 유니코드 정규화: 한글 자모 분리 방지 + 전각→반각."""
    if not text:
        return text
    return unicodedata.normalize('NFKC', text)


def remove_emojis(text: str) -> str:
    """모든 이모지를 제거한다."""
    if not text:
        return text
    return emoji.replace_emoji(text, replace='')


def reduce_repeated_chars(text: str) -> str:
    """동일 문자 3회 이상 연속 반복을 최대 2개로 축약한다."""
    if not text:
        return text
    return re.sub(r'(.)\1{2,}', r'\1\1', text)


def remove_special_chars(text: str) -> str:
    """문장부호(!?.,)와 PII 플레이스홀더 괄호([])를 제외한 특수기호를 제거한다."""
    if not text:
        return text
    return re.sub(r'[^\w\s!?.,\[\]]', '', text)


def normalize_whitespace(text: str) -> str:
    """연속 공백·탭·줄바꿈을 단일 공백으로 정규화한다."""
    if not text:
        return text
    return re.sub(r'\s+', ' ', text)


_ACCOUNT_PATTERN = re.compile(
    r'(?<!\d)\d{10,14}(?!\d)'
    r'|\b\d{3,6}-\d{2,6}-\d{3,6}\b'
)
_PHONE_PATTERN = re.compile(
    r'01[016789][-\s.]?\d{3,4}[-\s.]?\d{4}'
)
_EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'
)


def mask_pii(text: str) -> str:
    """계좌번호, 전화번호, 이메일을 마스킹한다."""
    if not text:
        return text
    text = _EMAIL_PATTERN.sub('[EMAIL]', text)
    text = _PHONE_PATTERN.sub('[TEL]', text)
    text = _ACCOUNT_PATTERN.sub('[ACC]', text)
    return text


# =========================================================
# ReviewCleaner: 8-step 정제 파이프라인 클래스
# =========================================================

import json
from flashtext import KeywordProcessor


class ReviewCleaner:
    """텍스트 정제 파이프라인 클래스.

    정제 순서: NFKC → 이모지 제거 → 반복문자 축약 → PII 마스킹
            특수문자 제거 → 오타 교정 → 비속어 마스킹 → 공백 정규화
    """

    def __init__(self, synonyms_path: str, profanity_path: str):
        self._synonym_processor = self._load_synonyms(synonyms_path)
        self._profanity_processor = self._load_profanity(profanity_path)

    def _load_synonyms(self, path: str) -> KeywordProcessor:
        processor = KeywordProcessor()
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # dict 형식: {"오타": "정답", ...}
        for wrong, correct in data.items():
            processor.add_keyword(wrong, correct)
        return processor

    def _load_profanity(self, path: str) -> KeywordProcessor:
        processor = KeywordProcessor()
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # dict 형식: {"비속어": "[CATEGORY]", ...} — 카테고리 값을 그대로 사용
        # list 형식: ["비속어", ...] — 하위 호환으로 [SLANG] 사용
        if isinstance(data, dict):
            for word, category in data.items():
                processor.add_keyword(word, category)
        else:
            for word in data:
                processor.add_keyword(word, '[SLANG]')
        return processor

    def clean(self, text: str) -> str:
        """텍스트에 전체 정제 파이프라인을 적용한다."""
        if not text:
            return text
        text = normalize_unicode(text)
        text = remove_emojis(text)
        text = reduce_repeated_chars(text)
        text = mask_pii(text)
        text = remove_special_chars(text)
        text = self._synonym_processor.replace_keywords(text)
        text = self._profanity_processor.replace_keywords(text)
        text = normalize_whitespace(text)
        return text.strip()


# =========================================================
# Bronze 로더 / Silver 라이터
# =========================================================

from datetime import date as DateType
from collections import defaultdict
from typing import List, Dict, Any
import time
from uuid import UUID
import pyarrow as pa

from src.utils.logger import get_logger
from src.utils.minio_client import MinIOClient

logger = get_logger(__name__)


def load_bronze_parquet(
    minio: MinIOClient,
    target_date: DateType,
) -> List[Dict[str, Any]]:
    """지정 날짜의 Bronze Parquet을 모두 읽어 dict 리스트로 반환한다.

    Bronze 경로: bronze/app_reviews/year={YYYY}/month={MM}/day={DD}/
    """
    prefix = (
        f"bronze/app_reviews/"
        f"year={target_date.year}/"
        f"month={target_date.month:02d}/"
        f"day={target_date.day:02d}/"
    )
    keys = minio.list_objects(prefix)
    rows: List[Dict[str, Any]] = []
    for key in keys:
        table = minio.get_parquet(key)
        rows.extend(table.to_pylist())
    return rows


def write_silver_parquet(
    minio: MinIOClient,
    app_id: str,
    target_date: DateType,
    records: List[Dict[str, Any]],
) -> None:
    """정제된 레코드를 Silver Parquet으로 MinIO에 업로드한다 (overwrite).

    Silver 경로: silver/reviews/app_id={app_id}/dt={YYYY-MM-DD}/refined.parquet
    """
    key = (
        f"silver/reviews/"
        f"app_id={app_id}/"
        f"dt={target_date.isoformat()}/"
        f"refined.parquet"
    )
    table = pa.Table.from_pylist(records)
    minio.put_parquet(key, table)


# =========================================================
# ReviewCleaningPipeline
# =========================================================

class ReviewCleaningPipeline:
    """Bronze → Silver 정제 파이프라인.

    Args:
        minio_client: MinIOClient 인스턴스
        db_connector: DatabaseConnector 인스턴스
        synonyms_path: 동의어 사전 JSON 경로
        profanity_path: 비속어 목록 JSON 경로
    """

    def __init__(
        self,
        minio_client: MinIOClient,
        db_connector,
        synonyms_path: str,
        profanity_path: str,
    ):
        self.minio = minio_client
        self.db = db_connector
        self.cleaner = ReviewCleaner(
            synonyms_path=synonyms_path,
            profanity_path=profanity_path,
        )

    def run(self, target_date: DateType) -> Dict[str, Any]:
        """지정 날짜의 Bronze 데이터를 정제하여 Silver에 저장한다.

        Returns:
            dict: {'processed': int, 'skipped': int, 'elapsed_sec': float}
        """
        start = time.time()
        logger.info(f"[CleansePipeline] Start: target_date={target_date}")

        bronze_rows = load_bronze_parquet(self.minio, target_date)
        logger.info(f"  Loaded {len(bronze_rows)} rows from Bronze")

        processed = 0
        skipped = 0
        groups: Dict[str, List[Dict]] = defaultdict(list)
        review_ids_by_app: Dict[str, List[str]] = defaultdict(list)

        for row in bronze_rows:
            text = row.get('review_text') or ''
            if not text.strip():
                skipped += 1
                continue
            review_id = row.get('review_id')
            try:
                review_id = row['review_id']
                cleaned = self.cleaner.clean(text)
                app_id = row['app_id']
                platform_review_id = row['platform_review_id']
            except Exception as exc:
                err_msg = f"Cleanse failed: {type(exc).__name__}: {exc}"
                logger.exception(f"  Row cleanse failed: review_id={review_id}")
                if review_id:
                    self._mark_review_failed(review_id, err_msg)
                continue

            groups[app_id].append({
                'review_id': review_id,
                'platform_review_id': platform_review_id,
                'refined_text': cleaned,
            })
            review_ids_by_app[app_id].append(review_id)
            processed += 1

        for app_id, records in groups.items():
            write_silver_parquet(self.minio, app_id, target_date, records)
            logger.info(f"  Wrote {len(records)} rows → Silver (app_id={app_id})")
            self._update_db_status(review_ids_by_app[app_id])

        elapsed = round(time.time() - start, 2)
        logger.info(
            f"[CleansePipeline] Done: processed={processed}, "
            f"skipped={skipped}, elapsed={elapsed}s"
        )
        return {'processed': processed, 'skipped': skipped, 'elapsed_sec': elapsed}

    def _mark_review_failed(self, review_id: str, error_message: str) -> None:
        """Record a row-level cleanse failure in ReviewMasterIndex."""
        from src.models.review_master_index import ReviewMasterIndex
        from src.models.enums import ProcessingStatusType

        try:
            parsed_review_id = UUID(str(review_id))
        except (TypeError, ValueError):
            logger.warning(f"  Invalid review_id for failure tracking: {review_id}")
            return

        session = self.db.get_session()
        try:
            record = session.get(ReviewMasterIndex, parsed_review_id)
            if record is None:
                logger.warning(f"  ReviewMasterIndex not found for failed review: {review_id}")
                return
            record.processing_status = ProcessingStatusType.FAILED
            record.error_message = error_message
            record.retry_count = (record.retry_count or 0) + 1
            session.commit()
        except Exception:
            session.rollback()
            logger.exception(f"  Failed to mark review as FAILED: review_id={review_id}")
        finally:
            session.close()

    def _update_db_status(self, review_ids: List[str]) -> None:
        """ReviewMasterIndex의 처리 상태를 RAW → CLEANED로 업데이트한다."""
        from src.models.review_master_index import ReviewMasterIndex
        from src.models.enums import ProcessingStatusType

        if not review_ids:
            return

        session = self.db.get_session()
        try:
            session.query(ReviewMasterIndex).filter(
                ReviewMasterIndex.review_id.in_(review_ids),
                ReviewMasterIndex.processing_status == ProcessingStatusType.RAW,
            ).update(
                {'processing_status': ProcessingStatusType.CLEANED},
                synchronize_session=False,
            )
            session.commit()
            logger.info(f"  Updated {len(review_ids)} records to CLEANED")
        except Exception as e:
            session.rollback()
            logger.exception("DB status update failed")
            raise
        finally:
            session.close()
