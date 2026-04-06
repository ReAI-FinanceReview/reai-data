"""Tests for GoldEmbeddingGenerator (Gold Layer - Issue #37)

Coverage:
- process(): 단일 review_id 임베딩 생성 및 DB 적재
- process(): 이미 임베딩 존재 시 skip (idempotency)
- process(): refined_text 없을 때 True 반환 (skip)
- process(): OpenAI API 실패 시 False 반환
- process_batch(): CLEANED 리뷰만 대상으로 처리
- process_batch(): 이미 임베딩된 리뷰 제외
- _generate_embedding(): rate limit 재시도 로직
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import UUID
from uuid6 import uuid7

from src.gold.embedding_generator import GoldEmbeddingGenerator
from src.models.review_embedding import ReviewEmbedding
from src.models.review_preprocessed import ReviewPreprocessed
from src.models.review_master_index import ReviewMasterIndex
from src.models.enums import ProcessingStatusType, PlatformType


# ============================================================
# Helpers
# ============================================================

def _make_generator(client=None) -> GoldEmbeddingGenerator:
    gen = GoldEmbeddingGenerator.__new__(GoldEmbeddingGenerator)
    gen.model_name = "text-embedding-3-small"
    gen.logger = MagicMock()
    gen.db_connector = MagicMock()
    gen._client = client
    return gen


def _fake_vector() -> list:
    return [0.1] * 1536


def _add_preprocessed(session, review_id: UUID, text: str = "테스트 리뷰"):
    rp = ReviewPreprocessed(
        review_id=review_id,
        platform_review_id=f"plat_{review_id}",
        refined_text=text,
    )
    session.add(rp)
    session.flush()
    return rp


def _add_master_index(session, review_id: UUID, status: ProcessingStatusType):
    app_id = uuid7()
    rmi = ReviewMasterIndex(
        review_id=review_id,
        app_id=app_id,
        platform_review_id=f"plat_{review_id}",
        platform_type=PlatformType.APPSTORE,
        ingested_at=datetime.now(timezone.utc),
        processing_status=status,
        is_active=True,
        is_reply=False,
        retry_count=0,
    )
    session.add(rmi)
    session.flush()
    return rmi


# ============================================================
# A. process() — unit tests (no DB)
# ============================================================

def test_process_success():
    """정상적으로 임베딩이 생성되고 session.add 호출."""
    review_id = uuid7()
    mock_client = MagicMock()
    mock_client.embeddings.create.return_value = MagicMock(
        data=[MagicMock(embedding=_fake_vector())]
    )
    gen = _make_generator(client=mock_client)

    session = MagicMock()
    session.get.side_effect = lambda model, rid: (
        None if model is ReviewEmbedding
        else ReviewPreprocessed(review_id=rid, platform_review_id="p1", refined_text="리뷰 텍스트")
    )

    result = gen.process(session, review_id)

    assert result is True
    session.add.assert_called_once()
    added = session.add.call_args[0][0]
    assert isinstance(added, ReviewEmbedding)
    assert added.review_id == review_id
    assert added.source_content_type == "preprocessed"
    assert added.model_name == "text-embedding-3-small"
    assert added.vector == _fake_vector()


def test_process_skips_already_embedded():
    """이미 review_embeddings에 존재하면 skip하고 True 반환."""
    review_id = uuid7()
    gen = _make_generator()

    session = MagicMock()
    session.get.side_effect = lambda model, rid: (
        ReviewEmbedding(review_id=rid) if model is ReviewEmbedding else None
    )

    result = gen.process(session, review_id)

    assert result is True
    session.add.assert_not_called()


def test_process_skips_missing_preprocessed():
    """reviews_preprocessed 레코드 없으면 True 반환 (skip)."""
    review_id = uuid7()
    gen = _make_generator()

    session = MagicMock()
    session.get.return_value = None  # ReviewEmbedding도 없고 ReviewPreprocessed도 없음

    result = gen.process(session, review_id)

    assert result is True
    session.add.assert_not_called()


def test_process_skips_empty_refined_text():
    """refined_text가 None이면 True 반환 (skip)."""
    review_id = uuid7()
    gen = _make_generator()

    session = MagicMock()
    session.get.side_effect = lambda model, rid: (
        None if model is ReviewEmbedding
        else ReviewPreprocessed(review_id=rid, platform_review_id="p1", refined_text=None)
    )

    result = gen.process(session, review_id)

    assert result is True
    session.add.assert_not_called()


def test_process_returns_false_on_api_failure():
    """OpenAI API 오류 시 False 반환."""
    review_id = uuid7()
    gen = _make_generator(client=MagicMock())
    gen._generate_embedding = MagicMock(return_value=None)

    session = MagicMock()
    session.get.side_effect = lambda model, rid: (
        None if model is ReviewEmbedding
        else ReviewPreprocessed(review_id=rid, platform_review_id="p1", refined_text="텍스트")
    )

    result = gen.process(session, review_id)

    assert result is False
    session.add.assert_not_called()


# ============================================================
# B. _generate_embedding() — retry logic
# ============================================================

def test_generate_embedding_retries_on_rate_limit():
    """RateLimitError 발생 시 최대 3회 재시도."""
    try:
        from openai import RateLimitError as OAIRateLimitError
    except ImportError:
        pytest.skip("openai not installed")

    mock_client = MagicMock()
    mock_client.embeddings.create.side_effect = [
        OAIRateLimitError("rate limit", response=MagicMock(), body={}),
        OAIRateLimitError("rate limit", response=MagicMock(), body={}),
        MagicMock(data=[MagicMock(embedding=_fake_vector())]),
    ]
    gen = _make_generator(client=mock_client)

    with patch("src.gold.embedding_generator.time.sleep"):
        result = gen._generate_embedding("테스트")

    assert result == _fake_vector()
    assert mock_client.embeddings.create.call_count == 3


def test_generate_embedding_returns_none_after_max_retries():
    """3회 모두 실패하면 None 반환."""
    try:
        from openai import RateLimitError as OAIRateLimitError
    except ImportError:
        pytest.skip("openai not installed")

    mock_client = MagicMock()
    mock_client.embeddings.create.side_effect = OAIRateLimitError(
        "rate limit", response=MagicMock(), body={}
    )
    gen = _make_generator(client=mock_client)

    with patch("src.gold.embedding_generator.time.sleep"):
        result = gen._generate_embedding("텍스트")

    assert result is None


# ============================================================
# C. process_batch() — integration (requires DB)
# ============================================================

@pytest.mark.requires_db
def test_process_batch_only_processes_cleaned(test_db_session):
    """CLEANED 상태 리뷰만 임베딩 대상으로 처리."""
    cleaned_id = uuid7()
    raw_id = uuid7()

    _add_master_index(test_db_session, cleaned_id, ProcessingStatusType.CLEANED)
    _add_master_index(test_db_session, raw_id, ProcessingStatusType.RAW)
    _add_preprocessed(test_db_session, cleaned_id, "클린 리뷰")
    _add_preprocessed(test_db_session, raw_id, "원시 리뷰")
    test_db_session.commit()

    gen = _make_generator()
    gen.db_connector.get_session.return_value = test_db_session
    gen._generate_embedding = MagicMock(return_value=_fake_vector())

    count = gen.process_batch(batch_size=50)

    assert count == 1
    assert test_db_session.get(ReviewEmbedding, cleaned_id) is not None
    assert test_db_session.get(ReviewEmbedding, raw_id) is None


@pytest.mark.requires_db
def test_process_batch_skips_already_embedded(test_db_session):
    """이미 임베딩 존재하는 리뷰는 재처리하지 않음 (idempotency)."""
    review_id = uuid7()

    _add_master_index(test_db_session, review_id, ProcessingStatusType.CLEANED)
    _add_preprocessed(test_db_session, review_id, "리뷰 텍스트")
    test_db_session.add(ReviewEmbedding(
        review_id=review_id,
        source_content_type="preprocessed",
        model_name="text-embedding-3-small",
        vector=_fake_vector(),
    ))
    test_db_session.commit()

    gen = _make_generator()
    gen.db_connector.get_session.return_value = test_db_session
    gen._generate_embedding = MagicMock(return_value=_fake_vector())

    count = gen.process_batch()

    gen._generate_embedding.assert_not_called()
    assert count == 0  # _fetch_pending_review_ids가 NOT EXISTS로 이미 임베딩된 리뷰를 제외하므로


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
