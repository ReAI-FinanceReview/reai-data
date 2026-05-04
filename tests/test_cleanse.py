from datetime import date
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from uuid6 import uuid7

from src.models.enums import ProcessingStatusType
from src.models.review_preprocessed import ReviewPreprocessed
from src.processing.cleanse import (
    ReviewCleaner,
    ReviewCleaningPipeline,
    load_bronze_parquet,
    normalize_unicode,
    normalize_whitespace,
    remove_emojis,
    reduce_repeated_chars,
    remove_special_chars,
    mask_pii,
    write_silver_parquet,
)


# ========================================
# normalize_unicode
# ========================================

def test_normalize_unicode_fullwidth_to_ascii():
    assert normalize_unicode('１２３') == '123'

def test_normalize_unicode_hangul_composed():
    # NFKC should compose decomposed hangul
    # 'ㄱ' + 'ㅏ' + 'ㄱ' decomposed form
    decomposed = '\u1100\u1161\u11A8'
    result = normalize_unicode(decomposed)
    assert result == '각'

def test_normalize_unicode_empty():
    assert normalize_unicode('') == ''


# ========================================
# remove_emojis
# ========================================

def test_remove_emojis_removes_emoji():
    result = remove_emojis('앱이 너무 좋아요 😊👍')
    assert '😊' not in result
    assert '👍' not in result

def test_remove_emojis_keeps_text():
    result = remove_emojis('앱이 너무 좋아요 😊')
    assert '앱이' in result

def test_remove_emojis_no_emoji():
    text = '그냥 텍스트입니다'
    assert remove_emojis(text) == text

def test_remove_emojis_only_emojis():
    result = remove_emojis('😀😃😄')
    assert result.strip() == ''


# ========================================
# reduce_repeated_chars
# ========================================

def test_reduce_repeated_hangul_jamo():
    assert reduce_repeated_chars('ㅋㅋㅋㅋ') == 'ㅋㅋ'

def test_reduce_repeated_ascii():
    assert reduce_repeated_chars('hhhh') == 'hh'

def test_reduce_repeated_exactly_two_unchanged():
    assert reduce_repeated_chars('ㅋㅋ') == 'ㅋㅋ'

def test_reduce_repeated_mixed():
    result = reduce_repeated_chars('좋아요ㅎㅎㅎㅎ')
    assert 'ㅎㅎㅎㅎ' not in result
    assert '좋아요' in result


# ========================================
# remove_special_chars
# ========================================

def test_remove_special_chars_keeps_punctuation():
    result = remove_special_chars('좋아요! 정말? 최고.')
    assert '!' in result
    assert '?' in result
    assert '.' in result

def test_remove_special_chars_removes_colon_parenthesis():
    result = remove_special_chars('계좌: (비밀)')
    assert ':' not in result
    assert '(' not in result
    assert ')' not in result

def test_remove_special_chars_keeps_korean_numbers():
    result = remove_special_chars('앱 평점 5점!')
    assert '앱' in result
    assert '5' in result
    assert '!' in result


# ========================================
# mask_pii
# ========================================

def test_mask_pii_account_10digit():
    result = mask_pii('계좌번호는 1234567890 입니다')
    assert '[ACC]' in result
    assert '1234567890' not in result

def test_mask_pii_account_dash():
    result = mask_pii('계좌: 123-456-789012')
    assert '[ACC]' in result

def test_mask_pii_phone():
    result = mask_pii('전화번호 010-1234-5678로 연락')
    assert '[TEL]' in result
    assert '010-1234-5678' not in result

def test_mask_pii_email():
    result = mask_pii('이메일: user@example.com 보내주세요')
    assert '[EMAIL]' in result
    assert 'user@example.com' not in result

def test_mask_pii_no_pii():
    text = '그냥 좋은 앱입니다'
    assert mask_pii(text) == text

def test_mask_pii_preserves_name():
    result = mask_pii('홍길동씨가 계좌 1234567890으로 이체')
    assert '홍길동' in result
    assert '[ACC]' in result


# ========================================
# normalize_whitespace
# ========================================

def test_normalize_whitespace_multiple_spaces():
    assert normalize_whitespace('편리  하고  좋아요') == '편리 하고 좋아요'

def test_normalize_whitespace_tabs_and_newlines():
    assert normalize_whitespace('편리\t빠르고\n좋아요') == '편리 빠르고 좋아요'

def test_normalize_whitespace_mixed():
    assert normalize_whitespace('좋은  \t앱\n\n입니다') == '좋은 앱 입니다'

def test_normalize_whitespace_single_space_unchanged():
    text = '편리하고 빠른 앱'
    assert normalize_whitespace(text) == text

def test_normalize_whitespace_empty():
    assert normalize_whitespace('') == ''


# ========================================
# ReviewCleaner
# ========================================


@pytest.fixture
def tmp_synonyms(tmp_path):
    data = {"게좌이체": "계좌이체", "이채": "이체"}
    (tmp_path / "synonyms.json").write_text(json.dumps(data, ensure_ascii=False))
    return str(tmp_path / "synonyms.json")


@pytest.fixture
def tmp_profanity(tmp_path):
    # list 형식으로 테스트 (dict 형식도 지원해야 함)
    data = ["욕설1", "욕설2"]
    (tmp_path / "profanity.json").write_text(json.dumps(data, ensure_ascii=False))
    return str(tmp_path / "profanity.json")


def test_cleaner_synonym_correction(tmp_synonyms, tmp_profanity):
    cleaner = ReviewCleaner(synonyms_path=tmp_synonyms, profanity_path=tmp_profanity)
    result = cleaner.clean('게좌이체가 안 돼요')
    assert '계좌이체' in result
    assert '게좌이체' not in result


def test_cleaner_profanity_masking(tmp_synonyms, tmp_profanity):
    cleaner = ReviewCleaner(synonyms_path=tmp_synonyms, profanity_path=tmp_profanity)
    result = cleaner.clean('욕설1 진짜 별로야')
    assert '[SLANG]' in result
    assert '욕설1' not in result


def test_cleaner_full_pipeline(tmp_synonyms, tmp_profanity):
    cleaner = ReviewCleaner(synonyms_path=tmp_synonyms, profanity_path=tmp_profanity)
    text = '게좌이체😊ㅋㅋㅋㅋ 욕설1 010-1234-5678'
    result = cleaner.clean(text)
    assert '계좌이체' in result
    assert '😊' not in result
    assert 'ㅋㅋㅋㅋ' not in result
    assert '[SLANG]' in result
    assert '[TEL]' in result


def test_cleaner_empty_text(tmp_synonyms, tmp_profanity):
    cleaner = ReviewCleaner(synonyms_path=tmp_synonyms, profanity_path=tmp_profanity)
    assert cleaner.clean('') == ''
    assert cleaner.clean(None) is None


@pytest.fixture
def tmp_profanity_dict(tmp_path):
    # dict 형식 profanity도 지원 확인
    data = {"욕설A": "[MASK]", "욕설B": "[MASK]"}
    (tmp_path / "profanity_dict.json").write_text(json.dumps(data, ensure_ascii=False))
    return str(tmp_path / "profanity_dict.json")


def test_cleaner_profanity_dict_format(tmp_synonyms, tmp_profanity_dict):
    cleaner = ReviewCleaner(synonyms_path=tmp_synonyms, profanity_path=tmp_profanity_dict)
    result = cleaner.clean('욕설A 이 앱은 별로')
    assert '[MASK]' in result
    assert '욕설A' not in result


# ========================================
# Bronze 로더 / Silver 라이터 / Pipeline
# ========================================


def _make_bronze_minio(table: pa.Table):
    mock = MagicMock()
    mock.list_objects.return_value = [
        'bronze/app_reviews/year=2026/month=03/data.parquet'
    ]
    mock.get_parquet.return_value = table
    return mock


# --- load_bronze_parquet ---

def test_load_bronze_returns_rows():
    sample = pa.table({
        'review_id': ['r1', 'r2'],
        'app_id': ['app1', 'app1'],
        'platform_review_id': ['p1', 'p2'],
        'review_text': ['좋아요', '별로'],
    })
    mock_minio = _make_bronze_minio(sample)
    rows = load_bronze_parquet(mock_minio, target_date=date(2026, 3, 4))
    assert len(rows) == 2
    assert rows[0]['review_id'] == 'r1'


def test_load_bronze_calls_correct_prefix():
    mock_minio = MagicMock()
    mock_minio.list_objects.return_value = []
    load_bronze_parquet(mock_minio, target_date=date(2026, 3, 4))
    prefix = mock_minio.list_objects.call_args[0][0]
    assert 'year=2026' in prefix
    assert 'month=03' in prefix


# --- write_silver_parquet ---

def test_write_silver_correct_key():
    mock_minio = MagicMock()
    records = [{'review_id': 'r1', 'platform_review_id': 'p1', 'refined_text': 'clean'}]
    write_silver_parquet(mock_minio, 'app_001', date(2026, 3, 4), records)
    key = mock_minio.put_parquet.call_args[0][0]
    assert key == 'silver/reviews/app_id=app_001/dt=2026-03-04/refined.parquet'


def test_write_silver_table_columns():
    mock_minio = MagicMock()
    records = [{'review_id': 'r1', 'platform_review_id': 'p1', 'refined_text': 'clean'}]
    write_silver_parquet(mock_minio, 'app_001', date(2026, 3, 4), records)
    table = mock_minio.put_parquet.call_args[0][1]
    assert 'review_id' in table.column_names
    assert 'platform_review_id' in table.column_names
    assert 'refined_text' in table.column_names


# --- ReviewCleaningPipeline ---

@pytest.fixture
def pipeline(tmp_path):
    review_id_1 = str(uuid7())
    review_id_2 = str(uuid7())
    synonyms = {"게좌이체": "계좌이체"}
    profanity = ["욕설1"]
    (tmp_path / "synonyms.json").write_text(json.dumps(synonyms, ensure_ascii=False))
    (tmp_path / "profanity.json").write_text(json.dumps(profanity, ensure_ascii=False))

    bronze = pa.table({
        'review_id': [review_id_1, review_id_2],
        'app_id': ['app_001', 'app_001'],
        'platform_review_id': ['p1', 'p2'],
        'review_text': ['게좌이체 안 돼요 😊ㅋㅋㅋ', '욕설1 진짜 별로'],
    })
    mock_minio = _make_bronze_minio(bronze)
    mock_db = MagicMock()
    mock_db.get_session.return_value = MagicMock()

    return ReviewCleaningPipeline(
        minio_client=mock_minio,
        db_connector=mock_db,
        synonyms_path=str(tmp_path / "synonyms.json"),
        profanity_path=str(tmp_path / "profanity.json"),
    ), mock_minio


def test_pipeline_writes_silver(pipeline):
    p, mock_minio = pipeline
    p.run(target_date=date(2026, 3, 4))
    mock_minio.put_parquet.assert_called_once()
    key = mock_minio.put_parquet.call_args[0][0]
    assert 'app_id=app_001' in key
    assert 'dt=2026-03-04' in key


def test_pipeline_applies_cleansing(pipeline):
    p, mock_minio = pipeline
    p.run(target_date=date(2026, 3, 4))
    table = mock_minio.put_parquet.call_args[0][1]
    texts = table.column('refined_text').to_pylist()
    assert any('계좌이체' in t for t in texts)
    assert not any('😊' in t for t in texts)
    assert any('[SLANG]' in t for t in texts)


def test_pipeline_returns_stats(pipeline):
    p, _ = pipeline
    result = p.run(target_date=date(2026, 3, 4))
    assert result['processed'] == 2
    assert result['skipped'] == 0
    assert 'elapsed_sec' in result


def test_cleaner_email_pii_before_special_char_removal(tmp_synonyms, tmp_profanity):
    """이메일 PII 마스킹이 특수문자 제거 전에 적용되어야 한다."""
    cleaner = ReviewCleaner(synonyms_path=tmp_synonyms, profanity_path=tmp_profanity)
    result = cleaner.clean('문의: user@example.com 으로 연락주세요')
    assert '[EMAIL]' in result
    assert 'user@example.com' not in result
    assert '@' not in result  # @ 제거됨 (마스킹 후 special char 제거)


def test_pipeline_skipped_rows_not_updated_to_cleaned(tmp_path):
    """빈 텍스트 행은 DB CLEANED 업데이트에서 제외된다."""
    synonyms = {}
    profanity = []
    (tmp_path / "synonyms.json").write_text(json.dumps(synonyms))
    (tmp_path / "profanity.json").write_text(json.dumps(profanity))
    review_id_1 = str(uuid7())
    review_id_2 = str(uuid7())

    # 2개 중 1개가 빈 텍스트
    bronze = pa.table({
        'review_id': [review_id_1, review_id_2],
        'app_id': ['app1', 'app1'],
        'platform_review_id': ['p1', 'p2'],
        'review_text': ['좋은 앱', ''],  # r2 는 빈 텍스트
    })
    mock_minio = _make_bronze_minio(bronze)
    mock_db = MagicMock()
    mock_session = MagicMock()
    mock_db.get_session.return_value = mock_session

    p = ReviewCleaningPipeline(
        minio_client=mock_minio,
        db_connector=mock_db,
        synonyms_path=str(tmp_path / "synonyms.json"),
        profanity_path=str(tmp_path / "profanity.json"),
    )
    result = p.run(target_date=date(2026, 3, 4))

    assert result['processed'] == 1
    assert result['skipped'] == 1

    # DB 세션이 커밋됐다는 것은 r1만 처리됐다는 것
    mock_session.commit.assert_called_once()


def test_pipeline_marks_failed_row_and_continues(tmp_path):
    """정제 중 특정 row가 실패해도 해당 row만 FAILED 처리하고 나머지는 계속 처리한다."""
    synonyms = {}
    profanity = []
    (tmp_path / "synonyms.json").write_text(json.dumps(synonyms))
    (tmp_path / "profanity.json").write_text(json.dumps(profanity))

    failed_review_id = str(uuid7())
    ok_review_id = str(uuid7())
    bronze = pa.table({
        'review_id': [failed_review_id, ok_review_id],
        'app_id': ['app1', 'app1'],
        'platform_review_id': ['p1', 'p2'],
        'review_text': ['실패할 리뷰', '정상 리뷰'],
    })
    mock_minio = _make_bronze_minio(bronze)
    mock_db = MagicMock()
    mock_db.get_session.return_value = MagicMock()

    p = ReviewCleaningPipeline(
        minio_client=mock_minio,
        db_connector=mock_db,
        synonyms_path=str(tmp_path / "synonyms.json"),
        profanity_path=str(tmp_path / "profanity.json"),
    )
    p.cleaner.clean = MagicMock(side_effect=[RuntimeError("cleanse boom"), "정상 리뷰"])
    p._mark_review_failed = MagicMock()

    result = p.run(target_date=date(2026, 3, 4))

    assert result['processed'] == 1
    assert result['skipped'] == 0
    p._mark_review_failed.assert_called_once()
    args = p._mark_review_failed.call_args.args
    assert args[0] == failed_review_id
    assert "RuntimeError: cleanse boom" in args[1]

    table = mock_minio.put_parquet.call_args[0][1]
    assert table.column('review_id').to_pylist() == [ok_review_id]


def test_mark_review_failed_updates_master_index(tmp_path):
    """row-level cleanse 실패는 review_master_index의 FAILED 상태와 retry_count에 기록된다."""
    synonyms = {}
    profanity = []
    (tmp_path / "synonyms.json").write_text(json.dumps(synonyms))
    (tmp_path / "profanity.json").write_text(json.dumps(profanity))

    mock_db = MagicMock()
    mock_session = MagicMock()
    mock_db.get_session.return_value = mock_session
    record = SimpleNamespace(
        processing_status=ProcessingStatusType.RAW,
        error_message=None,
        retry_count=2,
    )
    mock_session.get.return_value = record

    p = ReviewCleaningPipeline(
        minio_client=MagicMock(),
        db_connector=mock_db,
        synonyms_path=str(tmp_path / "synonyms.json"),
        profanity_path=str(tmp_path / "profanity.json"),
    )
    review_id = str(uuid7())

    p._mark_review_failed(review_id, "Cleanse failed: RuntimeError: cleanse boom")

    assert record.processing_status == ProcessingStatusType.FAILED
    assert record.error_message == "Cleanse failed: RuntimeError: cleanse boom"
    assert record.retry_count == 3
    mock_session.commit.assert_called_once()
    mock_session.close.assert_called_once()


def _make_pipeline_for_update_db_status(tmp_path, db_session):
    synonyms = {}
    profanity = []
    (tmp_path / "synonyms.json").write_text(json.dumps(synonyms))
    (tmp_path / "profanity.json").write_text(json.dumps(profanity))
    mock_db = MagicMock()
    mock_db.get_session.return_value = db_session
    return ReviewCleaningPipeline(
        minio_client=MagicMock(),
        db_connector=mock_db,
        synonyms_path=str(tmp_path / "synonyms.json"),
        profanity_path=str(tmp_path / "profanity.json"),
    )


@pytest.mark.requires_db
def test_update_db_status_upserts_preprocessed_text_without_rewriting_review_id(
    tmp_path,
    test_db_session,
):
    review_id = uuid7()
    platform_review_id = "cleanse-upsert-stable"
    test_db_session.add(
        ReviewPreprocessed(
            review_id=review_id,
            platform_review_id=platform_review_id,
            refined_text="old text",
        )
    )
    test_db_session.commit()

    pipeline = _make_pipeline_for_update_db_status(tmp_path, test_db_session)
    pipeline._update_db_status(
        [str(review_id)],
        [
            {
                "review_id": str(review_id),
                "platform_review_id": platform_review_id,
                "refined_text": "new text",
            }
        ],
    )

    stored = test_db_session.query(ReviewPreprocessed).filter_by(
        platform_review_id=platform_review_id
    ).one()
    assert stored.review_id == review_id
    assert stored.refined_text == "new text"


@pytest.mark.requires_db
def test_update_db_status_rejects_platform_review_id_review_id_mismatch(
    tmp_path,
    test_db_session,
):
    stored_review_id = uuid7()
    incoming_review_id = uuid7()
    platform_review_id = "cleanse-upsert-mismatch"
    test_db_session.add(
        ReviewPreprocessed(
            review_id=stored_review_id,
            platform_review_id=platform_review_id,
            refined_text="old text",
        )
    )
    test_db_session.commit()

    pipeline = _make_pipeline_for_update_db_status(tmp_path, test_db_session)
    with pytest.raises(ValueError, match="different review_id"):
        pipeline._update_db_status(
            [str(incoming_review_id)],
            [
                {
                    "review_id": str(incoming_review_id),
                    "platform_review_id": platform_review_id,
                    "refined_text": "new text",
                }
            ],
        )


def test_mark_review_failed_ignores_already_processed_review(tmp_path):
    """이미 처리된 리뷰는 cleanse 실패로 상태를 변경하지 않는다."""
    synonyms = {}
    profanity = []
    (tmp_path / "synonyms.json").write_text(json.dumps(synonyms))
    (tmp_path / "profanity.json").write_text(json.dumps(profanity))

    mock_db = MagicMock()
    mock_session = MagicMock()
    mock_db.get_session.return_value = mock_session
    record = SimpleNamespace(
        processing_status=ProcessingStatusType.ANALYZED,
        error_message="gold analyze failed",
        retry_count=4,
    )
    mock_session.get.return_value = record

    p = ReviewCleaningPipeline(
        minio_client=MagicMock(),
        db_connector=mock_db,
        synonyms_path=str(tmp_path / "synonyms.json"),
        profanity_path=str(tmp_path / "profanity.json"),
    )

    p._mark_review_failed(str(uuid7()), "Cleanse failed: RuntimeError: cleanse boom")

    assert record.processing_status == ProcessingStatusType.ANALYZED
    assert record.error_message == "gold analyze failed"
    assert record.retry_count == 4
    mock_session.commit.assert_not_called()
    mock_session.close.assert_called_once()


def test_mark_review_failed_ignores_non_cleanse_failed_review(tmp_path):
    """다른 단계에서 FAILED 된 리뷰는 cleanse 실패로 덮어쓰지 않는다."""
    synonyms = {}
    profanity = []
    (tmp_path / "synonyms.json").write_text(json.dumps(synonyms))
    (tmp_path / "profanity.json").write_text(json.dumps(profanity))

    mock_db = MagicMock()
    mock_session = MagicMock()
    mock_db.get_session.return_value = mock_session
    record = SimpleNamespace(
        processing_status=ProcessingStatusType.FAILED,
        error_message="gold analyze failed",
        retry_count=4,
    )
    mock_session.get.return_value = record

    p = ReviewCleaningPipeline(
        minio_client=MagicMock(),
        db_connector=mock_db,
        synonyms_path=str(tmp_path / "synonyms.json"),
        profanity_path=str(tmp_path / "profanity.json"),
    )

    p._mark_review_failed(str(uuid7()), "Cleanse failed: RuntimeError: cleanse boom")

    assert record.processing_status == ProcessingStatusType.FAILED
    assert record.error_message == "gold analyze failed"
    assert record.retry_count == 4
    mock_session.commit.assert_not_called()
    mock_session.close.assert_called_once()


def test_mark_review_failed_rejects_invalid_review_id(tmp_path):
    """유효하지 않은 review_id는 실패 추적 누락으로 이어지지 않도록 예외를 발생시킨다."""
    synonyms = {}
    profanity = []
    (tmp_path / "synonyms.json").write_text(json.dumps(synonyms))
    (tmp_path / "profanity.json").write_text(json.dumps(profanity))

    mock_db = MagicMock()
    p = ReviewCleaningPipeline(
        minio_client=MagicMock(),
        db_connector=mock_db,
        synonyms_path=str(tmp_path / "synonyms.json"),
        profanity_path=str(tmp_path / "profanity.json"),
    )

    with pytest.raises(ValueError, match="Invalid review_id"):
        p._mark_review_failed("not-a-uuid", "Cleanse failed: RuntimeError: boom")

    mock_db.get_session.assert_not_called()


def test_mark_review_failed_raises_when_master_index_missing(tmp_path):
    """master index row가 없으면 실패 추적 실패를 호출자에게 노출한다."""
    synonyms = {}
    profanity = []
    (tmp_path / "synonyms.json").write_text(json.dumps(synonyms))
    (tmp_path / "profanity.json").write_text(json.dumps(profanity))

    mock_db = MagicMock()
    mock_session = MagicMock()
    mock_session.get.return_value = None
    mock_db.get_session.return_value = mock_session
    p = ReviewCleaningPipeline(
        minio_client=MagicMock(),
        db_connector=mock_db,
        synonyms_path=str(tmp_path / "synonyms.json"),
        profanity_path=str(tmp_path / "profanity.json"),
    )

    with pytest.raises(LookupError, match="ReviewMasterIndex not found"):
        p._mark_review_failed(str(uuid7()), "Cleanse failed: RuntimeError: boom")

    mock_session.rollback.assert_called_once()
    mock_session.commit.assert_not_called()
    mock_session.close.assert_called_once()


def test_mark_review_failed_reraises_db_failure(tmp_path):
    """DB 기록 실패는 rollback 후 다시 발생시켜 운영자가 감지할 수 있게 한다."""
    synonyms = {}
    profanity = []
    (tmp_path / "synonyms.json").write_text(json.dumps(synonyms))
    (tmp_path / "profanity.json").write_text(json.dumps(profanity))

    mock_db = MagicMock()
    mock_session = MagicMock()
    mock_session.get.return_value = SimpleNamespace(
        processing_status=ProcessingStatusType.RAW,
        error_message=None,
        retry_count=0,
    )
    mock_session.commit.side_effect = RuntimeError("db down")
    mock_db.get_session.return_value = mock_session
    p = ReviewCleaningPipeline(
        minio_client=MagicMock(),
        db_connector=mock_db,
        synonyms_path=str(tmp_path / "synonyms.json"),
        profanity_path=str(tmp_path / "profanity.json"),
    )

    with pytest.raises(RuntimeError, match="db down"):
        p._mark_review_failed(str(uuid7()), "Cleanse failed: RuntimeError: boom")

    mock_session.rollback.assert_called_once()
    mock_session.close.assert_called_once()


def test_pipeline_surfaces_failure_tracking_error(tmp_path):
    """row 실패를 master index에 기록하지 못하면 파이프라인 실패로 노출한다."""
    synonyms = {}
    profanity = []
    (tmp_path / "synonyms.json").write_text(json.dumps(synonyms))
    (tmp_path / "profanity.json").write_text(json.dumps(profanity))

    review_id = str(uuid7())
    bronze = pa.table({
        'review_id': [review_id],
        'app_id': ['app1'],
        'platform_review_id': ['p1'],
        'review_text': ['실패할 리뷰'],
    })
    mock_minio = _make_bronze_minio(bronze)
    mock_db = MagicMock()
    mock_db.get_session.return_value = MagicMock()

    p = ReviewCleaningPipeline(
        minio_client=mock_minio,
        db_connector=mock_db,
        synonyms_path=str(tmp_path / "synonyms.json"),
        profanity_path=str(tmp_path / "profanity.json"),
    )
    p.cleaner.clean = MagicMock(side_effect=RuntimeError("cleanse boom"))
    p._mark_review_failed = MagicMock(side_effect=RuntimeError("tracking down"))

    with pytest.raises(RuntimeError, match="tracking down"):
        p.run(target_date=date(2026, 3, 4))

    mock_minio.put_parquet.assert_not_called()


def test_update_db_status_recovers_prior_cleanse_failures(tmp_path):
    """성공적으로 재처리된 cleanse 실패 row는 CLEANED로 복구되고 오류 메시지가 지워진다."""
    synonyms = {}
    profanity = []
    (tmp_path / "synonyms.json").write_text(json.dumps(synonyms))
    (tmp_path / "profanity.json").write_text(json.dumps(profanity))

    mock_db = MagicMock()
    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_query.filter.return_value = mock_query
    mock_session.query.return_value = mock_query
    mock_db.get_session.return_value = mock_session
    p = ReviewCleaningPipeline(
        minio_client=MagicMock(),
        db_connector=mock_db,
        synonyms_path=str(tmp_path / "synonyms.json"),
        profanity_path=str(tmp_path / "profanity.json"),
    )

    p._update_db_status([str(uuid7())])

    status_filter = mock_query.filter.call_args.args[1]
    filter_text = str(
        status_filter.compile(compile_kwargs={"literal_binds": True})
    )
    update_values = mock_query.update.call_args.args[0]
    assert mock_query.filter.called
    assert "Cleanse failed" in filter_text
    assert update_values["processing_status"] == ProcessingStatusType.CLEANED
    assert update_values["error_message"] is None
    mock_session.commit.assert_called_once()


@pytest.mark.requires_db
def test_update_db_status_upserts_reviews_preprocessed_rows(tmp_path, test_db_session):
    """Bronze→Silver 성공 시 DB Gold 입력 테이블도 같이 적재되어야 한다."""
    from datetime import datetime, timezone

    from src.models.apps import App
    from src.models.enums import PlatformType
    from src.models.review_master_index import ReviewMasterIndex
    from src.models.review_preprocessed import ReviewPreprocessed

    synonyms = {}
    profanity = []
    (tmp_path / "synonyms.json").write_text(json.dumps(synonyms))
    (tmp_path / "profanity.json").write_text(json.dumps(profanity))

    app_id = uuid7()
    review_id = uuid7()
    platform_review_id = "seed-review-001"
    test_db_session.add(
        App(
            app_id=app_id,
            platform_app_id="seeded_appstore_001",
            platform_type=PlatformType.APPSTORE,
            name="Seeded App",
        )
    )
    test_db_session.add(
        ReviewMasterIndex(
            review_id=review_id,
            app_id=app_id,
            platform_review_id=platform_review_id,
            platform_type=PlatformType.APPSTORE,
            review_created_at=datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc),
            ingested_at=datetime(2026, 5, 3, 12, 1, tzinfo=timezone.utc),
            processing_status=ProcessingStatusType.RAW,
            parquet_written_at=datetime(2026, 5, 3, 12, 1, tzinfo=timezone.utc),
            storage_path="bronze/app_reviews/year=2026/month=05/day=03/data.parquet",
            retry_count=0,
            is_active=True,
            is_reply=False,
        )
    )
    test_db_session.commit()

    mock_db = MagicMock()
    mock_db.get_session.return_value = test_db_session
    pipeline = ReviewCleaningPipeline(
        minio_client=MagicMock(),
        db_connector=mock_db,
        synonyms_path=str(tmp_path / "synonyms.json"),
        profanity_path=str(tmp_path / "profanity.json"),
    )

    pipeline._update_db_status(
        [str(review_id)],
        preprocessed_records=[
            {
                "review_id": str(review_id),
                "platform_review_id": platform_review_id,
                "refined_text": "공식 seed 앱 리뷰 정제문",
            }
        ],
    )

    preprocessed = test_db_session.get(ReviewPreprocessed, review_id)
    master = test_db_session.get(ReviewMasterIndex, review_id)
    assert preprocessed is not None
    assert preprocessed.platform_review_id == platform_review_id
    assert preprocessed.refined_text == "공식 seed 앱 리뷰 정제문"
    assert master.processing_status == ProcessingStatusType.CLEANED
