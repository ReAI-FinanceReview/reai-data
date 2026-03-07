import pytest
from src.processing.cleanse import (
    normalize_unicode,
    remove_emojis,
    reduce_repeated_chars,
    remove_special_chars,
    mask_pii,
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
# ReviewCleaner
# ========================================

import json
import tempfile
from pathlib import Path
from src.processing.cleanse import ReviewCleaner


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
    assert '[SLANG]' in result
    assert '욕설A' not in result


# ========================================
# Bronze 로더 / Silver 라이터 / Pipeline
# ========================================

import pyarrow as pa
from datetime import date
from unittest.mock import MagicMock
from src.processing.cleanse import (
    load_bronze_parquet,
    write_silver_parquet,
    ReviewCleaningPipeline,
)


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
    synonyms = {"게좌이체": "계좌이체"}
    profanity = ["욕설1"]
    (tmp_path / "synonyms.json").write_text(json.dumps(synonyms, ensure_ascii=False))
    (tmp_path / "profanity.json").write_text(json.dumps(profanity, ensure_ascii=False))

    bronze = pa.table({
        'review_id': ['r1', 'r2'],
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

    # 2개 중 1개가 빈 텍스트
    bronze = pa.table({
        'review_id': ['r1', 'r2'],
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
