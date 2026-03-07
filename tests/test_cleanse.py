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
