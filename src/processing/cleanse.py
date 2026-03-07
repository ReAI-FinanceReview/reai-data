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
    """문장부호(!?.,)를 제외한 특수기호를 제거한다."""
    if not text:
        return text
    return re.sub(r'[^\w\s!?.,]', '', text)


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
# ReviewCleaner: 7-step 정제 파이프라인 클래스
# =========================================================

import json
from flashtext import KeywordProcessor


class ReviewCleaner:
    """텍스트 정제 파이프라인 클래스.

    정제 순서: NFKC → 이모지 제거 → 반복문자 축약 → 특수문자 제거
               → 오타 교정 → PII 마스킹 → 비속어 마스킹
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
        # list 또는 dict 형식 모두 지원
        words = data.keys() if isinstance(data, dict) else data
        for word in words:
            processor.add_keyword(word, '[SLANG]')
        return processor

    def clean(self, text: str) -> str:
        """텍스트에 전체 정제 파이프라인을 적용한다."""
        if not text:
            return text
        text = normalize_unicode(text)
        text = remove_emojis(text)
        text = reduce_repeated_chars(text)
        text = remove_special_chars(text)
        text = self._synonym_processor.replace_keywords(text)
        text = mask_pii(text)
        text = self._profanity_processor.replace_keywords(text)
        return text.strip()
