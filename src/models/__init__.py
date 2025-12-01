"""
데이터 모델 패키지 초기화
"""

__version__ = "3.0.0"

# Bronze Layer Models
from .base import Base
from .app import App
from .review import Review

# Silver Layer Models
from .review_preprocessed import ReviewPreprocessed
from .review_feature import ReviewFeature
from .review_embedding import ReviewEmbedding
from .llm_analysis_log import LLMAnalysisLog

# Gold Layer Models
from .review_assigned import ReviewAssigned, DepartmentMapping

# Reference Data Models
from .dictionary import Synonym, Profanity, FinancialTerm

__all__ = [
    'Base',
    # Bronze
    'App',
    'Review',
    # Silver
    'ReviewPreprocessed',
    'ReviewFeature',
    'ReviewEmbedding',
    'LLMAnalysisLog',
    # Gold
    'ReviewAssigned',
    'DepartmentMapping',
    # Reference
    'Synonym',
    'Profanity',
    'FinancialTerm',
]
