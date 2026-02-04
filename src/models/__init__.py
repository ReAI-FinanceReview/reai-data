"""데이터 모델 패키지 초기화

SQLAlchemy models for schema_v2.sql (hybrid DB+NAS architecture)
"""

__version__ = "4.0.0"  # Bump version for major schema change

from .base import Base

# App-related models
from .app_service import AppService
from .app import App
from .app_metadata import AppMetadata, AppType

# Review master index
from .review_master_index import ReviewMasterIndex, PlatformType, ProcessingStatusType

# Bronze Layer (NAS Parquet)
from .review import Review

# Silver Layer
from .review_preprocessed import ReviewPreprocessed
from .review_embedding import ReviewEmbedding
from .review_aspects import ReviewAspect
from .review_action_analysis import ReviewActionAnalysis
from .llm_analysis_log import LLMAnalysisLog, AnalysisStatusType

# Gold Layer
from .review_assigned import ReviewAssigned

# Reference Data
from .dictionary import Synonym, Profanity, FinancialTerm
from .organizations import Organization

__all__ = [
    'Base',
    # App
    'AppService',
    'App',
    'AppMetadata',
    'AppType',
    # Review Index
    'ReviewMasterIndex',
    'PlatformType',
    'ProcessingStatusType',
    # Bronze (NAS)
    'Review',
    # Silver
    'ReviewPreprocessed',
    'ReviewEmbedding',
    'ReviewAspect',
    'ReviewActionAnalysis',
    'LLMAnalysisLog',
    'AnalysisStatusType',
    # Gold
    'ReviewAssigned',
    # Reference
    'Synonym',
    'Profanity',
    'FinancialTerm',
    'Organization',
]
