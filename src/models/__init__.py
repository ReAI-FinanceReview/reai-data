"""데이터 모델 패키지 초기화

SQLAlchemy models for schema_v2.sql (hybrid DB+NAS architecture)
"""

__version__ = "5.0.0"  # Bump version for schema v4 (ingestion_batch)

from .base import Base

# Central ENUM definitions
from .enums import (
    PlatformType,
    AppType,
    ProcessingStatusType,
    AnalysisStatusType,
    SentimentType,
    IngestionBatchStatusType,
    CategoryType,
)

# App-related models
from .app_service import AppService
from .apps import App
from .app_metadata import AppMetadata

# Review master index
from .review_master_index import ReviewMasterIndex

# Ingestion DLQ
from .ingestion_batch import IngestionBatch

# Bronze Layer (NAS Parquet)
from .review import Review

# Silver Layer
from .review_preprocessed import ReviewPreprocessed
from .review_embedding import ReviewEmbedding
from .review_aspects import ReviewAspect
from .review_action_analysis import ReviewActionAnalysis
from .llm_analysis_log import LLMAnalysisLog

# Gold Layer
from .review_assigned import ReviewAssigned

# Gold Fact Tables & Serving Mart
from .fact_service_review_daily import FactServiceReviewDaily
from .fact_service_aspect_daily import FactServiceAspectDaily
from .fact_category_radar_scores import FactCategoryRadarScores
from .srv_daily_review_list import SrvDailyReviewList

# Reference Data
from .dictionary import Synonym, Profanity, FinancialTerm
from .organizations import Organization

__all__ = [
    'Base',
    # ENUMs
    'PlatformType',
    'AppType',
    'ProcessingStatusType',
    'AnalysisStatusType',
    'SentimentType',
    'IngestionBatchStatusType',
    'CategoryType',
    # App
    'AppService',
    'App',
    'AppMetadata',
    # Review Index
    'ReviewMasterIndex',
    # Ingestion DLQ
    'IngestionBatch',
    # Bronze (NAS)
    'Review',
    # Silver
    'ReviewPreprocessed',
    'ReviewEmbedding',
    'ReviewAspect',
    'ReviewActionAnalysis',
    'LLMAnalysisLog',
    # Gold
    'ReviewAssigned',
    # Gold Fact Tables & Serving Mart
    'FactServiceReviewDaily',
    'FactServiceAspectDaily',
    'FactCategoryRadarScores',
    'SrvDailyReviewList',
    # Reference
    'Synonym',
    'Profanity',
    'FinancialTerm',
    'Organization',
]
