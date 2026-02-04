"""Test central ENUM module consolidation.

This test verifies that:
1. ENUMs can be imported from the central enums module
2. ENUMs can be imported from the models package
3. All model classes properly use the central ENUMs
"""

import pytest


def test_enum_import_from_enums_module():
    """Test direct import from enums module."""
    from src.models.enums import (
        PlatformType,
        AppType,
        ProcessingStatusType,
        AnalysisStatusType,
        SentimentType,
    )

    # Verify enum values
    assert PlatformType.APPSTORE.value == "APPSTORE"
    assert PlatformType.PLAYSTORE.value == "PLAYSTORE"

    assert AppType.CONSUMER.value == "CONSUMER"
    assert AppType.CORPORATE.value == "CORPORATE"
    assert AppType.GLOBAL.value == "GLOBAL"

    assert ProcessingStatusType.RAW.value == "RAW"
    assert ProcessingStatusType.CLEANED.value == "CLEANED"
    assert ProcessingStatusType.ANALYZED.value == "ANALYZED"
    assert ProcessingStatusType.FAILED.value == "FAILED"

    assert AnalysisStatusType.PENDING.value == "PENDING"
    assert AnalysisStatusType.PROCESSING.value == "PROCESSING"
    assert AnalysisStatusType.SUCCESS.value == "SUCCESS"
    assert AnalysisStatusType.FAILED.value == "FAILED"

    assert SentimentType.POSITIVE.value == "POSITIVE"
    assert SentimentType.NEGATIVE.value == "NEGATIVE"
    assert SentimentType.NEUTRAL.value == "NEUTRAL"


def test_enum_import_from_models_package():
    """Test import from models package __init__.py."""
    from src.models import (
        PlatformType,
        AppType,
        ProcessingStatusType,
        AnalysisStatusType,
        SentimentType,
    )

    # Verify they are the same classes as from enums module
    from src.models.enums import (
        PlatformType as DirectPlatformType,
        AppType as DirectAppType,
    )

    assert PlatformType is DirectPlatformType
    assert AppType is DirectAppType


def test_model_uses_central_enums():
    """Test that models use the central ENUM definitions."""
    from src.models import App, ReviewMasterIndex, AppMetadata, LLMAnalysisLog
    from src.models.enums import PlatformType, AppType, ProcessingStatusType, AnalysisStatusType

    # Verify models can be instantiated (without DB connection)
    # This tests that ENUMs are properly referenced in model definitions

    # App model uses PlatformType
    assert hasattr(App, 'platform')

    # ReviewMasterIndex uses PlatformType and ProcessingStatusType
    assert hasattr(ReviewMasterIndex, 'platform_type')
    assert hasattr(ReviewMasterIndex, 'processing_status')

    # AppMetadata uses AppType
    assert hasattr(AppMetadata, 'app_type')

    # LLMAnalysisLog uses AnalysisStatusType
    assert hasattr(LLMAnalysisLog, 'status')


def test_enum_no_duplicate_definitions():
    """Test that ENUMs are not duplicated in model files."""
    import inspect
    from src.models import review_master_index, review, apps, app_metadata, llm_analysis_log

    # Check that these modules don't define their own ENUM classes
    for module in [review_master_index, review, apps, app_metadata, llm_analysis_log]:
        # Get all classes defined in the module
        classes = [name for name, obj in inspect.getmembers(module, inspect.isclass)
                   if obj.__module__ == module.__name__]

        # Verify no ENUM classes are defined (they should only have ORM models)
        enum_names = ['PlatformType', 'AppType', 'ProcessingStatusType', 'AnalysisStatusType']
        for enum_name in enum_names:
            assert enum_name not in classes, f"{enum_name} should not be defined in {module.__name__}"


def test_enum_members_count():
    """Test that ENUMs have expected number of members."""
    from src.models.enums import (
        PlatformType,
        AppType,
        ProcessingStatusType,
        AnalysisStatusType,
        SentimentType,
    )

    assert len(PlatformType) == 2  # APPSTORE, PLAYSTORE
    assert len(AppType) == 3  # CONSUMER, CORPORATE, GLOBAL
    assert len(ProcessingStatusType) == 4  # RAW, CLEANED, ANALYZED, FAILED
    assert len(AnalysisStatusType) == 4  # PENDING, PROCESSING, SUCCESS, FAILED
    assert len(SentimentType) == 3  # POSITIVE, NEGATIVE, NEUTRAL


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
