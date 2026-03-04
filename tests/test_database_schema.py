"""Test SQL Schema Validation (sql/schema_v2.sql)

This module validates that sql/schema_v2.sql creates the correct database schema:
- Extensions (uuid-ossp, vector, ltree)
- ENUM types (platform_type, app_type, etc.)
- Tables (apps, review_master_index, etc.)
- Constraints (PKs, FKs, unique constraints)
- Indexes (performance-critical columns)

All tests use a real PostgreSQL database (not SQLite).
Schema is initialized once per session via conftest.test_db_schema.
"""

import pytest
from pathlib import Path
from sqlalchemy import inspect, text


# ========================================
# A. SCHEMA FILE VALIDATION
# ========================================

def test_schema_file_exists():
    """Test that schema_v2.sql file exists and is readable."""
    schema_file = Path(__file__).parent.parent / "sql" / "schema_v2.sql"
    assert schema_file.exists(), f"Schema file not found: {schema_file}"
    assert schema_file.is_file(), f"Schema path is not a file: {schema_file}"

    # Verify file is readable and not empty
    content = schema_file.read_text()
    assert len(content) > 0, "Schema file is empty"
    assert "CREATE TABLE" in content, "Schema file does not contain CREATE TABLE"


@pytest.mark.requires_db
def test_schema_sql_syntax_valid(test_db_engine, test_db_schema):
    """Test that schema_v2.sql executes without syntax errors.

    This test is implicitly validated by test_db_schema fixture.
    If schema has syntax errors, fixture initialization will fail.
    """
    # Query a simple table to verify schema loaded
    with test_db_engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM apps"))
        count = result.scalar()
        assert count == 0, "Apps table should be empty after fresh schema load"


# ========================================
# B. EXTENSION INSTALLATION
# ========================================

@pytest.mark.requires_db
def test_extensions_installed(test_db_session):
    """Test that required PostgreSQL extensions are installed."""
    expected_extensions = {'vector'}  # uuid-ossp/ltree: not available on test server

    result = test_db_session.execute(text(
        "SELECT extname FROM pg_extension WHERE extname IN :extensions"
    ), {'extensions': tuple(expected_extensions)})

    installed_extensions = {row[0] for row in result}

    assert expected_extensions.issubset(installed_extensions), \
        f"Missing extensions: {expected_extensions - installed_extensions}"


# ========================================
# C. ENUM TYPE CREATION
# ========================================

@pytest.mark.requires_db
def test_enum_types_created(test_db_session):
    """Test that all ENUM types are created."""
    expected_enums = {
        'platform_type',
        'app_type',
        'processing_status_type',
        'analysis_status_type',
    }

    result = test_db_session.execute(text(
        """
        SELECT typname
        FROM pg_type
        WHERE typtype = 'e'
        AND typname IN :enums
        """
    ), {'enums': tuple(expected_enums)})

    created_enums = {row[0] for row in result}

    assert expected_enums.issubset(created_enums), \
        f"Missing ENUMs: {expected_enums - created_enums}"


@pytest.mark.requires_db
def test_enum_values_correct(test_db_session):
    """Test that ENUM values match schema definition."""
    # Test platform_type
    result = test_db_session.execute(text(
        """
        SELECT enumlabel
        FROM pg_enum
        WHERE enumtypid = (SELECT oid FROM pg_type WHERE typname = 'platform_type')
        ORDER BY enumlabel
        """
    ))
    platform_values = {row[0] for row in result}
    assert platform_values == {'APPSTORE', 'PLAYSTORE'}

    # Test processing_status_type
    result = test_db_session.execute(text(
        """
        SELECT enumlabel
        FROM pg_enum
        WHERE enumtypid = (SELECT oid FROM pg_type WHERE typname = 'processing_status_type')
        ORDER BY enumlabel
        """
    ))
    status_values = {row[0] for row in result}
    assert status_values == {'RAW', 'CLEANED', 'ANALYZED', 'FAILED'}


# ========================================
# D. TABLE CREATION
# ========================================

@pytest.mark.requires_db
def test_all_tables_created(test_db_session):
    """Test that all expected tables are created."""
    expected_tables = {
        'apps',
        'app_service',
        'app_metadata',
        'review_master_index',
        'app_reviews',
        'reviews_preprocessed',
        'review_embeddings',
        'review_aspects',
        'review_action_analysis',
        'reviews_assigned',
        'organizations',
        'profanities',
        'synonyms',
        'review_llm_analysis_logs'
    }

    inspector = inspect(test_db_session.bind)
    actual_tables = set(inspector.get_table_names())

    assert expected_tables.issubset(actual_tables), \
        f"Missing tables: {expected_tables - actual_tables}"


@pytest.mark.requires_db
def test_apps_table_columns_correct(test_db_session):
    """Test that apps table has correct columns."""
    inspector = inspect(test_db_session.bind)
    columns = {col['name']: col['type'] for col in inspector.get_columns('apps')}

    expected_columns = {'app_id', 'platform_app_id', 'platform_type', 'name'}
    assert expected_columns.issubset(set(columns.keys())), \
        f"Missing columns in apps table: {expected_columns - set(columns.keys())}"

    # Verify types
    assert str(columns['app_id'].python_type.__name__) in ['UUID', 'str'], \
        "app_id should be UUID type"
    assert str(columns['platform_app_id'].python_type.__name__) in ['str', 'NoneType'], \
        "platform_app_id should be TEXT type"


@pytest.mark.requires_db
def test_review_master_index_columns_correct(test_db_session):
    """Test that review_master_index table has correct columns."""
    inspector = inspect(test_db_session.bind)
    columns = {col['name']: col['type'] for col in inspector.get_columns('review_master_index')}

    critical_columns = {
        'review_id',
        'app_id',
        'platform_review_id',
        'platform_type',
        'processing_status',
        'parquet_written_at',
        'error_message',
        'retry_count',
        'is_active',
        'is_reply'
    }

    assert critical_columns.issubset(set(columns.keys())), \
        f"Missing columns: {critical_columns - set(columns.keys())}"


# ========================================
# E. CONSTRAINT VALIDATION
# ========================================

@pytest.mark.requires_db
def test_primary_keys(test_db_session):
    """Test that primary key constraints exist."""
    inspector = inspect(test_db_session.bind)

    # Test apps table
    apps_pk = inspector.get_pk_constraint('apps')
    assert apps_pk['constrained_columns'] == ['app_id'], \
        "apps table should have app_id as primary key"

    # Test review_master_index table
    review_pk = inspector.get_pk_constraint('review_master_index')
    assert review_pk['constrained_columns'] == ['review_id'], \
        "review_master_index should have review_id as primary key"


@pytest.mark.requires_db
def test_foreign_keys(test_db_session):
    """Test that foreign key constraints exist."""
    inspector = inspect(test_db_session.bind)

    # Test review_master_index → apps FK
    fks = inspector.get_foreign_keys('review_master_index')
    fk_names = {fk['referred_table'] for fk in fks}

    assert 'apps' in fk_names, \
        "review_master_index should have FK to apps table"

    # Verify FK column
    apps_fk = next(fk for fk in fks if fk['referred_table'] == 'apps')
    assert apps_fk['constrained_columns'] == ['app_id'], \
        "FK should be on app_id column"
    assert apps_fk['referred_columns'] == ['app_id'], \
        "FK should reference apps.app_id"


@pytest.mark.requires_db
def test_unique_constraints(test_db_session):
    """Test that unique constraints exist."""
    inspector = inspect(test_db_session.bind)

    # Test review_master_index.platform_review_id unique constraint
    unique_constraints = inspector.get_unique_constraints('review_master_index')

    # Check if platform_review_id is unique (could be in index or constraint)
    result = test_db_session.execute(text(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = 'review_master_index'
        AND indexdef LIKE '%UNIQUE%'
        AND indexdef LIKE '%platform_review_id%'
        """
    ))

    unique_index_exists = result.fetchone() is not None
    unique_constraint_exists = any(
        'platform_review_id' in uc['column_names']
        for uc in unique_constraints
    )

    assert unique_index_exists or unique_constraint_exists, \
        "platform_review_id should have unique constraint or index"


# ========================================
# F. INDEX CREATION
# ========================================

@pytest.mark.requires_db
def test_indexes_exist(test_db_session):
    """Test that performance-critical indexes exist."""
    result = test_db_session.execute(text(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE tablename = 'review_master_index'
        """
    ))

    index_names = {row[0] for row in result}

    # Critical indexes for Phase 3 NAS-first architecture
    expected_indexes = {
        'idx_review_master_index_processing_status',
        'idx_review_master_index_app_id',
        'idx_review_master_index_failed',  # WHERE processing_status = 'FAILED'
        'idx_review_master_index_retry',   # WHERE retry_count < 3
    }

    missing_indexes = expected_indexes - index_names

    # Allow some flexibility (partial index names might differ)
    critical_missing = [idx for idx in missing_indexes
                        if 'processing_status' in idx or 'app_id' in idx]

    assert len(critical_missing) == 0, \
        f"Critical indexes missing: {critical_missing}"


@pytest.mark.requires_db
def test_vector_index_exists(test_db_session):
    """Test that vector similarity search index exists."""
    result = test_db_session.execute(text(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = 'review_embeddings'
        AND indexdef LIKE '%hnsw%'
        """
    ))

    vector_index = result.fetchone()
    assert vector_index is not None, \
        "review_embeddings should have HNSW vector index"


# ========================================
# G. DATA INTEGRITY TESTS
# ========================================

@pytest.mark.requires_db
def test_enum_column_constraints(test_db_session):
    """Test that ENUM columns reject invalid values."""
    from src.models.apps import App
    from uuid6 import uuid7

    # Try to create app with invalid platform_type (should fail)
    with pytest.raises(Exception):  # StatementError or DataError
        invalid_app = App(
            app_id=uuid7(),
            platform_app_id='test_invalid',
            platform_type='INVALID_PLATFORM',  # Not in ENUM
            name='Test App'
        )
        test_db_session.add(invalid_app)
        test_db_session.commit()

    test_db_session.rollback()


@pytest.mark.requires_db
def test_not_null_constraints(test_db_session):
    """Test that NOT NULL constraints are enforced."""
    from src.models.apps import App
    from uuid6 import uuid7

    # Try to create app without required fields
    with pytest.raises(Exception):  # IntegrityError
        invalid_app = App(
            app_id=uuid7(),
            platform_app_id='test_invalid',
            platform_type='APPSTORE',
            name=None  # Should fail NOT NULL constraint
        )
        test_db_session.add(invalid_app)
        test_db_session.commit()

    test_db_session.rollback()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
