"""Test Infrastructure and Shared Fixtures

This module provides comprehensive test infrastructure for the review ETL pipeline,
including:
- Database setup and teardown (PostgreSQL with schema initialization)
- Temporary storage fixtures (Parquet directories)
- Mock data generators (sample reviews, apps)
- Mock API fixtures (App Store, Play Store)
- Database state helpers (pre-populated tables)

Architecture:
- Session-scoped DB engine (shared across all tests)
- Function-scoped DB sessions (isolated with rollback)
- Automatic schema initialization from sql/schema_v2.sql
- Real PostgreSQL for schema validation (no SQLite mocks)
"""

import pytest
import tempfile
import shutil
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any
from uuid import UUID
from uuid6 import uuid7

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool

from src.models.base import Base
from src.models.apps import App
from src.models.review_master_index import ReviewMasterIndex
from src.models.enums import PlatformType, ProcessingStatusType
from src.schemas.parquet.app_review import AppReviewSchema


# ========================================
# DATABASE FIXTURES
# ========================================

@pytest.fixture(scope="session")
def test_db_url() -> str:
    """Get test database URL from environment or use default.

    Priority:
    1. TEST_DATABASE_URL environment variable
       - 서버 개발 시 (VS Code Remote SSH): postgresql://vector_mgr:...@reai.kro.kr:55000/testdb
       - 로컬 개발 시: docker-compose.test.yml 실행 후 localhost:5433/testdb
    2. Default: postgresql://testuser:testpass@localhost:5433/testdb (docker-compose.test.yml 기본값)

    Note: Tests require a real PostgreSQL database (not SQLite).
    testdb는 프로덕션 DB(postgres)와 완전히 분리된 전용 테스트 DB여야 한다.
    DROP SCHEMA public CASCADE가 실행되므로 절대 프로덕션 DB를 가리켜선 안 된다.
    """
    default_url = "postgresql://testuser:testpass@localhost:5433/testdb"
    return os.getenv("TEST_DATABASE_URL", default_url)


@pytest.fixture(scope="session")
def test_db_engine(test_db_url: str):
    """Create a session-scoped database engine.

    This engine is shared across all tests in the session.
    Uses NullPool to avoid connection pooling issues in tests.
    """
    engine = create_engine(
        test_db_url,
        poolclass=NullPool,  # No connection pooling for tests
        echo=False  # Set to True for SQL debugging
    )

    yield engine

    # Cleanup: dispose of the engine
    engine.dispose()


@pytest.fixture(scope="session")
def test_db_schema(test_db_engine):
    """Initialize test database schema from sql/schema_v3.sql.

    This fixture:
    1. Drops the public schema and recreates it (clean slate)
    2. Executes schema_v3.sql to create all 18 tables, 5 ENUMs, and indexes
    3. Runs once per test session

    Note: uuid-ossp extension line is filtered out because Python uuid7()
    is used for UUID generation and the DB extension is not required.
    """
    schema_file = Path(__file__).parent.parent / "sql" / "schema_v3.sql"
    sql_content = schema_file.read_text()

    # Filter/replace lines incompatible with test server:
    # - uuid-ossp: not installed (Python uuid7() handles UUID generation)
    # - ltree: not available on test server (org_id column replaced with TEXT)
    filtered_lines = []
    for line in sql_content.splitlines():
        code = line.split('--')[0]  # strip inline comments
        if 'uuid-ossp' in code:
            continue  # skip uuid-ossp extension
        if 'ltree' in code and 'CREATE EXTENSION' in code:
            continue  # skip ltree extension
        if 'USING GIST' in code and 'org_id' in code:
            continue  # skip ltree GIST index (not supported without ltree)
        if 'ltree' in code and 'COMMENT' not in code:
            line = line.replace('ltree', 'TEXT')  # replace ltree type with TEXT
        filtered_lines.append(line)
    sql_filtered = '\n'.join(filtered_lines)

    # Use raw psycopg2 connection to execute multi-statement SQL
    raw_conn = test_db_engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        cursor.execute("DROP SCHEMA public CASCADE;")
        cursor.execute("CREATE SCHEMA public;")
        cursor.execute(sql_filtered)
        raw_conn.commit()
    finally:
        raw_conn.close()

    yield


@pytest.fixture
def test_db_session(test_db_engine, test_db_schema) -> Session:
    """Create an isolated database session for each test.

    This fixture:
    1. Creates a new connection
    2. Begins a transaction
    3. Creates a session bound to that transaction
    4. Yields the session for the test
    5. Rolls back the transaction (no changes committed)
    6. Closes the connection

    This ensures test isolation without recreating the schema.
    """
    # Create a connection
    connection = test_db_engine.connect()

    # Begin a transaction
    transaction = connection.begin()

    # Create a session bound to this connection
    SessionLocal = sessionmaker(bind=connection)
    session = SessionLocal()

    yield session

    # Rollback transaction (discard all changes)
    session.close()
    transaction.rollback()
    connection.close()


# ========================================
# TEMPORARY STORAGE FIXTURES
# ========================================

@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests.

    Automatically cleaned up after test completion.
    """
    temp_path = tempfile.mkdtemp()
    yield Path(temp_path)
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def temp_parquet_dir(temp_dir):
    """Create temporary Parquet storage directory.

    Returns: Path to temp directory for Parquet files
    """
    parquet_dir = temp_dir / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    return parquet_dir


@pytest.fixture
def temp_bronze_dir(temp_dir):
    """Create Bronze layer directory structure.

    Creates: temp_dir/bronze/app_reviews/
    This mirrors the production medallion architecture.
    """
    bronze_dir = temp_dir / "bronze" / "app_reviews"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    return bronze_dir


# ========================================
# SAMPLE DATA GENERATORS
# ========================================

@pytest.fixture
def sample_appstore_reviews() -> List[Dict[str, Any]]:
    """Generate sample App Store API response data.

    Returns: List of 5 review dictionaries matching App Store RSS feed format
    """
    base_time = datetime(2026, 2, 4, 12, 0, 0, tzinfo=timezone.utc)

    reviews = []
    for i in range(5):
        review = {
            'id': {'label': f'appstore_review_{i}'},
            'author': {'name': {'label': f'Reviewer_{i}'}},
            'im:name': {'label': 'Test App'},
            'content': {'label': f'This is test review {i}. Great app!'},
            'im:rating': {'label': str(5 - (i % 5))},
            'updated': {
                'label': base_time.replace(minute=i).isoformat().replace('+00:00', 'Z')
            },
            'im:version': {'label': '1.0.0'}
        }
        reviews.append(review)

    return reviews


@pytest.fixture
def sample_playstore_reviews() -> List[Dict[str, Any]]:
    """Generate sample Play Store API response data.

    Returns: List of 5 review dictionaries matching google-play-scraper format
    """
    base_time = datetime(2026, 2, 4, 12, 0, 0, tzinfo=timezone.utc)

    reviews = []
    for i in range(5):
        review = {
            'reviewId': f'playstore_review_{i}',
            'userName': f'PlayUser_{i}',
            'content': f'This is test review {i} from Play Store.',
            'score': 5 - (i % 5),
            'at': base_time.replace(minute=i),  # datetime object
            'appVersion': '1.0.0'
        }
        reviews.append(review)

    return reviews


@pytest.fixture
def sample_app_id_file(tmp_path):
    """Create a temporary app_ids.txt file.

    Returns: Path to temporary app_ids.txt with 3 valid app IDs
    """
    app_ids_file = tmp_path / "app_ids.txt"
    content = """# Test App IDs
123456789
987654321

# Another app
555555555
"""
    app_ids_file.write_text(content)
    return app_ids_file


# ========================================
# DATABASE STATE HELPERS
# ========================================

@pytest.fixture
def db_with_apps(test_db_session: Session) -> Session:
    """Create a test database with pre-populated App records.

    Creates 2 apps:
    - App Store app: platform_app_id='123456789'
    - Play Store app: platform_app_id='com.example.testapp'

    Returns: Session with apps pre-loaded
    """
    # Create App Store app
    app_appstore = App(
        app_id=uuid7(),
        platform_app_id='123456789',
        platform_type=PlatformType.APPSTORE,
        name='Test AppStore App'
    )

    # Create Play Store app
    app_playstore = App(
        app_id=uuid7(),
        platform_app_id='com.example.testapp',
        platform_type=PlatformType.PLAYSTORE,
        name='Test PlayStore App'
    )

    test_db_session.add_all([app_appstore, app_playstore])
    test_db_session.commit()

    return test_db_session


@pytest.fixture
def db_with_failed_reviews(test_db_session: Session) -> Session:
    """Create a test database with FAILED review records.

    Creates:
    - 1 App
    - 3 ReviewMasterIndex records with processing_status=FAILED
    - Various retry_count values (0, 1, 3)

    Returns: Session with failed reviews pre-loaded
    """
    # Create app
    app = App(
        app_id=uuid7(),
        platform_app_id='999999999',
        platform_type=PlatformType.APPSTORE,
        name='Failed Reviews Test App'
    )
    test_db_session.add(app)
    test_db_session.flush()

    # Create failed reviews
    now = datetime.now(timezone.utc)

    failed_reviews = [
        ReviewMasterIndex(
            review_id=uuid7(),
            app_id=app.app_id,
            platform_review_id=f'failed_review_0',
            platform_type=PlatformType.APPSTORE,
            review_created_at=now,
            ingested_at=now,
            processing_status=ProcessingStatusType.FAILED,
            error_message='PARQUET_WRITE_FAILED: Disk full',
            retry_count=0,
            is_active=True,
            is_reply=False
        ),
        ReviewMasterIndex(
            review_id=uuid7(),
            app_id=app.app_id,
            platform_review_id=f'failed_review_1',
            platform_type=PlatformType.APPSTORE,
            review_created_at=now,
            ingested_at=now,
            processing_status=ProcessingStatusType.FAILED,
            error_message='DB_COMMIT_FAILED: Connection timeout',
            retry_count=1,
            is_active=True,
            is_reply=False
        ),
        ReviewMasterIndex(
            review_id=uuid7(),
            app_id=app.app_id,
            platform_review_id=f'failed_review_3',
            platform_type=PlatformType.APPSTORE,
            review_created_at=now,
            ingested_at=now,
            processing_status=ProcessingStatusType.FAILED,
            error_message='PARQUET_WRITE_FAILED: Permission denied',
            retry_count=3,  # Max retries reached
            is_active=True,
            is_reply=False
        ),
    ]

    test_db_session.add_all(failed_reviews)
    test_db_session.commit()

    return test_db_session


# ========================================
# MOCK API FIXTURES
# ========================================

@pytest.fixture
def mock_appstore_api(requests_mock):
    """Mock App Store RSS feed API responses.

    Usage in tests:
        def test_something(mock_appstore_api):
            # API is already mocked, just call the crawler
            crawler.get_app_store_reviews_and_appname('123456789')

    Returns: requests_mock fixture with App Store endpoints configured
    """
    # Mock successful response
    base_url = "https://itunes.apple.com/kr/rss/customerreviews"

    # Page 1 response (with app name in first entry)
    page1_data = {
        'feed': {
            'entry': [
                # First entry is app metadata
                {'im:name': {'label': 'Mock App'}},
                # Actual reviews
                {
                    'id': {'label': 'mock_review_1'},
                    'author': {'name': {'label': 'MockUser1'}},
                    'content': {'label': 'Great app!'},
                    'im:rating': {'label': '5'},
                    'updated': {'label': '2026-02-04T12:00:00Z'},
                },
                {
                    'id': {'label': 'mock_review_2'},
                    'author': {'name': {'label': 'MockUser2'}},
                    'content': {'label': 'Good app'},
                    'im:rating': {'label': '4'},
                    'updated': {'label': '2026-02-04T11:00:00Z'},
                }
            ]
        }
    }

    # Page 2 response (no more reviews)
    page2_data = {'feed': {}}

    requests_mock.get(
        f"{base_url}/page=1/id=123456789/sortby=mostRecent/json",
        json=page1_data
    )
    requests_mock.get(
        f"{base_url}/page=2/id=123456789/sortby=mostRecent/json",
        json=page2_data
    )

    return requests_mock


@pytest.fixture
def mock_playstore_api(monkeypatch):
    """Mock google-play-scraper functions.

    Usage in tests:
        def test_something(mock_playstore_api):
            # google_play_scraper.reviews() is already mocked
            crawler.crawl_reviews('com.example.app')

    Returns: Monkeypatch fixture with Play Store functions mocked
    """
    from unittest.mock import MagicMock

    # Mock reviews() function
    mock_reviews_result = (
        [
            {
                'reviewId': 'mock_play_review_1',
                'userName': 'PlayUser1',
                'content': 'Excellent app!',
                'score': 5,
                'at': datetime(2026, 2, 4, 12, 0, 0, tzinfo=timezone.utc),
                'appVersion': '1.0.0'
            },
            {
                'reviewId': 'mock_play_review_2',
                'userName': 'PlayUser2',
                'content': 'Very good',
                'score': 4,
                'at': datetime(2026, 2, 4, 11, 0, 0, tzinfo=timezone.utc),
                'appVersion': '1.0.0'
            }
        ],
        None  # continuation_token
    )

    mock_reviews = MagicMock(return_value=mock_reviews_result)

    # Mock app() function
    mock_app_result = {
        'title': 'Mock Play Store App',
        'installs': '1,000+',
        'score': 4.5
    }

    mock_app = MagicMock(return_value=mock_app_result)

    # Patch google_play_scraper
    monkeypatch.setattr('google_play_scraper.reviews', mock_reviews)
    monkeypatch.setattr('google_play_scraper.app', mock_app)

    return monkeypatch


# ========================================
# PYTEST CONFIGURATION
# ========================================

def pytest_configure(config):
    """Configure custom pytest markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "requires_db: marks tests requiring real PostgreSQL database"
    )
