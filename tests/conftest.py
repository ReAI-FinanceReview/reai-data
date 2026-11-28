"""Test Infrastructure and Shared Fixtures

This module provides comprehensive test infrastructure for the review ETL pipeline,
including:
- Database setup and teardown (PostgreSQL with schema initialization)
- Temporary storage fixtures (Parquet directories)
- Mock data generators (sample reviews, apps)
- Mock API fixtures (App Store, Play Store)
- Real local HTTP servers for the App Store RSS and the Play reviews batchexecute endpoint
- Database state helpers (pre-populated tables)

Architecture:
- Session-scoped DB engine (shared across all tests)
- Function-scoped DB sessions (isolated with rollback)
- Automatic schema initialization from sql/schema_v4.sql
- Real PostgreSQL for schema validation (no SQLite mocks)
"""

import pytest
import tempfile
import shutil
import os
import json
import re
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from urllib.parse import parse_qs, urlsplit
from uuid6 import uuid7

import yaml
from sqlalchemy import create_engine, make_url, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool

from src.models.apps import App
from src.models.review_master_index import ReviewMasterIndex
from src.models.ingestion_batch import IngestionBatch
from src.models.enums import PlatformType, ProcessingStatusType, IngestionBatchStatusType
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
         (pgvector/pgvector:pg17 이미지 필요: schema_v4.sql의 vector extension 사용)
    2. Default: postgresql://testuser:testpass@localhost:{TEST_POSTGRES_PORT:-5433}/testdb
       (docker-compose.test.yml 기본값과 동일한 포트)

    Note: Tests require a real PostgreSQL database (not SQLite).
    testdb는 프로덕션 DB(postgres)와 완전히 분리된 전용 테스트 DB여야 한다.
    DROP SCHEMA public CASCADE가 실행되므로 절대 프로덕션 DB를 가리켜선 안 된다.
    """
    test_postgres_port = os.getenv("TEST_POSTGRES_PORT", "5433")
    default_url = f"postgresql://testuser:testpass@localhost:{test_postgres_port}/testdb"
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


def _apply_migrations(database_url: str) -> None:
    """베이스라인 이후 리비전을 테스트 스키마에도 적용한다.

    schema_v4.sql 은 Alembic 베이스라인 스냅샷이므로 그 이후 리비전이 추가한
    컬럼/제약은 여기에 없다. 프로덕션과 CI(.github/workflows/bootstrap-db.yml)는
    `alembic upgrade head` 를 돌리므로, 테스트 DB 만 베이스라인에 머물면 마이그레이션이
    추가한 컬럼을 쓰는 코드가 테스트에서만 깨진다.

    이미 head 인 DB 에 다시 부르면 아무것도 하지 않는다. 두 호출부가 지금은
    DROP SCHEMA 직후에만 부르지만, 그 규율이 깨지면 리비전이 두 번 적용되어
    ADD COLUMN 이 DuplicateColumn 으로 죽는다. 안전성을 호출부가 아니라 이
    함수가 보장하게 한다.
    """
    from alembic import command
    from alembic.config import Config
    from alembic.runtime.migration import MigrationContext
    from alembic.script import ScriptDirectory
    from sqlalchemy import create_engine

    config = Config(str(Path(__file__).parent.parent / "alembic.ini"))
    config.attributes["database_url"] = database_url

    head = ScriptDirectory.from_config(config).get_current_head()
    engine = create_engine(database_url, poolclass=NullPool)
    try:
        with engine.connect() as connection:
            current = MigrationContext.configure(connection).get_current_revision()
    finally:
        engine.dispose()
    if current == head:
        return

    command.stamp(config, "20260430_0001", purge=True)
    command.upgrade(config, "head")


@pytest.fixture(scope="session")
def test_db_schema(test_db_engine, test_db_url):
    """Initialize test database schema from sql/schema_v4.sql, then migrate to head.

    This fixture:
    1. Drops the public schema and recreates it (clean slate)
    2. Executes schema_v4.sql to create all 19 tables, 6 ENUMs, and indexes
    3. Applies Alembic revisions after the baseline (see _apply_migrations)
    4. Runs once per test session

    Step 3 exists because schema_v4.sql is the frozen baseline snapshot, so
    columns and constraints added by later revisions are absent from it, while
    production and CI both run `alembic upgrade head`.

    Note: Local Docker tests require a pgvector-capable PostgreSQL 17 image.
    uuid-ossp is filtered out because Python uuid7() is used for UUID generation
    and the DB extension is not required.
    """
    schema_file = Path(__file__).parent.parent / "sql" / "schema_v4.sql"
    sql_content = schema_file.read_text()

    # Filter/replace lines incompatible with test server:
    # - uuid-ossp: not installed (Python uuid7() handles UUID generation)
    # - ltree: not available on test server (org_id column replaced with TEXT)
    filtered_lines = []
    for line in sql_content.splitlines():
        code = line.split('--')[0]  # strip inline comments
        if 'uuid-ossp' in code:
            continue  # skip uuid-ossp extension
        if 'uuid_generate_v4()' in code:
            line = line.replace('DEFAULT uuid_generate_v4()', '')  # uuid-ossp not installed in test DB
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

    _apply_migrations(test_db_url)

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
            platform_review_id='failed_review_0',
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
            platform_review_id='failed_review_1',
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
            platform_review_id='failed_review_3',
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
# INGESTION BATCH FIXTURES (Issue #19)
# ========================================

@pytest.fixture
def db_with_pending_batches(test_db_session: Session, temp_bronze_dir) -> Session:
    """Create ingestion_batch records in PENDING state.

    Creates:
    - 1 IngestionBatch record with status=PENDING
    - Matching Parquet file in temp_bronze_dir

    Returns: Session with pending batch pre-loaded
    """
    from src.utils.parquet_writer import ParquetWriter

    now = datetime.now(timezone.utc)

    # Create a Parquet file
    writer = ParquetWriter(base_path=str(temp_bronze_dir), partition_by='year_month')

    records_1 = [
        AppReviewSchema(
            review_id=str(uuid7()),
            app_id=str(uuid7()),
            platform_type='APPSTORE',
            platform_review_id=f'pending_review_{i}',
            reviewer_name=f'User{i}',
            review_text=f'Review {i}',
            rating=5,
            reviewed_at=datetime(2026, 2, 4, 12, i, tzinfo=timezone.utc),
            is_reply=False,
            reply_comment=None
        )
        for i in range(3)
    ]
    parquet_path_1 = writer.write_batch(records_1)

    batch_1 = IngestionBatch(
        batch_id=uuid7(),
        source_type=PlatformType.APPSTORE,
        platform_app_id='123456789',
        app_name='Test App',
        storage_path=str(parquet_path_1),
        file_format='parquet',
        record_count=len(records_1),
        status=IngestionBatchStatusType.PENDING,
        retry_count=0,
        max_retries=3,
        created_at=now,
        updated_at=now
    )
    test_db_session.add(batch_1)
    test_db_session.commit()
    return test_db_session


@pytest.fixture
def db_with_failed_batches(test_db_session: Session, temp_bronze_dir) -> Session:
    """Create ingestion_batch records with various failure states.

    Creates:
    - 3 IngestionBatch records:
      - status=FAILED, retry_count=0
      - status=FAILED, retry_count=1
      - status=DEAD_LETTER, retry_count=3 (max reached)

    Returns: Session with failed batches pre-loaded
    """
    now = datetime.now(timezone.utc)

    batches = [
        IngestionBatch(
            batch_id=uuid7(),
            source_type=PlatformType.APPSTORE,
            platform_app_id='111111111',
            app_name='Failed App 1',
            storage_path='/tmp/fake_batch_0.parquet',
            file_format='parquet',
            record_count=5,
            status=IngestionBatchStatusType.FAILED,
            retry_count=0,
            max_retries=3,
            error_message='DB connection failed',
            created_at=now,
            updated_at=now
        ),
        IngestionBatch(
            batch_id=uuid7(),
            source_type=PlatformType.APPSTORE,
            platform_app_id='222222222',
            app_name='Failed App 2',
            storage_path='/tmp/fake_batch_1.parquet',
            file_format='parquet',
            record_count=3,
            status=IngestionBatchStatusType.FAILED,
            retry_count=1,
            max_retries=3,
            error_message='Parquet parse error',
            created_at=now,
            updated_at=now
        ),
        IngestionBatch(
            batch_id=uuid7(),
            source_type=PlatformType.APPSTORE,
            platform_app_id='333333333',
            app_name='Dead Letter App',
            storage_path='/tmp/fake_batch_3.parquet',
            file_format='parquet',
            record_count=2,
            status=IngestionBatchStatusType.DEAD_LETTER,
            retry_count=3,
            max_retries=3,
            error_message='Max retries reached',
            created_at=now,
            updated_at=now
        ),
    ]
    test_db_session.add_all(batches)
    test_db_session.commit()
    return test_db_session


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


# ========================================
# APP VERSION BACKFILL (Issue #125)
# ========================================

def _rss_review_entry(review_id: str, version: Optional[str] = None) -> Dict[str, Any]:
    """One review entry of the App Store customerreviews RSS JSON (keys copied from a live page)."""
    entry = {
        'author': {
            'uri': {'label': 'https://itunes.apple.com/kr/reviews/id1335187572'},
            'name': {'label': '리뷰어'},
            'label': '',
        },
        'updated': {'label': '2026-11-18T21:37:26-07:00'},
        'im:rating': {'label': '1'},
        'id': {'label': review_id},
        'title': {'label': '로그인 오류'},
        'content': {'label': '로그인이 자꾸 풀려요', 'attributes': {'type': 'text'}},
        'link': {'attributes': {
            'rel': 'related',
            'href': 'https://itunes.apple.com/kr/review?id=1067748687&type=Purple%20Software',
        }},
        'im:voteSum': {'label': '0'},
        'im:contentType': {'attributes': {'term': 'Application', 'label': '앱'}},
        'im:voteCount': {'label': '0'},
    }
    if version is not None:
        entry['im:version'] = {'label': version}
    return entry


@pytest.fixture
def rss_review_entry():
    """Builder for App Store RSS review entries: ``rss_review_entry(review_id, version=None)``."""
    return _rss_review_entry


class AppStoreRssServer:
    """A real HTTP server on 127.0.0.1 that answers App Store customerreviews RSS URLs.

    A page that was not set answers 200 with a feed that has no ``entry``, which is what the
    RSS returns past its last review and also, intermittently, inside the page range.
    """

    PATH = re.compile(
        r'^/(?P<country>[^/]+)/rss/customerreviews/page=(?P<page>\d+)/id=(?P<app_id>[^/]+)'
        r'/sortby=mostRecent/json$'
    )

    def __init__(self):
        self.pages: Dict[tuple, List[Dict[str, Any]]] = {}
        self.statuses: Dict[tuple, int] = {}
        self.requests: List[tuple] = []
        self.on_request = None  # called with (app_id, page) inside the handler, before answering
        rss = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                match = rss.PATH.match(self.path)
                if match is None:
                    self.send_error(404)
                    return
                key = (match['app_id'], int(match['page']))
                rss.requests.append(key)
                if rss.on_request is not None:
                    rss.on_request(key)
                status = rss.statuses.get(key, 200)
                if status != 200:
                    self.send_error(status)
                    return
                feed = {'author': {'name': {'label': 'iTunes Store'}}, 'title': {'label': 'iTunes Store:사용자 후기'}}
                if rss.pages.get(key):
                    feed['entry'] = rss.pages[key]
                body = json.dumps({'feed': feed}).encode('utf-8')
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self._httpd = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, kwargs={'poll_interval': 0.05}, daemon=True
        )
        self._thread.start()
        port = self._httpd.server_address[1]
        self.url_template = (
            f'http://127.0.0.1:{port}/{{country}}/rss/customerreviews/page={{page}}'
            f'/id={{app_id}}/sortby=mostRecent/json'
        )

    def set_pages(self, app_id: str, *pages: List[Dict[str, Any]]) -> None:
        """Serve ``pages[0]`` as page 1 of ``app_id``, ``pages[1]`` as page 2, and so on."""
        for number, entries in enumerate(pages, start=1):
            self.pages[(app_id, number)] = entries

    def fail(self, app_id: str, page: int, status: int = 500) -> None:
        self.statuses[(app_id, page)] = status

    def requested_pages(self, app_id: str) -> List[int]:
        return [page for requested_app, page in self.requests if requested_app == app_id]

    def close(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join()


@pytest.fixture
def appstore_rss_server(monkeypatch):
    """AppStoreRssServer for the test; requests to it bypass any configured HTTP proxy."""
    no_proxy = os.environ.get('NO_PROXY', '')
    monkeypatch.setenv('NO_PROXY', ','.join(filter(None, [no_proxy, '127.0.0.1'])))
    server = AppStoreRssServer()
    yield server
    server.close()


# ========================================
# PLAY REVIEWS (Issue #133)
# ========================================

PLAY_REVIEW_AT = 1795507631  # epoch seconds of a synthetic review (2026-11-24 UTC)
PLAY_AVATAR_URL = 'https://play-lh.googleusercontent.com/a/example'


def _play_review_item(
    review_id: str,
    version: Optional[str] = None,
    *,
    at: int = PLAY_REVIEW_AT,
    score: int = 4,
    content: str = '업데이트 후 로그인이 빨라졌어요',
    user_name: str = '사용자',
    thumbs_up: int = 0,
    reply: Optional[str] = None,
    replied_at: Optional[int] = None,
) -> List[Any]:
    """One review array of a Play batchexecute reviews payload, laid out as on live pages
    (2026-11-28, ko/kr): 17 elements, [5] = [epoch seconds, nanos], [7] = [replier, reply,
    [epoch seconds, nanos]] or null, [9] author info, [10] the version or null. Values are synthetic.
    """
    avatar = [None, 2, None, [None, None, PLAY_AVATAR_URL]]
    return [
        review_id,
        [user_name, avatar],
        score,
        None,
        content,
        [at, 0],
        thumbs_up,
        None if reply is None else ['테스트은행', reply, [replied_at, 0]],
        None,
        [f'author-{review_id}', user_name, None, [avatar, False], avatar],
        version,
        None, None, None, None, None,
        1,
    ]


@pytest.fixture
def play_review_item():
    """Builder for Play review arrays: ``play_review_item(review_id, version=None, **fields)``."""
    return _play_review_item


@dataclass
class PlayAnswer:
    """A one-off answer queued with PlayReviewsServer.push: an HTTP ``status``, a raw ``body``
    (``None``: the page the request asks for), after waiting ``delay`` seconds."""

    status: int = 200
    body: Optional[str] = None
    delay: float = 0


class PlayReviewsServer:
    """A real HTTP server on 127.0.0.1 that answers the Play batchexecute reviews POST.

    Each app serves a list of review arrays, newest first. A request reads ``count`` of them from
    the start, or from where its continuation token points, and the answer carries a next-page
    token only while reviews remain: that is how the live store answered (2026-11-28: full pages
    with a token, a last partial page without one). An app with no reviews answers an empty page
    without a token, as the live store does for an app id it does not know; an unknown token
    answers the entry without a payload the live store sends for a bad token.
    """

    PATH = '/_/PlayStoreUi/data/batchexecute'
    PREFIX = ")]}'\n\n"

    def __init__(self):
        self.reviews: Dict[str, List[List[Any]]] = {}
        self.answers: Dict[str, List[PlayAnswer]] = {}
        self.requests: List[Dict[str, Any]] = []
        self._tokens: Dict[str, tuple] = {}
        self._release = threading.Event()
        play = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                url = urlsplit(self.path)
                if url.path != play.PATH:
                    self.send_error(404)
                    return
                length = int(self.headers.get('Content-Length', 0))
                form = parse_qs(self.rfile.read(length).decode('utf-8'))
                [[[rpc_id, query_json, _, _]]] = json.loads(form['f.req'][0])
                query = json.loads(query_json)
                paging = query[1][2]
                request = {
                    'app_id': query[2][0], 'count': paging[0], 'token': paging[2] if len(paging) > 2 else None,
                    'sort': query[1][1], 'rpc_id': rpc_id, 'query': parse_qs(url.query),
                }
                play.requests.append(request)
                queued = play.answers.get(request['app_id'])
                answer = queued.pop(0) if queued else PlayAnswer()
                if answer.delay:
                    play._release.wait(answer.delay)
                if answer.status != 200:
                    self.send_error(answer.status)
                    return
                body = answer.body if answer.body is not None else play.page_body(request)
                data = body.encode('utf-8')
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args):
                pass

        self._httpd = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, kwargs={'poll_interval': 0.05}, daemon=True
        )
        self._thread.start()
        port = self._httpd.server_address[1]
        self.url_template = f'http://127.0.0.1:{port}{self.PATH}?hl={{language}}&gl={{country}}'

    @classmethod
    def envelope(cls, rpc_id: str, payload: Optional[str], error: Optional[List[Any]] = None) -> str:
        """A response body: the ``)]}'`` prefix, then the wrb.fr entry and the two trailing entries."""
        entries = [
            ['wrb.fr', rpc_id, payload, None, None, error, 'generic'],
            ['di', 48],
            ['af.httprm', 47, '-7539252846892424692', 28],
        ]
        return cls.PREFIX + json.dumps(entries, separators=(',', ':'))

    @classmethod
    def page_payload(cls, items: List[List[Any]], token: Optional[str]) -> str:
        payload = [items or None, [None, token] if token else None, [[None, [[1800], None, [72000]]]]]
        return json.dumps(payload, separators=(',', ':'))

    @classmethod
    def gateway_error_body(cls, rpc_id: str) -> str:
        """An entry without a payload whose error names PlayGatewayError. Not captured live: the
        detail around the name is made up; the name is what google-play-scraper 1.2.7 matched."""
        detail = [['type.googleapis.com/com.google.play.gateway.proto.PlayGatewayError', []]]
        return cls.envelope(rpc_id, None, [8, None, detail])

    def page_body(self, request: Dict[str, Any]) -> str:
        app_id, count, token = request['app_id'], request['count'], request['token']
        if token is None:
            offset = 0
        elif self._tokens.get(token, (None,))[0] == app_id:
            offset = self._tokens[token][1]
        else:
            return self.envelope(request['rpc_id'], None, [3])
        items = self.reviews.get(app_id, [])
        end = offset + count
        next_token = None
        if end < len(items):
            next_token = f'tok-{app_id}-{end}'
            self._tokens[next_token] = (app_id, end)
        return self.envelope(request['rpc_id'], self.page_payload(items[offset:end], next_token))

    def set_reviews(self, app_id: str, items: List[List[Any]]) -> None:
        self.reviews[app_id] = items

    def push(self, app_id: str, *answers: PlayAnswer) -> None:
        """Answer the next requests for ``app_id`` with ``answers``, in order, then serve pages again."""
        self.answers.setdefault(app_id, []).extend(answers)

    def requests_for(self, app_id: str) -> List[tuple]:
        """(count, token) of each request for ``app_id``, in order."""
        return [(r['count'], r['token']) for r in self.requests if r['app_id'] == app_id]

    def close(self) -> None:
        self._release.set()
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join()


@pytest.fixture
def play_reviews_server(monkeypatch):
    """PlayReviewsServer for the test; requests to it bypass any configured HTTP proxy."""
    no_proxy = os.environ.get('NO_PROXY', '')
    monkeypatch.setenv('NO_PROXY', ','.join(filter(None, [no_proxy, '127.0.0.1'])))
    server = PlayReviewsServer()
    yield server
    server.close()


def _point_play_at(config: Dict[str, Any], server: PlayReviewsServer) -> None:
    """Send the config's Play requests to ``server`` and retry without waiting."""
    config['playstore']['reviews_url'] = server.url_template
    config['playstore']['retry_backoff_seconds'] = 0


@pytest.fixture
def backfill_config(tmp_path, appstore_rss_server) -> str:
    """config/crawler_config.yml with the RSS URL pointed at appstore_rss_server and no delay."""
    config = yaml.safe_load(
        (Path(__file__).resolve().parents[1] / 'config' / 'crawler_config.yml').read_text(encoding='utf-8')
    )
    config['global']['delay_between_requests'] = 0
    config['appstore']['reviews_rss_url'] = appstore_rss_server.url_template
    path = tmp_path / 'crawler_config.yml'
    path.write_text(yaml.safe_dump(config, allow_unicode=True), encoding='utf-8')
    return str(path)


class BackfillDb:
    """Committed apps / app_reviews rows for tests that run the backfill on its own connection.

    The backfill opens its own DatabaseConnector session, so rows seeded through the rollback-only
    ``test_db_session`` would be invisible to it; these are committed and deleted afterwards.
    """

    APPLICATION_NAME = 'reai-backfill-under-test'  # application_name of the backfill's connection
    # The backfill's connection gives up on a row lock after this long instead of waiting forever,
    # so a test can make one app's UPDATE fail by holding that app's rows from another transaction.
    LOCK_TIMEOUT = '500ms'
    OLD_UPDATED_AT = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def __init__(self, engine):
        self.engine = engine
        self.app_ids: List[Any] = []

    def seed_app(self, platform_type: PlatformType, platform_app_id: str, reviews) -> Any:
        """Commit one app and its reviews, given as (platform_review_id, app_version) pairs."""
        app_id = uuid7()
        self.app_ids.append(app_id)
        with self.engine.begin() as conn:
            conn.execute(
                text("INSERT INTO apps (app_id, platform_app_id, platform_type, name) "
                     "VALUES (:app_id, :platform_app_id, :platform_type, 'Backfill App')"),
                {'app_id': app_id, 'platform_app_id': platform_app_id, 'platform_type': platform_type.value},
            )
            for platform_review_id, app_version in reviews:
                conn.execute(
                    text("""
                        INSERT INTO app_reviews (
                            review_id, app_id, platform_type, platform_review_id, review_text,
                            rating, app_version, reviewed_at, updated_at
                        ) VALUES (
                            :review_id, :app_id, :platform_type, :platform_review_id, '리뷰 본문',
                            4, :app_version, :reviewed_at, :updated_at
                        )
                    """),
                    {
                        'review_id': uuid7(), 'app_id': app_id, 'platform_type': platform_type.value,
                        'platform_review_id': platform_review_id, 'app_version': app_version,
                        'reviewed_at': datetime(2026, 8, 10, 9, 0, tzinfo=timezone.utc),
                        'updated_at': self.OLD_UPDATED_AT,
                    },
                )
        return app_id

    def versions(self, app_id) -> Dict[str, Optional[str]]:
        """{platform_review_id: app_version} as a separate connection sees it."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT platform_review_id, app_version FROM app_reviews WHERE app_id = :app_id"),
                {'app_id': app_id},
            ).all()
        return dict(rows)

    def connection_states(self) -> List[str]:
        """pg_stat_activity states of the backfill's connections, read from a separate connection."""
        with self.engine.connect() as conn:
            return list(conn.execute(
                text("SELECT state FROM pg_stat_activity WHERE application_name = :name"),
                {'name': self.APPLICATION_NAME},
            ).scalars())

    def cleanup(self) -> None:
        with self.engine.begin() as conn:
            for app_id in self.app_ids:
                conn.execute(text("DELETE FROM app_reviews WHERE app_id = :app_id"), {'app_id': app_id})
                conn.execute(text("DELETE FROM apps WHERE app_id = :app_id"), {'app_id': app_id})


@pytest.fixture
def backfill_db(test_db_engine, test_db_schema, test_db_url, monkeypatch):
    """BackfillDb on the test database; the backfill's DatabaseConnector is pointed at it,
    with BackfillDb.APPLICATION_NAME and BackfillDb.LOCK_TIMEOUT set on its connections.

    Fails up front if the database already holds rows without app_version: the backfill would
    fetch those apps too, and for Play that means the live store.
    """
    with test_db_engine.connect() as conn:
        stray = conn.execute(text("SELECT count(*) FROM app_reviews WHERE app_version IS NULL")).scalar_one()
    assert stray == 0, f'{stray} app_reviews rows without app_version left in the test database'
    url = make_url(test_db_url).update_query_dict({
        'application_name': BackfillDb.APPLICATION_NAME,
        'options': f'-c lock_timeout={BackfillDb.LOCK_TIMEOUT}',
    })
    monkeypatch.setenv('DATABASE_URL', url.render_as_string(hide_password=False))
    db = BackfillDb(test_db_engine)
    yield db
    db.cleanup()


# ========================================
# CRAWL STEP (Issue #127)
# ========================================

class CrawlConfig:
    """config/crawler_config.yml for a crawl test, written under ``tmp_path``.

    The App Store RSS URL points at ``appstore_rss_server`` and the Play reviews URL at
    ``play_reviews_server`` (so no test reaches a live store), there is no delay or retry wait, the
    crawler's output directories live under ``tmp_path``, and each store reads its app ids from a
    file of its own that starts empty, so that store's crawler raises before any request until
    set_app_ids fills it.
    """

    def __init__(self, tmp_path: Path, rss_url_template: str, play_server: PlayReviewsServer):
        config = yaml.safe_load(
            (Path(__file__).resolve().parents[1] / 'config' / 'crawler_config.yml').read_text(encoding='utf-8')
        )
        config['global']['delay_between_requests'] = 0
        config['appstore']['reviews_rss_url'] = rss_url_template
        _point_play_at(config, play_server)
        config['output']['base_directory'] = str(tmp_path / 'crawler_output')
        self.app_id_files = {store: tmp_path / f'{store}_app_ids.txt' for store in ('appstore', 'playstore')}
        for store, path in self.app_id_files.items():
            path.write_text('', encoding='utf-8')
            config['app_ids'][store] = str(path)
        self.config = config
        self.path = str(tmp_path / 'crawler_config.yml')
        Path(self.path).write_text(yaml.safe_dump(config, allow_unicode=True), encoding='utf-8')

    def set_app_ids(self, store: str, *app_ids: str) -> None:
        self.app_id_files[store].write_text(''.join(f'{app_id}\n' for app_id in app_ids), encoding='utf-8')


@pytest.fixture
def crawl_config(
    tmp_path, appstore_rss_server, play_reviews_server, test_db_schema, test_db_url, monkeypatch
) -> CrawlConfig:
    """CrawlConfig on the test database with Parquet writes off (no object store, no DB rows).

    The crawlers' DatabaseConnector is pointed at the test database through ``DATABASE_URL``;
    with ``ENABLE_PARQUET_WRITE=false`` a crawl only reads the store and touches no table.
    """
    monkeypatch.setenv('DATABASE_URL', test_db_url)
    monkeypatch.setenv('ENABLE_PARQUET_WRITE', 'false')
    return CrawlConfig(tmp_path, appstore_rss_server.url_template, play_reviews_server)


@pytest.fixture
def minio_bucket(monkeypatch):
    """A new bucket on the test MinIO, exported as ``MINIO_*`` for MinIOClient; removed afterwards.

    The server is docker-compose.test.yml's ``test-minio`` (``TEST_MINIO_API_PORT``, default 9002)
    unless ``TEST_MINIO_ENDPOINT`` names another; the credentials default to that compose file's.
    The test is skipped when no MinIO answers there; a MinIO that answers with an error (wrong
    credentials, say) fails it.
    """
    import boto3
    from botocore.config import Config
    from botocore.exceptions import BotoCoreError, ClientError

    endpoint = os.getenv('TEST_MINIO_ENDPOINT', f"http://127.0.0.1:{os.getenv('TEST_MINIO_API_PORT', '9002')}")
    access_key = os.getenv('TEST_MINIO_ACCESS_KEY', 'minio')
    secret_key = os.getenv('TEST_MINIO_SECRET_KEY', 'minio123')
    bucket = f'reai-test-{uuid7().hex}'
    no_proxy = os.environ.get('NO_PROXY', '')
    monkeypatch.setenv('NO_PROXY', ','.join(filter(None, [no_proxy, '127.0.0.1', 'localhost'])))
    s3 = boto3.client(
        's3', endpoint_url=endpoint, aws_access_key_id=access_key, aws_secret_access_key=secret_key,
        config=Config(connect_timeout=2, retries={'max_attempts': 1}),
    )
    try:
        s3.create_bucket(Bucket=bucket)
    except BotoCoreError as exc:  # no answer: connection refused, timed out, unknown host
        pytest.skip(f'test MinIO not reachable at {endpoint}: {exc!r}')
    except ClientError as exc:  # it answered and refused, e.g. InvalidAccessKeyId
        pytest.fail(f'test MinIO at {endpoint} refused to create bucket {bucket}: {exc!r}')
    for name, value in {
        'MINIO_ENDPOINT': endpoint, 'MINIO_ACCESS_KEY': access_key,
        'MINIO_SECRET_KEY': secret_key, 'MINIO_BUCKET': bucket,
    }.items():
        monkeypatch.setenv(name, value)
    yield s3, bucket
    for obj in s3.list_objects_v2(Bucket=bucket).get('Contents', []):
        s3.delete_object(Bucket=bucket, Key=obj['Key'])
    s3.delete_bucket(Bucket=bucket)
