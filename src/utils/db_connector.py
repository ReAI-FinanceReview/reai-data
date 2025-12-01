"""
데이터베이스 연결 유틸리티
"""
import os
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.base import Engine

# 이 모듈을 사용하려면 psycopg2-binary가 설치되어 있어야 합니다.
# pip install psycopg2-binary


def _env_db_url() -> str:
    """환경변수에서 DB 접속 정보를 읽어 SQLAlchemy URL을 구성합니다.
    우선순위:
      1) DATABASE_URL (표준 URI)
      2) DB_HOST/DB_PORT/DB_USER/DB_PASSWORD(or DB_PASS)/DB_NAME, DB_TYPE (기본 postgresql)
    """
    # 1) 전체 URI 우선
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return db_url

    # 2) 개별 항목 조합
    db_type = os.getenv("DB_TYPE")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    dbname = os.getenv("DB_NAME")

    # 필수 항목이 모두 있는 경우만 조합
    if all([host, port, user, password, dbname]):
        return f"{db_type}+psycopg2://{user}:{password}@{host}:{port}/{dbname}"

    return ""


class DatabaseConnector:
    """데이터베이스 연결 및 세션 관리를 담당하는 클래스"""

    def __init__(self, config_path: str = 'config/crawler_config.yml'):
        # 환경변수 우선(DB URL 또는 개별 항목)
        env_db_url = _env_db_url()
        self.config_path = config_path
        self.config = self._load_config(config_path) if not env_db_url else {}
        self.db_url = env_db_url or self._build_url_from_config()

        if not self.db_url:
            raise ValueError("데이터베이스 접속 정보가 없습니다. 환경변수 또는 config/crawler_config.yml을 확인하세요.")

        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)

    def _load_config(self, config_path: str) -> dict:
        """YAML 설정 파일을 로드합니다."""
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def _build_url_from_config(self) -> str:
        """config 파일에서 DB URL 생성"""
        db_config = self.config.get('database') if self.config else None
        if not db_config:
            return ""

        db_type = db_config.get('type', 'postgresql')
        username = db_config.get('username')
        password = db_config.get('password')
        host = db_config.get('host')
        port = db_config.get('port')
        dbname = db_config.get('dbname')

        if not all([username, password, host, port, dbname]):
            return ""

        return f"{db_type}+psycopg2://{username}:{password}@{host}:{port}/{dbname}"

    def get_session(self):
        """데이터베이스 세션을 반환합니다."""
        return self.Session()

    def create_tables(self, base):
        """테이블을 생성합니다."""
        base.metadata.create_all(self.engine)
