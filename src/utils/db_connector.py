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
    """
    Resolve the database connection URL from environment variables.
    
    Prefers the standard DATABASE_URL environment variable; if not present, composes a SQLAlchemy-style URL from DB_TYPE, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, and DB_NAME. Returns an empty string when the required components are not all available.
    
    Returns:
        str: Database connection URL in the form "db_type+psycopg2://user:password@host:port/dbname" if resolvable, otherwise an empty string.
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
        """
        Initialize the DatabaseConnector by resolving a database URL and creating the SQLAlchemy engine and session factory.
        
        Resolution prioritizes an environment-provided database URL; if absent, the URL is built from the provided YAML configuration file. On success, sets instance attributes: `config_path`, `config`, `db_url`, `engine`, and `Session`.
        
        Parameters:
            config_path (str): Path to the YAML configuration file used to build the DB URL when no environment URL is present.
        
        Raises:
            ValueError: If no database connection URL can be determined from the environment or configuration.
        """
        env_db_url = _env_db_url()
        self.config_path = config_path
        self.config = self._load_config(config_path) if not env_db_url else {}
        self.db_url = env_db_url or self._build_url_from_config()

        if not self.db_url:
            raise ValueError("데이터베이스 접속 정보가 없습니다. 환경변수 또는 config/crawler_config.yml을 확인하세요.")

        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)

    def _load_config(self, config_path: str) -> dict:
        """
        Load a YAML configuration file and return its contents as a dictionary.
        
        Parameters:
            config_path (str): Path to the YAML configuration file.
        
        Returns:
            dict: Parsed YAML content as a Python dictionary (empty file may produce None).
        """
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def _build_url_from_config(self) -> str:
        """
        Build a database connection URL from the loaded configuration's `database` section.
        
        Reads `database` settings from the instance config and, when all required fields are present, returns a connection URL in the form `db_type+psycopg2://username:password@host:port/dbname`. If `type` is missing it defaults to `postgresql`. If the `database` section is absent or any required field (username, password, host, port, dbname) is missing, an empty string is returned.
        
        Returns:
            str: The constructed database connection URL, or an empty string if it cannot be built.
        """
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
        """
        Return a new SQLAlchemy session bound to this connector's engine.
        
        Returns:
            sqlalchemy.orm.Session: A new session instance from the connector's session factory.
        """
        return self.Session()

    def create_tables(self, base):
        """
        Create database tables defined on the given SQLAlchemy declarative base.
        
        Parameters:
            base: The SQLAlchemy declarative base (its `metadata`) whose tables will be created against the connector's engine.
        """
        base.metadata.create_all(self.engine)