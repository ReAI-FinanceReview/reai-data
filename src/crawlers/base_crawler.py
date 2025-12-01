"""
기본 크롤러 클래스
"""
import time
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from pathlib import Path

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

from ..utils.logger import get_logger
from ..utils.file_manager import FileManager
from ..utils.data_processor import DataProcessor


class BaseCrawler(ABC):
    """기본 크롤러 추상 클래스"""
    
    def __init__(self, config_path: str = None):
        self.logger = get_logger(self.__class__.__name__.lower())

        # 설정 로드
        self.config = self._load_config(config_path)

        output_cfg = self.config.get("output", {}) if isinstance(self.config, dict) else {}
        output_enabled = output_cfg.get("enabled", True)
        output_base = output_cfg.get("base_directory", "data")

        self.file_manager = FileManager(base_path=output_base, enabled=output_enabled)
        self.data_processor = DataProcessor()
        
        # 공통 설정
        self.delay = self.config.get('global', {}).get('delay_between_requests', 2)
        self.max_retries = self.config.get('global', {}).get('max_retries', 3)
        self.timeout = self.config.get('global', {}).get('timeout', 30)
    
    def _load_config(self, config_path: str = None) -> Dict[str, Any]:
        """설정 파일 로드"""
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config" / "crawler_config.yml"
        
        if not YAML_AVAILABLE:
            self.logger.warning("PyYAML이 설치되지 않았습니다. 기본 설정을 사용합니다.")
            return self._get_default_config()
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger.error(f"설정 파일을 로드할 수 없습니다: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """기본 설정 반환"""
        return {
            'global': {
                'delay_between_requests': 2,
                'max_retries': 3,
                'timeout': 30
            },
            'output': {
                'base_directory': 'data/raw',
                'file_format': 'csv',
                'encoding': 'utf-8-sig'
            }
        }
    
    def read_app_ids(self, filename: str) -> List[str]:
        """앱 ID 파일 읽기"""
        app_ids = []
        try:
            with open(filename, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    # 주석이 붙은 경우 분리
                    app_id = line.split('#')[0].strip()
                    if app_id:
                        app_ids.append(app_id)
        except FileNotFoundError:
            self.logger.error(f"앱 ID 파일을 찾을 수 없습니다: {filename}")
        except Exception as e:
            self.logger.error(f"앱 ID 파일 읽기 오류: {e}")
        
        return app_ids
    
    def wait_between_requests(self):
        """요청 간 대기"""
        time.sleep(self.delay)
    
    @abstractmethod
    def crawl_reviews(self, app_id: str) -> List[Dict[str, Any]]:
        """리뷰 크롤링 (하위 클래스에서 구현)"""
        pass
    
    @abstractmethod
    def run(self) -> str:
        """크롤러 실행 (하위 클래스에서 구현)"""
        pass
