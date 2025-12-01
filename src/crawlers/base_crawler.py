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
        """
        Initialize the BaseCrawler instance, loading configuration and creating its logger, file manager, and data processor.
        
        If a YAML configuration path is provided, it will be loaded; otherwise the default configuration path or defaults are used. The constructor also reads output and global settings from the loaded configuration to initialize the FileManager and set request pacing and retry parameters.
        
        Parameters:
            config_path (str, optional): Path to a YAML configuration file. If omitted, a default config path is used or a built-in default configuration is applied when the file cannot be read.
        
        Attributes:
            logger: Logger named after the crawler class.
            config (dict): Loaded configuration dictionary (or default config on failure).
            file_manager (FileManager): Manages output storage using the configured base directory and enabled flag.
            data_processor (DataProcessor): Processor instance for handling crawled data.
            delay (int): Seconds to wait between requests (from config 'global.delay_between_requests', defaults to 2).
            max_retries (int): Maximum retry attempts for requests (from config 'global.max_retries', defaults to 3).
            timeout (int): Request timeout in seconds (from config 'global.timeout', defaults to 30).
        """
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
        """
        Load the crawler configuration from a YAML file or fall back to the built-in defaults.
        
        Parameters:
            config_path (str): Filesystem path to a YAML config file. If None, uses the package's default config path.
        
        Returns:
            Dict[str, Any]: Configuration dictionary (typically containing `global` and `output` sections). If PyYAML is not available or the file cannot be loaded, returns the default configuration.
        """
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
        """
        Return the default configuration used by the crawler.
        
        Returns:
            dict: A configuration dictionary with two keys:
                - 'global': contains timing and retry defaults:
                    - 'delay_between_requests' (int): seconds to wait between requests.
                    - 'max_retries' (int): number of retry attempts.
                    - 'timeout' (int): request timeout in seconds.
                - 'output': contains output file settings:
                    - 'base_directory' (str): base path for saved data.
                    - 'file_format' (str): default output file format.
                    - 'encoding' (str): file encoding to use.
        """
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
        """
        Parse app IDs from a text file, ignoring blank lines and comments.
        
        Reads the file using UTF-8 encoding. Lines that are empty or start with '#' are skipped.
        Inline comments (text after a '#') are removed before trimming; non-empty tokens remaining
        after stripping are collected in order.
        
        Parameters:
            filename (str): Path to the file containing one app ID per line (may include comments).
        
        Returns:
            List[str]: Collected app IDs in file order. Returns an empty list if the file is not found
            or if an error occurs while reading (errors are logged).
        """
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
        """
        Pause execution for the configured delay between requests.
        
        Blocks the current thread for self.delay seconds, using the instance's configured delay value.
        """
        time.sleep(self.delay)
    
    @abstractmethod
    def crawl_reviews(self, app_id: str) -> List[Dict[str, Any]]:
        """
        Crawl and return reviews for the given app identifier.
        
        Subclasses must implement this method to fetch reviews for the specified app_id.
        
        Returns:
            List[Dict[str, Any]]: A list of review records where each record is a dictionary containing review fields (for example: author, rating, title, content, timestamp).
        """
        pass
    
    @abstractmethod
    def run(self) -> str:
        """
        Execute the crawler's run cycle; intended to be implemented by subclasses.
        
        Returns:
            result (str): A status message or the path to the produced output.
        """
        pass