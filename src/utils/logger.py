"""
로깅 유틸리티
"""
import logging
import logging.config
import yaml
import os
from pathlib import Path


class Logger:
    """로깅 관리 클래스"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        """
        Ensure only one instance of the class is created and returned (singleton behavior).
        
        Returns:
            The single instance of the class (an instance of `cls`), creating it on first call.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """
        Initialize the Logger instance and configure logging on first construction.
        
        Only the first initialization performs logging setup; subsequent initializations are no-ops to preserve the singleton's existing configuration.
        """
        if not self._initialized:
            self.setup_logging()
            self._initialized = True
    
    def setup_logging(self, config_path: str = None):
        """
        Configure the logging system from a YAML configuration file or fall back to a basic configuration.
        
        If `config_path` is not provided, a default file is used at three levels up from this module into `config/logging_config.yml`. Ensures common log directories exist before attempting to load the YAML file; if loading or applying the configuration fails, applies a basic logging configuration and logs an error.
        
        Parameters:
            config_path (str | Path, optional): Path to a YAML logging configuration file. If omitted, a default project-relative path is used.
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config" / "logging_config.yml"
        
        # 로그 디렉토리 생성
        log_dirs = ['logs/crawler', 'logs/error', 'logs/debug']
        for log_dir in log_dirs:
            os.makedirs(log_dir, exist_ok=True)
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                logging.config.dictConfig(config)
        except Exception as e:
            # 기본 로깅 설정
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            logging.error(f"로깅 설정 파일을 로드할 수 없습니다: {e}")
    
    def get_logger(self, name: str) -> logging.Logger:
        """
        Retrieve a logger configured by the logging system.
        
        Parameters:
            name (str): Name of the logger to retrieve.
        
        Returns:
            logger (logging.Logger): Logger instance associated with the given name.
        """
        return logging.getLogger(name)


# 전역 로거 인스턴스
logger_manager = Logger()

def get_logger(name: str) -> logging.Logger:
    """
    Obtain a logger configured by the global Logger manager.
    
    Parameters:
        name (str): Name of the logger (e.g., a hierarchical dot-separated name).
    
    Returns:
        logging.Logger: Logger instance corresponding to the given name.
    """
    return logger_manager.get_logger(name)