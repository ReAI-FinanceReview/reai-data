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
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.setup_logging()
            self._initialized = True
    
    def setup_logging(self, config_path: str = None):
        """로깅 설정"""
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
        """로거 인스턴스 반환"""
        return logging.getLogger(name)


# 전역 로거 인스턴스
logger_manager = Logger()

def get_logger(name: str) -> logging.Logger:
    """로거 가져오기"""
    return logger_manager.get_logger(name)
