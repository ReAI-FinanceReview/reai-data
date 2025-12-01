"""
파일 관리 유틸리티
"""
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import pandas as pd


class FileManager:
    """파일 관리 클래스"""
    
    def __init__(self, base_path: str = "data", enabled: bool = True):
        self.base_path = Path(base_path)
        self.enabled = enabled
        if self.enabled:
            self.ensure_directories()
    
    def ensure_directories(self):
        """필요한 디렉토리 생성"""
        directories = [
            self.base_path / "raw" / "appstore",
            self.base_path / "raw" / "playstore",
            self.base_path / "raw" / "unified",
            self.base_path / "processed" / "normalized",
            self.base_path / "processed" / "cleaned",
            self.base_path / "processed" / "analyzed",
            self.base_path / "backup",
        ]

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def get_output_path(self, platform: str, date_str: str = None) -> Path:
        """출력 경로 생성"""
        if not self.enabled:
            return Path("")

        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
        
        output_dir = self.base_path / "raw" / platform / date_str
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir
    
    def save_reviews(self, data: pd.DataFrame, platform: str, 
                    filename: str = None, date_str: str = None) -> str:
        """리뷰 데이터 저장"""
        if not self.enabled:
            return ""

        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')

        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{platform}_reviews_{timestamp}.csv"

        output_path = self.get_output_path(platform, date_str)
        file_path = output_path / filename

        data.to_csv(file_path, index=False, encoding='utf-8-sig')
        return str(file_path)
    
    def backup_file(self, file_path: str) -> str:
        """파일 백업"""
        if not self.enabled:
            return ""

        source = Path(file_path)
        if not source.exists():
            raise FileNotFoundError(f"파일을 찾을 수 없습니다: {file_path}")
        
        # 백업 디렉토리 생성
        backup_dir = self.base_path / "backup" / datetime.now().strftime('%Y-%m')
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        # 백업 파일명 생성
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_filename = f"{source.stem}_{timestamp}{source.suffix}"
        backup_path = backup_dir / backup_filename
        
        # 파일 복사
        shutil.copy2(source, backup_path)
        return str(backup_path)
    
    def list_files(self, platform: str, date_pattern: str = None) -> List[str]:
        """파일 목록 조회"""
        if not self.enabled:
            return []

        platform_dir = self.base_path / "raw" / platform
        
        if not platform_dir.exists():
            return []
        
        files = []
        for file_path in platform_dir.rglob("*.csv"):
            if date_pattern is None or date_pattern in str(file_path):
                files.append(str(file_path))
        
        return sorted(files)
    
    def cleanup_old_files(self, platform: str, days_to_keep: int = 30):
        """오래된 파일 정리"""
        cutoff_date = datetime.now().timestamp() - (days_to_keep * 24 * 60 * 60)
        if not self.enabled:
            return []

        platform_dir = self.base_path / "raw" / platform
        
        if not platform_dir.exists():
            return
        
        deleted_files = []
        for file_path in platform_dir.rglob("*.csv"):
            if file_path.stat().st_mtime < cutoff_date:
                # 백업 후 삭제
                backup_path = self.backup_file(str(file_path))
                file_path.unlink()
                deleted_files.append(str(file_path))
        
        return deleted_files
