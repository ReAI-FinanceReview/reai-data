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
        """
        Initialize the FileManager with a base directory and an enabled flag.
        
        Parameters:
        	base_path (str): Root directory where data, processed, and backup folders will be created and managed.
        	enabled (bool): If `True`, the manager performs file operations and will create required directories on initialization; if `False`, operations become no-ops.
        """
        self.base_path = Path(base_path)
        self.enabled = enabled
        if self.enabled:
            self.ensure_directories()
    
    def ensure_directories(self):
        """
        Create the required directory structure under the FileManager's base path.
        
        Ensures the following subdirectories exist (creating parents as needed):
        - raw/appstore
        - raw/playstore
        - raw/unified
        - processed/normalized
        - processed/cleaned
        - processed/analyzed
        - backup
        """
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
        """
        Return the filesystem path for storing raw data for a given platform and date.
        
        Parameters:
            platform (str): Platform subdirectory name under `raw` (e.g., "appstore", "playstore", "unified").
            date_str (str, optional): Date folder name in `YYYY-MM-DD` format; when omitted the current date is used.
        
        Returns:
            Path: Path to the created output directory (base_path / "raw" / platform / date_str).
        """
        if not self.enabled:
            return Path("")

        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
        
        output_dir = self.base_path / "raw" / platform / date_str
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir
    
    def save_reviews(self, data: pd.DataFrame, platform: str, 
                    filename: str = None, date_str: str = None) -> str:
        """
                    Save review data as a CSV file under the manager's raw output directory for the given platform and date.
                    
                    Parameters:
                        data (pd.DataFrame): Review records to be written to CSV.
                        platform (str): Platform subdirectory name (e.g., "appstore" or "playstore") where the file will be placed.
                        filename (str, optional): Desired filename; when omitted a timestamped filename of the form "{platform}_reviews_{timestamp}.csv" is generated.
                        date_str (str, optional): Date subdirectory in "YYYY-MM-DD" format; when omitted the current date is used.
                    
                    Returns:
                        str: Filesystem path to the saved CSV file as a string, or an empty string if the FileManager is disabled.
                    
                    Notes:
                        The CSV is written with UTF-8 BOM encoding ("utf-8-sig") and is placed under:
                        base_path/raw/{platform}/{date_str}
                    """
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
        """
        Create a time-stamped backup copy of the given file under the backup directory for the current month.
        
        If the FileManager is disabled (enabled is False), no backup is created and an empty string is returned.
        
        Parameters:
            file_path (str): Path to the source file to back up.
        
        Returns:
            str: Path to the created backup file as a string, or an empty string if no backup was performed.
        
        Raises:
            FileNotFoundError: If the source file does not exist.
        """
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
        """
        Collects CSV file paths for a given platform under the raw data directory.
        
        Parameters:
            platform (str): Platform subdirectory name under `base_path/raw` (e.g., "appstore", "playstore").
            date_pattern (str, optional): Substring to filter file paths; only files whose path contains this pattern are included.
        
        Returns:
            List[str]: Sorted list of matching CSV file paths as strings. Empty list if disabled or platform directory does not exist.
        """
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
        """
        Delete CSV files older than a retention window for the given platform after creating backups.
        
        Parameters:
            platform (str): Platform subdirectory under `base_path/raw` to scan for CSV files.
            days_to_keep (int): Number of days to retain files; files older than this are deleted.
        
        Returns:
            list[str]: Paths of files that were deleted.
            Returns an empty list if the FileManager is disabled.
            Returns None if the platform directory does not exist.
        """
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