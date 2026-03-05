"""공통 크롤러 예외 클래스"""


class ParquetWriteError(Exception):
    """Parquet 쓰기 실패 (crawl stage)."""
    pass
