"""Parquet writer utility for NAS storage.

This module provides utilities for writing Pydantic schemas to Parquet files
with support for partitioning and batch operations.
"""

import os
from pathlib import Path
from typing import List, Optional, Union, Literal
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel

from src.utils.logger import get_logger

logger = get_logger(__name__)


class ParquetWriter:
    """Write Pydantic schemas to Parquet files with partitioning support.

    This class handles writing validated Pydantic data to Parquet files on NAS,
    with support for:
    - Time-based partitioning (year/month/day)
    - Batch writing for efficiency
    - Schema evolution tracking
    - Compression (snappy by default)

    Attributes:
        base_path: Root directory for Parquet files
        partition_by: Partitioning strategy ('none', 'year', 'year_month', 'year_month_day')
        compression: Compression codec (default: 'snappy')

    Example:
        >>> from src.schemas.parquet import AppReviewSchema
        >>> from datetime import datetime, timezone
        >>>
        >>> writer = ParquetWriter(
        ...     base_path="/mnt/nas/bronze/app_reviews",
        ...     partition_by="year_month"
        ... )
        >>>
        >>> reviews = [
        ...     AppReviewSchema(
        ...         app_id="...",
        ...         platform_type="APPSTORE",
        ...         platform_review_id="123",
        ...         review_text="Great!",
        ...         rating=5,
        ...         reviewed_at=datetime.now(timezone.utc)
        ...     )
        ... ]
        >>>
        >>> writer.write_batch(reviews)
    """

    def __init__(
        self,
        base_path: Union[str, Path],
        partition_by: Literal['none', 'year', 'year_month', 'year_month_day'] = 'none',
        compression: str = 'snappy'
    ):
        """Initialize ParquetWriter.

        Args:
            base_path: Root directory for Parquet files
            partition_by: Partitioning strategy
                - 'none': No partitioning, write to base_path directly
                - 'year': Partition by year (year=2026/)
                - 'year_month': Partition by year and month (year=2026/month=02/)
                - 'year_month_day': Partition by year, month, day
            compression: Compression codec ('snappy', 'gzip', 'brotli', 'zstd', 'none')
        """
        self.base_path = Path(base_path)
        self.partition_by = partition_by
        self.compression = compression

        # Create base directory if it doesn't exist
        self.base_path.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Initialized ParquetWriter: base_path={base_path}, "
            f"partition_by={partition_by}, compression={compression}"
        )

    def write_batch(
        self,
        records: List[BaseModel],
        partition_date: Optional[datetime] = None
    ) -> Path:
        """Write a batch of Pydantic records to Parquet.

        Args:
            records: List of Pydantic model instances
            partition_date: Date to use for partitioning (default: current UTC time)

        Returns:
            Path: Path to the written Parquet file

        Raises:
            ValueError: If records is empty or contains invalid data
            IOError: If write operation fails

        Example:
            >>> reviews = [AppReviewSchema(...), AppReviewSchema(...)]
            >>> path = writer.write_batch(reviews)
            >>> print(f"Written to: {path}")
        """
        if not records:
            raise ValueError("Cannot write empty batch")

        if partition_date is None:
            partition_date = datetime.now(timezone.utc)

        # Convert Pydantic models to dictionaries
        data_dicts = [record.model_dump() for record in records]

        # Create PyArrow table
        table = pa.Table.from_pylist(data_dicts)

        # Determine output path with partitioning
        output_path = self._get_partition_path(partition_date)
        output_path.mkdir(parents=True, exist_ok=True)

        # Generate filename with timestamp (including microseconds for uniqueness)
        now = datetime.now(timezone.utc)
        timestamp = now.strftime('%Y%m%d_%H%M%S_%f')
        microseconds = f"{now.microsecond:06d}"
        filename = f"data_{timestamp}_{microseconds}.parquet"
        file_path = output_path / filename

        # Write to Parquet
        pq.write_table(
            table,
            file_path,
            compression=self.compression,
            # Use Parquet format version 2.6 for better compatibility
            version='2.6',
            # Write statistics for better query performance
            write_statistics=True
        )

        logger.info(
            f"Wrote {len(records)} records to {file_path} "
            f"(compressed: {self.compression})"
        )

        return file_path

    def write_single(
        self,
        record: BaseModel,
        partition_date: Optional[datetime] = None
    ) -> Path:
        """Write a single Pydantic record to Parquet.

        Convenience method for writing a single record.
        Note: For efficiency, prefer batch writing when possible.

        Args:
            record: Pydantic model instance
            partition_date: Date to use for partitioning

        Returns:
            Path: Path to the written Parquet file
        """
        return self.write_batch([record], partition_date)

    def append_to_partition(
        self,
        records: List[BaseModel],
        partition_date: Optional[datetime] = None
    ) -> Path:
        """Append records to an existing partition.

        This creates a new Parquet file in the partition directory.
        Multiple Parquet files in the same partition will be read together
        when querying.

        Args:
            records: List of Pydantic model instances
            partition_date: Date to use for partitioning

        Returns:
            Path: Path to the written Parquet file
        """
        # Same as write_batch - Parquet naturally supports multiple files per partition
        return self.write_batch(records, partition_date)

    def _get_partition_path(self, partition_date: datetime) -> Path:
        """Get partition directory path based on partitioning strategy.

        Args:
            partition_date: Date to use for partitioning

        Returns:
            Path: Partition directory path

        Example:
            >>> # With partition_by='year_month' and date=2026-02-04
            >>> path = writer._get_partition_path(datetime(2026, 2, 4))
            >>> str(path)
            '/mnt/nas/bronze/app_reviews/year=2026/month=02'
        """
        if self.partition_by == 'none':
            return self.base_path

        year = partition_date.year
        month = f"{partition_date.month:02d}"
        day = f"{partition_date.day:02d}"

        if self.partition_by == 'year':
            return self.base_path / f"year={year}"
        elif self.partition_by == 'year_month':
            return self.base_path / f"year={year}" / f"month={month}"
        elif self.partition_by == 'year_month_day':
            return self.base_path / f"year={year}" / f"month={month}" / f"day={day}"
        else:
            raise ValueError(f"Invalid partition_by: {self.partition_by}")

    def list_partitions(self) -> List[Path]:
        """List all partition directories.

        Returns:
            List[Path]: List of partition directory paths

        Example:
            >>> partitions = writer.list_partitions()
            >>> for partition in partitions:
            ...     print(partition)
            /mnt/nas/bronze/app_reviews/year=2026/month=01
            /mnt/nas/bronze/app_reviews/year=2026/month=02
        """
        if self.partition_by == 'none':
            return [self.base_path]

        partitions = []

        if self.partition_by == 'year':
            pattern = "year=*"
        elif self.partition_by == 'year_month':
            pattern = "year=*/month=*"
        elif self.partition_by == 'year_month_day':
            pattern = "year=*/month=*/day=*"
        else:
            return []

        # Use glob to find partition directories
        for path in self.base_path.glob(pattern):
            if path.is_dir():
                partitions.append(path)

        return sorted(partitions)

    def get_partition_stats(self) -> dict:
        """Get statistics about partitions and files.

        Returns:
            dict: Dictionary with partition statistics

        Example:
            >>> stats = writer.get_partition_stats()
            >>> print(f"Total partitions: {stats['num_partitions']}")
            >>> print(f"Total files: {stats['num_files']}")
        """
        partitions = self.list_partitions()
        total_files = 0
        total_size = 0

        for partition in partitions:
            parquet_files = list(partition.glob("*.parquet"))
            total_files += len(parquet_files)
            total_size += sum(f.stat().st_size for f in parquet_files)

        return {
            'num_partitions': len(partitions),
            'num_files': total_files,
            'total_size_bytes': total_size,
            'total_size_mb': total_size / (1024 * 1024),
            'partitions': [str(p) for p in partitions]
        }


def read_parquet_to_schemas(
    file_path: Union[str, Path],
    schema_class: type[BaseModel]
) -> List[BaseModel]:
    """Read Parquet file and convert to Pydantic schemas.

    Utility function to read a Parquet file and validate records using
    a Pydantic schema.

    Args:
        file_path: Path to Parquet file or directory
        schema_class: Pydantic model class to validate with

    Returns:
        List[BaseModel]: List of validated Pydantic instances

    Example:
        >>> from src.schemas.parquet import AppReviewSchema
        >>> reviews = read_parquet_to_schemas(
        ...     "/mnt/nas/bronze/app_reviews/year=2026/month=02/data.parquet",
        ...     AppReviewSchema
        ... )
        >>> print(f"Read {len(reviews)} reviews")
    """
    # Read Parquet file
    table = pq.read_table(file_path)

    # Convert to list of dictionaries
    data_dicts = table.to_pylist()

    # Validate with Pydantic
    validated_records = [schema_class(**record) for record in data_dicts]

    logger.info(f"Read and validated {len(validated_records)} records from {file_path}")

    return validated_records
