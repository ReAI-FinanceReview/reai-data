# -*- coding: utf-8 -*-
"""MinIO S3-compatible client wrapper using boto3."""
import io
import os
from typing import List

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.logger import get_logger

logger = get_logger(__name__)


class MinIOClient:
    """boto3 기반 MinIO(S3-compatible) 클라이언트.

    환경변수 우선순위:
        1. 생성자 파라미터
        2. MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET
    """

    def __init__(
        self,
        endpoint: str = None,
        access_key: str = None,
        secret_key: str = None,
        bucket: str = None,
    ):
        self.endpoint = endpoint or os.environ['MINIO_ENDPOINT']
        self.access_key = access_key or os.environ['MINIO_ACCESS_KEY']
        self.secret_key = secret_key or os.environ['MINIO_SECRET_KEY']
        self.bucket = bucket or os.environ['MINIO_BUCKET']

        self._client = boto3.client(
            's3',
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

    def list_objects(self, prefix: str) -> List[str]:
        """주어진 prefix의 모든 객체 키를 반환한다."""
        response = self._client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        contents = response.get('Contents', [])
        keys = [obj['Key'] for obj in contents]
        logger.info(f"Listed {len(keys)} objects under '{prefix}'")
        return keys

    def get_parquet(self, key: str) -> pa.Table:
        """S3에서 Parquet 파일을 읽어 PyArrow Table로 반환한다."""
        response = self._client.get_object(Bucket=self.bucket, Key=key)
        buf = io.BytesIO(response['Body'].read())
        table = pq.read_table(buf)
        logger.info(f"Read {table.num_rows} rows from '{key}'")
        return table

    def put_parquet(self, key: str, table: pa.Table) -> None:
        """PyArrow Table을 Snappy 압축 Parquet으로 S3에 업로드한다 (overwrite)."""
        buf = io.BytesIO()
        pq.write_table(table, buf, compression='snappy')
        buf.seek(0)
        self._client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=buf.getvalue(),
        )
        logger.info(f"Uploaded {table.num_rows} rows to '{key}'")
