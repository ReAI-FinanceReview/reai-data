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
_UNSET = object()


class MinIOClient:
    """boto3 기반 MinIO(S3-compatible) 클라이언트.

    환경변수 우선순위:
        1. 생성자 파라미터
        2. MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET
    """

    def __init__(
        self,
        endpoint: str | None | object = _UNSET,
        access_key: str | None | object = _UNSET,
        secret_key: str | None | object = _UNSET,
        bucket: str | None | object = _UNSET,
    ):
        raw_endpoint = os.environ.get('MINIO_ENDPOINT', '') if endpoint is _UNSET else (endpoint or '')
        if raw_endpoint and not raw_endpoint.startswith(('http://', 'https://')):
            use_ssl = os.environ.get('MINIO_USE_SSL', 'false').lower() == 'true'
            scheme = 'https' if use_ssl else 'http'
            raw_endpoint = f'{scheme}://{raw_endpoint}'
        self.endpoint = raw_endpoint or None
        self.access_key = os.environ.get('MINIO_ACCESS_KEY') if access_key is _UNSET else access_key
        self.secret_key = os.environ.get('MINIO_SECRET_KEY') if secret_key is _UNSET else secret_key
        self.bucket = os.environ['MINIO_BUCKET'] if bucket is _UNSET else bucket

        if bool(self.access_key) != bool(self.secret_key):
            raise ValueError(
                "access_key와 secret_key는 둘 다 설정하거나 둘 다 생략해야 합니다 "
                "(둘 다 생략 시 IAM/기본 인증 체계 사용)."
            )

        client_kwargs = dict(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )
        if self.endpoint:
            client_kwargs['endpoint_url'] = self.endpoint
        self._client = boto3.client('s3', **client_kwargs)

        logger.info(
            f"Initialized MinIOClient: mode={'custom endpoint' if self.endpoint else 'native AWS S3'}, "
            f"endpoint={self.endpoint}, bucket={self.bucket}, "
            f"credentials={'set' if self.access_key else 'not set (using IAM/default chain)'}"
        )

    def list_objects(self, prefix: str) -> List[str]:
        """주어진 prefix의 모든 객체 키를 반환한다 (1000개 초과 페이지네이션 지원)."""
        keys: List[str] = []
        kwargs = {'Bucket': self.bucket, 'Prefix': prefix}
        while True:
            response = self._client.list_objects_v2(**kwargs)
            keys.extend(obj['Key'] for obj in response.get('Contents', []))
            if not response.get('IsTruncated'):
                break
            kwargs['ContinuationToken'] = response['NextContinuationToken']
        logger.info(f"Listed {len(keys)} objects under '{prefix}'")
        return keys

    def get_parquet(self, key: str) -> pa.Table:
        """S3에서 Parquet 파일을 읽어 PyArrow Table로 반환한다."""
        response = self._client.get_object(Bucket=self.bucket, Key=key)
        buf = io.BytesIO(response['Body'].read())
        table = pq.read_table(buf)
        logger.info(f"Read {table.num_rows} rows from '{key}'")
        return table

    def delete_object(self, key: str) -> None:
        """S3에서 객체를 삭제한다."""
        self._client.delete_object(Bucket=self.bucket, Key=key)
        logger.info(f"Deleted object '{key}'")

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
