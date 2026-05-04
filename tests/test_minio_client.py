import io
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from unittest.mock import patch, MagicMock
from src.utils.minio_client import MinIOClient


@pytest.fixture
def mock_s3():
    with patch('boto3.client') as mock_boto3:
        mock_client = MagicMock()
        mock_boto3.return_value = mock_client
        yield mock_client


@pytest.fixture
def mock_s3_constructor():
    with patch('boto3.client') as mock_boto3:
        mock_boto3.return_value = MagicMock()
        yield mock_boto3


def make_client():
    return MinIOClient(
        endpoint='http://localhost:9000',
        access_key='test',
        secret_key='test',
        bucket='reai-data'
    )


def test_list_objects_returns_keys(mock_s3):
    mock_s3.list_objects_v2.return_value = {
        'Contents': [
            {'Key': 'bronze/year=2026/month=03/data1.parquet'},
            {'Key': 'bronze/year=2026/month=03/data2.parquet'},
        ],
        'IsTruncated': False,
    }
    client = make_client()
    keys = client.list_objects('bronze/year=2026/month=03/')
    assert len(keys) == 2
    assert all('bronze' in k for k in keys)


def test_list_objects_empty(mock_s3):
    mock_s3.list_objects_v2.return_value = {'IsTruncated': False}
    client = make_client()
    keys = client.list_objects('bronze/year=2099/')
    assert keys == []


def test_list_objects_pagination(mock_s3):
    """list_objects follows ContinuationToken across multiple pages."""
    mock_s3.list_objects_v2.side_effect = [
        {
            'Contents': [{'Key': 'bronze/page1.parquet'}],
            'IsTruncated': True,
            'NextContinuationToken': 'token123',
        },
        {
            'Contents': [{'Key': 'bronze/page2.parquet'}],
            'IsTruncated': False,
        },
    ]
    client = make_client()
    keys = client.list_objects('bronze/')
    assert keys == ['bronze/page1.parquet', 'bronze/page2.parquet']
    assert mock_s3.list_objects_v2.call_count == 2


def test_get_parquet_returns_table(mock_s3):
    sample = pa.table({'review_id': ['r1', 'r2'], 'review_text': ['hello', 'world']})
    buf = io.BytesIO()
    pq.write_table(sample, buf)
    buf.seek(0)
    mock_s3.get_object.return_value = {'Body': buf}

    client = make_client()
    table = client.get_parquet('bronze/data.parquet')

    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    assert 'review_id' in table.column_names


def test_put_parquet_uploads(mock_s3):
    sample = pa.table({'review_id': ['r1'], 'refined_text': ['clean']})
    client = make_client()
    client.put_parquet('silver/reviews/app_id=app1/dt=2026-03-04/refined.parquet', sample)

    mock_s3.put_object.assert_called_once()
    kwargs = mock_s3.put_object.call_args.kwargs
    assert kwargs['Key'] == 'silver/reviews/app_id=app1/dt=2026-03-04/refined.parquet'
    assert kwargs['Bucket'] == 'reai-data'
    assert 'Body' in kwargs


def test_init_with_endpoint_passes_endpoint_url(mock_s3_constructor):
    """endpoint 있을 때 endpoint_url이 boto3에 전달된다."""
    MinIOClient(endpoint='http://localhost:9000', access_key='a', secret_key='b', bucket='test')
    _, kwargs = mock_s3_constructor.call_args
    assert kwargs.get('endpoint_url') == 'http://localhost:9000'


def test_init_without_endpoint_omits_endpoint_url(mock_s3_constructor):
    """endpoint 없을 때 endpoint_url이 boto3에 전달되지 않는다."""
    MinIOClient(endpoint=None, access_key='a', secret_key='b', bucket='test')
    _, kwargs = mock_s3_constructor.call_args
    assert 'endpoint_url' not in kwargs


def test_init_partial_credentials_raises(mock_s3_constructor):
    """access_key만 있고 secret_key가 없으면 즉시 ValueError."""
    with pytest.raises(ValueError, match="둘 다 설정하거나 둘 다 생략"):
        MinIOClient(endpoint=None, access_key='a', secret_key=None, bucket='test')


def test_init_missing_bucket_raises(mock_s3_constructor, monkeypatch):
    """bucket 파라미터와 MINIO_BUCKET 환경변수가 모두 없으면 즉시 실패한다."""
    monkeypatch.delenv("MINIO_BUCKET", raising=False)

    with pytest.raises(ValueError, match="MINIO_BUCKET"):
        MinIOClient(endpoint=None, access_key=None, secret_key=None)


def test_init_explicit_none_bucket_raises(mock_s3_constructor):
    """bucket=None은 지연 실패가 아니라 생성자 실패로 처리한다."""
    with pytest.raises(ValueError, match="MINIO_BUCKET"):
        MinIOClient(endpoint=None, access_key=None, secret_key=None, bucket=None)


def test_init_no_credentials_allowed(mock_s3_constructor):
    """둘 다 None이면 IAM 모드로 정상 초기화된다."""
    client = MinIOClient(endpoint=None, access_key=None, secret_key=None, bucket='test')
    assert client.access_key is None
    assert client.secret_key is None
