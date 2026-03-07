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


def make_client(mock_s3=None):
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
        ]
    }
    client = make_client(mock_s3)
    keys = client.list_objects('bronze/year=2026/month=03/')
    assert len(keys) == 2
    assert all('bronze' in k for k in keys)


def test_list_objects_empty(mock_s3):
    mock_s3.list_objects_v2.return_value = {}
    client = make_client(mock_s3)
    keys = client.list_objects('bronze/year=2099/')
    assert keys == []


def test_get_parquet_returns_table(mock_s3):
    sample = pa.table({'review_id': ['r1', 'r2'], 'review_text': ['hello', 'world']})
    buf = io.BytesIO()
    pq.write_table(sample, buf)
    buf.seek(0)
    mock_s3.get_object.return_value = {'Body': buf}

    client = make_client(mock_s3)
    table = client.get_parquet('bronze/data.parquet')

    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    assert 'review_id' in table.column_names


def test_put_parquet_uploads(mock_s3):
    sample = pa.table({'review_id': ['r1'], 'refined_text': ['clean']})
    client = make_client(mock_s3)
    client.put_parquet('silver/reviews/app_id=app1/dt=2026-03-04/refined.parquet', sample)

    mock_s3.put_object.assert_called_once()
    kwargs = mock_s3.put_object.call_args.kwargs
    assert kwargs['Key'] == 'silver/reviews/app_id=app1/dt=2026-03-04/refined.parquet'
    assert kwargs['Bucket'] == 'reai-data'
    assert 'Body' in kwargs
