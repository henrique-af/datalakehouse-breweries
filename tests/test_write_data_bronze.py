from unittest.mock import MagicMock, patch
import pytest
from airflow.models import TaskInstance
from bronze_operators import WriteDataToBronzeOperator
from minio import Minio
from io import BytesIO
import json

@patch('bronze_operators.Minio')
def test_write_data_to_bronze(mock_minio_client):
    # Test data
    test_data = [{"id": "1", "name": "Test Brewery", "city": "Test City"}]

    # Mock the Minio client
    mock_client_instance = MagicMock()
    mock_minio_client.return_value = mock_client_instance

    # Create operator instance
    operator = WriteDataToBronzeOperator(
        task_id='test_write_to_bronze',
        minio_endpoint='test-endpoint:9000',
        minio_access_key='test-access-key',
        minio_secret_key='test-secret-key',
        bronze_bucket='test-bucket',
        data_key='test_data'
    )

    # Mock the Airflow context
    mock_context = {'ti': MagicMock()}
    mock_context['ti'].xcom_pull.return_value = test_data

    # Execute the operator
    result = operator.execute(mock_context)

    # Assertions
    assert result is not None
    assert result.endswith('.json')

    # Verify that put_object was called
    mock_client_instance.put_object.assert_called_once()

    # Get the call arguments
    call_args, call_kwargs = mock_client_instance.put_object.call_args

    # Check bucket name (should be the first positional argument)
    assert call_args[0] == 'test-bucket'

    # Check that the filename ends with .json (should be the second positional argument)
    assert call_args[1].endswith('.json')

    # Check that data was passed (either as a positional argument or as a keyword argument)
    data_arg = call_args[2]  # The third positional argument is the data (BytesIO object)
    assert data_arg is not None

    # Verify the content of the data
    data_arg.seek(0)  # Reset the BytesIO pointer to the beginning
    data_content = data_arg.read().decode('utf-8')
    assert json.loads(data_content) == test_data