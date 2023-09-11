import boto3
import pytest
from moto import mock_s3
from src.copy_files_to_s3 import upload_files_to_s3, check_file

# Define paths and name variables for testing
local_base_path = '/Users/aryanarora/PycharmProjects/kiwibank_test_project/test/resources'
root_directory = 'my-data-product'
bucket_name = 'test-bucket'
airflow_file_path = '/Users/aryanarora/PycharmProjects/kiwibank_test_project/test/resources/my-data-product/airflow/src/airflowFile.txt'
dbt_file_path = '/Users/aryanarora/PycharmProjects/kiwibank_test_project/test/resources/my-data-product/dbt/dbtFile.txt'
dbt_log_file_path = '/Users/aryanarora/PycharmProjects/kiwibank_test_project/test/resources/my-data-product/dbt/logs/logFile.txt'


@pytest.fixture
def s3_client_mock():
    """
    Fixture to mock the S3 client using 'moto'.
    """
    with mock_s3():
        region_name = 'ap-southeast-2'
        s3 = boto3.client('s3', region_name=region_name)
        # Create a test bucket
        s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region_name})
        yield s3


def test_upload_files_to_s3(s3_client_mock):
    """
    Test for uploading files to S3 and verifying their presence.
    """
    # Calling the upload_files_to_s3 function
    upload_files_to_s3(local_base_path, root_directory, bucket_name)

    # Registering expected call to list_objects_v2
    list_objects_response = {
        'Contents': [
            {'Key': 'my-data-product/airflow/src/airflowFile.txt'},
            {'Key': 'my-data-product/dbt/dbtFile.txt'}
        ]
    }
    # Asserting that the files were uploaded
    response = s3_client_mock.list_objects_v2(Bucket=bucket_name)
    actual_keys = [obj['Key'] for obj in response.get('Contents', [])]

    for expected_key in list_objects_response['Contents']:
        assert expected_key['Key'] in actual_keys


def test_check_file():
    """
    Test for the check_file function to validate file inclusion.
    """
    assert check_file(airflow_file_path, local_base_path, root_directory) == True
    assert check_file(dbt_file_path, local_base_path, root_directory) == True
    assert check_file(dbt_log_file_path, local_base_path, root_directory) == False


def test_files_not_uploaded_to_s3(s3_client_mock):
    """
    Test to verify files that should not be uploaded to S3 are not present.
    """
    # Calling the upload_files_to_s3 function
    upload_files_to_s3(local_base_path, root_directory, bucket_name)

    list_objects_response = {
        'Contents': [{'Key': 'my-data-product/airflow/tests/testFile.txt'},
                     {'Key': 'my-data-product/dbt/logs/logFile.txt'}]
    }

    # Asserting that the files were not uploaded
    response = s3_client_mock.list_objects_v2(Bucket=bucket_name)
    actual_keys = [obj['Key'] for obj in response.get('Contents', [])]

    for expected_key in list_objects_response['Contents']:
        assert expected_key['Key'] not in actual_keys


# Test case for handling a FileNotFoundError
def test_upload_files_to_s3_file_not_found_error():
    """
    Test for handling a FileNotFoundError when the local path does not exist.
    """
    test_root_dir = "non-existing-path"
    # Call the upload_files_to_s3 function with a non-existent local path
    with pytest.raises(FileNotFoundError):
        upload_files_to_s3(local_base_path, test_root_dir, bucket_name)
