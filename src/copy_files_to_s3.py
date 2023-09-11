import os
import logging
import boto3

# Configure logging
boto3.set_stream_logger(name='botocore.credentials', level=logging.WARNING)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_file(file_path, local_path, root_dir):
    """
    Check if a file should be included for copying to S3 based on specific conditions.

    Args:
        file_path (str): The path of the file to check.
        local_path (str): The base directory of the local file system.
        root_dir (str): The root directory within the local_path that contains the files to be copied.

    Returns:
        bool: True if the file should be included, False otherwise.
    """
    # Check condition/rule if the file path meets the inclusion criteria
    if (file_path.startswith(os.path.join(local_path, root_dir, 'airflow', 'src'))) \
            or (file_path.startswith(os.path.join(local_path, root_dir, 'dbt')) and not file_path.startswith(
        os.path.join(local_path, root_dir, 'dbt', 'logs'))):
        return True
    else:
        return False


def upload_files_to_s3(local_path, root_dir, s3_bucket_name):
    """
    Copy files from a local directory to an S3 bucket while preserving directory structure.

    Args:
        local_path (str): The base directory of the local file system.
        root_dir (str): The root directory within the local_path that contains the files to be copied.
        s3_bucket_name (str): The name of the S3 bucket where files will be copied.
    """
    try:
        # Check if the local path exists, if not raise an exception
        if not os.path.exists(os.path.join(local_path, root_dir)):
            raise FileNotFoundError(f"Local path '{os.path.join(local_path, root_dir)}' does not exist.")

        # Initialize the S3 client
        s3_client = boto3.client('s3')
        # Declare a blank list, it will hold files to copy
        files_to_copy = []

        # Traverse the local directory and add eligible files to the list
        for root, dirs, files in os.walk(os.path.join(local_path, root_dir)):
            for file_name in files:
                # Create absolute path of files in local
                file_path = os.path.join(root, file_name)
                # Check if file path satisfy the condition/rule for copying
                if check_file(file_path, local_path, root_dir):
                    # Extract relative file path and create s3 key for each file
                    s3_key = os.path.relpath(file_path, local_path)
                    files_to_copy.append((file_path, s3_key))

        # Upload eligible files to S3
        for local_file, s3_key in files_to_copy:
            logger.info(f"Local File to upload : {local_file}")
            logger.info(f"S3 Key : {s3_key}")
            s3_client.upload_file(local_file, s3_bucket_name, s3_key)

    except FileNotFoundError:
        raise

    except Exception as e:
        # Handle other exceptions, including S3 upload errors
        logger.exception(f"Error in upload objects to bucket {s3_bucket_name}, {e}")
        raise


if __name__ == "__main__":
    local_base_path = "/Users/aryanarora"
    root_directory = "my-data-product"
    bucket_name = "bank-data-platform"
    # call function to copy file to S3 from local
    upload_files_to_s3(local_base_path, root_directory, bucket_name)
