# My Data Product S3 File Uploader

## Overview

This Python project provides a utility for copying specific files from a local directory to an Amazon S3 bucket. It is designed to meet the following requirements:

- Include files and subdirectories from the 'airflow/src' directory in the S3 upload.
- Exclude files from the 'airflow' directory (except 'airflow/src') from the S3 upload.
- Include all files from the 'dbt' directory, except those under the 'dbt/logs' subdirectory.

## Project Structure

The project follows a standard Python project structure:

- `copy_files_to_s3.py`: Contains the `upload_files_to_s3` function for copying files to S3.
- `tests/`: Holds pytest test cases to ensure the correctness of the code.
- `requirements.txt` (Optional): Lists project dependencies and their versions.

## Usage

Please add the values of the below variable before running

local_path = "/path/to/your/local/directory"
root_directory = "your-root-directory"
s3_bucket_name = "your-s3-bucket-name"

## Running Test

pytest tests/test_copy_files_to_s3.py



