"""
This module, utils.py, provides a set of utility functions for common tasks in data processing and storage operations. 
These include functions for writing dictionaries to CSV files both locally and on AWS S3, reading data from S3, 
handling date conversions, and setting up logging. It leverages external libraries like boto3 for AWS interactions, 
csv and json for file operations, and datetime for date manipulations.
"""
import csv
import json
import boto3
import yaml
from io import StringIO
from datetime import datetime, timedelta
import logging
from io import IOBase


RODAAPP_BUCKET_PREFIX = "s3://rodaapp-rappidriverchain"
DATE_FORMAT = "%Y-%m-%d"
s3_client = boto3.client('s3')

logger = logging.getLogger()

def setup_local_logger() -> None:
    """
    Set up a basic logger for local development of AWS Lambda functions in a Docker environment.

    This function configures the root logger to display log messages at the INFO level and above, 
    particularly useful when running the Lambda function locally in a Docker container. In this setup,
    standard logging configurations may not output logs as expected, and this function ensures that
    log messages are visible in the standard output of the Docker container.

    Note:
    - This configuration is specifically intended for local debugging and testing. 
    - When the Lambda function is deployed to AWS, the logging configuration provided by AWS Lambda is 
      sufficient, and this function does not need to be invoked.

    No parameters or return values.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def split_s3(s3_path: str) -> tuple:
    """
    Split an S3 path into its bucket name and key components.

    :param s3_path: The full S3 path in the format 's3://bucket_name/key'.
    :return: A tuple (bucket_name, key) extracted from the provided S3 path.

    Example:
    >>> split_s3("s3://mybucket/myfile.txt")
    ("mybucket", "myfile.txt")
    """
    # Remove the 's3://' prefix
    path_without_prefix = s3_path[5:]

    # Split the remaining path at the first '/'
    bucket, key = path_without_prefix.split('/', 1)

    return bucket, key


def dicts_to_csv_local(dict_list: list, file_path: str) -> None:
    """
    Write a list of dictionaries to a CSV file stored locally.

    Each dictionary in the list should have the same keys, representing CSV column headers.

    :param dict_list: List of dictionaries with consistent keys.
    :param file_path: The local file path where the CSV file will be saved.
    """
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=dict_list[0].keys())
        writer.writeheader()
        writer.writerows(dict_list)


def dicts_to_csv_s3(dict_list: list, s3_path: str) -> None:
    """
    Write a list of dictionaries to a CSV file and upload it to an S3 bucket.

    Each dictionary in the list should have the same keys, which are used as CSV column headers.

    :param dict_list: List of dictionaries with consistent keys.
    :param s3_path: The S3 path (e.g., 's3://bucket_name/key') where the CSV file will be uploaded.
    """
    with StringIO() as csv_buffer:
        writer = csv.DictWriter(csv_buffer, fieldnames=dict_list[0].keys())
        writer.writeheader()
        writer.writerows(dict_list)

        upload_buffer_to_s3(s3_path, csv_buffer)


def upload_buffer_to_s3(s3_path: str, buff: IOBase) -> None:
    """
    Upload a buffer (like StringIO or BytesIO) to an S3 bucket.

    :param s3_path: The S3 path (e.g., 's3://bucket_name/key') where the buffer will be uploaded.
    :param buff: An IO buffer (StringIO, BytesIO, etc.) containing the data to upload.
    """
    bucket_name, file_name = split_s3(s3_path)
    s3_client.put_object(
            Body=buff.getvalue(),
            Bucket=bucket_name, Key=file_name)


def read_from_s3(s3_path: str) -> str:
    """
    Read a file from an S3 bucket and return its content as a string.

    :param s3_path: The S3 path (e.g., 's3://bucket_name/key') to the file.
    :return: The content of the file as a string.
    """
    bucket_name, key_path = split_s3(s3_path)
    response = s3_client.get_object(Bucket=bucket_name, Key=key_path)
    return response['Body'].read().decode('utf-8')


def read_json_from_s3(s3_path: str) -> dict:
    """
    Read a JSON file from an S3 bucket and return its content as a dictionary.

    :param s3_path: The S3 path (e.g., 's3://bucket_name/key') to the JSON file.
    :return: A dictionary representing the parsed JSON data.
    """
    return json.loads(read_from_s3(s3_path))


def read_yaml_from_s3(s3_path: str) -> dict:
    """
    Read a YAML file from an S3 bucket and return its content.

    :param s3_path: The S3 path (e.g., 's3://bucket_name/key') to the YAML file.
    :return: A dictionary or list representing the parsed YAML data.
    """
    return yaml.safe_load(StringIO(read_from_s3(s3_path)))


def read_csv_from_s3(s3_path: str) -> list:
    """
    Read a CSV file with a header from an S3 bucket and return its content.

    This function assumes that the first row of the CSV file contains headers.
    Each row of the CSV file is converted into a dictionary where the keys are the headers.

    :param s3_path: The S3 path (e.g., 's3://bucket_name/key') to the CSV file.
    :return: A list of dictionaries, each representing a row in the CSV file.
    """
    csv_content = read_from_s3(s3_path)
    csv_reader = csv.DictReader(StringIO(csv_content))

    return [row for row in csv_reader]


def dict_to_yaml_s3(data_dict, s3_path):
    with StringIO() as yaml_buffer:
        yaml.dump(data_dict, yaml_buffer)
        upload_buffer_to_s3(s3_path, yaml_buffer)


def dicts_to_csv(dict_list: list, filepath: str) -> None:
    """
    Convert a list of dictionaries with the same keys into a CSV file.

    The file is saved locally or uploaded to S3 based on the filepath format. 
    If filepath starts with 's3://', the file is uploaded to S3; otherwise, it is saved locally.

    :param dict_list: List of dictionaries with consistent keys.
    :param filepath: Filepath for the output CSV file. Can be a local path or an S3 path.
    :raises ValueError: If `dict_list` is empty, indicating no data to write.

    Example:
    >>> dicts_to_csv([{"name": "Alice", "age": 30}], "s3://mybucket/myfile.csv")
    """
    if not dict_list:
        raise ValueError("The list of dictionaries is empty.")
    
    logger.info(f"Writting {len(dict_list)} records to {filepath}")
    
    if "s3://" in filepath:
        dicts_to_csv_s3(dict_list, filepath)
    else:
        dicts_to_csv_local(dict_list, filepath)


def list_s3_files(s3_path: str):
    bucket, prefix = split_s3(s3_path)
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [item['Key'] for item in response.get('Contents', [])]


def validate_date(date_str: str) -> datetime:
    """
    Validate and convert a string to a datetime object based on a pre-defined date format (DATE_FORMAT).

    :param date_str: Date string to be validated and converted.
    :return: A datetime object corresponding to the date string.
    :raises ValueError: If the date string does not match the expected format.
    """
    return datetime.strptime(date_str, DATE_FORMAT)


def format_dashed_date(o_date: datetime) -> str:
    """
    Format a datetime object to a string using a pre-defined date format (DATE_FORMAT).

    :param o_date: The datetime object to be formatted.
    :return: A string representing the formatted date.
    """
    return o_date.strftime(DATE_FORMAT)


def yesterday() -> datetime:
    """
    Calculate and return the date for yesterday.

    :return: A datetime object representing yesterday's date.
    """
    one_day = timedelta(days=1)
    return datetime.now() - one_day
