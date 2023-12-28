"""
This module, utils.py, provides a set of utility functions for common tasks.
These include functions for writing dictionaries to CSV files both locally and
on AWS S3, reading data from S3, handling dates, and setting up logging. It is 
designed to support miscellaneous tasks in data processing and storage operations.
"""
import csv
import json
import boto3
import yaml
from io import StringIO
from datetime import datetime, timedelta
import logging


RODAAPP_BUCKET_PREFIX = "s3://rodaapp-rappidriverchain"
DATE_FORMAT = "%Y-%m-%d"
s3_client = boto3.client('s3')

logger = logging.getLogger()

def setup_local_logger():
    """
    Set up a basic logger with INFO level and a specified format.

    This function configures the root logger to display log messages at
    the INFO level and above. The log messages include timestamps, log
    level, and the log message.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def split_s3(s3_path):
    """
    Split an S3 path into bucket name and key.

    :param s3_path: The full S3 path in the format 's3://bucket_name/key'.
    :return: A tuple containing the bucket name and key.
    """
    # Remove the 's3://' prefix
    path_without_prefix = s3_path[5:]

    # Split the remaining path at the first '/'
    bucket, key = path_without_prefix.split('/', 1)

    return bucket, key


def dicts_to_csv_local(dict_list, file_path):
    """
    Write a list of dictionaries to a CSV file locally.

    :param dict_list: List of dictionaries. All dictionaries should have the same keys.
    :param file_path: The local path where the CSV file will be saved.
    """
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=dict_list[0].keys())
        writer.writeheader()
        writer.writerows(dict_list)


def dicts_to_csv_s3(dict_list, s3_path):
    """
    Write a list of dictionaries to a CSV file and upload it to S3.

    :param dict_list: List of dictionaries. All dictionaries should have the same keys.
    :param s3_path: The S3 path where the CSV file will be uploaded, in the format 's3://bucket_name/key'.
    """
    bucket_name, file_name = split_s3(s3_path)
    with StringIO() as csv_buffer:
        writer = csv.DictWriter(csv_buffer, fieldnames=dict_list[0].keys())
        writer.writeheader()
        writer.writerows(dict_list)

        s3_client.put_object(
            Body=csv_buffer.getvalue(),
            Bucket=bucket_name, Key=file_name)


def upload_buffer_to_s3(s3_path, buff):
    """
    Upload a buffer IO to S3.

    :param s3_path: The S3 path where the IO buffer will be uploaded, in the format 's3://bucket_name/key'.
    :param buff: IO buffer.
    """
    bucket_name, file_name = split_s3(s3_path)
    s3_client.put_object(
            Body=buff.getvalue(),
            Bucket=bucket_name, Key=file_name)


def read_from_s3(s3_path):
    """
    Read a file from S3 and return its content as a string.

    :param s3_path: The S3 path to the file, in the format 's3://bucket_name/key'.
    :return: The file content as a string.
    """
    bucket_name, key_path = split_s3(s3_path)
    response = s3_client.get_object(Bucket=bucket_name, Key=key_path)
    return response['Body'].read().decode('utf-8')


def read_json_from_s3(s3_path):
    """
    Read a JSON file from S3 and return its content.

    :param s3_path: The S3 path to the JSON file, in the format 's3://bucket_name/key'.
    :return: The parsed JSON data.
    """
    return json.loads(read_from_s3(s3_path))


def read_yaml_from_s3(s3_path):
    """
    Read a YAML file from S3 and return its content.

    :param s3_path: The S3 path to the YAML file, in the format 's3://bucket_name/key'.
    :return: The parsed YAML data.
    """
    return yaml.safe_load(StringIO(read_from_s3(s3_path)))


def dicts_to_csv(dict_list, filepath):
    """
    Converts a list of dictionaries with the same keys into a CSV file.
    The file can be saved locally or uploaded to S3 based on the filepath.

    :param dict_list: List of dictionaries.
    :param filepath: Filepath for the output CSV file. If it starts with 's3://', the file is uploaded to S3.
    :raises ValueError: If `dict_list` is empty.
    """
    if not dict_list:
        raise ValueError("The list of dictionaries is empty.")
    
    logger.info(f"Writting {len(dict_list)} records to {filepath}")
    
    if "s3://" in filepath:
        dicts_to_csv_s3(dict_list, filepath)
    else:
        dicts_to_csv_local(dict_list, filepath)


def validate_date(date_str):
    """
    Validate and convert a string to a datetime object based on the DATE_FORMAT.

    :param date_str: Date string to be validated and converted.
    :return: Datetime object corresponding to the date string.
    """
    return datetime.strptime(date_str, DATE_FORMAT)


def format_dashed_date(o_date):
    """
    Format a datetime object to a string based on the DATE_FORMAT.

    :param o_date: Datetime object to be formatted.
    :return: Formatted date string.
    """
    return o_date.strftime(DATE_FORMAT)


def yesterday():
    """
    Calculate and return the date for yesterday.

    :return: A datetime object representing yesterday's date.
    """
    one_day = timedelta(days=1)
    return datetime.now() - one_day
