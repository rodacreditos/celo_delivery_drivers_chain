"""
This module is for having a miscellaneus of utility functions to help with
common tasks such as storing a dict to csv locally or to s3.
"""
import csv
import boto3
from io import StringIO
from datetime import datetime


def split_s3(s3_path):
    # Remove the 's3://' prefix
    path_without_prefix = s3_path[5:]

    # Split the remaining path at the first '/'
    bucket, key = path_without_prefix.split('/', 1)

    return bucket, key


def dicts_to_csv_local(dict_list, file_path):
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=dict_list[0].keys())
        writer.writeheader()
        writer.writerows(dict_list)


def dicts_to_csv_s3(dict_list, s3_path):
    bucket_name, file_name = split_s3(s3_path)
    with StringIO() as csv_buffer:
        writer = csv.DictWriter(csv_buffer, fieldnames=dict_list[0].keys())
        writer.writeheader()
        writer.writerows(dict_list)

        s3_client = boto3.client('s3')
        s3_client.put_object(
            Body=csv_buffer.getvalue(),
            Bucket=bucket_name, Key=file_name)


def dicts_to_csv(dict_list, filepath):
    """
    Converts a list of dictionaries with the same keys into a CSV file.

    :param dict_list: List of dictionaries
    :param filepath: Filename for the output CSV file
    """
    if not dict_list:
        raise ValueError("The list of dictionaries is empty.")
    
    if "s3://" in filepath:
        dicts_to_csv_s3(dict_list, filepath)
    else:
        dicts_to_csv_local(dict_list, filepath)


def validate_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d")