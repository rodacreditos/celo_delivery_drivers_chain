"""
This module is for having a miscellaneus of utility functions to help with
common tasks such as storing a dict to csv locally or to s3.
"""
import csv
import json
import boto3
import os
from io import StringIO
from datetime import datetime, timedelta
import logging


RODAAPP_BUCKET_PREFIX = "s3://rodaapp-rappidriverchain"
DATE_FORMAT = "%Y-%m-%d"
s3_client = boto3.client('s3')
logger = logging.getLogger()


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

        s3_client.put_object(
            Body=csv_buffer.getvalue(),
            Bucket=bucket_name, Key=file_name)


def read_from_s3(s3_path):
    bucket_name, key_path = split_s3(s3_path)
    response = s3_client.get_object(Bucket=bucket_name, Key=key_path)
    return response['Body'].read().decode('utf-8')


def read_json_from_s3(s3_path):
    return json.loads(read_from_s3(s3_path))


def dicts_to_csv(dict_list, filepath):
    """
    Converts a list of dictionaries with the same keys into a CSV file.

    :param dict_list: List of dictionaries
    :param filepath: Filename for the output CSV file
    """
    if not dict_list:
        raise ValueError("The list of dictionaries is empty.")
    
    logger.info(f"Writting {len(dict_list)} records to {filepath}")
    
    if "s3://" in filepath:
        dicts_to_csv_s3(dict_list, filepath)
    else:
        dicts_to_csv_local(dict_list, filepath)


def validate_date(date_str):
    return datetime.strptime(date_str, DATE_FORMAT)


def format_dashed_date(o_date):
    return o_date.strftime(DATE_FORMAT)


def yesterday():
    one_day = timedelta(days=1)
    return datetime.now() - one_day
