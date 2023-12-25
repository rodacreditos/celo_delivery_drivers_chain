import csv
import boto3
from io import StringIO


def split_s3(s3_path):
    # Remove the 's3://' prefix
    path_without_prefix = s3_path[5:]

    # Split the remaining path at the first '/'
    bucket, key = path_without_prefix.split('/', 1)

    return bucket, key


def dicts_to_csv(dict_list, filepath):
    """
    Converts a list of dictionaries with the same keys into a CSV file.

    :param dict_list: List of dictionaries
    :param filepath: Filename for the output CSV file
    """
    if not dict_list:
        raise ValueError("The list of dictionaries is empty.")
    
    if "s3://" in filepath:
        bucket_name, file_name = split_s3(filepath)
        with StringIO() as csv_buffer:
            writer = csv.DictWriter(csv_buffer, fieldnames=dict_list[0].keys())
            writer.writeheader()
            writer.writerows(dict_list)

            s3_client = boto3.client('s3')
            s3_client.put_object(
                Body=csv_buffer.getvalue(),
                Bucket=bucket_name, Key=file_name)
    else:
        with open(filepath, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=dict_list[0].keys())
            writer.writeheader()
            writer.writerows(dict_list)