import csv
import boto3


def dicts_to_csv(dict_list, filename):
    """
    Converts a list of dictionaries with the same keys into a CSV file.

    :param dict_list: List of dictionaries
    :param filename: Filename for the output CSV file
    """
    if not dict_list:
        raise ValueError("The list of dictionaries is empty.")

    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=dict_list[0].keys())
        writer.writeheader()
        writer.writerows(dict_list)