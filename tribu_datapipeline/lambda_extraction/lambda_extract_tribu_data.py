"""
This script connects to the Tribu API to retrieve GPS data of Rappi drivers. It handles two types of datasets: 
'roda', primarily consisting of motorbike data, and 'guajira', primarily consisting of bicycle data. The script 
fetches appropriate Tribu API credentials based on the specified dataset type and downloads the relevant dataset.
Designed for deployment in a Docker container, it's suitable for execution in an AWS Lambda function and supports
local testing.

The script can be executed in various environments:
1. As an AWS Lambda function - It is designed to run within AWS Lambda, fetching parameters from the event object.
2. In a Docker container - Suitable for local testing or deployment.
3. Directly via CLI - For local execution and testing.

The script supports command-line arguments for easy local testing and debugging. It leverages functionality 
from an accompanying 'utils.py' module for tasks like data processing and AWS S3 interactions.

Environment Variables:
- AWS_LAMBDA_RUNTIME_API: Used to determine if the script is running in an AWS Lambda environment.

Usage:
- AWS Lambda: Deploy the script as a Lambda function. The handler function will be invoked with event and context parameters.
- Docker Container/CLI: Run the script with optional command-line arguments to specify the dataset type and processing date.

Command-Line Arguments:
- --date (-d): Optional. Specify the date for data retrieval in 'YYYY-MM-DD' format. If not provided, defaults to yesterday's date.
- --dataset-type (-t): Required. Specify the dataset type. Accepts 'roda' or 'guajira'.

Examples:
- CLI: python script.py --date 2023-12-01 --dataset-type roda
- Docker: docker run --rm \
		-v ~/.aws:/root/.aws \
		-v $(shell pwd):/var/task \
		-i --entrypoint python rodaapp:tribu_extraction \
		lambda_extract_tribu_data.py --dataset-type roda --date 2023-12-01

Output:
- The script retrieves data from the Tribu API and writes it to a CSV file on AWS S3.

Note:
- The script requires access to AWS S3 for fetching credentials and storing output.
"""
import requests
import argparse
import logging
import os
from datetime import datetime
from typing import List, Dict
from python_utilities.utils import dicts_to_csv, validate_date, read_json_from_s3, \
                    format_dashed_date, yesterday, logger, setup_local_logger, RODAAPP_BUCKET_PREFIX


# Tribu API endpoint
TRIBU_URL = "https://tribugps.com/controlador.php"

def login(dataset_type: str) -> str:
    """
    Authenticate with the Tribu API using credentials based on the dataset type.

    Fetches Tribu API credentials from an AWS S3 bucket and uses them to log in
    to the Tribu API. The dataset type ('roda' or 'guajira') determines the specific credentials used,
    corresponding to motorbike or bicycle data, respectively.

    :param dataset_type: A string indicating the type of dataset ('roda' or 'guajira').
    :return: A token string used for authenticated API requests.
    :raises Exception: If the API response status is not 200.
    """
    logger.info(f"Downloading tribu {dataset_type} credentials")
    tribu_credentials_path = os.path.join(RODAAPP_BUCKET_PREFIX, "credentials", f"tribu_{dataset_type}_credentials.json")
    tribu_credentials = read_json_from_s3(tribu_credentials_path)
    form_data = {
        "tipo": "usuario",
        "funcion": "login",
        "user": tribu_credentials["user"],
        "password": tribu_credentials["password"],
        "isAdmin": "true"
    }

    response = requests.post(TRIBU_URL, data=form_data)

    if response.status_code == 200:
        response_json = response.json()
        token = response_json.get('body', {}).get('o_token')
        logger.info("Logged in to the tribu api")
        return token
    else:
        raise Exception("\t".join(["Error:", response.status_code, response.text]))


def get_tribu_data(token: str, date: datetime.date) -> List[Dict]:
    """
    Fetch GPS data for Rappi drivers from the Tribu API for a given date.

    Sends a request to the Tribu API to retrieve GPS data, using an authentication
    token and the specified date. The data pertains to Rappi drivers using motorbikes ('roda') 
    or bicycles ('guajira'), depending on the dataset type.

    :param token: Authentication token obtained from the login function.
    :param date: A datetime.date object representing the date for which data is to be retrieved.
    :return: A list of dictionaries containing the GPS data.
    :raises Exception: If the API response status is not 200.
    """
    logger.info("Downloading routes from tribu API")
    dashed_date = format_dashed_date(date)
    form_data = {
        "tipo": "ruta",
        "funcion": "verRutasSubAdmin",
        "d_fechaIni": dashed_date,
        "d_fechaFin": dashed_date
    }

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.post(TRIBU_URL, data=form_data, headers=headers)

    if response.status_code == 200:
        return response.json()['body']
    else:
        raise Exception("\t".join(["Error:", response.status_code, response.text]))


def handler(event: Dict, context) -> None:
    """
    Handler function for processing Tribu API data retrieval tasks.

    Intended for use as the entry point in AWS Lambda, but also supports local execution.
    Retrieves and processes GPS data from the Tribu API for a given date, then stores
    the data in a specified location. The 'dataset_type' in the event determines whether
    the data is primarily motorbike ('roda') or bicycle ('guajira') related.

    :param event: A dictionary containing 'dataset_type' and optionally 'processing_date'.
                  If 'processing_date' is not provided, defaults to yesterday's date.
    :param context: Context information provided by AWS Lambda (unused in this function).
    """
    logger.setLevel(logging.INFO)
    logger.info(f"STARTING: Tribu data extraction task. Parameters: \n{event}")
    tribu_token = login(event["dataset_type"])
    processing_date = event.get("processing_date")
    processing_date = validate_date(processing_date) if processing_date else yesterday()
    output_path = os.path.join(RODAAPP_BUCKET_PREFIX, "tribu_data", f"date={format_dashed_date(processing_date)}", 
                               f"source={event['dataset_type']}", f"tribu_{event['dataset_type']}_routes.csv")

    tribu_data = get_tribu_data(tribu_token, processing_date)
    
    dicts_to_csv(tribu_data, output_path)
    logger.info("FINISHED SUCCESSFULLY: Tribu data extraction task")


if __name__ == "__main__":
    """
    Main entry point for script execution.

    Supports running in a Docker container, AWS Lambda, or directly via CLI.
    Parses command-line arguments for dataset type and optional processing date.
    Executes the handler function with the appropriate parameters.
    """
    if 'AWS_LAMBDA_RUNTIME_API' in os.environ:
        # Running in AWS Lambda environment
        from awslambdaric import bootstrap
        bootstrap.run(handler, '/var/runtime/bootstrap')
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("-d", "--date", help="date of the execution of this script", type=validate_date, required=False)
        parser.add_argument("-t", "--dataset-type", help="Given the dataset type (roda or guajira)", choices=['guajira', 'roda'], required=True)
        
        args = parser.parse_args()
        setup_local_logger() # when it does not have env vars from aws, it means that this script is running locally 
        if args.date:
            handler(dict(processing_date=format_dashed_date(args.date),
                            dataset_type=args.dataset_type), "dockerlocal")
        else:
            handler(dict(dataset_type=args.dataset_type), "dockerlocal")
