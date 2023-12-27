"""
This script helps to process data stored in S3, that were previously extracted from Tribu's API. It handles two types of datasets: 
'roda', primarily consisting of motorbike data, and 'guajira', primarily consisting of bicycle data. The script 
fetches appropriate transformation parameters based on the specified dataset type, downloads the relevant dataset,
processes data from  a CSV file, applies filters based on distance, duration, and client reference criteria, formats datetime 
fields, adjusts GPS coordinates to standard precision, exports the processed data into a new CSV file 
with renamed columns according to a predefined mapping, and uploads it to a further specified AWS S3 location.
Designed for deployment in a Docker container, it's suitable for execution in an AWS Lambda function and supports
local testing.

Key Features:
- Read data from a CSV file.
- Filter records based on distance, duration range, and client reference availability.
- Format datetime fields to a specific format.
- Rename and reorder DataFrame columns according to a predefined mapping.
- Export processed data to a new CSV file on AWS S3.

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
		-i --entrypoint python rodaapp:tribu_processing \
		lambda_process_tribu_data.py --dataset-type roda --date 2023-12-01

Output:
- The script processes data from Tribu on S3 and writes it back to a new CSV file on AWS S3.

Note:
- The script requires access to AWS S3 for fetching parameters, reading input data from tribu, and storing output.
"""
import argparse
import logging
import os
from utils import validate_date, format_dashed_date, yesterday, logger, setup_local_logger, RODAAPP_BUCKET_PREFIX


def handler(event, context):
    """
    Handler function for processing Tribu data.

    Intended for use as the entry point in AWS Lambda, but also supports local execution.
    The 'dataset_type' in the event determines whether the data is primarily motorbike ('roda') 
    or bicycle ('guajira') related.

    :param event: A dictionary containing 'dataset_type' and optionally 'processing_date'.
                  If 'processing_date' is not provided, defaults to yesterday's date.
    :param context: Context information provided by AWS Lambda (unused in this function).
    """
    logger.setLevel(logging.INFO)
    logger.info(f"STARTING: Tribu data processing task. Parameters: \n{event}")
    processing_date = event.get("processing_date")
    processing_date = validate_date(processing_date) if processing_date else yesterday()
    dataset_type = event.get("dataset_type")
    logger.info(f"Parameter: dataset type {dataset_type}, processing date: {processing_date}")


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