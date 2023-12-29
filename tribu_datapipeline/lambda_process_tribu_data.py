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
import pandas as pd
from io import BytesIO
from utils import validate_date, read_from_s3, read_yaml_from_s3, upload_buffer_to_s3, format_dashed_date, yesterday, logger, \
    				setup_local_logger, RODAAPP_BUCKET_PREFIX

MAXIMUM_DISTANCE = 9000000 # Meters = 9000km
MINIMUM_DISTANCE = 0
MAXIMUM_DURATION = 90 # Minutes
MINIMUM_DURATION = 2
COLUMN_RENAME_MAP = {
    "k_dispositivo": "gpsID",
    "o_fecha_inicial": "timestampStart",
    "o_fecha_final": "timestampEnd",
    "f_distancia": "measuredDistance",
}
INPUT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
OUTPUT_DATETIME_FORMAT = "%Y-%m-%d %H:%M"


def get_transformation_parameters(dataset_type):
    """
    Get transformation parameters based on the dataset type. Expected transformation parameters
    are: maximun and minimum distance, maximun and minimum duration, a column name map for renaming
    columns, and format for reading and writing datetime values.

    Fetches transformation parameters from an AWS S3 bucket and return it as a dict objects.
    The dataset type ('roda' or 'guajira') determines the specific transformation parameters used,
    corresponding to motorbike or bicycle data, respectively.

    :param dataset_type: A string indicating the type of dataset ('roda' or 'guajira').
    :return: A dict containing tranformating parameters used for processing tribu data.
    """
    params_path = os.path.join(RODAAPP_BUCKET_PREFIX, "tribu_metadata", f"transformations_{dataset_type}.yaml")
    logger.info(f"Fetching transformation parameters for {dataset_type}: {params_path}")
    return read_yaml_from_s3(params_path)


def read_csv_into_pandas_from_s3(s3_path):
    """
    Read a csv file from S3 and return its content into a pandas dataframe.

    :param s3_path: The S3 path to the csv file, in the format 's3://bucket_name/key'.
    :return: The parsed csv data.
    """
    logger.info(f"Fetching tribu routes data from {s3_path}")
    csv_string = read_from_s3(s3_path)
    return pd.read_csv(BytesIO(csv_string.encode()))


def upload_pandas_to_s3(s3_path, df):
    """
    Upload a pandas dataframe to S3.

    :param s3_path: The S3 path where the IO buffer will be uploaded, in the format 's3://bucket_name/key'.
    :param df: Dataframe to be uploaded.
    """
    logger.info(f"Uploading rappi delivery routes to {s3_path}")
    with BytesIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)
        upload_buffer_to_s3(s3_path, csv_buffer)
        

def format_output_df(df, column_rename_map=COLUMN_RENAME_MAP, output_datetime_format=OUTPUT_DATETIME_FORMAT):
    """
    This function renames the columns of the input DataFrame according to the 
    column_rename_map dictionary and writes the resulting DataFrame to a CSV file at 
    the specified output path. The order of the columns in the output CSV will follow 
    the order they are defined in column_rename_map. It also fix the timesatmp output format.
    
    :param df (pandas.DataFrame): The DataFrame to be processed.
    :param column_rename_map (dict): The column_rename_map for renaming. Defaults to COLUMN_RENAME_MAP.
    """
    logger.info("Selecting and renaming columns")
    # Rename columns and reorder according to column_rename_map
    df = df[list(column_rename_map.keys())].rename(columns=column_rename_map)
    
	# Fix the output format for timestamp columns according to output_datetime_format
    df['timestampStart'] = df['timestampStart'].dt.strftime(output_datetime_format)
    df['timestampEnd'] = df['timestampEnd'].dt.strftime(output_datetime_format)
    
    return df


def filter_by_distance_range(df, min_dist=MINIMUM_DISTANCE, max_dist=MAXIMUM_DISTANCE):
    """
    Filter a DataFrame based on a distance range.

    Parameters:
    df (pandas.DataFrame): The DataFrame to filter.
    min_dist (float): The minimum distance for filtering. Defaults to MINIMUM_DISTANCE.
    max_dist (float): The maximum distance for filtering. Defaults to MAXIMUM_DISTANCE.

    Returns:
    pandas.DataFrame: A filtered DataFrame where the 'f_distancia' column values 
                      fall within the specified distance range.
    """
    return df[(df['f_distancia'] > min_dist) & (df['f_distancia'] <= max_dist)]


def filter_by_duration_range(df, min_dur=MINIMUM_DURATION, max_dur=MAXIMUM_DURATION):
    """
    Filter a DataFrame based on a duration in minutes range.

    This function calculates the duration in minutes between two timestamps 
    in the DataFrame columns 'o_fecha_final' and 'o_fecha_inicial'. It then filters 
    the DataFrame to include only the rows where the calculated duration 
    (in the 'durationMinutes' column) falls within the specified minimum 
    and maximum duration range.

    Parameters:
    - df (pandas.DataFrame): The DataFrame to filter. It must contain 
      'o_fecha_final' and 'o_fecha_inicial' columns with timestamp data.
    - min_dur (float): The minimum duration in minutes for filtering. 
      Defaults to MINIMUM_DURATION.
    - max_dur (float): The maximum duration in minutes for filtering. 
      Defaults to MAXIMUM_DURATION.

    Returns:
    - pandas.DataFrame: A filtered DataFrame where the 'durationMinutes' 
      column values fall within the specified duration in minutes range. 
      The 'durationMinutes' column is added to the DataFrame to show the 
      calculated duration for each row.
    """
    df['durationMinutes'] = (df['o_fecha_final'] - df['o_fecha_inicial']).dt.total_seconds() / 60
    return df[(df['durationMinutes'] > min_dur) & (df['durationMinutes'] <= max_dur)]


def filter_by_missing_client_reference(df):
    """
    Filter a DataFrame to include only rows with non-null client references.

    This function filters the DataFrame to retain only those rows where the 
    'Referencia' column is not null. The 'Referencia' column represents a 
    reference to the client that holds the GPS device. Rows without a client reference (null 
    values in 'Referencia') likely indicate routes that can be discarded, as 
    they may represent test devices or devices not yet assigned to a client. 
    This filtering is crucial for focusing on relevant data. Later we could generate 
    alerts when detecting this null values in 'Referencia' to investigate whether any unassigned devices 
    are due to oversight or are intentional for testing purposes.

    Parameters:
    - df (pandas.DataFrame): The DataFrame to filter. Must contain a 'Referencia' column.

    Returns:
    - pandas.DataFrame: A filtered DataFrame containing only rows where the 
      'Referencia' column is not null, indicating the presence of a client 
      reference and hence, a relevant route.
    """
    return df[df["Referencia"].notnull()]


def format_datetime_column(df, dt_column, input_datetime_format=INPUT_DATETIME_FORMAT):
    """
    Convert and format a datetime column in a DataFrame.

    Parameters:
    df (pandas.DataFrame): The DataFrame containing the datetime column to be formatted.
    dt_column (str): The name of the column to format.

    Returns:
    None: The function modifies the DataFrame in place, converting the datetime column
          to a specified format.
    """
    df[dt_column] = pd.to_datetime(df[dt_column], format=input_datetime_format)


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
    logger.info("STARTING: Tribu data processing task.")
    processing_date = event.get("processing_date")
    processing_date = validate_date(processing_date) if processing_date else yesterday()
    dataset_type = event.get("dataset_type")
    logger.info(f"Parameters: dataset type {dataset_type}, processing date: {processing_date}")

    trans_params = get_transformation_parameters(dataset_type)
    logger.info(f"Transformation parameters: {trans_params}")
    input_datetime_format = trans_params.get("input_datetime_format", INPUT_DATETIME_FORMAT)
    output_datetime_format = trans_params.get("output_datetime_format", OUTPUT_DATETIME_FORMAT)
    column_rename_map = trans_params.get("column_rename_map", COLUMN_RENAME_MAP)
    input_path = os.path.join(RODAAPP_BUCKET_PREFIX, "tribu_data", f"date={format_dashed_date(processing_date)}",
                               f"source={event['dataset_type']}", f"tribu_{event['dataset_type']}_routes.csv")
    output_path = os.path.join(RODAAPP_BUCKET_PREFIX, "rappi_driver_routes", f"date={format_dashed_date(processing_date)}",
                               f"source=tribu_{event['dataset_type']}", f"tribu_{event['dataset_type']}_routes.csv")
    
    df = read_csv_into_pandas_from_s3(input_path)

    logger.info("Applying filters")

    # this are GPS devices that we cannot relate to any rappi driver
    filter_by_missing_client_reference(df)

    # format datetime on input data in order to make it easier to do datetime operations
    format_datetime_column(df, "o_fecha_inicial", input_datetime_format)
    format_datetime_column(df, "o_fecha_final", input_datetime_format)

    if "distance_filter" in trans_params:
        distance_filter = trans_params["distance_filter"]
        filter_by_distance_range(df, distance_filter["min"], distance_filter["min"])

    if "duration_filter" in trans_params:
        duration_filter = trans_params["duration_filter"]
        filter_by_duration_range(df, duration_filter["min"], duration_filter["max"])

    logger.info("Preparing output data")

    # format output and upload it to s3 as a csv file
    df = format_output_df(df, column_rename_map, output_datetime_format)
    upload_pandas_to_s3(output_path, df)

    logger.info("FINISHED SUCCESSFULLY: Tribu data processing task")
    return "FINISHED SUCCESSFULLY: Tribu data processing task"


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