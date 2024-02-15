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
from typing import Dict, Any
from python_utilities.utils import validate_date, read_from_s3, read_yaml_from_s3, upload_buffer_to_s3, format_dashed_date, yesterday, logger, \
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


def get_transformation_parameters(dataset_type: str) -> Dict[str, Any]:
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


def read_csv_into_pandas_from_s3(s3_path: str) -> pd.DataFrame:
    """
    Read a csv file from S3 and return its content into a pandas dataframe.

    :param s3_path: The S3 path to the csv file, in the format 's3://bucket_name/key'.
    :return: The parsed csv data.
    """
    logger.info(f"Fetching tribu routes data from {s3_path}")
    csv_string = read_from_s3(s3_path)
    return pd.read_csv(BytesIO(csv_string.encode()))


def upload_pandas_to_s3(s3_path: str, df: pd.DataFrame) -> None:
    """
    Upload a pandas dataframe to S3.

    :param s3_path: The S3 path where the IO buffer will be uploaded, in the format 's3://bucket_name/key'.
    :param df: Dataframe to be uploaded.
    """
    logger.info(f"Uploading rappi delivery routes to {s3_path}")
    with BytesIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)
        upload_buffer_to_s3(s3_path, csv_buffer)
        

def format_output_df(df: pd.DataFrame, column_rename_map: Dict[str, str] = COLUMN_RENAME_MAP, 
                     output_datetime_format: str = OUTPUT_DATETIME_FORMAT) -> pd.DataFrame:
    """
    Formats datetime fields, renames columns, and reorders columns of a DataFrame.

    This function first formats the datetime fields 'o_fecha_inicial' and 'o_fecha_final'
    to a specific string format. If output_datetime_format is set as `unix`, then it will format
    'o_fecha_inicial' and 'o_fecha_final' as unix timestamp fotmat (number of seconds since January 1, 1970.).
    Finally it renames and reorders the columns based on the column_rename_map.

    :param df: The DataFrame to be processed.
    :param column_rename_map: A map for renaming columns. Defaults to COLUMN_RENAME_MAP.
    :param output_datetime_format: The format for datetime columns. Defaults to OUTPUT_DATETIME_FORMAT.
    :return: A DataFrame with formatted datetime fields and renamed columns.
    """
    logger.info("Formatting datetime fields, selecting, and renaming columns")

    def convert_datetime(df, datetime_column, output_datetime_format):
        if  output_datetime_format == 'unix':
            return pd.to_datetime(df[datetime_column]).astype('int64') // 10**9
        else:
            return pd.to_datetime(df[datetime_column]).dt.strftime(output_datetime_format)

    # Format datetime fields before renaming
    df['o_fecha_inicial'] = convert_datetime(df, 'o_fecha_inicial', output_datetime_format)
    df['o_fecha_final'] = convert_datetime(df, 'o_fecha_final', output_datetime_format)

    # Rounding the f_distancia values to the nearest integer
    df['f_distancia'] = df['f_distancia'].round(0).astype(int)

    # Adding cello_address in the output.
    column_rename_map['celo_address'] = 'celo_address'

    # Select and rename columns based on column_rename_map
    df = df[[col for col in column_rename_map.keys() if col in df.columns]].rename(columns=column_rename_map)

    return df


def filter_by_distance_range(df: pd.DataFrame, min_dist: float = MINIMUM_DISTANCE, 
                             max_dist: float = None) -> pd.DataFrame:
    """
    Filters a DataFrame based on a specified distance range. If max_dist is not specified,
    then maximum distance filter is not applied.

    :param df: DataFrame to filter.
    :param min_dist: Minimum distance for filtering. Defaults to MINIMUM_DISTANCE.
    :param max_dist: Maximum distance for filtering. Defaults to None.
    :return: A DataFrame filtered by the specified distance range.
    """
    df = df[df['f_distancia'] > float(min_dist)]
    if max_dist:
        df = df[df['f_distancia'] <= float(max_dist)]
    return df


def filter_by_duration_range(df: pd.DataFrame, min_dur: float = MINIMUM_DURATION, 
                             max_dur: float = None) -> pd.DataFrame:
    """
    Filters a DataFrame based on a duration in minutes range. If max_dur is not specified,
    then maximum duration filter is not applied.

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
      Defaults to None.

    Returns:
    - pandas.DataFrame: A filtered DataFrame where the 'durationMinutes' 
      column values fall within the specified duration in minutes range. 
      The 'durationMinutes' column is added to the DataFrame to show the 
      calculated duration for each row.
    """
    df['durationMinutes'] = (df['o_fecha_final'] - df['o_fecha_inicial']).dt.total_seconds() / 60
    df = df[df['durationMinutes'] > float(min_dur)]
    if max_dur:
        df = df[df['durationMinutes'] <= float(max_dur)]
    return df


def fix_distance_by_max_per_hour(df: pd.DataFrame, max_distance_per_hour: float) -> pd.DataFrame:
    """
    Fixes 'f_distancia' in the given DataFrame based on the duration in minutes and the distance in meters.

    This function calculates the duration in minutes between 'o_fecha_final' and 'o_fecha_inicial' timestamps 
    in the DataFrame. It then computes the maximum distance expected based on the duration, using the
    maximum distance per hour (max_distance_per_hour). The 'f_distancia' column is then adjusted to ensure 
    that it does not exceed the maximum expected distance for the calculated duration.

    Parameters:
    - df (pandas.DataFrame): DataFrame with 'o_fecha_final' and 'o_fecha_inicial' timestamp columns.
    - max_distance_per_hour (float): Maximum distance expected in meters per hour.

    Returns:
    - pandas.DataFrame: Modified DataFrame with adjusted 'f_distancia'. 
      Includes 'durationMinutes' and 'maxExpectedDistance' columns for reference.
    """
    df['durationMinutes'] = (df['o_fecha_final'] - df['o_fecha_inicial']).dt.total_seconds() / 60
    df['maxExpectedDistance'] = (df['durationMinutes'] / 60) * max_distance_per_hour
    df.loc[df['f_distancia'] > df['maxExpectedDistance'], 'f_distancia'] = df['maxExpectedDistance']
    return df

def apply_split_routes(df: pd.DataFrame, avg_distance = float, max_distance = float) -> pd.DataFrame:
    pd.set_option('display.max_columns', None)
    logger.info("Splitting routes...")
    print(df.head())

    def adjust_route_distribution(route_distance, max_distance, avg_distance):
        if route_distance > max_distance:
            real_routes = route_distance // avg_distance
            real_route_distance = route_distance / real_routes  # Mantiene la distancia original distribuida equitativamente
        else:
            real_routes = 1
            real_route_distance = route_distance

        # Asegura que la distancia total no exceda la original ajustando la última ruta si es necesario
        total_distributed_distance = real_route_distance * real_routes
        if total_distributed_distance > route_distance:
            # Ajusta la distancia de la última ruta para corregir cualquier excedente
            real_route_distance -= (total_distributed_distance - route_distance) / real_routes

        return pd.Series([real_routes, real_route_distance], index=['real_routes', 'real_f_distancia'])

    # Aplica la función a cada fila
    df[['real_routes', 'real_f_distancia']] = df['f_distancia'].apply(lambda x: adjust_route_distribution(x, max_distance, avg_distance))

    print("'real_routes', 'real_route_distance' added")
    print(df)

    # Realizar el cálculo
    total_real_routes = df['real_routes'].sum()
    original_route_count = len(df)
    extra_routes_added = total_real_routes - original_route_count

    # Formatear y publicar el mensaje del logger
    logger.info(f"About to split {extra_routes_added} extra routes...")



    def expand_routes_based_on_real_routes(df: pd.DataFrame) -> pd.DataFrame:
        # Lista para almacenar las filas replicadas
        rows_list = []
        
        for _, row in df.iterrows():
            # Replicar la fila actual real_routes veces
            for _ in range(int(row['real_routes'])):
                # Crear una copia de la fila actual para modificarla
                new_row = row.copy()
                # Ajustar la columna 'f_distancia' al valor de 'real_f_distancia'
                new_row['f_distancia'] = row['real_f_distancia']
                # Añadir la fila modificada a la lista
                rows_list.append(new_row)
        
        # Crear un nuevo DataFrame a partir de la lista de filas
        new_df = pd.DataFrame(rows_list)
        
        # Restablecer el índice del nuevo DataFrame
        new_df.reset_index(drop=True, inplace=True)
        
        # Devolver el nuevo DataFrame con las filas expandidas
        return new_df


    # Aplicar la función al DataFrame resultante de apply_split_routes
    df_expanded = expand_routes_based_on_real_routes(df)

    # Opcional: puedes querer eliminar las filas originales que ya no son necesarias
    # Esto ya está manejado en la función al crear un nuevo DataFrame con solo las filas necesarias

    # Mostrar el nuevo DataFrame para verificar el resultado
    print(df_expanded.head())

    return df


def add_celo_contract_address(df):
    """
    Adds a 'celo_address' column to the given DataFrame by mapping GPS IDs to Celo addresses.

    This function reads a mapping of GPS IDs to Celo addresses from a YAML file stored in S3. 
    It then adds a new column to the input DataFrame, where each row's 'celo_address' is determined 
    by looking up the corresponding Celo address using the 'k_dispositivo' column as the key in the mapping.

    Parameters:
        df (pandas.DataFrame): A DataFrame containing at least one column named 'k_dispositivo' which holds the GPS IDs.

    Returns:
        pandas.DataFrame: The original DataFrame with an additional 'celo_address' column. Each row in this column 
                          contains the Celo address mapped from the GPS ID found in 'k_dispositivo'.

    Notes:
        - The function assumes the presence of a YAML file in the S3 bucket, which contains the mapping of GPS IDs to Celo addresses.
        - It logs the process of fetching the celo_address_map for monitoring and debugging purposes.
        - The function will not modify other existing columns in the DataFrame.
    """
    logger.info("Fetching celo_address_map...")
    gps_to_celo_address_map_path = os.path.join(RODAAPP_BUCKET_PREFIX, "roda_metadata", "gps_to_celo_address_map.yaml")
    celo_address_map = read_yaml_from_s3(gps_to_celo_address_map_path)
    
    # Get Celo Address for every gpsID
    df['celo_address'] = df['k_dispositivo'].map(celo_address_map)

    return df


def format_datetime_column(df: pd.DataFrame, dt_column: str, 
                           input_datetime_format: str = INPUT_DATETIME_FORMAT) -> None:
    """
    Converts and formats a datetime column in a DataFrame.

    Parameters:
    df (pandas.DataFrame): The DataFrame containing the datetime column to be formatted.
    dt_column (str): The name of the column to format.

    Returns:
    None: The function modifies the DataFrame in place, converting the datetime column
          to a specified format.
    """
    df[dt_column] = pd.to_datetime(df[dt_column], format=input_datetime_format)


def get_missing_celo_addresses(df):
    """
    Filters and returns rows from the input DataFrame where the 'celo_address' is missing.
    
    This function creates and returns a new DataFrame consisting only of rows from the input DataFrame 
    where the 'celo_address' column is missing.

    Parameters:
        df (pandas.DataFrame): The input DataFrame with a 'celo_address' column.

    Returns:
        pandas.DataFrame: A new DataFrame containing only the rows where 'celo_address' is missing.
    """
    # Create a new DataFrame with rows where 'celo_address' is missing
    missing_celo_df = df[df['celo_address'].isnull()]

    return missing_celo_df


def filter_out_known_unassigned_devices(main_df: pd.DataFrame, known_unassigned_device_list: list) -> pd.DataFrame:
    """
    Filters out rows from a DataFrame where the 'k_dispositivo' value is in a provided list of known unassigned devices.

    This function is used to remove rows from the DataFrame based on the criteria that the device identifiers ('k_dispositivo')
    are known to be unassigned and are thus irrelevant for certain analyses or operations.

    Parameters:
        main_df (pd.DataFrame): The input DataFrame containing device data.
        known_unassigned_device_list (list): A list of device identifiers that are known to be unassigned.

    Returns:
        pd.DataFrame: A DataFrame after excluding rows with 'k_dispositivo' present in the known unassigned device list.
    """
    # Exclude rows where 'k_dispositivo' is in the known unassigned device list
    return main_df[~main_df['k_dispositivo'].isin(known_unassigned_device_list)]



def get_known_unassigned_devices(routes_missing_celo: pd.DataFrame) -> list:
    """
    Fetches and filters a list of known unassigned devices that are currently missing a Celo address.

    This function reads a list of known unassigned device identifiers from a YAML file stored in S3. It then filters
    this list to include only those devices that are also present in the input DataFrame and are missing a Celo address.
    This is to ensure that the list is up-to-date and reflects any recent assignments of devices to clients.

    Parameters:
        routes_missing_celo (pd.DataFrame): A DataFrame with device data, specifically missing Celo addresses.

    Returns:
        list: A list of device identifiers (from the known unassigned list) that are also missing a Celo address.
    """
    logger.info("Fetching known_unassigned_device_list...")
    known_unassigned_device_list_path = os.path.join(RODAAPP_BUCKET_PREFIX, "tribu_metadata", "tribu_known_unassigned_divices.yaml")
    known_unassigned_device_list = read_yaml_from_s3(known_unassigned_device_list_path)

    # Filter the known unassigned device list to include only those devices that are also missing a Celo address
    missing_celo_address_device_list = routes_missing_celo['k_dispositivo'].unique().tolist()
    known_unassigned_device_list = [gps_id for gps_id in known_unassigned_device_list if gps_id in missing_celo_address_device_list]

    return known_unassigned_device_list



def handler(event: Dict[str, Any], context: Any) -> None:
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

    # format datetime on input data in order to make it easier to do datetime operations
    format_datetime_column(df, "o_fecha_inicial", input_datetime_format)
    format_datetime_column(df, "o_fecha_final", input_datetime_format)

    if "distance_filter" in trans_params:
        distance_filter = trans_params["distance_filter"]
        df = filter_by_distance_range(df, distance_filter["min"], distance_filter.get("max"))

    if "duration_filter" in trans_params:
        duration_filter = trans_params["duration_filter"]
        df = filter_by_duration_range(df, duration_filter["min"], duration_filter.get("max"))

    if "distance_fix" in trans_params:
        distance_fix = trans_params["distance_fix"]
        df = fix_distance_by_max_per_hour(df, distance_fix["expected_max_per_hour"])

    if "split_big_routes" in trans_params:
        split_big_routes = trans_params["split_big_routes"]
        df = apply_split_routes(df, split_big_routes["avg_distance"], split_big_routes["max_distance"])

    # Add Celo contract addresses to the DataFrame
    df = add_celo_contract_address(df)

    # Filter the DataFrame to get only routes that are missing a Celo address
    routes_missing_celo = get_missing_celo_addresses(df)

    # Fetch a list of known unassigned devices that currently lack a Celo address
    known_unassigned_devices_list = get_known_unassigned_devices(routes_missing_celo)

    # Remove the known unassigned devices from the list of routes missing a Celo address
    routes_missing_celo = filter_out_known_unassigned_devices(routes_missing_celo, known_unassigned_devices_list)

    # Check if there are any remaining GPS devices without an associated client in Airtable
    if not routes_missing_celo.empty:
        # Extract the list of devices still missing a Celo address
        devices_missing_celo = routes_missing_celo['k_dispositivo'].unique().tolist()
        
        # Raise an exception with a message listing these devices, prompting for a fix
        raise Exception("There are GPS devices not associated to a client in Airtable.\n    "
                        f"* Please fix and retry following list of devices: {', '.join(devices_missing_celo)}")

    # Final filter to remove routes associated with devices that are known to be unassigned and lack a Celo address
    # This step ensures that the final dataset does not include routes without a valid Celo address
    df = filter_out_known_unassigned_devices(df, known_unassigned_devices_list)


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