"""
Tribu Data Processing Script

This script is designed for loading, processing, and exporting GPS data sourced from Tribu. 
It includes functionalities for reading data from a CSV file, filtering based on distance criteria, 
formatting datetime fields, and finally exporting the processed data into a new CSV file. The script 
is structured to serve as a preliminary test for data transformation processes. The ultimate goal is 
to develop this script into a Python Lambda function that will handle data extraction from the Tribu API, 
followed by another Lambda function for data transformation.

The script uses Pandas for data manipulation and supports command-line arguments for specifying input and 
output file paths.

Example usage:
    python tribu_data_processing.py -p input.csv -o output.csv

Where:
    -p --path      specifies the path to the input CSV file.
    -o --output    specifies the path for the output CSV file.

Features:
    - Read data from a CSV file into a Pandas DataFrame.
    - Filter records based on a specified distance range.
    - Format datetime columns from one format to another.
    - Export the processed data to a new CSV file.

Future Development:
    This script is intended to be adapted into AWS Lambda functions for a more automated and scalable 
    approach to handling Tribu GPS data.

Note:
    This script is currently in a testing phase and is designed for initial data transformation 
    exploration. It will be modified and expanded for production use in the future.
"""
import pandas as pd
import argparse


MAXIMUM_DISTANCE = 1000
MINIMUM_DISTANCE = 10
OUTPUT_COLUMN_MAP = {
    "Dispositivo": "gpsID",
    "Fecha Inicio": "timestampStart",
    "Fecha Fin": "timestampEnd",
    "Distancia": "measuredDistance",
}
INPUT_DATETIME_FORMAT = "%m/%d/%Y %H:%M"
OUTPUT_DATETIME_FORMAT = "%Y-%m-%d %H:%M"
PRECISION_DIGITS_FOR_GPS_LOCATION = 8


def get_data_from_csv(path):
    """
    Read data from a CSV file into a pandas DataFrame.

    Parameters:
    path (str): The file path of the CSV file to be read.

    Returns:
    pandas.DataFrame: A DataFrame containing the data from the CSV file.
    """
    return pd.read_csv(path)


def filter_by_distance_range(df, min_dist=MINIMUM_DISTANCE, max_dist=MAXIMUM_DISTANCE):
    """
    Filter a DataFrame based on a distance range.

    Parameters:
    df (pandas.DataFrame): The DataFrame to filter.
    min_dist (float): The minimum distance for filtering. Defaults to MINIMUM_DISTANCE.
    max_dist (float): The maximum distance for filtering. Defaults to MAXIMUM_DISTANCE.

    Returns:
    pandas.DataFrame: A filtered DataFrame where the 'Distancia' column values 
                      fall within the specified distance range.
    """
    return df[df['Distancia'] > min_dist & df['Distancia'] <= max_dist]


def format_datetime_column(df, dt_column):
    """
    Convert and format a datetime column in a DataFrame.

    Parameters:
    df (pandas.DataFrame): The DataFrame containing the datetime column to be formatted.
    dt_column (str): The name of the column to format.

    Returns:
    None: The function modifies the DataFrame in place, converting the datetime column
          to a specified format.
    """
    df[dt_column] = pd.to_datetime(df[dt_column], format=INPUT_DATETIME_FORMAT)
    df[dt_column] = df[dt_column].dt.strftime(OUTPUT_DATETIME_FORMAT)


def write_to_local_csv(df, output_path):
    """
    Write a DataFrame to a CSV file.

    Parameters:
    df (pandas.DataFrame): The DataFrame to write to a CSV file.
    output_path (str): The file path where the CSV file will be saved.

    Returns:
    None: A CSV file is created at the specified path with the DataFrame's data.
    """
    df.to_csv(output_path, index=False)


def fix_gps_coordinates(df):
    """
    Adjusts GPS coordinates in the DataFrame for standard geospatial precision and creates new columns for start and end locations.

    This function corrects the longitude and latitude columns ('Lng. Inicial', 'Lat. Inicial', 
    'Lng. Final', 'Lat. Final') by dividing their values by 10 raised to the power of 
    PRECISION_DIGITS_FOR_GPS_LOCATION. This adjustment is necessary to introduce decimal points 
    into the coordinates, converting them into a standard GPS coordinate format. Subsequently, 
    it creates two new columns ('startLocation' and 'endLocation'), each containing a tuple 
    of longitude and latitude representing the start and end locations, respectively.

    Parameters:
    df (pandas.DataFrame): The DataFrame containing the initial and final GPS data with 
                           integer-based longitude and latitude values.

    Returns:
    None: The function modifies the DataFrame in place, adding corrected coordinate columns 
          and new location pair columns.

    Note:
    The constant PRECISION_DIGITS_FOR_GPS_LOCATION is used to define the level of decimal 
    precision for the GPS coordinates.
    """
    df['Lng. Inicial'] = df['Lng. Inicial'] / 10**PRECISION_DIGITS_FOR_GPS_LOCATION
    df['Lat. Inicial'] = df['Lat. Inicial'] / 10**PRECISION_DIGITS_FOR_GPS_LOCATION
    df['Lng. Final'] = df['Lng. Final'] / 10**PRECISION_DIGITS_FOR_GPS_LOCATION
    df['Lat. Final'] = df['Lat. Final'] / 10**PRECISION_DIGITS_FOR_GPS_LOCATION
    df['startLocation'] = df.apply(lambda row: (row['Lng. Inicial'], row['Lat. Inicial']), axis=1)
    df['endLocation'] = df.apply(lambda row: (row['Lng. Final'], row['Lat. Final']), axis=1)


def main(args):
    df = get_data_from_csv(args.path)
    df = filter_by_distance_range(df)
    format_datetime_column(df, "Fecha Inicio")
    format_datetime_column(df, "Fecha Fin")
    fix_gps_coordinates(df)
    write_to_local_csv(df, args.out)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-p", "--path", help="Path to the CSV file to be parsed", required=True)
    parser.add_argument("-o", "--output", help="Output path of the results of this script", required=True)
    args = parser.parse_args()
    main(args)