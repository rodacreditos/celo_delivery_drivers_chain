"""
Tribu Data Processing Script

This script is developed for processing GPS data obtained from Tribu. It provides functionalities 
to read, filter, format, adjust GPS coordinates, and export GPS data. The script processes data from 
a CSV file, applies filters based on distance criteria, formats datetime fields, adjusts GPS coordinates 
to standard precision, and exports the processed data into a new CSV file with renamed columns according 
to a predefined mapping.

Key Features:
- Read data from a CSV file.
- Filter records based on distance.
- Format datetime fields to a specific format.
- Adjust GPS coordinates to standard format and create location pairs.
- Rename and reorder DataFrame columns according to a predefined mapping.
- Export processed data to a new CSV file.

Usage:
    Run the script with command-line arguments specifying the input and output file paths.
    Example: python tribu_data_processing.py -i input.csv -o output.csv

Arguments:
    -i --input: Specifies the path to the input CSV file.
    -o --output: Specifies the path for the output CSV file.

COLUMN_RENAME_MAP:
    A dictionary defining the new column names for the output CSV file. It maps original column names 
    to their respective new names. The function 'write_to_local_csv' uses this mapping to rename and 
    reorder columns in the output file.

Future Development:
    Planned adaptation into AWS Lambda functions for a more automated and scalable 
    approach to handling Tribu GPS data.

Note:
    The script is in a testing phase for initial data transformation exploration and 
    will be modified for production use in the future.
"""
import pandas as pd
import argparse


MAXIMUM_DISTANCE = 1000
MINIMUM_DISTANCE = 10
COLUMN_RENAME_MAP = {
    "Dispositivo": "gpsID",
    "Fecha Inicio": "timestampStart",
    "Fecha Fin": "timestampEnd",
    "Distancia": "measuredDistance",
    "startLocation": "startLocation",
    "endLocation": "endLocation",
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
    Write a DataFrame to a CSV file after renaming its columns.

    This function renames the columns of the input DataFrame according to the 
    COLUMN_RENAME_MAP dictionary and writes the resulting DataFrame to a CSV file at 
    the specified output path. The order of the columns in the output CSV will follow 
    the order they are defined in COLUMN_RENAME_MAP.

    Parameters:
    df (pandas.DataFrame): The DataFrame to be processed.
    output_path (str): The file path where the CSV file will be saved.

    Returns:
    None
    """
    # Rename columns and reorder according to COLUMN_RENAME_MAP
    df = df[list(COLUMN_RENAME_MAP.keys())].rename(columns=COLUMN_RENAME_MAP)
    
    # Write to CSV without index
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
    parser.add_argument("-i", "--input", help="Input path to the CSV file to be parsed", required=True)
    parser.add_argument("-o", "--output", help="Output path of the results of this script", required=True)
    args = parser.parse_args()
    main(args)