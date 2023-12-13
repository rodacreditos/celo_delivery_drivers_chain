"""
Script for processing tribu data
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


def get_data_from_csv(path):
    return pd.read_csv(path)


def filter_by_distance_range(df, min_dist=MINIMUM_DISTANCE, max_dist=MAXIMUM_DISTANCE):
    return df[df['Distancia'] > min_dist & df['Distancia'] <= max_dist]


def format_datetime_column(df, dt_column):
    df[dt_column] = pd.to_datetime(df[dt_column], format=INPUT_DATETIME_FORMAT)
    df[dt_column] = df[dt_column].dt.strftime(OUTPUT_DATETIME_FORMAT)


def write_to_local_csv(df, output_path):
    df.to_csv(output_path, index=False)


def main(args):
    df = get_data_from_csv(args.path)
    df = filter_by_distance_range(df)
    format_datetime_column(df, "Fecha Inicio")
    format_datetime_column(df, "Fecha Fin")
    write_to_local_csv(df, args.out)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-p", "--path", "path to the CSV file to be parsed", required=True)
    parser.add_argument("-o", "--output", "output path of the results of this script", required=True)
    args = parser.parse_args()
    main(args)