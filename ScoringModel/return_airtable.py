import os
import argparse
import logging
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from python_utilities.utils import read_yaml_from_s3, read_from_s3, RODAAPP_BUCKET_PREFIX, logger, setup_local_logger, yesterday
from api_airtable import return_column_airtable

airtable_credentials_path = os.path.join(RODAAPP_BUCKET_PREFIX, "credentials", "roda_airtable_credentials.yaml")
airtable_credentials = read_yaml_from_s3(airtable_credentials_path)

base_key = airtable_credentials['BASE_ID']
personal_access_token = airtable_credentials['PERSONAL_ACCESS_TOKEN']

# def yesterday():
#    """
#    Returns the date of the day before today, formatted as 'YYYY-MM-DD'.
#    """
#    return (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

def read_csv_into_pandas_from_s3(s3_path: str) -> pd.DataFrame:
    """
    Read a csv file from S3 and return its content into a pandas dataframe.

    :param s3_path: The S3 path to the csv file, in the format 's3://bucket_name/key'.
    :return: The parsed csv data.
    """
    logger.info(f"Fetching tribu routes data from {s3_path}")
    csv_string = read_from_s3(s3_path)
    return pd.read_csv(BytesIO(csv_string.encode()))


def handler(event, context):
    logger.setLevel(logging.INFO)
    logger.info("Starting Airtable update...")

    try:
        # Configura los parámetros del entorno
        airtable_credentials_path = os.path.join(RODAAPP_BUCKET_PREFIX, "credentials", "roda_airtable_credentials.yaml")
        airtable_credentials = read_yaml_from_s3(airtable_credentials_path)
        base_key = airtable_credentials['BASE_ID']
        personal_access_token = airtable_credentials['PERSONAL_ACCESS_TOKEN']
        # Determina la fecha para leer el archivo
        processing_date_str = event.get("date", yesterday().strftime("%Y-%m-%d"))

        print(f"processing_date_str: {processing_date_str}")
        input_path = os.path.join(RODAAPP_BUCKET_PREFIX, "daily_scoring", f"date_{processing_date_str}_scores.csv")
        
        print(f"input_path: {input_path}")
        # Lee el archivo CSV como DataFrame
        df_contactos_procesados = read_csv_into_pandas_from_s3(input_path)
        
        # Actualiza Airtable (ajusta esta llamada según tus necesidades)
        return_column_airtable('Contactos', personal_access_token, base_key, 'Info_Referidos','Puntaje_Final_Ajustado', df_contactos_procesados)

        logger.info("Airtable update completed successfully.")
        return {
            'statusCode': 200,
            'body': 'Airtable updated successfully.'
        }
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        return {
            'statusCode': 500,
            'body': 'Error during processing.'
        }


if __name__ == "__main__":
    # Determina el entorno de ejecución
    if 'AWS_LAMBDA_RUNTIME_API' in os.environ:
        # Estamos ejecutando en el entorno de AWS Lambda
        from awslambdaric import bootstrap
        bootstrap.run(handler, '/var/runtime/bootstrap')
    else:
        # Estamos ejecutando localmente o en otro entorno fuera de AWS Lambda
        parser = argparse.ArgumentParser(description="Return scores to Airtable")
        parser.add_argument("-e", "--environment", help="El entorno de ejecución (staging o production)", choices=['staging', 'production'], required=False, default="staging")
        parser.add_argument("-d", "--date", help="Date of the execution of this script in YYYY-MM-DD format", required=False, default=yesterday().strftime("%Y-%m-%d"), type=str)

        args = parser.parse_args()
        setup_local_logger() # Configura un logger para ejecución local si es necesario
        # Simula el evento y el contexto que AWS Lambda pasaría a tu función 'handler'
        event = {"environment": args.environment, "date":args.date}
        context = "LocalExecution"  # Puedes proporcionar un objeto de contexto más detallado si es necesario
        handler(event, context)