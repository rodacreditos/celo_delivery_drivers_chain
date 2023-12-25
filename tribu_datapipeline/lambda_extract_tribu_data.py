"""
This script connects to Tribu API and gets the GPS data of the rappi driver bicycles, from especified date.
Given the dataset type, this script will search for the credentials of tribu api in order to download the right dataset
"""
import requests
import argparse
import logging
import os
from utils import dicts_to_csv, validate_date, read_json_from_s3, logger, RODAAPP_BUCKET_PREFIX


# Tribu API endpoint
TRIBU_URL = "https://tribugps.com/controlador.php"

def login(dataset_type):
    logger.info(f"Downloading tribu {dataset_type} credentials")
    tribu_credentials = os.path.join(RODAAPP_BUCKET_PREFIX, f"tribu_{dataset_type}_credentials.json")
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


def get_tribu_data(token, date):
    logger.info(f"Downloading routes from tribu API")
    form_data = {
        "tipo": "ruta",
        "funcion": "verRutasSubAdmin",
        "d_fechaIni": "2023-12-18",
        "d_fechaFin": "2023-12-19"
    }

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.post(TRIBU_URL, data=form_data, headers=headers)

    if response.status_code == 200:
        return response.json()['body']
    else:
        raise Exception("\t".join(["Error:", response.status_code, response.text]))


def handler(event, context):
    tribu_token = login()
    tribu_data = get_tribu_data(tribu_token)
    dicts_to_csv(tribu_data, event["output"])


if __name__ == "__main__":
    if 'AWS_LAMBDA_RUNTIME_API' in os.environ:
        # Running in AWS Lambda environment
        from awslambdaric import bootstrap
        logger.setLevel(logging.INFO)
        bootstrap.run(handler, '/var/runtime/bootstrap')
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("-d", "--date", help="date of the execution of this script", type=validate_date, required=True)
        parser.add_argument("-t", "--dataset-type", help="Given the dataset type (roda or guajira)", choices=['guajira', 'roda'], required=True)
        
        args = parser.parse_args()
        logger.setLevel(logging.DEBUG)
        handler(dict(date=args.date, dataset_type=args.dataset_type), "dockerlocal")
