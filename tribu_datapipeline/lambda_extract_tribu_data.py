"""
This script connects to Tribu API and gets the GPS data of the rappi driver bicycles, from yesterday.
"""
import requests
import argparse
from utils import dicts_to_csv


# Tribu API endpoint
TRIBU_URL = "https://tribugps.com/controlador.php"

def login():
    form_data = {
        "tipo": "usuario",
        "funcion": "login",
        "user": "901405927",
        "password": "haztuparada",
        "isAdmin": "true"
    }

    response = requests.post(TRIBU_URL, data=form_data)

    if response.status_code == 200:
        response_json = response.json()
        token = response_json.get('body', {}).get('o_token')
        return token
    else:
        raise Exception("\t".join(["Error:", response.status_code, response.text]))


def get_tribu_data(token):
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




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-o", "--output", help="Output path of the results of this script", required=True)
    args = parser.parse_args()

    tribu_token = login()
    tribu_data = get_tribu_data(tribu_token)
    
    dicts_to_csv(tribu_data, args.output)