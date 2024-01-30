from web3 import Web3
import datetime
import json
import os
from web3 import Web3, HTTPProvider, Account
from web3.middleware import geth_poa_middleware
from python_utilities.utils import validate_date, read_csv_from_s3, read_yaml_from_s3, read_json_from_s3, format_dashed_date, yesterday, logger, \
    				setup_local_logger, list_s3_files, RODAAPP_BUCKET_PREFIX

if __name__ == "__main__":
    provider_url = "https://alfajores-forno.celo-testnet.org"
    celo_contracts = read_json_from_s3(os.path.join(RODAAPP_BUCKET_PREFIX, f"credentials/roda_celo_contracts_staging.json"))
    contract_address = celo_contracts['RODA_ROUTE_CONTRACT_ADDR']
    contract_abi = celo_contracts['RODA_ROUTE_CONTRACT_ABI']


    # Conectar con el nodo de Celo Alfajores
    w3 = Web3(HTTPProvider(provider_url))


    # Crear una instancia del contrato
    contract = w3.eth.contract(address=contract_address, abi=contract_abi)

    # Filtrar eventos RouteRecorded desde el bloque de despliegue hasta el último bloque
    events = contract.events.RouteRecorded.create_filter(fromBlock=0, toBlock='latest').get_all_entries()
    
    # Extraer y agrupar por fecha
    routes_per_day = {}
    for event in events:
        timestamp = event['args']['timestampStart']
        date = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
        routes_per_day[date] = routes_per_day.get(date, 0) + 1

    for date, routes in routes_per_day.items():
        print(date, routes)
