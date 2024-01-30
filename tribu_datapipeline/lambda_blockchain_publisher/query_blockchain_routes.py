"""
This module is a prototype script for interacting with the Celo Alfajores blockchain. Its primary functions include:
- Connecting to the Celo Alfajores network.
- Reading recorded route events from a specified contract.
- Counting and printing the number of route events per date, derived from the `timestampStart` of each event.
- The script is designed to be extendable for interaction with contracts on the mainnet.
- Future enhancements include filtering events within a specific time range, a functionality that is currently under exploration.

Note: The script requires the `web3` library for blockchain interaction and a custom utility module for reading JSON data from an S3 bucket.
"""

import datetime
import os
from web3 import Web3, HTTPProvider
from python_utilities.utils import read_json_from_s3, RODAAPP_BUCKET_PREFIX

if __name__ == "__main__":
    # Connect to the Celo Alfajores node
    provider_url = "https://alfajores-forno.celo-testnet.org"
    celo_contracts = read_json_from_s3(os.path.join(RODAAPP_BUCKET_PREFIX, f"credentials/roda_celo_contracts_staging.json"))
    contract_address = celo_contracts['RODA_ROUTE_CONTRACT_ADDR']
    contract_abi = celo_contracts['RODA_ROUTE_CONTRACT_ABI']

    w3 = Web3(HTTPProvider(provider_url))

    # Create an instance of the contract
    contract = w3.eth.contract(address=contract_address, abi=contract_abi)

    # Filter RouteRecorded events from the deployment block to the latest block
    events = contract.events.RouteRecorded.create_filter(fromBlock=0, toBlock='latest').get_all_entries()
    
    # Extract and group by date
    routes_per_day = {}
    for event in events:
        timestamp = event['args']['timestampStart']
        date = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
        routes_per_day[date] = routes_per_day.get(date, 0) + 1

    # Print the number of routes per date
    for date, routes in routes_per_day.items():
        print(date, routes)
