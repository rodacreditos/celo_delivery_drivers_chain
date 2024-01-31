"""
This script helps to publish data stored in S3, that were previously processed, to a blockchain platform. The script 
fetches roda celo credentials and the address for the Route contract from S3, downloads the relevant dataset,
and uploads routes as NFTs to a blockchain platform. Designed for deployment in a Docker container, it's suitable for
execution in an AWS Lambda function and supports local testing.

The script can be executed in various environments:
1. staging (Default): It will use Celo Alfajores Testnet for publishing the routes.
2. production: It will send routes to Celo mainnet.

The script supports command-line arguments for easy local testing and debugging. It leverages functionality 
from an accompanying 'utils.py' module for tasks like data processing and AWS S3 interactions.

Environment Variables:
- AWS_LAMBDA_RUNTIME_API: Used to determine if the script is running in an AWS Lambda environment.

Usage:
- AWS Lambda: Deploy the script as a Lambda function. The handler function will be invoked with event and context parameters.
- Docker Container/CLI: Run the script with optional command-line arguments to specify the environment and processing date.

Command-Line Arguments:
- --date (-d): Optional. Specify the date for data retrieval in 'YYYY-MM-DD' format. If not provided, defaults to yesterday's date.
- --environment (-e): Required. Specify the environment. Accepts 'staging' or 'production'.

Examples:
- CLI: python lambda_blockchain_publish.py --date 2023-12-01 --environment staging
- Docker: docker run --rm \
		-v ~/.aws:/root/.aws \
		-v $(shell pwd):/var/task \
		-i --entrypoint python rodaapp:tribu_processing \
		lambda_blockchain_publish.py --environment staging --date 2023-12-01

Output:
- The script fetches data processed from Tribu on S3 and publishes it to a blockchain platform like Celo.

Note:
- The script requires access to AWS S3 for fetching parameters, and reading input data from tribu.
- The script also requires access to the blockchain platform to publish routes.
"""
import argparse
import logging
import os
import time
from typing import Dict, Any
from web3 import Web3, HTTPProvider, Account
from web3.middleware import geth_poa_middleware
from python_utilities.utils import validate_date, read_csv_from_s3, read_yaml_from_s3, read_json_from_s3, format_dashed_date, yesterday, logger, \
    				setup_local_logger, list_s3_files, RODAAPP_BUCKET_PREFIX


def fetch_celo_credentials(environment: str):
    celo_credentials = read_yaml_from_s3(os.path.join(RODAAPP_BUCKET_PREFIX, "credentials/roda_celo_credentials.yaml"))
    celo_alfajores_rpc_url = "https://alfajores-forno.celo-testnet.org"
    provider_url = celo_credentials['PROVIDER_URL'] if environment == "production" else celo_alfajores_rpc_url
    return celo_credentials['MNEMONIC'], provider_url


def fetch_contract_info(environment: str):
    celo_contracts = read_json_from_s3(os.path.join(RODAAPP_BUCKET_PREFIX, f"credentials/roda_celo_contracts_{environment}.json"))
    return celo_contracts['RODA_ROUTE_CONTRACT_ADDR'], celo_contracts['RODA_ROUTE_CONTRACT_ABI']


def connect_to_blockchain(provider_url: str):
    web3 = Web3(HTTPProvider(provider_url))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    return web3


def wait_for_transaction_receipt(web3, tx_hash, poll_interval=10, timeout=600):
    """
    Waits for the transaction to be mined and gets the transaction receipt, with a timeout.

    :param web3: Web3 instance connected to the Celo network.
    :param tx_hash: The hash of the transaction to monitor.
    :param poll_interval: Time in seconds between checks.
    :param timeout: Time in seconds to wait before giving up.

    :return: The transaction receipt, or None if timed out.
    """
    logger.info(f"    -> Waiting for transaction to be mined (tx hash: {tx_hash.hex()})")
    start_time = time.time()

    while True:
        if time.time() - start_time > timeout:
            logger.warning(f"    -> Transaction receipt timeout for tx hash: {tx_hash.hex()}")
            return None
        tx_receipt = web3.eth.get_transaction_receipt(tx_hash)
        if tx_receipt:
            return tx_receipt
        time.sleep(poll_interval)


def publish_to_celo(web3, contract_address, abi, data, mnemonic):
    """
    Publishes transactions to the Celo blockchain, stops if any transaction fails.

    :param web3: Web3 instance connected to the Celo network.
    :param contract_address: The address of the smart contract on Celo.
    :param abi: The ABI of the contract.
    :param data: The data to be published to the blockchain.
    :param mnemonic: The mnemonic for the wallet.

    :return: A dictionary of published routes with transaction details and status.
    """
    logger.info(f"About to publish {len(data)} transactions...")
    contract = web3.eth.contract(address=contract_address, abi=abi)


    # Enable unaudited HD wallet features in order to allow using the mnemonic features
    Account.enable_unaudited_hdwallet_features()

    # Derive the account from the mnemonic
    account = Account.from_mnemonic(mnemonic)
    nonce = web3.eth.get_transaction_count(account.address)

    published_routes = {}
    all_success = True
    
    # Iterate over the data and publish each row to Celo
    for route in data:
        try:
            route_id = route['routeID']
            timestamp_start = route['timestampStart']
            timestamp_end = route['timestampEnd']
            measured_distance = route['measuredDistance']
            celo_address = route['celo_address']

            # Estimate gas for the transaction
            estimated_gas = contract.functions.recordRoute(
                                to=celo_address,
                                routeId=int(route_id),
                                _timestampStart=int(timestamp_start),
                                _timestampEnd=int(timestamp_end),
                                _distance=int(measured_distance)
                            ).estimate_gas({'from': account.address})

            gas_price = web3.eth.gas_price

            tx = contract.functions.recordRoute(
                to=celo_address,
                routeId=int(route_id),
                _timestampStart=int(timestamp_start),
                _timestampEnd=int(timestamp_end),
                _distance=int(measured_distance)
            ).build_transaction({
                'from': account.address,
                'nonce': nonce,
                'gas': estimated_gas + 100000,  # extra margin for gas
                'gasPrice': gas_price
            })

            # Sign the transaction
            signed_tx = account.sign_transaction(tx)
            tx_hash = Web3.keccak(signed_tx.rawTransaction)
            logger.info(f"Publishing route id {route['routeID']}, with: nonce = {nonce}, gas_price = {gas_price}, and tx_hash = {tx_hash.hex()}")

            # Send the transaction
            tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
            logger.info(f"    -> Sent transaction for route id {route_id}, awaiting receipt...")

            # Wait until transaction is successfully receipt
            tx_receipt = wait_for_transaction_receipt(web3, tx_hash)

            if not tx_receipt:
                logger.error(f"    -> Failed to get receipt for route id {route_id}. Stopping further transactions.")
                all_success = False
                break

            logger.info(f"    -> Transaction successfully sent: route id {route['routeID']}, hash {tx_hash.hex()}")
            published_routes[route_id] = {
                "nonce": nonce,
                "gas_price": gas_price,
                "tx_hash": tx_hash.hex()
            }

            # Increment the nonce for subsequent transactions
            nonce += 1

        except Exception as e:
            logger.error(f"    -> Error publishing route id {route['routeID']}: {e}")
            all_success = False
            break

    return all_success, published_routes

def handler(event: Dict[str, Any], context: Any) -> None:
    """
    Handler function for processing Tribu data.

    Intended for use as the entry point in AWS Lambda, but also supports local execution.
    The 'environment' in the event determines whether the data is to be publish to a TestNet or
    to a production MainNet.

    :param event: A dictionary containing 'environment' and optionally 'processing_date'.
                  If 'processing_date' is not provided, defaults to yesterday's date.
    :param context: Context information provided by AWS Lambda (unused in this function).
    """
    logger.setLevel(logging.INFO)
    logger.info("STARTING: Blockchain Publisher task.")
    processing_date = event.get("processing_date")
    processing_date = validate_date(processing_date) if processing_date else yesterday()
    environment = event.get("environment", "staging")

    logger.info(f"Parameters: environment: {environment}, processing date: {processing_date}")

    mnemonic, provider_url = fetch_celo_credentials(environment)
    roda_route_contract_addr, roda_route_contract_abi = fetch_contract_info(environment)
    web3 = connect_to_blockchain(provider_url)

    logger.info('Reading CSV data:')
    csv_file_keys = list_s3_files(os.path.join(RODAAPP_BUCKET_PREFIX, f"rappi_driver_routes/date={format_dashed_date(processing_date)}/"))
    csv_data = []
    for key in csv_file_keys:
        logger.info(f"    -> reading {key}")
        for row in read_csv_from_s3(os.path.join(RODAAPP_BUCKET_PREFIX, key)):
            csv_data.append(row)

    publish_to_celo(web3, roda_route_contract_addr, roda_route_contract_abi, csv_data, mnemonic)


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
        parser.add_argument("-e", "--environment", help="Given the environment (staging or production)", choices=['staging', 'production'], required=True)
        
        args = parser.parse_args()
        setup_local_logger() # when it does not have env vars from aws, it means that this script is running locally 
        if args.date:
            handler(dict(processing_date=format_dashed_date(args.date),
                            environment=args.environment), "dockerlocal")
        else:
            handler(dict(environment=args.environment), "dockerlocal")