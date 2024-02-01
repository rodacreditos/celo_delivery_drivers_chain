"""
This Python script automates the publication of processed data from AWS S3 to the Celo blockchain platform, efficiently managing data as NFTs. It is designed with
robustness in mind, incorporating features that ensure reliability and effectiveness in various execution environments, including AWS Lambda, Docker containers, and
local execution setups.

Key Features:
- Environment Flexibility: Supports both staging (Celo Alfajores Testnet) and production (Celo Mainnet) environments, allowing for flexible deployment and testing.
- Progress Tracking: Saves current progress to S3, enabling the script to resume from where it left off in subsequent executions, thereby avoiding the republishing of routes.
- Timeout Management: Monitors execution time against a specified timeout, ensuring the script halts before reaching the limit. This feature is crucial for operations
  within AWS Lambda, where execution time is capped.
- Transaction Confirmation: Waits for blockchain transaction confirmations before proceeding, enhancing the reliability of the NFT publishing process.

Usage:
The script supports various execution modes, detailed as follows:
- AWS Lambda: Deploy and execute as a Lambda function, where it processes data based on event triggers.
- Docker/CLI: Run the script within a Docker container or directly from the command line, utilizing arguments to specify operational parameters.

Command-Line Arguments:
- `--date` (`-d`): Specifies the date for data retrieval and processing. Defaults to yesterday's date if not provided.
- `--environment` (`-e`): Determines the execution environment ('staging' or 'production'). Required.
- `--timeout` (`-t`): Sets the maximum execution time in seconds, ensuring the script concludes gracefully before reaching this limit. Optional, with a default of 900 seconds.

Execution Examples:
- CLI: `python lambda_blockchain_publish.py --date 2023-12-01 --environment staging`
- Docker: 
    ```bash
    docker run --rm \
        -v ~/.aws:/root/.aws \
        -v $(pwd):/var/task \
        -i --entrypoint python rodaapp:tribu_processing \
        lambda_blockchain_publish.py --environment staging --date 2023-12-01
    ```

Dependencies:
- AWS S3 for data storage and retrieval.
- Celo blockchain for publishing routes as NFTs.
- Various Python libraries including `boto3`, `web3`, and utilities from an accompanying 'utils.py' module.

Note:
This script is part of a larger system designed for processing and publishing route data to the blockchain. It ensures data integrity and efficient processing by
leveraging cloud storage and blockchain technologies.
"""
import argparse
import logging
import os
import time
from typing import Dict, Any
from web3 import Web3, HTTPProvider, Account
from web3.middleware import geth_poa_middleware
from botocore.exceptions import ClientError
from python_utilities.utils import validate_date, read_csv_from_s3, read_yaml_from_s3, read_json_from_s3, format_dashed_date, yesterday, logger, \
    				setup_local_logger, list_s3_files, dict_to_json_s3, RODAAPP_BUCKET_PREFIX


def fetch_celo_credentials(environment: str):
    """
    Fetches the Celo network credentials from S3 based on the specified environment.

    Retrieves the mnemonic and provider URL for either the production or staging environment. The staging
    environment defaults to using the Celo Alfajores Testnet.

    Parameters:
    - environment (str): Specifies the environment ('production' or 'staging') to determine which credentials to use.

    Returns:
    - tuple: Contains the mnemonic (str) and provider URL (str) for the specified environment.
    """
    celo_credentials = read_yaml_from_s3(os.path.join(RODAAPP_BUCKET_PREFIX, "credentials/roda_celo_credentials.yaml"))
    celo_alfajores_rpc_url = "https://alfajores-forno.celo-testnet.org"
    provider_url = celo_credentials['PROVIDER_URL'] if environment == "production" else celo_alfajores_rpc_url
    return celo_credentials['MNEMONIC'], provider_url


def fetch_contract_info(environment: str):
    """
    Fetches the smart contract information for interacting with the blockchain.

    Retrieves the contract address and ABI from S3, which are necessary for publishing routes to the blockchain.
    The information is environment-specific, supporting different contracts for staging and production.

    Parameters:
    - environment (str): The execution environment ('production' or 'staging').

    Returns:
    - tuple: Contains the contract address (str) and ABI (list) for route publishing.
    """
    celo_contracts = read_json_from_s3(os.path.join(RODAAPP_BUCKET_PREFIX, f"credentials/roda_celo_contracts_{environment}.json"))
    return celo_contracts['RODA_ROUTE_CONTRACT_ADDR'], celo_contracts['RODA_ROUTE_CONTRACT_ABI']


def fetch_published_routes(s3_path: str):
    """
    Retrieves the list of routes already published to the blockchain from S3.

    This function aims to prevent re-publishing of routes by fetching a record of routes that have already been
    successfully uploaded. If the specified S3 path does not exist, an empty dictionary is returned to signify
    no previously published routes.

    Parameters:
    - s3_path (str): The S3 path where the record of published routes is stored.

    Returns:
    - dict: A dictionary of published routes, or an empty dictionary if the record does not exist.
    """
    try:
        return read_json_from_s3(s3_path)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            # Return an empty dictionary if the S3 path does not exist
            return {}
        else:
            # Re-raise the exception if it's not a missing key error
            raise


def connect_to_blockchain(provider_url: str):
    """
    Establishes a connection to the blockchain network.

    Utilizes the provided URL to connect to the blockchain via Web3. This connection is essential for
    interacting with the blockchain, including publishing transactions.

    Parameters:
    - provider_url (str): The URL of the blockchain provider to connect to.

    Returns:
    - Web3: An instance of Web3 connected to the specified blockchain network.
    """
    web3 = Web3(HTTPProvider(provider_url))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    return web3


def wait_for_transaction_receipt(web3, tx_hash, poll_interval=10, timeout=300, max_attempts=5):
    """
    Waits for a blockchain transaction to be mined and retrieves the transaction receipt.

    Periodically polls the blockchain for the transaction receipt until it is found or until a timeout is reached.
    This ensures that a transaction has been successfully processed before proceeding.

    Parameters:
    - web3 (Web3): The Web3 instance connected to the blockchain.
    - tx_hash (HexBytes): The hash of the transaction to monitor.
    - poll_interval (int, optional): Time in seconds between each poll. Defaults to 10.
    - timeout (int, optional): Maximum time in seconds to wait for the transaction receipt. Defaults to 300.
    - max_attempts (int, optional): Maximum number of attempts to fetch the transaction receipt. Defaults to 5.

    Returns:
    - dict or None: The transaction receipt if successful, None if timed out or after max attempts without success.
    """
    logger.info(f"    -> Waiting for transaction to be mined (tx hash: {tx_hash.hex()})")
    start_time = time.time()
    attempts = 0

    while True:
        try:
            logger.info("        -> still wating for transaction to be mined")
            tx_receipt = web3.eth.get_transaction_receipt(tx_hash)
            if tx_receipt:
                return tx_receipt
        except Exception as e:
            error_message = str(e)
            if "not found" in error_message and attempts < max_attempts:
                logger.warning(f"    -> Transaction {tx_hash.hex()} not found. Retrying...")
                attempts += 1
            else:
                # Handle other errors or give up after max_attempts
                logger.error(f"    -> Error fetching receipt for tx hash: {tx_hash.hex()}: {e}")
                return None

        if time.time() - start_time > timeout:
            logger.error(f"    -> Transaction receipt timeout for tx hash: {tx_hash.hex()}")
            return None

        time.sleep(poll_interval)


def publish_to_celo(web3, contract_address, abi, all_routes, published_routes, mnemonic, timeout):
    """
    Publishes route data to the Celo blockchain and return progress.

    Iterates over all provided routes, publishes each to the blockchain, and saves the progress to avoid
    re-publishing. Monitors execution time to stop before the specified timeout, ensuring there's enough
    time to save the current progress to S3.

    Parameters:
    - web3 (Web3): Web3 instance for blockchain interactions.
    - contract_address (str): The blockchain contract address.
    - abi (list): The ABI of the blockchain contract.
    - all_routes (list): List of all routes to be published.
    - published_routes (dict): Record of routes already published to prevent duplicates.
    - mnemonic (str): The mnemonic for accessing the blockchain wallet.
    - timeout (int): Maximum allowed time (in seconds) for the function execution to ensure progress saving.

    Returns:
    - tuple: Contains a boolean indicating overall success and a dictionary of the updated published routes.
    """
    logger.info(f"About to publish {len(all_routes)} transactions...")
    start_time = time.time()
    contract = web3.eth.contract(address=contract_address, abi=abi)


    # Enable unaudited HD wallet features in order to allow using the mnemonic features
    Account.enable_unaudited_hdwallet_features()

    # Derive the account from the mnemonic
    account = Account.from_mnemonic(mnemonic)
    nonce = web3.eth.get_transaction_count(account.address)

    all_success = True
    
    # Iterate over the data and publish each row to Celo
    for route in all_routes:
        try:
            route_id = route['routeID']
            timestamp_start = route['timestampStart']
            timestamp_end = route['timestampEnd']
            measured_distance = route['measuredDistance']
            celo_address = route['celo_address']

            # Check if the route has already been published and skip if it has
            if route_id in published_routes:
                logger.info(f"Route id {route_id} is already published. Skipping re-publishing.")
                continue

            # Check if the elapsed time has exceeded 90% of the specified timeout duration.
            # If so, stop publishing routes. This precaution ensures that the system has
            # enough time to save progress and perform any necessary cleanup operations
            # before the total timeout period is reached.
            current_time = time.time()
            elapsed_time = current_time - start_time
            if elapsed_time  > timeout * 0.9:
                logger.error(
                    f"Approaching timeout limit ({timeout} seconds). Elapsed time: {elapsed_time:.2f} seconds. "
                    "Stopping route publishing as a precaution."
                )
                all_success = False
                break

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
            time.sleep(2) # wait 2 seconds before verifying transaction receipt
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
            error_message = str(e)
            if "ERC721: token already minted" in error_message:
                logger.info(f"Token already minted for route id {route_id}. Continuing with next transaction.")
                published_routes[route_id] = {
                    "nonce": "unkown",
                    "gas_price": "unkown",
                    "tx_hash": "already minted"
                }
                continue
            else:
                logger.error(f"    -> Error publishing route id {route_id}: {e}")
                all_success = False
                break

    return all_success, published_routes


def fetch_input_csv_data(input_prefix):
    """
    Fetches and reads CSV data from S3 based on the specified prefix.

    Parameters:
    - input_prefix (str): The S3 prefix to list and read CSV files from.

    Returns:
    - list: A list of dictionaries, each representing a row from the CSV files found at the specified prefix.
    """
    csv_file_keys = list_s3_files(input_prefix)
    csv_data = []
    for key in csv_file_keys:
        logger.info(f"    -> reading {key}")
        for row in read_csv_from_s3(os.path.join(RODAAPP_BUCKET_PREFIX, key)):
            csv_data.append(row)
    return csv_data


def handler(event: Dict[str, Any], context: Any) -> None:
    """
    The primary handler function for processing and publishing route data to the blockchain.

    This function is designed for compatibility with AWS Lambda but also supports local execution or execution
    within a Docker container. It processes Tribu data by publishing it to the blockchain and intelligently manages
    execution time. It monitors for timeouts, ensuring there is sufficient time to save progress and avoid
    republishing routes that have already been published. The function dynamically adjusts to prevent execution
    from exceeding a specified timeout, allowing for efficient retries and progress continuation.

    Parameters:
    - event (Dict[str, Any]): A dictionary containing the execution parameters. Key parameters include
      'environment' for specifying the execution context (staging or production, optional, defaults to staging), 'processing_date' for the
      target date of the data to process (optional, defaults to yesterday date), and 'timeout' for the maximum allowed execution time in seconds
      (optional, defaults to 900 seconds if not provided).
    - context (Any): Context information provided by AWS Lambda. This parameter is not used within the function
      but is required for AWS Lambda compatibility.

    Returns:
    - None: The function does not return a value but logs its progress and outcomes.

    Note:
    - For local or Docker execution, the function parses command-line arguments to populate the `event` dictionary.
      The AWS Lambda execution environment provides the `event` and `context` parameters directly.
    - The 'timeout' parameter in the `event` dictionary controls how long the function will attempt to publish routes
      before stopping to save progress. This mechanism ensures that the function can halt gracefully before reaching
      the Lambda execution time limit or other defined timeouts.
    """
    logger.setLevel(logging.INFO)
    logger.info("STARTING: Blockchain Publisher task.")
    processing_date = event.get("processing_date")
    processing_date = validate_date(processing_date) if processing_date else yesterday()
    environment = event.get("environment", "staging")
    timeout = int(event.get("timeout", 900))
    input_prefix = os.path.join(RODAAPP_BUCKET_PREFIX, f"rappi_driver_routes/date={format_dashed_date(processing_date)}/")
    celo_published_path = os.path.join(RODAAPP_BUCKET_PREFIX, environment, "celo_published_routes",
                                           f"date={format_dashed_date(processing_date)}", "already_published_routes.json")

    logger.info(f"Parameters: environment: {environment}, processing date: {processing_date}")

    mnemonic, provider_url = fetch_celo_credentials(environment)
    roda_route_contract_addr, roda_route_contract_abi = fetch_contract_info(environment)
    web3 = connect_to_blockchain(provider_url)

    logger.info('Reading CSV data:')
    all_routes = fetch_input_csv_data(input_prefix)
    published_routes = fetch_published_routes(celo_published_path)

    all_success, published_routes = publish_to_celo(web3, roda_route_contract_addr, roda_route_contract_abi, all_routes, published_routes, mnemonic, timeout)
    logger.info(f"uploading to s3 routes that already were published: {celo_published_path}")
    dict_to_json_s3(published_routes, celo_published_path)

    if all_success:
        logger.info("FINISHED SUCCESSFULLY: blockchain publisher task")
        return "FINISHED SUCCESSFULLY: blockchain publisher task"
    else:
        raise Exception(f"Only {len(published_routes)} transaction were published of {len(all_routes)}.")


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
        parser.add_argument(
            "-t", "--timeout",
            type=int,  # or float if you need fractional seconds
            help="Sets the maximum time (in seconds) for the function to run. "
                "If this timeout is reached, the function will attempt to save the current progress in s3 before stopping. "
                "Default is 900 seconds.",
            default=900,
            required=False
        )

        args = parser.parse_args()
        setup_local_logger() # when it does not have env vars from aws, it means that this script is running locally 
        if args.date:
            handler(dict(processing_date=format_dashed_date(args.date),
                            environment=args.environment, timeout=args.timeout), "dockerlocal")
        else:
            handler(dict(environment=args.environment, timeout=args.timeout), "dockerlocal")