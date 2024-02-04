import argparse
import logging
import os
import re
import time
from typing import Dict, Any
from airtable import Airtable
from web3 import Web3, HTTPProvider, Account
from web3.middleware import geth_poa_middleware
from bip_utils import Bip39SeedGenerator, Bip44, Bip44Coins, Bip44Changes
from python_utilities.utils import to_unix_timestamp, read_yaml_from_s3, read_json_from_s3, logger, \
    				setup_local_logger, RODAAPP_BUCKET_PREFIX


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
    celo_contracts = read_json_from_s3(os.path.join(RODAAPP_BUCKET_PREFIX, f"credentials/roda_credits_contract_{environment}.json"))
    return celo_contracts['RODA_CREDIT_CONTRACT_ADDR'], celo_contracts['RODA_CREDIT_CONTRACT_ABI']


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


def fetch_airtable_credentials():
    logger.info("Fetching Airtable credentials...")
    airtable_credentials_path = os.path.join(RODAAPP_BUCKET_PREFIX, "credentials", "roda_airtable_credentials.yaml")
    airtable_credentials = read_yaml_from_s3(airtable_credentials_path)
    return airtable_credentials['BASE_ID'],  airtable_credentials['PERSONAL_ACCESS_TOKEN']


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


def parse_days_from_credit_repayment(days_from_credit_repayment: str) -> int:
    """
    Extracts the leading integer number from a string formatted like '45 días (6 semanas)'
    and returns the number of days as an int.
    
    Parameters:
    - days_from_credit_repayment (str): The input string from which to extract the number of days.
    
    Returns:
    - int: The extracted number of days as an integer.
    """
    # Use regular expression to find the first sequence of digits in the string
    match = re.search(r'\d+', days_from_credit_repayment)
    if match:
        # Convert the matched string to an integer and return it
        return int(match.group(0))
    else:
        # If no digits were found, you might want to handle this case, e.g., raise an error
        raise ValueError("No digits found in input string.")


def publish_to_celo(web3, contract_address, abi, credit_records, contacts_table, mnemonic, env):
    logger.info(f"About to publish {len(credit_records)} transactions...")
    start_time = time.time()
    contract = web3.eth.contract(address=contract_address, abi=abi)


    # Enable unaudited HD wallet features in order to allow using the mnemonic features
    Account.enable_unaudited_hdwallet_features()

    # Derive the account from the mnemonic
    account = Account.from_mnemonic(mnemonic)
    nonce = web3.eth.get_transaction_count(account.address)

    all_success = True
    count_published_routes = 0
    
    # Iterate over the data and publish each row to Celo
    for credit in credit_records:
        try:
            id_credit = int(credit['ID CRÉDITO'])
            client_record_id = credit['ID Cliente'][0]
            Investment = int(credit['Inversión'])
            initial_debt = int(credit['Deuda Inicial SUMA'])
            
            disbursement_date = credit['Fecha desembolso corregida']
            disbursement_date = disbursement_date[:-1] if disbursement_date.endswith('Z') else disbursement_date
            disbursement_date = to_unix_timestamp(disbursement_date, "%Y-%m-%dT%H:%M:%S.%f")

            time_for_credit_repayment = int(parse_days_from_credit_repayment(credit['¿Tiempo para el pago del crédito?']))
            client_celo_address = credit['ClientCeloAddress',]
            is_published_to_celo = credit[f'PublishedToCelo{env.capitalize()}']

            ### Debugging
            print(id_credit, client_record_id, Investment, initial_debt, disbursement_date, time_for_credit_repayment, client_celo_address, is_published_to_celo)
            continue

            if not client_celo_address:
                client_id = contacts_table.get(client_record_id)['fields'].get('ID CLIENTE')
                client_celo_address = generate_celo_address(mnemonic, client_id)
                update_client_celo_address(contacts_table, client_record_id, client_celo_address)

            logger.info(f"Publishing credit id {id_credit}:")

            # Check if the route has already been published and skip if it has
            if is_published_to_celo:
                logger.info(f"    -> Credit id {id_credit} is already published. Skipping re-publishing.")
                continue

            # Estimate gas for the transaction
            estimated_gas = contract.functions.CreditIssued(
                                to=client_celo_address,
                                tokenId=id_credit,
                                principal=Investment,
                                totalRepaymentAmount=initial_debt,
                                issuanceDate=disbursement_date,
                                creditTerm=time_for_credit_repayment
                            ).estimate_gas({'from': account.address})

            gas_price = web3.eth.gas_price

            tx = contract.functions.issueCredit(
                to=client_celo_address,
                creditId=id_credit,
                _principal=Investment,
                totalRepaymentAmount=initial_debt,
                _issuanceDate=disbursement_date,
                _creditTerm=time_for_credit_repayment
            ).build_transaction({
                'from': account.address,
                'nonce': nonce,
                'gas': estimated_gas + 100000,  # extra margin for gas
                'gasPrice': gas_price
            })

            # Sign the transaction
            signed_tx = account.sign_transaction(tx)
            tx_hash = Web3.keccak(signed_tx.rawTransaction)
            
            logger.info(f"    -> with: nonce = {nonce}, gas_price = {gas_price}, and tx_hash = {tx_hash.hex()}")

            # Send the transaction
            tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
            logger.info(f"    -> Sent transaction for credit id {id_credit}, awaiting receipt...")

            # Wait until transaction is successfully receipt
            time.sleep(2) # wait 2 seconds before verifying transaction receipt
            tx_receipt = wait_for_transaction_receipt(web3, tx_hash)

            if not tx_receipt:
                logger.error(f"    -> Failed to get receipt for credit id {id_credit}. Stopping further transactions.")
                all_success = False
                break

            logger.info(f"    -> Transaction successfully sent: credit id {id_credit}, hash {tx_hash.hex()}")
            set_credit_as_published(contacts_table, client_record_id, env)
            count_published_routes += 1

            # Increment the nonce for subsequent transactions
            nonce += 1

        except Exception as e:
            error_message = str(e)
            if "ERC721: token already minted" in error_message:
                logger.info(f"    -> Token already minted for credit id {id_credit}. Continuing with next transaction.")
                count_published_routes += 1
                continue
            else:
                logger.error(f"    -> Error publishing credit id {id_credit}: {e}")
                all_success = False
                break

    return all_success, count_published_routes


def generate_celo_address(mnemonic, index=0):
    """
    Generates a Celo address from a mnemonic and an index.

    Args:
    celo_credentials (dict): Dictionary containing the mnemonic.
    index (int): Index to alter the derivation path for different addresses.

    Returns:
    str: A Celo blockchain address.
    """
    # Generate seed from mnemonic
    seed = Bip39SeedGenerator(mnemonic).Generate()

    # Generate the Bip44 wallet for the Celo coin
    bip44_mst_ctx = Bip44.FromSeed(seed, Bip44Coins.ETHEREUM)

    # Derive the address at the specified index
    bip44_acc_ctx = bip44_mst_ctx.Purpose().Coin().Account(0)
    bip44_chg_ctx = bip44_acc_ctx.Change(Bip44Changes.CHAIN_EXT)
    bip44_addr_ctx = bip44_chg_ctx.AddressIndex(index)

    return bip44_addr_ctx.PublicKey().ToAddress()


def fetch_credits_from_airtable(credits_table: Airtable, env: str):
    logger.info("Fetching creditos from airtable (view CREDIT_TO_CELO_PIPELINE_VIEW)...")
    credit_records = credits_table.get_all(
        view='CREDIT_TO_CELO_PIPELINE_VIEW', 
        fields=['ID CRÉDITO', 'ID CLIENTE', 'Inversión', 'Deuda Inicial SUMA', 'Fecha desembolso corregida', '¿Tiempo para el pago del crédito?',
                'ClientCeloAddress', f'PublishedToCelo{env.capitalize()}']
        )
    logger.info(f"    --> Fetched {len(credit_records)} contacts that has at least one GPD ID associated.")
    return credit_records


def update_client_celo_address(contacts_table: Airtable, record_id: str, celo_address: str):
    return contacts_table.update(record_id, {'Celo Address': celo_address})


def set_credit_as_published(credits_table: Airtable, record_id: str, env: str):
    return credits_table.update(record_id, {f'PublishedToCelo{env.capitalize()}': True})


def handler(event: Dict[str, Any], context: Any) -> None:
    logger.setLevel(logging.INFO)
    logger.info("STARTING: Blockchain Publisher task.")
    
    environment = event.get("environment", "staging")

    logger.info(f"Parameters: environment: {environment}")

    mnemonic, provider_url = fetch_celo_credentials(environment)
    credit_contract_addr, credit_contract_abi = fetch_contract_info(environment)
    web3 = connect_to_blockchain(provider_url)
    
    base_id, access_token = fetch_airtable_credentials()
    credits_table = Airtable(base_id, "Creditos", access_token) 
    contacts_table = Airtable(base_id, "Contactos", access_token)

    credit_records = fetch_credits_from_airtable(credits_table, environment)

    all_success, number_published_records = publish_to_celo(web3, credit_contract_addr, credit_contract_abi, credit_records,
                                                            contacts_table, mnemonic, environment)

    if all_success:
        logger.info("FINISHED SUCCESSFULLY: blockchain publisher task")
        return "FINISHED SUCCESSFULLY: blockchain publisher task"
    else:
        raise Exception(f"Only {number_published_records} transaction were published of {len(credit_records)}.")


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
        parser.add_argument("-e", "--environment", help="Given the environment (staging or production)", choices=['staging', 'production'], required=True)

        args = parser.parse_args()
        setup_local_logger() # when it does not have env vars from aws, it means that this script is running locally 
        handler(dict(environment=args.environment), "dockerlocal")