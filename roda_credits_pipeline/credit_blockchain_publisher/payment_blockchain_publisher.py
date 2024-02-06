"""
payment_blockchain_publisher.py

This module contains functionalities for fetching payment records from Airtable, publishing them to the Celo blockchain,
and updating the publication status back in Airtable. It is designed to be used within AWS Lambda, facilitating automated,
secure, and reliable publishing of payment transactions to the blockchain.

Features include:
- Fetching credentials and smart contract information for interacting with the Celo blockchain.
- Establishing a Web3 connection to the blockchain network.
- Retrieving non-published payment records from Airtable.
- Publishing payment records to the blockchain and marking them as published in Airtable.
- Handling retries and failures gracefully, ensuring idempotency and robustness in operation.

Usage:
The module is intended to be deployed as an AWS Lambda function, triggered by events that require the publishing of payment
records to the blockchain. It can also be executed in local or Docker environments for testing and development purposes.

Dependencies:
- Web3.py for blockchain interactions.
- Airtable Python Wrapper for working with Airtable APIs.
- BIP utilities for address generation and transaction signing.
- Custom utilities for configuration and logging.

Note:
Ensure proper configuration of environment variables and AWS IAM permissions for access to S3, Airtable, and the blockchain network.
"""
import argparse
import logging
import os
import re
import time
from typing import List, Tuple, Dict, Any
from airtable import Airtable
from web3 import Web3, Account
from python_utilities.utils import to_unix_timestamp, logger, setup_local_logger
from credit_blockchain_publisher import fetch_celo_credentials, fetch_contract_info, connect_to_blockchain, \
                                         fetch_airtable_credentials, wait_for_transaction_receipt


def publish_to_celo(
    web3: Web3, 
    contract_address: str, 
    abi: List[Dict[str, Any]], 
    payment_records: List[Dict[str, Any]], 
    payments_table: Airtable, 
    mnemonic: str, 
    env: str
) -> Tuple[bool, int]:
    """
    Publishes payment records to the Celo blockchain by creating transactions for each payment.

    This function iterates through a list of payment records, constructs and signs transactions using the provided
    mnemonic, and publishes each transaction to the Celo blockchain. It handles the derivation of Celo addresses,
    calculates necessary transaction parameters (e.g., gas), and ensures transactions are successfully mined.
    Additionally, it updates the publication status in Airtable to prevent re-publishing of already processed payments.

    Parameters:
    - web3 (Web3): An instance of the Web3 class, connected to the Celo blockchain.
    - contract_address (str): The address of the smart contract on the Celo blockchain to interact with.
    - abi (List[Dict[str, Any]]): The ABI (Application Binary Interface) of the contract, defining how to interact with it.
    - payment_records (List[Dict[str, Any]]): A list of dictionaries, each representing a payment record to be published.
    - payments_table (Airtable): An instance of the Airtable class for accessing the payments table.
    - mnemonic (str): The mnemonic phrase used to derive blockchain addresses and sign transactions.
    - env (str): The environment context ('staging' or 'production') which affects the publication process,
                 particularly in how the function tracks whether a payment has been published.

    Returns:
    Tuple[bool, int]: A tuple containing two elements:
        - all_success (bool): Indicates whether all payments were successfully published. True if all transactions
                              were successful, False otherwise.
        - count_published_routes (int): The number of payments successfully published to the blockchain.

    Raises:
    - Exception: If an error occurs during the transaction creation, signing, or submission process, an exception
                 is raised with a detailed message about the failure.

    Notes:
    - The function uses the 'PublishedToCeloStaging' or 'PublishedToCeloProduction' fields in Airtable to track
      publication status, updating these fields as payments are processed to ensure idempotency and facilitate
      error recovery.
    - Transactions are constructed and signed using the account derived from the provided mnemonic. This requires
      enabling unaudited HD wallet features in the Web3.py library.
    """
    logger.info(f"About to publish {len(payment_records)} transactions...")
    contract = web3.eth.contract(address=contract_address, abi=abi)


    # Enable unaudited HD wallet features in order to allow using the mnemonic features
    Account.enable_unaudited_hdwallet_features()

    # Derive the account from the mnemonic
    account = Account.from_mnemonic(mnemonic)
    nonce = web3.eth.get_transaction_count(account.address)

    all_success = True
    count_published_routes = 0
    
    # Iterate over the data and publish each row to Celo
    for payment in payment_records:
        try:
            payment_record_id = payment['id']
            payment_fields = payment['fields']
            id_payment = int(payment_fields['ID Pagos'])
            id_credit = int(payment_fields['ID Credito Nocode'])
            payment_date = to_unix_timestamp(payment_fields['Fecha de pago'], '%Y-%m-%d')
            amount = int(payment_fields['MONTO'])
            is_published_to_celo = payment_fields.get(f'PublishedToCelo{env.capitalize()}', False)
            is_credit_published_to_celo = payment_fields.get(f'CreditPublishedToCelo{env.capitalize()}', [False])[0]

            # following credits has issues to publish new payments
            ignored_credits = [322]

            if id_credit in ignored_credits:
                logger.info(f"Ignoring payment id {id_payment} because related credir {id_credit} has more amount in payments than the initial debt of the credit.")    
                continue

            logger.info(f"Publishing payment id {id_payment}:")

            # Check if the payment has already been published and skip if it has
            if is_published_to_celo:
                logger.info(f"    -> Payment id {id_payment} is already published. Skipping re-publishing.")
                continue

            # Check if the credit has already been published and break publication if not
            if not is_credit_published_to_celo:
                logger.info(f"    -> Payment id {id_payment} belongs to credit {id_credit} which is not published to celo yet")
                logger.info("Please ensure all the credits are already published, before start publishing payments")
                break

            # Estimate gas for the transaction
            estimated_gas = contract.functions.recordPayment(
                                creditId=id_credit,
                                paymentId=id_payment,
                                paymentAmount=amount,
                                paymentDate=payment_date
                            ).estimate_gas({'from': account.address})

            gas_price = web3.eth.gas_price

            tx = contract.functions.recordPayment(
                creditId=id_credit,
                paymentId=id_payment,
                paymentAmount=amount,
                paymentDate=payment_date
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
            logger.info(f"    -> Sent transaction for payment id {id_payment}, awaiting receipt...")

            # Wait until transaction is successfully receipt
            time.sleep(2) # wait 2 seconds before verifying transaction receipt
            tx_receipt = wait_for_transaction_receipt(web3, tx_hash)

            if not tx_receipt:
                logger.error(f"    -> Failed to get receipt for payment id {id_payment}. Stopping further transactions.")
                all_success = False
                break

            logger.info(f"    -> Transaction successfully sent: payment id {id_payment}, hash {tx_hash.hex()}")
            set_payment_as_published(payments_table, payment_record_id, env)
            count_published_routes += 1

            # Increment the nonce for subsequent transactions
            nonce += 1

        except Exception as e:
            error_message = str(e)
            if "execution reverted" in error_message:
                logger.info(f"    -> Payment {id_payment} is already published. Continuing with next transaction.")
                set_payment_as_published(payments_table, payment_record_id, env)
                count_published_routes += 1
                continue
            else:
                logger.error(f"    -> Error publishing payment id {id_payment}")
                all_success = False
                raise e

    return all_success, count_published_routes


def fetch_non_published_payments_from_airtable(payments_table: Airtable, env: str):
    """
    Retrieves a list of payment records from Airtable that have not yet been published to the Celo blockchain.

    This function queries the Airtable payments table to find all records that are marked as not published
    according to the 'PublishedToCeloStaging' or 'PublishedToCeloProduction' column, depending on the
    specified environment. It is used to identify payments that need to be processed and published to Celo,
    supporting the workflow of ensuring all relevant payments are eventually pushed to the blockchain.

    Parameters:
    - payments_table (Airtable): An instance of the Airtable class, configured to interact with the payments table.
    - env (str): The environment context ('staging' or 'production') which influences the filter criteria
                 for fetching non-published payments.

    Returns:
    list[dict]: A list of payment records that have not been marked as published in the specified environment.
                Each record is represented as a dictionary.
    """
    logger.info("Fetching pagos from airtable (view PAYMENT_TO_CELO_PIPELINE_VIEW)...")
    payment_published_to_celo_field_name = f'PublishedToCelo{env.capitalize()}'
    credit_published_to_celo_field_name = f'CreditPublishedToCelo{env.capitalize()}'

    payment_records = payments_table.get_all(
        view='PAYMENT_TO_CELO_PIPELINE_VIEW', 
        fields=['ID Pagos', 'Fecha de pago', 'MONTO', 'ID Credito Nocode',
                payment_published_to_celo_field_name, credit_published_to_celo_field_name],
        formula=f'AND(NOT({{{payment_published_to_celo_field_name}}}), {{{credit_published_to_celo_field_name}}})'
        )
    logger.info(f"    --> Fetched {len(payment_records)} payments.")
    return payment_records


def set_payment_as_published(payments_table: Airtable, record_id: str, env: str):
    """
    Marks a payment record in Airtable as published to the Celo blockchain by updating the appropriate column
    based on the execution environment.

    This function targets a specific record, identified by its record ID, and updates its status to indicate
    that it has been successfully published. This is achieved by setting the 'PublishedToCeloStaging' or
    'PublishedToCeloProduction' column to True, depending on whether the function is operating in a staging
    or production environment.

    Parameters:
    - payments_table (Airtable): An instance of the Airtable class, configured to interact with the payments table.
    - record_id (str): The unique identifier of the payment record to update in the Airtable.
    - env (str): The environment context ('staging' or 'production') that dictates which column to update.

    Returns:
    dict: The response from the Airtable API after updating the record, which includes the updated fields.
    """
    return payments_table.update(record_id, {f'PublishedToCelo{env.capitalize()}': True})


def handler(event: Dict[str, Any], context: Any) -> None:
    """
    The entry point for the AWS Lambda function that orchestrates the process of publishing non-published payments
    from Airtable to the Celo blockchain. It ensures idempotency by tracking the publication status of each payment
    in Airtable, allowing for safe retries without duplicating publications.

    This function initializes the necessary configurations, establishes connections to external resources (Airtable,
    Celo blockchain), fetches payments that have not been published to Celo, and attempts to publish each payment
    individually. It intelligently resumes operations by skipping payments already marked as published in Airtable,
    leveraging the 'PublishedToCeloStaging' and 'PublishedToCeloProduction' columns. This mechanism ensures that
    the function can be retried safely after failures, continuing from the last unpublished payment.

    Parameters:
    - event (Dict[str, Any]): A dictionary containing event data that the function uses to execute. It should include:
        - "environment": A string specifying the execution environment ("staging" or "production"). This determines
                         which credentials and configurations are used for connections to external services.
    - context (Any): The runtime information provided by AWS Lambda, which is not used in this function but is required
                     by the AWS Lambda handler signature.

    Returns:
    None. The function logs its progress and results, reporting any failures directly through logging mechanisms.

    Raises:
    - Exception: If not all payments could be successfully published, an exception is raised with details about
                 the number of payments attempted and the number of successes.

    Note:
    This function is designed to be triggered by AWS Lambda events, making it suitable for automated tasks within
    AWS infrastructure, such as scheduled publishing of payments or reacting to specific triggers in AWS services.
    Its resilience to partial failures and ability to continue from the point of interruption make it particularly
    effective for operations that may encounter transient issues or require retrying until complete success is achieved.
    """
    logger.setLevel(logging.INFO)
    logger.info("STARTING: Blockchain Publisher task.")
    
    environment = event.get("environment", "staging")

    logger.info(f"Parameters: environment: {environment}")

    mnemonic, provider_url = fetch_celo_credentials(environment)
    payment_contract_addr, payment_contract_abi = fetch_contract_info(environment)
    web3 = connect_to_blockchain(provider_url)
    
    base_id, access_token = fetch_airtable_credentials()
    payments_table = Airtable(base_id, "Pagos", access_token) 

    payment_records = fetch_non_published_payments_from_airtable(payments_table, environment)

    all_success, number_published_records = publish_to_celo(web3, payment_contract_addr, payment_contract_abi, payment_records,
                                                            payments_table, mnemonic, environment)

    if all_success:
        logger.info("FINISHED SUCCESSFULLY: blockchain publisher task")
        return "FINISHED SUCCESSFULLY: blockchain publisher task"
    else:
        raise Exception(f"Only {number_published_records} transaction were published of {len(payment_records)}.")


if __name__ == "__main__":
    """
    Main entry point for script execution.

    Supports running in a Docker container, AWS Lambda, or directly via CLI.
    Parses command-line arguments for environment is optional, it dafaults to staging.
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
