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

# Definiciones de las excepciones
class PaymentTransactionError(Exception):
    """Base class for exceptions in this module."""
    pass

class OverflowError(PaymentTransactionError):
    """Exception raised when a payment causes overflow."""
    def __init__(self, message="Panic error 0x11: Arithmetic operation results in underflow or overflow"):
        self.message = message
        super().__init__(self.message)

class RevertError(PaymentTransactionError):
    """Exception raised when the transaction is reverted."""
    def __init__(self, message="Transaction reverted by the EVM"):
        self.message = message
        super().__init__(self.message)


def get_outstanding_balance(contract, credit_id):
    return contract.functions.outstandingBalance(credit_id).call()

def send_transaction_and_update_airtable(web3, contract, account, nonce, payment_details, payments_table, env):
    """
    Creates, signs, and sends a transaction to the Celo blockchain, and updates Airtable as needed.

    This function estimates the gas required for the transaction, builds the transaction with the specified parameters,
    signs it with the provided account, and sends it to the Celo blockchain. It waits for the transaction to be confirmed
    and updates the payment status in Airtable.

    Parameters:
    - web3 (Web3): An instance of the Web3 class connected to the Celo blockchain.
    - contract (Contract): An instance of the smart contract to interact with.
    - account (Account): The account derived from the mnemonic phrase for signing and sending transactions.
    - nonce (int): The current nonce of the account for the transaction.
    - payment_details (dict): A dictionary containing details of the payment, including id_payment, id_credit, amount, and payment_date.
    - payments_table (Airtable): An instance of the Airtable class to access the payments table.
    - env (str): The environment context ('staging' or 'production') affecting the publication process.

    Returns:
    - Tuple[bool, int]: A tuple where the first element is True if the transaction was successful, False otherwise,
      and the second element is the updated nonce after sending the transaction.

    Raises:
    - OverflowError: If the transaction causes an overflow error.
    - RevertError: If the transaction is reverted by the EVM.
    - PaymentTransactionError: For any other general errors during the transaction processing.
    """
    
    id_payment = payment_details['id_payment']
    id_credit = payment_details['id_credit']
    amount = payment_details['amount']
    payment_date = payment_details['payment_date']

    # Logging the start of transaction process for a payment
    logger.info(f"Starting transaction for payment: {payment_details}")
    try:
        # Estimating gas for the transaction and preparing transaction details
        logger.debug(f"Estimating gas for the payment transaction ID: {id_payment}")

        estimated_gas = contract.functions.recordPayment(
            creditId=id_credit,
            paymentId=id_payment,
            paymentAmount=amount,
            paymentDate=payment_date
        ).estimate_gas({'from': account.address})

        gas_price = web3.eth.gas_price

        # Building the transaction
        tx = contract.functions.recordPayment(
            creditId=id_credit,
            paymentId=id_payment,
            paymentAmount=amount,
            paymentDate=payment_date
        ).build_transaction({
            'from': account.address,
            'nonce': nonce,
            'gas': estimated_gas + 100000,  # margen extra de gas
            'gasPrice': gas_price
        })

        # Signing the transaction
        signed_tx = account.sign_transaction(tx)

        # Sending the transaction
        tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        logger.info(f"    -> Sent transaction for payment id {id_payment}, awaiting receipt...")

        # Waiting for the transaction to be confirmed
        tx_receipt = wait_for_transaction_receipt(web3, tx_hash)
        logger.debug(f"Transaction receipt: {tx_receipt}")
        if tx_receipt:
            # Updating Airtable with the payment status
            set_payment_as_published(payments_table, payment_details['payment_record_id'], env)
            logger.info(f"    -> Transaction successfully sent: payment id {id_payment}, hash {tx_hash.hex()}")
            return True , nonce + 1
        else:
            logger.error(f"    -> Failed to get receipt for payment id {id_payment}.")
            return False, nonce

    except Exception as e:
        error_message = str(e)
        # Checking for specific error messages and raising custom exceptions accordingly
        if "Panic error 0x11" in error_message:
            # Lanza una OverflowError personalizada si se detecta un error de overflow.
            raise OverflowError("Panic error 0x11: Arithmetic operation results in underflow or overflow.") from e
        elif "revert" in error_message.lower():
            # Suponiendo que puedas detectar errores de revert por el mensaje, lanza RevertError.
            raise RevertError("Transaction reverted by the EVM.") from e
        else:
            # Para cualquier otro tipo de error, lanza una PaymentTransactionError genérica.
            raise PaymentTransactionError("Failed to process payment.") from e




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
    Tuple[bool, int]: 
        - all_success (bool): True if all transactions were published successfully.
        - count_published_routes (int): Number of payments published.

    Raises:
    - OverflowError: Occurs when the payment amount exceeds the recipient's outstanding balance. The function handles this error by adjusting the payment amount to the outstanding balance and attempting to republish the transaction. If adjustment and republishing are successful, the payment record is updated accordingly in the Airtable database; otherwise, the function logs the failure and sets `all_success` to False.

    - RevertError: Indicates a transaction has been reverted by the EVM, possibly due to business logic in the smart contract (e.g., payment conditions not met). The function logs the specific reason for the revert and skips the current transaction.

    - PaymentTransactionError: Custom exception indicating issues with transaction submission, such as gas estimation failures or network congestion. The function logs the error details and does not update the payment record in Airtable, allowing for retry.

    Notes:
    - Utilizes 'PublishedToCeloStaging' or 'PublishedToCeloProduction' fields in Airtable to track publication status and ensure idempotency.
    - Requires enabling unaudited HD wallet features in Web3.py for mnemonic-based account derivation.
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

        payment_record_id = payment['id']
        payment_fields = payment['fields']
        id_payment = int(payment_fields['ID Pagos'])
        id_credit = int(payment_fields['ID Credito Nocode'])
        payment_date = to_unix_timestamp(payment_fields['Fecha de pago'], '%Y-%m-%d')
        amount = int(payment_fields['MONTO'])
        is_published_to_celo = payment_fields.get(f'PublishedToCelo{env.capitalize()}', False)

        if is_published_to_celo:
            logger.info(f"    -> Payment id {id_payment} is already published. Skipping re-publishing.")
            continue

        payment_details = {
            'payment_record_id': payment_record_id,
            'id_payment': id_payment,
            'id_credit': id_credit,
            'amount': amount,
            'payment_date': payment_date
        }

        try:
            logger.info(f"web3: {web3}, contract: {contract}, account: {account}, nonce: {nonce}, payment_details: {payment_details}, payments_table: {payments_table}, env: {env}")
            success, nonce = send_transaction_and_update_airtable(web3, contract, account, nonce, payment_details, payments_table, env)
            if success:
                count_published_routes += 1
            else:
                all_success = False
        except OverflowError as oe:

            logger.info(f"    -> Overflow error recording payment {id_payment} for credit {id_credit}. Adjusting payment to outstanding balance.")
            print(f"amount to pay before get_oustandingbalance is: {amount}")
            outstanding_balance = get_outstanding_balance(contract, id_credit)
            print(f"Outstanding-balance for credit {id_credit} is: {outstanding_balance}")
            payment_details['amount'] = outstanding_balance
            try:
                success, nonce = send_transaction_and_update_airtable(web3, contract, account, nonce, payment_details, payments_table, env)
                if success:
                    logger.info(f"    -> Successfully adjusted and published payment id {id_payment}.")
                    count_published_routes += 1
                else:
                    logger.error(f"    -> Failed to adjust and publish payment id {id_payment}.")
                    all_success = False
            except Exception as ex:
                logger.error(f"    -> Error adjusting payment id {id_payment}: {ex}")
                all_success = False
        except RevertError as re:
                logger.info(f"    -> Payment {id_payment} is already published. Continuing with next transaction.")
                set_payment_as_published(payments_table, payment_record_id, env)
                count_published_routes += 1
                continue
        except PaymentTransactionError as e:
            logger.error(f"    -> Error publishing payment id {id_payment}: {e}. See above for more details.")
            all_success = False


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
