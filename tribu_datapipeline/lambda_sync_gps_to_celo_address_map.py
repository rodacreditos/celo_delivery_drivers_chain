"""
This script connects to RODA Airtable database for synchronizing the gps_to_celo_address_map. It also creates
the celo address when it does not exist and update it in the Airtable database. Designed for deployment in a Docker 
container, it's suitable for execution in an AWS Lambda function and supports local testing.


The script can be executed in various environments:
1. As an AWS Lambda function - It is designed to run within AWS Lambda, fetching parameters from the event object.
2. In a Docker container - Suitable for local testing or deployment.
3. Directly via CLI - For local execution and testing.

The script supports command-line arguments for easy local testing and debugging. It leverages functionality 
from an accompanying 'utils.py' module for tasks like data processing and AWS S3 interactions.

Environment Variables:
- AWS_LAMBDA_RUNTIME_API: Used to determine if the script is running in an AWS Lambda environment.

Usage:
- AWS Lambda: Deploy the script as a Lambda function. The handler function will be invoked with event and context parameters.
- Docker Container/CLI: Run the script with optional command-line arguments to specify the dataset type and processing date.

Examples:
- CLI: python lambda_sync_gps_to_celo_address_map.py
- Docker: docker run --rm \
		-v ~/.aws:/root/.aws \
		-v $(shell pwd):/var/task \
		-i --entrypoint python rodaapp:tribu_processing \
		lambda_sync_gps_to_celo_address_map.py

Output:
- The script retrieves data from the Roda Airtable API and writes it to a YAML file on AWS S3.

Note:
- The script requires access to AWS S3 for fetching Airtable credentials and storing output.
"""
import argparse
import logging
import os
from airtable import Airtable
from bip_utils import Bip39SeedGenerator, Bip44, Bip44Coins, Bip44Changes
from typing import Dict, Any
from utils import read_yaml_from_s3, logger, dict_to_yaml_s3, \
    				setup_local_logger, RODAAPP_BUCKET_PREFIX


def generate_celo_address(celo_credentials, index=0):
    """
    Generates a Celo address from a mnemonic and an index.

    Args:
    celo_credentials (dict): Dictionary containing the mnemonic.
    index (int): Index to alter the derivation path for different addresses.

    Returns:
    str: A Celo blockchain address.
    """
    # Generate seed from mnemonic
    seed = Bip39SeedGenerator(celo_credentials["MNEMONIC"]).Generate()

    # Generate the Bip44 wallet for the Celo coin
    bip44_mst_ctx = Bip44.FromSeed(seed, Bip44Coins.ETHEREUM)

    # Derive the address at the specified index
    bip44_acc_ctx = bip44_mst_ctx.Purpose().Coin().Account(0)
    bip44_chg_ctx = bip44_acc_ctx.Change(Bip44Changes.CHAIN_EXT)
    bip44_addr_ctx = bip44_chg_ctx.AddressIndex(index)

    return bip44_addr_ctx.PublicKey().ToAddress()


def update_celo_addresses(records_to_update):
    pass


def get_gps_to_celo_map():
    logger.info("Fetching Airtable credentials...")
    airtable_credentials_path = os.path.join(RODAAPP_BUCKET_PREFIX, "credentials", "roda_airtable_credentials.yaml")
    airtable_credentials = read_yaml_from_s3(airtable_credentials_path)

    base_id = airtable_credentials['BASE_ID']
    celo_access_token = airtable_credentials['PERSONAL_ACCESS_TOKEN']

    logger.info("Fetching Celo credentials...")
    s3_path = os.path.join(RODAAPP_BUCKET_PREFIX, "credentials", "roda_celo_credentials.yaml")
    celo_credentials = read_yaml_from_s3(s3_path)

    # Initialize Airtable client for creditos and contactos table
    contactos_table = Airtable(base_id, 'Contactos', celo_access_token)

    logger.info("Fetching contactos from airtable that has at least one GPS ID associated (view TRIBU_PIPELINE_VIEW)...")
    contactos_records = contactos_table.get_all(view='TRIBU_PIPELINE_VIEW', fields=['ID CLIENTE', 'GPS ID List', 'Celo Address'])
    logger.info(f"    --> Fetched {len(contactos_records)} contacts that has at least one GPD ID associated.")

    gps_to_celo_address_map = {}
    records_to_update = []
    for record in contactos_records:
        record_id = record['id']
        celo_address = record['fields'].get('Celo Address')
        gps_list = record['fields'].get('GPS ID List')
        client_id = record['fields'].get('ID CLIENTE')
        for gps_id in gps_list:
            if gps_id in gps_to_celo_address_map:
                raise Exception("   -> Found a gpsID duplicated for more than one credit/contact, please fix this issue before running this task.")
            else:
                if not celo_address:
                    celo_address = generate_celo_address(celo_credentials, index=client_id)
                    records_to_update.append({'id': record_id, 'fields': {'Celo Address': celo_address}})
                gps_to_celo_address_map[gps_id] = celo_address

    logger.info(f"    --> Fetched {len(gps_to_celo_address_map)} GPS IDs associated to a client.")
    logger.info(f"    --> There are {len(records_to_update)} contacts needing to update the recently created celo address.")

    # Updates records in airtable for ensuring integrity
    logger.info("Updating Airtable contacts with the recently created celo addresses...")
    contactos_table.batch_update(records_to_update)

    return gps_to_celo_address_map



def handler(event: Dict[str, Any], context: Any) -> None:
    """
    Handler function for processing Tribu data.

    Intended for use as the entry point in AWS Lambda, but also supports local execution.
    The 'dataset_type' in the event determines whether the data is primarily motorbike ('roda') 
    or bicycle ('guajira') related.

    :param event: A dictionary containing 'dataset_type' and optionally 'processing_date'.
                  If 'processing_date' is not provided, defaults to yesterday's date.
    :param context: Context information provided by AWS Lambda (unused in this function).
    """
    logger.setLevel(logging.INFO)
    logger.info("STARTING: Roda Airtable - celo_address_map sync task.")

    gps_to_celo_address_map = get_gps_to_celo_map()
    
    gps_to_celo_address_map_path = os.path.join(RODAAPP_BUCKET_PREFIX, "roda_metadata", "gps_to_celo_address_map.yaml")
    logger.info(f"Uploading gps to celo address mapping to {gps_to_celo_address_map_path}")
    dict_to_yaml_s3(gps_to_celo_address_map, gps_to_celo_address_map_path)

    logger.info("FINISHED SUCCESSFULLY: Roda Airtable - celo_address_map sync task.")
    return "FINISHED SUCCESSFULLY: Roda Airtable - celo_address_map sync task."


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
        args = parser.parse_args()
        setup_local_logger() # when it does not have env vars from aws, it means that this script is running locally 
        handler(dict(), "dockerlocal")
