from airtable import Airtable
import pandas as pd
import numpy as np
from python_utilities.utils import logger


def get_table_Airtable(table_name1, personal_access_token,base_key, fields=None, view_name=None):

    """
    Gets data from a specific Airtable table, optionally through a custom view.
    This function connects to an Airtable database using the API and extracts all records from a specified table
    If a view name is provided, only records from that view are returned.
    The records are converted to a pandas DataFrame for easy manipulation and analysis.
    
    Parameters:
    - table_name1 (str): name of the table in Airtable from which you want to get the data.
    - personal_access_token (str): Personal access token for the Airtable API.
    - view_name (str, optional): Name of the view in Airtable to filter the records. If not provided,
    all records in the table are returned.
    """

    table_name = table_name1  # Nombre de tu tabla
    api_key = personal_access_token  # Tu Personal Access Token
    airtable = Airtable(base_key, table_name, api_key)
    # Determina los parámetros para obtener los registros
    params = {}
    if fields:
        params['fields'] = fields
    if view_name:
        params['view'] = view_name

    logger.info(f"Obteniendo registros de tabla {table_name}...")
    records = airtable.get_all(**params)
    # Convierte los registros en una lista de diccionarios
    records_dict = [record['fields'] for record in records]

    # Conviértelo en un DataFrame
    df = pd.DataFrame(records_dict)

    return df


def return_column_airtable(table_name, personal_access_token, base_key, name_column, df_contacto):
    """
    Updates a specified column in an Airtable table for each record that matches 'ID CLIENTE' in the provided DataFrame.

    Args:
        table_name (str): Name of the table in Airtable to update.
        personal_access_token (str): Personal access token for authenticating with the Airtable API.
        base_key (str): The base key of the Airtable base containing the target table.
        name_column (str): The name of the column in Airtable to be updated.
        df_contacto (pd.DataFrame): DataFrame containing 'ID CLIENTE' and the values to update in the specified column.

    Returns:
        None: The function updates the records in Airtable and does not return a value.
    """

    try:
        # Inicializar el cliente de Airtable
        airtable = Airtable(base_key, table_name, api_key=personal_access_token)
        
        # Función para obtener todos los registros y crear un mapa de 'ID CLIENTE' a ID de Airtable
        def fetch_all_records(airtable, field_name):
            """Fetch all records from Airtable and return a map of field_name to record ID."""
            records = airtable.get_all(fields=[field_name])
            return {record['fields'].get(field_name): record['id'] for record in records if field_name in record['fields']}
        
        # Usar la función para crear un mapa de IDs
        id_map = fetch_all_records(airtable, 'ID CLIENTE')
    except Exception as e:
        logger.error(f"Error initializing Airtable client or fetching records: {e}")
        return

    updated_count = 0
    not_found_count = 0

    if name_column not in df_contacto.columns:
        logger.error(f"The column '{name_column}' does not exist in the provided DataFrame.")
        return

    for index, row in df_contacto.iterrows():
        id_cliente = row['ID CLIENTE']
        if id_cliente in id_map:
            record_id = id_map[id_cliente]
            update_data = {name_column: row[name_column]}
            try:
                airtable.update(record_id, update_data)
                updated_count += 1
                logger.info(f"Updated record for ID CLIENTE {id_cliente} in Airtable.")
            except Exception as e:
                logger.error(f"Error updating record for ID CLIENTE {id_cliente} in Airtable: {e}")
        else:
            not_found_count += 1
            logger.info(f"No matching record found for ID CLIENTE {id_cliente} in Airtable.")

    logger.info(f"Update complete. {updated_count} records updated, {not_found_count} records not found.")