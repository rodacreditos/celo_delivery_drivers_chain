from airtable import Airtable
import pandas as pd
import numpy as np

def get_table_Airtable(table_name1, personal_access_token,view_name=None):

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

    base_key = 'appL6Qzg4ivN62g2e'  # Tu Base ID  WRITE_YOUR_BASE_ID
    table_name = table_name1  # Nombre de tu tabla
    api_key = personal_access_token  # Tu Personal Access Token

    airtable = Airtable(base_key, table_name, api_key)

    # Si se proporciona una vista, se obtienen los registros de esa vista; de lo contrario, de toda la tabla
    if view_name:
        records = airtable.get_all(view=view_name)
    else:
        records = airtable.get_all()

    # Convierte los registros en una lista de diccionarios
    records_dict = [record['fields'] for record in records]

    # Conviértelo en un DataFrame
    df = pd.DataFrame(records_dict)

    return df