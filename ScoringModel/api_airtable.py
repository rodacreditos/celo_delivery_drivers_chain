from airtable import Airtable
import pandas as pd
import numpy as np

def get_table_Airtable(table_name1, personal_access_token):

    base_key = 'WRITE_YOUR_BASE_ID'  # Tu Base ID  WRITE_YOUR_BASE_ID
    table_name = table_name1  # Nombre de tu tabla
    api_key = personal_access_token  # Tu Personal Access Token

    airtable = Airtable(base_key, table_name, api_key)

    # Obtén todos los registros
    records = airtable.get_all()

    # Convierte los registros en una lista de diccionarios
    records_dict = [record['fields'] for record in records]

    # Conviértelo en un DataFrame
    df = pd.DataFrame(records_dict)

    # Imprime el DataFrame
    return df