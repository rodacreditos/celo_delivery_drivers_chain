import pandas as pd
import numpy as np
import os
import sys
sys.path.append('../')  # Asume que la carpeta contenedora está un nivel arriba en la jerarquía

from api_airtable import get_table_Airtable
from python_utilities.utils import read_yaml_from_s3, RODAAPP_BUCKET_PREFIX, logger, setup_local_logger



def validacion_creditos_en_proceso(df_contacto, df_credito):

    """
    Valida si cada cliente en el DataFrame de contacto tiene créditos en proceso en el DataFrame de créditos. Crea una columna nueva en el DataFrame de contacto para indicar si el cliente tiene o no créditos en proceso. Además, actualiza el DataFrame de contacto con información sobre el último crédito en proceso para cada cliente, incluyendo días de atraso y el ID del crédito.

    Args:
    df_contacto (pd.DataFrame): DataFrame que contiene información de los clientes, incluyendo su ID.
    df_credito (pd.DataFrame): DataFrame que contiene información sobre los créditos, incluyendo el estado del crédito, la fecha de desembolso, el ID del cliente, y días de atraso.

    Returns:
    pd.DataFrame: El DataFrame de contacto actualizado con nuevas columnas: 'Créditos en Proceso' (indica si el cliente tiene créditos en proceso), 'Último Días de Atraso' (días de atraso del último crédito en proceso), y 'Último ID CRÉDITO' (ID del último crédito en proceso).
    """

    df_credito_en_proceso = df_credito[df_credito['ESTADO'] == 'EN PROCESO']
    ultimo_credito_por_cliente = df_credito_en_proceso.sort_values(by='Fecha desembolso', ascending=False).drop_duplicates('ID_cliente_nocode')

    tiene_credito_en_proceso = df_credito_en_proceso['ID_cliente_nocode'].unique()
    df_contacto['Créditos en Proceso'] = df_contacto['ID CLIENTE'].apply(lambda x: 'VERDADERO' if x in tiene_credito_en_proceso else 'FALSO')

    dias_atraso_dict = ultimo_credito_por_cliente.set_index('ID_cliente_nocode')['Días de atraso'].to_dict()
    id_credito_dict = ultimo_credito_por_cliente.set_index('ID_cliente_nocode')['ID CRÉDITO'].to_dict()
    monto_credito = ultimo_credito_por_cliente.set_index('ID_cliente_nocode')['Deuda actual 2.0'].to_dict()
    # Asigna directamente np.nan en lugar de 'N/A' antes de la conversión a numérico
    df_contacto['Último Días de Atraso'] = df_contacto['ID CLIENTE'].map(dias_atraso_dict)
    df_contacto['Último ID CRÉDITO'] = df_contacto['ID CLIENTE'].map(id_credito_dict)
    df_contacto['Último Deuda Actual'] = df_contacto['ID CLIENTE'].map(monto_credito)
    # Solo reemplazar por np.nan si 'Créditos en Proceso' es 'FALSO' (Omitido ya que np.nan será el valor por defecto si no se encuentra el mapeo)
    # No es necesario reasignar 'N/A' y luego reemplazarlo, ya que el mapeo con .map() ya asignará np.nan a los que no encuentre

    # Convertir las columnas a numérico, asumiendo que np.nan ya está asignado a los valores faltantes
    df_contacto['Último Días de Atraso'] = pd.to_numeric(df_contacto['Último Días de Atraso'], errors='coerce')
    df_contacto['Último ID CRÉDITO'] = pd.to_numeric(df_contacto['Último ID CRÉDITO'], errors='coerce')
    df_contacto['Último Deuda Actual'] = pd.to_numeric(df_contacto['Último Deuda Actual'], errors='coerce')


    return df_contacto

def ajuste_x_referidos_atrasados(id_referidor, dataframe_auxiliar, df_contacto, UMBRAL_BONUS, INCREMENTO_POR_REFERIDO, DECREMENTO_POR_REFERIDO):

    """
    Calculates adjustments for clients based on the status of their referrals, particularly focusing on those with delays.
    
    Args:
        id_referidor (int): The ID of the referring client.
        dataframe_auxiliar (pd.DataFrame): An auxiliary DataFrame containing information about the referred clients, including their final scores and days of delay.
        df_contacto (pd.DataFrame): The main contact DataFrame to which the calculated adjustments will be applied.
        UMBRAL_BONUS (float): Threshold percentage of referrals without delays required to apply a bonus.
        INCREMENTO_POR_REFERIDO (float): The amount to increase for each referral under certain conditions.
        DECREMENTO_POR_REFERIDO (float): The amount to decrease for each referral with days of delay.
    
    Returns:
        pd.DataFrame: The updated main contact DataFrame with calculated adjustments applied.
    """

    try:

        id_referidor = int(id_referidor)

        # logger.info(f"Calculando ajustes por referidos atrasados para el cliente {id_referidor}...")
        clientes_con_atraso = dataframe_auxiliar[dataframe_auxiliar['Último Días de Atraso'] > 0].shape[0]

        # Calcular porcentaje de referidos con atraso
        total_referidos = dataframe_auxiliar.shape[0]
        porcentaje_con_atraso = clientes_con_atraso / total_referidos if total_referidos > 0 else 0

        # Inicializar el ajuste calculado
        ajuste_calculado = 0.0

        # Para cada cliente en dataframe_auxiliar evaluar:
        for _, referido in dataframe_auxiliar.iterrows():
            if porcentaje_con_atraso < UMBRAL_BONUS and referido['Puntaje_Final'] > 800:
                ajuste_calculado += INCREMENTO_POR_REFERIDO
            if referido['Último Días de Atraso'] > 0:
                ajuste_calculado -= DECREMENTO_POR_REFERIDO

        # Actualizar df_contacto['ajuste_calculado'] para el cliente en cuestión
        df_contacto.loc[df_contacto['ID CLIENTE'] == id_referidor, 'ajuste_calculado'] += ajuste_calculado

    except ValueError:
        logger.error("ID del referidor debe ser un entero.")
        raise
    except KeyError as e:
        logger.error(f"Columna faltante en DataFrame: {e}")
        raise
    except Exception as e:
        logger.error(f"Error inesperado: {e}")
        raise

    return df_contacto


def get_info_referido(id_referidor, df_contacto):

    """
    Retrieves information for referrals of a specific client who have ongoing credits.
    
    Args:
        id_referidor (int): The ID of the referring client.
        df_contacto (pd.DataFrame): DataFrame containing clients' contact information along with their referral data.
    
    Returns:
        pd.DataFrame: A DataFrame containing filtered referral information based on the given criteria.
    """

    try:

        id_referidor = int(id_referidor)

        # logger.info(f"Obteniendo información sobre referidos del cliente {id_referidor}...")
        # Filtrar referidos que tienen "Créditos en Proceso" = 'VERDADERO'
        referidos = df_contacto[(df_contacto['ID Referidor Nocode'] == id_referidor) & (df_contacto['Créditos en Proceso'] == 'VERDADERO') & (df_contacto['¿Referido RODA?'] == 'Sí')]

        if not referidos.empty:
            # Extraer la información deseada para cada referido y almacenarla en una lista de diccionarios
            info_referidos_list = referidos.apply(lambda row: {
                'ID CLIENTE': row['ID CLIENTE'],
                'Nombre completo': row['Nombre completo'],
                # 'Puntaje_Final': row['Puntaje_Final'],
                'Créditos en Proceso': row['Créditos en Proceso'],
                'Días de Atraso Actuales': int(row['Último Días de Atraso']),
                'Tiene Credito Perdido': row['Tiene Credito Perdido'],
                'Deuda Actual': row['Último Deuda Actual']
                
            }, axis=1).tolist()

            # Convertir la lista de diccionarios a string para almacenamiento
            info_referidos_str = str(info_referidos_list)
            
            # Actualizar la columna 'Info_Referidos' en df_contacto para el cliente especificado
            df_contacto.loc[df_contacto['ID CLIENTE'] == id_referidor, 'Info_Referidos'] = info_referidos_str

            # Calcular la suma de deuda de referidos con más de 50 días de atraso
            deuda_perdidos50 = referidos[referidos['Último Días de Atraso'] > 50]['Último Deuda Actual'].sum()
            
            # Asignar la suma calculada a una nueva columna en el DataFrame principal para el referidor especificado
            df_contacto.loc[df_contacto['ID CLIENTE'] == id_referidor, 'deuda_perdidos50'] = deuda_perdidos50


    except ValueError:
        logger.error("ID del referidor debe ser un entero.")
        raise
    except KeyError as e:
        logger.error(f"Columna faltante en DataFrame: {e}")
        raise
    except Exception as e:
        logger.error(f"Error inesperado: {e}")
        raise

    return referidos


def cleaning_social_variables(df_contacto, df_credito):

    """
    Cleans and prepares the contact and credit DataFrames for further processing, ensuring proper ID management and NaN handling.
    
    Args:
        df_contacto (pd.DataFrame): The contact information DataFrame.
        df_credito (pd.DataFrame): The credit information DataFrame.
    
    Returns:
        tuple: A tuple containing the cleaned contact and credit DataFrames.
    """

    try:

        logger.info("Cleaning social variables...")

        # Asegura que los IDs se manejen correctamente y limpia valores NaN
        df_contacto['ID CLIENTE'] = pd.to_numeric(df_contacto['ID CLIENTE'], errors='coerce').astype('float64')
        df_credito['ID_cliente_nocode'] = pd.to_numeric(df_credito['ID_cliente_nocode'], errors='coerce').astype('float64')
        df_contacto['ID Referidor Nocode'] = pd.to_numeric(df_contacto['ID Referidor Nocode'], errors='coerce')
        df_contacto.dropna(subset=['ID CLIENTE'], inplace=True)
        df_credito.dropna(subset=['ID_cliente_nocode'], inplace=True)

    except KeyError as e:
        logger.error(f"Columna faltante en DataFrame: {e}")
        raise
    except Exception as e:
        logger.error(f"Error inesperado al limpiar variables sociales: {e}")
        raise

    return df_contacto, df_credito

def identificacion_referidores_perdidos(df_contacto):
    """
    Identifies lost referrers within the contact DataFrame, marking them as such for further processing.
    
    Args:
        df_contacto (pd.DataFrame): The contact information DataFrame.
    
    Returns:
        pd.DataFrame: The updated contact DataFrame with lost referrers identified.
    """

    try:

        logger.info("Identificando referidores perdidos...")

        # ----------Sección verificar si REFERIDOR del cliente está perdido-----------------
        df_contacto['REFERIDOR_Perdido'] = 'FALSO'
        df_contacto['Afectado_x_red'] = 'FALSO'

        for index, row in df_contacto.iterrows():
            id_referidor = row['ID Referidor Nocode']
            
            # Manejo de excepción para cuando no hay un referidor o el ID está vacío
            if pd.isna(id_referidor):
                continue  # No se realiza ninguna acción, el valor default 'FALSO' permanece

            # Consulta para encontrar al referidor
            referidor = df_contacto[df_contacto['ID CLIENTE'] == id_referidor]
            
            # Verificación y asignación basada en la existencia de crédito perdido
            if not referidor.empty and referidor.iloc[0]['Tiene Credito Perdido']:
                print(f"Referidor {id_referidor} Perdido del cliente {row['ID CLIENTE']} ")
                df_contacto.at[index, 'REFERIDOR_Perdido'] = 'VERDADERO'

    except KeyError as e:
        logger.error(f"Columna faltante en DataFrame: {e}")
        raise
    except Exception as e:
        logger.error(f"Error inesperado al identificar referidores perdidos: {e}")
        raise

    return df_contacto


def calculo_ajustes(df_contacto, UMBRAL_BONUS, INCREMENTO_POR_REFERIDO, DECREMENTO_POR_REFERIDO):

    """
    Calculates adjustments for each client based on the status of their referrals, including delays and lost credits.
    
    Args:
        df_contacto (pd.DataFrame): The main contact DataFrame.
        UMBRAL_BONUS (float): Threshold percentage of referrals without delays required to apply a bonus.
        INCREMENTO_POR_REFERIDO (float): The amount to increase for each referral under certain conditions.
        DECREMENTO_POR_REFERIDO (float): The amount to decrease for each referral with days of delay.
    
    Returns:
        pd.DataFrame: The updated main contact DataFrame with adjustments calculated and applied.
    """

    try:
        # Ensure UMBRAL_BONUS, INCREMENTO_POR_REFERIDO, and DECREMENTO_POR_REFERIDO are floats
        UMBRAL_BONUS = float(UMBRAL_BONUS)
        INCREMENTO_POR_REFERIDO = float(INCREMENTO_POR_REFERIDO)
        DECREMENTO_POR_REFERIDO = float(DECREMENTO_POR_REFERIDO)

        logger.info("Calculando ajustes x red social...")

        # Inicializar las columnas necesarias en df_contacto si aún no existen
        if 'REFERIDO_Perdido' not in df_contacto.columns:
            df_contacto['REFERIDO_Perdido'] = 'FALSO'
        if 'ajuste_calculado' not in df_contacto.columns:
            df_contacto['ajuste_calculado'] = 0.0
        if 'Afectado_x_red' not in df_contacto.columns:
            df_contacto['Afectado_x_red'] = 'FALSO'
        if 'Info_Referidos' not in df_contacto.columns:
            df_contacto['Info_Referidos'] = 'NaN'

        # Iterar sobre cada cliente único en df_contacto
        for id_cliente in df_contacto['ID CLIENTE'].unique():

            # Obtener información de los referidos para este cliente
            dataframe_auxiliar = get_info_referido(id_cliente, df_contacto)

            df_contacto = ajuste_x_referidos_atrasados(id_cliente, dataframe_auxiliar, df_contacto, UMBRAL_BONUS, INCREMENTO_POR_REFERIDO, DECREMENTO_POR_REFERIDO)

            if dataframe_auxiliar['Tiene Credito Perdido'].any():

                logger.info(f"el cliente {id_cliente} tiene un referido perdido")
                # Si hay algún referido perdido, actualiza 'REFERIDO_Perdido' en df_contacto para el cliente actual
                df_contacto.loc[df_contacto['ID CLIENTE'] == id_cliente, 'REFERIDO_Perdido'] = 'VERDADERO'

                referidos_sin_perdida = dataframe_auxiliar[~dataframe_auxiliar['Tiene Credito Perdido']]

                # Para cada cliente referido sin crédito perdido, realizar ajustes en df_contacto
                for id_referido in referidos_sin_perdida['ID CLIENTE']:
                    # Restar 0.5 del ajuste calculado

                    logger.info(f"Referido {id_referido} fue afectado por su red")
                    df_contacto.loc[df_contacto['ID CLIENTE'] == id_referido, 'ajuste_calculado'] -= 0.5
                    
                    # Actualizar 'Afectado_x_red' como 'VERDADERO'
                    df_contacto.loc[df_contacto['ID CLIENTE'] == id_referido, 'Afectado_x_red'] = 'VERDADERO'

    except ValueError:
        logger.error("Los umbrales y valores de incremento/decremento deben ser numéricos.")
        raise
    except KeyError as e:
        logger.error(f"Columna faltante en DataFrame: {e}")
        raise
    except Exception as e:
        logger.error(f"Error inesperado al calcular ajustes: {e}")
        raise

    return df_contacto



def afectaciones_por_referidos(df_contacto, df_credito, INCREMENTO_POR_REFERIDO, DECREMENTO_POR_REFERIDO, UMBRAL_BONUS, PARAM_POR_PERDIDO):
    """
    Adjusts final scores for clients based on the condition of their referrals, considering credits in process, days of delay, and any lost credits by referrals.
    
    Args:
        df_contacto (pd.DataFrame): DataFrame containing clients' contact information, including columns for client ID, final score, referrer ID, ongoing credits, and lost credits.
        df_credito (pd.DataFrame): DataFrame containing credit information, including columns for client ID and last days of delay, among others. This DataFrame is included for completeness and possible future use or validations.
        INCREMENTO_POR_REFERIDO (float): Value to be added to the client's final score for each referral with a final score over 800, provided less than the UMBRAL_BONUS percentage of their referrals have days of delay.
        DECREMENTO_POR_REFERIDO (float): Value to be subtracted from the client's final score for each referral with days of delay.
        UMBRAL_BONUS (float): Threshold used to determine if bonuses for referrals are applied based on the percentage of referrals without delays. Represents a percentage (e.g., 0.2 for 20%).
        PARAM_POR_PERDIDO (float): Not explicitly used in the provided function signature, but presumably a parameter related to adjustments for lost credits by referrals.
    
    Returns:
        pd.DataFrame: The updated 'df_contacto' DataFrame with three new columns added: 'Ajuste_calculado' showing the calculated adjustment based on referral logic, 'REFERIDO_Perdido' indicating with 'TRUE' or 'FALSE' if any referral has lost a credit, 'Info_Referidos' a string representing a list of dictionaries with detailed information on each evaluated referral, and 'Puntaje_Final_Ajustado' the new adjusted final score of the client calculated based on referral adjustments, ensuring it stays within the 0 to 1000 range.
    
    The function first cleans and prepares the data, then evaluates each client's referrals to calculate adjustments to their final score. It considers both bonuses for referrals with high scores and no delays and penalties for referrals with days of delay. Additionally, it marks those clients who have referrals with lost credits, which directly impacts their adjusted final score.
    """

    setup_local_logger()

    try:
        logger.info("Calculando social score...")

        df_contacto, df_credito = cleaning_social_variables(df_contacto, df_credito)

        df_contacto = identificacion_referidores_perdidos(df_contacto)
                
        # ---------Sección Validar créditos en proceso-----------------
        df_contacto = validacion_creditos_en_proceso(df_contacto, df_credito)

        # Aplicar ajustes y almacenar información de referidos

        df_contacto = calculo_ajustes(df_contacto, UMBRAL_BONUS, INCREMENTO_POR_REFERIDO, DECREMENTO_POR_REFERIDO)

        logger.info("Ajustes aplicados exitosamente...")

        # Calcular Puntaje_Final_Ajustado
        df_contacto['Puntaje_Final_Ajustado'] = df_contacto.apply(
            lambda row: 0 if row['REFERIDO_Perdido'] == 'VERDADERO' or row['Tiene Credito Perdido'] == True  or row['REFERIDOR_Perdido'] == 'VERDADERO'
            else row['Puntaje_Final'] + row['Puntaje_Final'] * row['ajuste_calculado'],
            axis=1
        )

        # Asegurar que el puntaje final ajustado esté en el rango de 0 a 1000
        df_contacto['Puntaje_Final_Ajustado'] = df_contacto['Puntaje_Final_Ajustado'].clip(lower=0, upper=1000)

        logger.info("Puntaje final actualizado existosamente...")

    except Exception as e:
        logger.error(f"Error en la función afectaciones_por_referidos: {str(e)}")
        raise e

    return df_contacto



