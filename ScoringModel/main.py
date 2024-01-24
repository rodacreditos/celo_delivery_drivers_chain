import pandas as pd
import numpy as np

from api_airtable import get_table_Airtable

# Constantes

personal_access_token = 'WRITE_YOUR_TOKEN_ID' # WRITE_YOUR_TOKEN_ID
estados_deseados = ["POR INICIAR", "RECHAZADO", "INACTIVO"]
estados_deseados_credito = ["PAGADO", "EN PROCESO"]
# Define los límites y puntajes para el cálculo de días de atraso promedio y acumulados
limites_atraso_promedio = [0, 7, 15, 26, 31, 60, 90]
puntajes_atraso_promedio = [1000, 800, 600, 400, 100,0]
limites_atraso_acumulado = [0, 20, 40, 69, 180, 250]
puntajes_atraso_acumulado = [1000, 700, 400, 200, 0]

# Ponderaciones para el cálculo del score 
W1=0
W2=0.1
W3=0.1
W4=0.8

nombre_archivo = "DF_contactos.xlsx"

# Functions

def replace_dict_with_empty(value):
    """
    Replaces dictionaries with a special value with an empty string.
    
    :param value: Value to be reviewed and possibly replaced.
    :return: An empty string if the value is a dictionary with 'specialValue', otherwise the original value.
    """
    if isinstance(value, dict) and 'specialValue' in value:
        return ''
    else:
        return value
    

def asignar_puntajes_por_cuartiles(df, columna):
    """
    Assigns scores to the values in a column of a DataFrame based on quartiles.
    
    :param df: DataFrame containing the column to be analyzed.
    :param columna: Name of the column to which the scores will be assigned.
    
    :return: DataFrame with a new column containing the assigned scores.
    """
    # Calcular cuartiles de la columna
    Q1 = df[columna].quantile(0.25)
    Q2 = df[columna].quantile(0.50)
    Q3 = df[columna].quantile(0.75)

    # Función para asignar puntajes
    def asignar_puntaje(valor):
        if valor <= Q1:
            return 250
        elif valor <= Q2:
            return 500
        elif valor <= Q3:
            return 750
        else:
            return 1000

    # Asignar puntajes
    df[f'{columna}_puntaje'] = df[columna].apply(asignar_puntaje)
    return df


def asignar_puntajes_personalizados(df, columna, limites, puntajes):
    """
    Assigns custom scores to a column of a DataFrame based on user-defined ranges.

    :param df: DataFrame containing the column to be analyzed.
    :param columna: Name of the column to which the scores will be assigned.
    :param limites: List of limits to define the ranges.
    :param puntajes: List of scores to be assigned for each rank.

    :return: DataFrame with a new column containing the assigned scores.
    """
    if len(limites) != len(puntajes) + 1:
        raise ValueError("The list of limits must have exactly one more element than the list of scores.")

    # Función para asignar puntajes basada en los rangos definidos por el usuario
    def asignar_puntaje(valor):
        for i in range(1, len(limites)):
            if valor < limites[i]:
                return puntajes[i-1]
        return puntajes[-1]  # Para valores en el último rango o superiores al último límite

    # Asignar puntajes
    df[f'{columna}_puntaje'] = df[columna].apply(asignar_puntaje)
    return df

def ponderar_puntajes(puntajes):
    """
    Weights a list of scores so that the most recent scores (toward the end of the list) have the greatest weight, 
    and the weight decreases linearly toward the earlier scores. 
    :param puntajes: List of scores to be weighted.
    :return: Total weighted score.
    """
    n = len(puntajes)
    
    # Si la lista está vacía o tiene un solo elemento, se devuelve el valor tal cual.
    if n == 0:
        return 0
    elif n == 1:
        return puntajes[0]

    # Calcular los pesos decrecientes
    pesos = [i for i in range(1, n + 1)]
    suma_pesos = sum(pesos)

    # Normalizar los pesos para que sumen 1
    pesos_normalizados = [peso / suma_pesos for peso in pesos]

    # Ponderar los puntajes
    puntaje_ponderado = sum(peso * puntaje for peso, puntaje in zip(pesos_normalizados, puntajes))
    return puntaje_ponderado

# Ejemplo de uso de la función
#puntajes_ejemplo = [200, 500, 800, 1000]  # Lista de ejemplo con puntajes
#puntaje_ponderado_ejemplo = ponderar_puntajes(puntajes_ejemplo)
#puntaje_ponderado_ejemplo


def calcular_score(puntaje_inicial, W1, W2, lambda_val, beta, puntajes_credito):
    """
    Calculate the score based on the formula provided.
    
    :param puntaje_inicial: Initial score.
    :param W1: Weight for initial score.
    :param W2: Weight for the sum of credit scores
    :param lambda_val: Lambda value in the formula.
    :param beta: Beta value in the formula.
    :param puntajes_credito: List of credit scores
    
    :return: Calculated score.
    """
    
    
    score = puntaje_inicial * W1 + (lambda_val*W2) + (beta*W3) + (puntajes_credito*W4)
    return score


# Paso 4: Modificar la función aplicar_calculo
def aplicar_calculo(row):
    """
    Applies a specific calculation to a DataFrame row.
    This function is mainly used with the `apply` method of pandas to apply a calculation to each row of a Dataframe
    :param row: The DataFrame row to which the calculation will be applied.
    :return: Result of the calculation applied to the row.
    """

    if row['Tiene Credito Perdido']:
        return 0
    else:
        return calcular_score(row['puntaje_inicial'], W1, W2, row['Numero de creditos REAL_puntaje'], row['Promedio monto créditos_puntaje'], row['Puntaje Ponderado Creditos'])

# Modular functions
    
def obtener_datos(token):

    """
    Gets data from Airtable for two specific tables: 'Credits' and 'Contacts'.

    :param token: Access token for the Airtable API.
    :return: Two DataFrames, one for credits and one for contacts.

    """
    DF_solicitud_credito = get_table_Airtable('Creditos', token, 'Scoring_View')
    DF_contactos = get_table_Airtable('Contactos', token, 'Scoring_View')
    return DF_solicitud_credito, DF_contactos

def transformar_datos(DF_contactos, DF_solicitud_credito):
    """
    Performs the cleaning and transformation of the obtained DataFrames.
    Convert data types where necessary and filter data according to certain criteria.

    :param DF_contactos: Contacts DataFrame.
    :param DF_solicitud_credito: DataFrame de solicitudes de crédito.
    :return: Transformed DataFrames.
    """

    # Conversión de tipos de datos y limpieza
    DF_contactos['Numero de creditos REAL'] = pd.to_numeric(DF_contactos['Numero de creditos REAL'])
    DF_contactos['Promedio monto créditos'] = DF_contactos['Promedio monto créditos'].apply(replace_dict_with_empty)
    DF_contactos['Promedio monto créditos'] = pd.to_numeric(DF_contactos['Promedio monto créditos'])

    DF_solicitud_credito['Días mora/atraso promedio'] = pd.to_numeric(DF_solicitud_credito['Días mora/atraso promedio'], errors='coerce')
    DF_solicitud_credito['Días mora/atraso acumulados'] = pd.to_numeric(DF_solicitud_credito['Días mora/atraso acumulados'], errors='coerce')
    DF_solicitud_credito['ID Cliente nocode'] = pd.to_numeric(DF_solicitud_credito['ID Cliente nocode'])

    # Filtrado de DataFrames
    DF_contactos = DF_contactos[~DF_contactos["Status"].isin(estados_deseados)]
    DF_solicitud_credito = DF_solicitud_credito[DF_solicitud_credito["ESTADO"].isin(estados_deseados_credito)]
    DF_contactos = DF_contactos[DF_contactos["ID's Créditos"].notna()]

    return DF_contactos, DF_solicitud_credito

def calcular_puntajes(DF_contactos, DF_solicitud_credito):

    """
    Calculates and assigns scores in DataFrames according to predefined criteria.

    :param DF_contactos: Contacts DataFrame.
    :param DF_solicitud_credito: DataFrame de solicitudes de crédito.
    :return: Contact DataFrame with calculated scores and an auxiliary DataFrame with weighted scores
    """

    # Asignación de puntajes
    DF_contactos = asignar_puntajes_por_cuartiles(DF_contactos, 'Promedio monto créditos')
    DF_contactos = asignar_puntajes_por_cuartiles(DF_contactos, 'Numero de creditos REAL')

    DF_solicitud_credito = asignar_puntajes_personalizados(DF_solicitud_credito, 'Días mora/atraso promedio', limites_atraso_promedio, puntajes_atraso_promedio)
    DF_solicitud_credito = asignar_puntajes_personalizados(DF_solicitud_credito, 'Días mora/atraso acumulados', limites_atraso_acumulado, puntajes_atraso_acumulado)

    DF_solicitud_credito['Puntaje Final'] = (DF_solicitud_credito['Días mora/atraso promedio_puntaje'] + DF_solicitud_credito['Días mora/atraso acumulados_puntaje']) / 2

    # Agrupación y ponderación de puntajes por cliente
    puntajes_por_cliente = DF_solicitud_credito.groupby('ID Cliente nocode')['Puntaje Final'].apply(list)
    puntajes_ponderados = puntajes_por_cliente.apply(ponderar_puntajes)

    puntajes_ponderados_df = puntajes_ponderados.reset_index()
    puntajes_ponderados_df.rename(columns={'Puntaje Final': 'Puntaje Ponderado Creditos', 'ID Cliente nocode': 'ID CLIENTE'}, inplace=True)

    return DF_contactos, puntajes_ponderados_df

def unir_dataframes_y_calcular_score(DF_contactos, puntajes_ponderados_df, DF_solicitud_credito):
    """
    Combine the DataFrames and calculate the final score for each contact.

    :param DF_contactos: Contacts DataFrame.
    :param puntajes_ponderados_df: DataFrame with weighted scores.
    :param DF_solicitud_credito: DataFrame de solicitudes de crédito.
    :return: Contacts DataFrame with the calculated score.
    """

    # Unir DF_contactos con los puntajes ponderados
    DF_contactos = DF_contactos.merge(puntajes_ponderados_df, on='ID CLIENTE', how='left')

    # Crear columna 'Tiene Credito Perdido' en DF_solicitud_credito y luego unirla con DF_contactos
    DF_solicitud_credito['Tiene Credito Perdido'] = DF_solicitud_credito['Clasificación perdidos/no perdidos'].apply(lambda x: x == 'Perdido')
    clientes_con_credito_perdido = DF_solicitud_credito.groupby('ID Cliente nocode')['Tiene Credito Perdido'].any()
    clientes_con_credito_perdido = clientes_con_credito_perdido.reset_index().rename(columns={'ID Cliente nocode': 'ID CLIENTE'})
    DF_contactos = DF_contactos.merge(clientes_con_credito_perdido, on='ID CLIENTE', how='left')

    DF_contactos['puntaje_inicial'] = 500

    # Calcular el score final
    DF_contactos['score_calculado'] = DF_contactos.apply(aplicar_calculo, axis=1)

    return DF_contactos

# Standard process

def run(token):
    DF_solicitud_credito, DF_contactos = obtener_datos(token)
    DF_contactos, DF_solicitud_credito = transformar_datos(DF_contactos, DF_solicitud_credito)
    DF_contactos, puntajes_ponderados_df = calcular_puntajes(DF_contactos, DF_solicitud_credito)
    DF_contactos = unir_dataframes_y_calcular_score(DF_contactos, puntajes_ponderados_df, DF_solicitud_credito)
    return DF_contactos


# Lambda handler

def handler(event, context):

    """
    Handler function for AWS Lambda.

    :param event: A dictionary containing event data (not used for now).
    :param context: AWS Lambda context object (not used for now).
    :return: A response dictionary with status and result.
    """

    print("inicio procesamiento handler")

    try:
        df_contactos_procesados = run(personal_access_token)
        # Aquí puedes añadir código para manejar df_contactos_procesados, 
        # como guardarlo en S3 o procesarlo de alguna manera.

        # Por ahora, solo vamos a imprimir un mensaje.
        print(f"Procesamiento completado con {len(df_contactos_procesados)} registros.")

        return {
            'statusCode': 200,
            'body': 'Procesamiento completado exitosamente.'
        }
    except Exception as e:
        print(f"Error durante el procesamiento: {e}")
        return {
            'statusCode': 500,
            'body': 'Error durante el procesamiento.'
        }
    print("Procesamiento completado handler")
# Script principal

if __name__ == '__main__':

    """
    Main entry point for local script execution.
    
    Executes the script directly and simulates an AWS Lambda event.
    """
    print("entró a main")

    # Simular un evento Lambda. Puedes modificar esto según tus necesidades.
    fake_lambda_event = {
        # Agrega aquí los datos que normalmente recibirías en un evento de Lambda
    }
    fake_lambda_context = None  # Context no es necesario para la ejecución local

    # Ejecutar el handler como si estuviera en Lambda
    response = handler(fake_lambda_event, fake_lambda_context)
    print(response)



