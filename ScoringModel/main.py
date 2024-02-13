import pandas as pd
import numpy as np
import os
import sys
sys.path.append('../')  # Asume que la carpeta contenedora está un nivel arriba en la jerarquía

from api_airtable import get_table_Airtable
from python_utilities.utils import read_yaml_from_s3, RODAAPP_BUCKET_PREFIX
from social_score import afectaciones_por_referidos
# Constantes

airtable_credentials_path = os.path.join(RODAAPP_BUCKET_PREFIX, "credentials", "roda_airtable_credentials.yaml")
airtable_credentials = read_yaml_from_s3(airtable_credentials_path)

base_key = airtable_credentials['BASE_ID']
personal_access_token = airtable_credentials['PERSONAL_ACCESS_TOKEN']

fields_credito = ["ID CRÉDITO", "ESTADO", "ID Cliente nocode", "Clasificación perdidos/no perdidos", "Días mora/atraso promedio", "Días mora/atraso acumulados", "# Acuerdos FECHA cumplido copy", "Cantidad acuerdos", "Días de atraso", "Fecha desembolso"]
fields_contactos = ["ID CLIENTE", "Status", "ID's Créditos", "Promedio monto créditos", "Numero de creditos REAL", "¿Referido RODA?", "ID Referidor Nocode"]


estados_deseados = ["POR INICIAR", "RECHAZADO", "INACTIVO"]
estados_deseados_credito = ["PAGADO", "EN PROCESO"]
# Define los límites y puntajes para el cálculo de días de atraso promedio y acumulados
limites_atraso_promedio = [0, 7, 15, 26, 31, 60, 90]
puntajes_atraso_promedio = [1000, 800, 600, 400, 100,0]
limites_atraso_acumulado = [0, 20, 40, 69, 180, 250]
puntajes_atraso_acumulado = [1000, 700, 400, 200, 0]

# Bonus Value

bonus_value = 50

# Ponderaciones para el cálculo del score 
W1=0
W2=0.1
W3=0.1
W4=0.8

nombre_archivo = "DF_contactos.xlsx"

# Social Score

INCREMENTO_POR_REFERIDO = 0.05  # 10% de incremento por cada referido que cumpla la condición
DECREMENTO_POR_REFERIDO = 0.1
UMBRAL_BONUS = 0.2


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


def asignar_bonus_acuerdos(DF_solicitud_credito, bonus):
    # Calcular la razón entre 'Num_Acuerdos_Cumplidos' y 'Cantidad acuerdos'
    ratio = DF_solicitud_credito['Num_Acuerdos_Cumplidos'] / DF_solicitud_credito['Cantidad acuerdos']
    
    # Identificar las filas donde la razón es igual a 1 y el 'Puntaje_Del_Credito' es diferente de cero
    condicion = (ratio == 1) & (DF_solicitud_credito['Puntaje_Del_Credito'] != 0)
    
    # Sumar el bonus al 'Puntaje_Del_Credito' donde la condición es verdadera
    DF_solicitud_credito.loc[condicion, 'Puntaje_Del_Credito'] += bonus
    
    # Crear una nueva columna 'Bono_Aplicado' que indica si se aplicó el bono
    DF_solicitud_credito['Bono_Aplicado'] = False  # Inicializar todos los valores como False
    DF_solicitud_credito.loc[condicion, 'Bono_Aplicado'] = True  # Asignar True donde se aplicó el bono
    
    # Devolver el DataFrame actualizado
    return DF_solicitud_credito

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
        return calcular_score(row['puntaje_inicial'], W1, W2, row['Num_Creditos_puntaje'], row['Monto_Prom_Creditos_puntaje'], row['Puntaje_Ponderado_Creditos'])

# Modular functions
    
def obtener_datos(token):

    """
    Gets data from Airtable for two specific tables: 'Credits' and 'Contacts'.

    :param token: Access token for the Airtable API.
    :return: Two DataFrames, one for credits and one for contacts.

    """
    DF_solicitud_credito = get_table_Airtable('Creditos', token, base_key,fields_credito, 'Scoring_View')
    DF_contactos = get_table_Airtable('Contactos', token, base_key,fields_contactos, 'Scoring_View')
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
    print("imprimiendo en transformar datos")
    DF_contactos['ID Referidor Nocode'] = pd.to_numeric(DF_contactos['ID Referidor Nocode'], errors='coerce')

    DF_solicitud_credito['Días mora/atraso promedio'] = pd.to_numeric(DF_solicitud_credito['Días mora/atraso promedio'], errors='coerce')
    DF_solicitud_credito['Días mora/atraso acumulados'] = pd.to_numeric(DF_solicitud_credito['Días mora/atraso acumulados'], errors='coerce')
    DF_solicitud_credito['ID Cliente nocode'] = pd.to_numeric(DF_solicitud_credito['ID Cliente nocode'])
    DF_solicitud_credito['# Acuerdos FECHA cumplido copy'] = pd.to_numeric(DF_solicitud_credito['# Acuerdos FECHA cumplido copy'])
    DF_solicitud_credito['Cantidad acuerdos'] = pd.to_numeric(DF_solicitud_credito['Cantidad acuerdos'])
    
    print(DF_solicitud_credito.loc[1298, ['Fecha desembolso', 'ID CRÉDITO']])

    #print(DF_solicitud_credito['Fecha desembolso'].apply(type).value_counts())

    DF_solicitud_credito['Fecha desembolso'] = pd.to_datetime(DF_solicitud_credito['Fecha desembolso'], errors='coerce', format='%d/%m/%Y') # Ajusta el formato según sea necesario
    DF_solicitud_credito['Días de atraso'] = pd.to_numeric(DF_solicitud_credito['Días de atraso'], errors= 'coerce')


    # Filtrado de DataFrames
    DF_contactos = DF_contactos[~DF_contactos["Status"].isin(estados_deseados)]
    DF_solicitud_credito = DF_solicitud_credito[DF_solicitud_credito["ESTADO"].isin(estados_deseados_credito)]
    DF_contactos = DF_contactos[DF_contactos["ID's Créditos"].notna()]


    # Cambiar el nombre de las columnas
    DF_solicitud_credito = DF_solicitud_credito.rename(columns={'Días mora/atraso promedio': 'Dias_Atraso_Prom', 'Días mora/atraso acumulados': 'Dias_Atraso_Acum', '# Acuerdos FECHA cumplido copy':'Num_Acuerdos_Cumplidos'})
    DF_contactos = DF_contactos.rename(columns={'Promedio monto créditos': 'Monto_Prom_Creditos', 'Numero de creditos REAL': 'Num_Creditos'})

    # Ahora las columnas tienen nuevos nombres en el DataFrame



    return DF_contactos, DF_solicitud_credito

def score_inicial(df):

    '''
    Not final version, here demografic scoring will be built

    '''

    df['puntaje_inicial'] = 500

    return df



def calcular_puntajes(DF_contactos, DF_solicitud_credito, limites_atraso_promedio, puntajes_atraso_promedio, limites_atraso_acumulado, puntajes_atraso_acumulado, bonus_value):
    """
    Calculates and assigns scores on DataFrames based on predefined criteria, then combines the DataFrames and calculates the final score for each contact.
    
    1. Assigns scores based on quartiles and custom criteria in the DataFrames of contacts and credit applications.
    2. Calculates a weighted score for each customer based on their credits, applying specific bonuses.
    3. Combine the DataFrames to include the weighted scores and calculate a final score considering if they have missing credits.

    Param DF_contacts: Contacts DataFrame.
    :param DF_credit_application: Credit applications DataFrame.
    :param average_delinquency_limits: Limits for the calculation of average days past due scores.
    :param average_delinquency_scores: Scores assigned to the ranges of average days past due.
    param cumulative_delinquency_limits: Limits for the calculation of cumulative overdue days scores.
    param cumulative_delay_scores: Scores assigned to the ranges of cumulative overdue days.
    :param bonus_value: Value of the bonus for 100% fulfilled agreements.
    :return: Contact DataFrame with the final score calculated.
    """

    # Asignación de puntajes
    DF_contactos = asignar_puntajes_por_cuartiles(DF_contactos, 'Monto_Prom_Creditos')  # Creación Monto_Prom_Puntaje
    DF_contactos = asignar_puntajes_por_cuartiles(DF_contactos, 'Num_Creditos')  # Creación Num_Creditos_Puntaje

    DF_solicitud_credito = asignar_puntajes_personalizados(DF_solicitud_credito, 'Dias_Atraso_Prom', limites_atraso_promedio, puntajes_atraso_promedio)  # Creación Dias_Atraso_Prom_Puntaje
    DF_solicitud_credito = asignar_puntajes_personalizados(DF_solicitud_credito, 'Dias_Atraso_Acum', limites_atraso_acumulado, puntajes_atraso_acumulado)  # Creación Dias_Atraso_Acum_Puntaje

    DF_solicitud_credito['Puntaje_Del_Credito'] = (DF_solicitud_credito['Dias_Atraso_Prom_puntaje'] + DF_solicitud_credito['Dias_Atraso_Acum_puntaje']) / 2

    # Bonus Por Acuerdos cumplidos al 100%
    DF_solicitud_credito = asignar_bonus_acuerdos(DF_solicitud_credito, bonus_value)

    # Agrupación y ponderación de puntajes por cliente
    puntajes_por_cliente = DF_solicitud_credito.groupby('ID Cliente nocode')['Puntaje_Del_Credito'].apply(list)
    puntajes_ponderados = puntajes_por_cliente.apply(ponderar_puntajes)

    puntajes_ponderados_df = puntajes_ponderados.reset_index()
    puntajes_ponderados_df.rename(columns={'Puntaje_Del_Credito': 'Puntaje_Ponderado_Creditos', 'ID Cliente nocode': 'ID CLIENTE'}, inplace=True)

    # Unión de DF_contactos con los puntajes ponderados
    DF_contactos = DF_contactos.merge(puntajes_ponderados_df, on='ID CLIENTE', how='left')

    # Crear columna 'Tiene Credito Perdido' y luego unirla con DF_contactos
    DF_solicitud_credito['Tiene Credito Perdido'] = DF_solicitud_credito['Clasificación perdidos/no perdidos'].apply(lambda x: x == 'Perdido')
    clientes_con_credito_perdido = DF_solicitud_credito.groupby('ID Cliente nocode')['Tiene Credito Perdido'].any()
    clientes_con_credito_perdido = clientes_con_credito_perdido.reset_index().rename(columns={'ID Cliente nocode': 'ID CLIENTE'})
    DF_contactos = DF_contactos.merge(clientes_con_credito_perdido, on='ID CLIENTE', how='left')

    DF_contactos = score_inicial(DF_contactos)

    # Cálculo del score final
    DF_contactos['Puntaje_Final'] = DF_contactos.apply(aplicar_calculo, axis=1)


    DF_contactos = afectaciones_por_referidos(DF_contactos, DF_solicitud_credito, INCREMENTO_POR_REFERIDO, DECREMENTO_POR_REFERIDO, UMBRAL_BONUS)
    print(DF_contactos)
    return DF_contactos


# Standard process

def run(token):
    DF_solicitud_credito, DF_contactos = obtener_datos(token)
    DF_contactos, DF_solicitud_credito = transformar_datos(DF_contactos, DF_solicitud_credito)
    DF_contactos = calcular_puntajes(DF_contactos, DF_solicitud_credito,limites_atraso_promedio, puntajes_atraso_promedio, limites_atraso_acumulado, puntajes_atraso_acumulado, bonus_value)
    return DF_contactos, DF_solicitud_credito


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
        df_contactos_procesados, df_creditos_procesados = run(personal_access_token)
        # Aquí puedes añadir código para manejar df_contactos_procesados, 
        # como guardarlo en S3 o procesarlo de alguna manera.

        nombre_archivo = "contactos_procesados.xlsx"
        nombre_archivo_2 = "creditos_procesados.xlsx"
        # Guarda el DataFrame en un archivo Excel en el directorio actual
        df_contactos_procesados.to_excel(nombre_archivo, index=False)
        df_creditos_procesados.to_excel(nombre_archivo_2, index=False)
        print(f"Procesamiento completado con {len(df_contactos_procesados)} registros.")
        print(df_contactos_procesados)
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





