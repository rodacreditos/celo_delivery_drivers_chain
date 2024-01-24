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
    Reemplaza diccionarios con un valor especial por un string vacío.
    
    :param value: Valor a ser revisado y posiblemente reemplazado.
    :return: Un string vacío si el valor es un diccionario con 'specialValue', de lo contrario, el valor original.
    """
    if isinstance(value, dict) and 'specialValue' in value:
        return ''
    else:
        return value
    

def asignar_puntajes_por_cuartiles(df, columna):
    """
    Asigna puntajes a los valores de una columna de un DataFrame basándose en los cuartiles.
    
    :param df: DataFrame que contiene la columna a analizar.
    :param columna: Nombre de la columna a la que se le asignarán los puntajes.
    
    :return: DataFrame con una nueva columna que contiene los puntajes asignados.
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
    Asigna puntajes personalizados a una columna de un DataFrame basándose en rangos definidos por el usuario.

    :param df: DataFrame que contiene la columna a analizar.
    :param columna: Nombre de la columna a la que se le asignarán los puntajes.
    :param limites: Lista de límites para definir los rangos.
    :param puntajes: Lista de puntajes a asignar para cada rango.

    :return: DataFrame con una nueva columna que contiene los puntajes asignados.
    """
    if len(limites) != len(puntajes) + 1:
        raise ValueError("La lista de limites debe tener exactamente un elemento más que la lista de puntajes.")

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
    Pondera una lista de puntajes de manera que los puntajes más recientes (hacia el final de la lista)
    tienen un mayor peso, y el peso disminuye linealmente hacia los primeros puntajes.

    :param puntajes: Lista de puntajes a ponderar.
    :return: Puntaje ponderado total.
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
    Calcula el score basado en la fórmula proporcionada.
    
    :param puntaje_inicial: El puntaje inicial.
    :param W1: Peso para el puntaje inicial.
    :param W2: Peso para la suma de los puntajes de crédito.
    :param lambda_val: Valor lambda en la fórmula.
    :param beta: Valor beta en la fórmula.
    :param puntajes_credito: Lista de puntajes de crédito.
    
    :return: El score calculado.
    """
    
    
    score = puntaje_inicial * W1 + (lambda_val*W2) + (beta*W3) + (puntajes_credito*W4)
    return score


# Paso 4: Modificar la función aplicar_calculo
def aplicar_calculo(row):
    """
    Aplica un cálculo específico a una fila del DataFrame.

    Esta función se utiliza principalmente con el método `apply` de pandas para 
    aplicar un cálculo a cada fila de un DataFrame.

    :param row: La fila del DataFrame a la que se le aplicará el cálculo.
    :return: Resultado del cálculo aplicado a la fila.
    """

    if row['Tiene Credito Perdido']:
        return 0
    else:
        return calcular_score(row['puntaje_inicial'], W1, W2, row['Numero de creditos REAL_puntaje'], row['Promedio monto créditos_puntaje'], row['Puntaje Ponderado Creditos'])

# Modular functions
    
def obtener_datos(token):

    """
    Obtiene datos de Airtable para dos tablas específicas: 'Creditos' y 'Contactos'.

    :param token: Token de acceso para la API de Airtable.
    :return: Dos DataFrames, uno para créditos y otro para contactos.
    """
    DF_solicitud_credito = get_table_Airtable('Creditos', token, 'Scoring_View')
    DF_contactos = get_table_Airtable('Contactos', token, 'Scoring_View')
    return DF_solicitud_credito, DF_contactos

def transformar_datos(DF_contactos, DF_solicitud_credito):
    """
    Realiza la limpieza y transformación de los DataFrames obtenidos.

    Convierte los tipos de datos donde sea necesario y filtra los datos según ciertos criterios.

    :param DF_contactos: DataFrame de contactos.
    :param DF_solicitud_credito: DataFrame de solicitudes de crédito.
    :return: Los DataFrames transformados.
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
    Calcula y asigna puntajes en los DataFrames según criterios predefinidos.

    :param DF_contactos: DataFrame de contactos.
    :param DF_solicitud_credito: DataFrame de solicitudes de crédito.
    :return: DataFrame de contactos con puntajes calculados y un DataFrame auxiliar con puntajes ponderados.
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
    Combina los DataFrames y calcula el score final para cada contacto.

    :param DF_contactos: DataFrame de contactos.
    :param puntajes_ponderados_df: DataFrame con puntajes ponderados.
    :param DF_solicitud_credito: DataFrame de solicitudes de crédito.
    :return: DataFrame de contactos con el score calculado.
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
    print("inicio procesamiento handler")
    """
    Handler function for AWS Lambda.

    :param event: A dictionary containing event data (not used for now).
    :param context: AWS Lambda context object (not used for now).
    :return: A response dictionary with status and result.
    """
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
    print("entró a main")
    """
    Main entry point for local script execution.
    
    Executes the script directly and simulates an AWS Lambda event.
    """
    # Simular un evento Lambda. Puedes modificar esto según tus necesidades.
    fake_lambda_event = {
        # Agrega aquí los datos que normalmente recibirías en un evento de Lambda
    }
    fake_lambda_context = None  # Context no es necesario para la ejecución local

    # Ejecutar el handler como si estuviera en Lambda
    response = handler(fake_lambda_event, fake_lambda_context)
    print(response)



