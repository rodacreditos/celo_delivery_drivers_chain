import pandas as pd
import numpy as np

# own .py

from api_airtable import get_table_Airtable


personal_access_token = 'patLJSYJ6ohmP3uZ9.a2ced0b4e7bfc72cc45f3a0df6d3b1b22dea317c1c8ca2c8955c262a9fc118e1'
DF_solicitud_credito = get_table_Airtable('Creditos', personal_access_token)
DF_contactos=get_table_Airtable('Contactos', personal_access_token)

DF_contactos['Numero de creditos REAL']=pd.to_numeric(DF_contactos['Numero de creditos REAL'])

# Define una función para convertir los valores con diccionarios en valores vacíos
def replace_dict_with_empty(value):
    if isinstance(value, dict) and 'specialValue' in value:
        return ''
    else:
        return value

# Aplica la función a la columna 'Promedio monto créditos'
DF_contactos['Promedio monto créditos'] = DF_contactos['Promedio monto créditos'].apply(replace_dict_with_empty)

DF_contactos['Promedio monto créditos']=pd.to_numeric(DF_contactos['Promedio monto créditos'])

# Hoja creditos
#DF_solicitud_credito['Dias de atraso']=pd.to_numeric(DF_contactos['Dias de atraso'])
DF_solicitud_credito['Días mora/atraso promedio'] = pd.to_numeric(DF_solicitud_credito['Días mora/atraso promedio'], errors='coerce')
DF_solicitud_credito['Días mora/atraso acumulados'] = pd.to_numeric(DF_solicitud_credito['Días mora/atraso acumulados'], errors='coerce')

DF_solicitud_credito['ID Cliente nocode']=pd.to_numeric(DF_solicitud_credito['ID Cliente nocode'])



# Filtrar Solo PAGADOS y EN PROCESO

# Filtrando el DataFrame para obtener solo las filas con los estados deseados
estados_deseados = ["POR INICIAR", "RECHAZADO", "INACTIVO"]
DF_contactos = DF_contactos[~DF_contactos["Status"].isin(estados_deseados)]

estados_deseados_credito = ["PAGADO", "EN PROCESO"]

DF_solicitud_credito = DF_solicitud_credito[DF_solicitud_credito["ESTADO"].isin(estados_deseados_credito)]


#FILTRAR SOLO CLIENTES QUE YA TENGAN AL MENOS 1 CREDITO

# Filtrar DF_contactos para mantener solo filas donde 'ID's Créditos' no está vacío
DF_contactos = DF_contactos[DF_contactos["ID's Créditos"].notna()]

# Ahora DF_contactos_filtrado contiene solo las filas con 'ID's Créditos' no vacío


# Calculo puntaje número de créditos y monto crédito


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


# Usar la función para asignar puntajes en Promedio monto créditos
DF_contactos = asignar_puntajes_por_cuartiles(DF_contactos, 'Promedio monto créditos')
DF_contactos = asignar_puntajes_por_cuartiles(DF_contactos, 'Numero de creditos REAL')


# Calculo sobre los créditos

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

# Días atraso promedio
limites_atraso_promedio = [0, 7, 15, 26, 31, 60, 90]
puntajes_atraso_promedio = [1000, 800, 600, 400, 100,0]

# Días atraso Acumulados

limites_atraso_acumulado = [0, 20, 40, 69, 180, 250]
puntajes_atraso_acumulado = [1000, 700, 400, 200, 0] #Pendiente validar 0

# Aplicar la función para asignar puntajes a la columna 'Días mora/atraso promedio'
DF_solicitud_credito = asignar_puntajes_personalizados(DF_solicitud_credito, 'Días mora/atraso promedio', limites_atraso_promedio, puntajes_atraso_promedio)
DF_solicitud_credito = asignar_puntajes_personalizados(DF_solicitud_credito, 'Días mora/atraso acumulados', limites_atraso_acumulado, puntajes_atraso_acumulado)

DF_solicitud_credito['Puntaje Final'] = (DF_solicitud_credito['Días mora/atraso promedio_puntaje'] + DF_solicitud_credito['Días mora/atraso acumulados_puntaje']) / 2

# Score Final del cliente en créditos

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


# Paso 1: Agrupar los datos de DF_solicitud_credito por 'ID Cliente' y obtener listas de 'Puntaje Final'
puntajes_por_cliente = DF_solicitud_credito.groupby('ID Cliente nocode')['Puntaje Final'].apply(list)

# Paso 2: Aplicar la función ponderar_puntajes a cada lista de puntajes
puntajes_ponderados = puntajes_por_cliente.apply(ponderar_puntajes)

# Paso 3: Preparar los resultados para unirlos a DF_contactos
# Convertimos los resultados a un DataFrame y reseteamos el índice para obtener 'ID Cliente' como columna
puntajes_ponderados_df = puntajes_ponderados.reset_index()
puntajes_ponderados_df.rename(columns={'Puntaje Final': 'Puntaje Ponderado Creditos', 'ID Cliente nocode': 'ID CLIENTE'}, inplace=True)

# Paso 4: Unir los puntajes ponderados con DF_contactos
# Utilizamos 'ID CLIENTE' como la llave para la unión
DF_contactos = DF_contactos.merge(puntajes_ponderados_df, on='ID CLIENTE', how='left')



# Cálculo de función scoring

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

# Definiendo puntaje inicial para cada cliente
DF_contactos['puntaje_inicial']=500

# Si está perdido 0

# Paso 1: Crear columna booleana en DF_solicitud_credito
DF_solicitud_credito['Tiene Credito Perdido'] = DF_solicitud_credito['Clasificación perdidos/no perdidos'].apply(lambda x: x == 'Perdido')

# Paso 2: Agrupar esta información por cliente
clientes_con_credito_perdido = DF_solicitud_credito.groupby('ID Cliente nocode')['Tiene Credito Perdido'].any()

# Antes de unir con DF_contactos, renombramos la columna de índice para que coincida con la de DF_contactos
clientes_con_credito_perdido = clientes_con_credito_perdido.reset_index().rename(columns={'ID Cliente nocode': 'ID CLIENTE'})

# Paso 3: Unir esta información con DF_contactos
DF_contactos = DF_contactos.merge(clientes_con_credito_perdido, on='ID CLIENTE', how='left')

# Aquí definimos una función auxiliar que extrae los argumentos de cada fila

W1=0
W2=0.1
W3=0.1
W4=0.8

# Paso 4: Modificar la función aplicar_calculo
def aplicar_calculo(row):
    if row['Tiene Credito Perdido']:
        return 0
    else:
        return calcular_score(row['puntaje_inicial'], W1, W2, row['Numero de creditos REAL_puntaje'], row['Promedio monto créditos_puntaje'], row['Puntaje Ponderado Creditos'])

# Ahora aplicamos la función a cada fila
DF_contactos['score_calculado'] = DF_contactos.apply(aplicar_calculo, axis=1)

# Mostramos el resultado
print(DF_contactos)