import pandas as pd
import numpy as np
import os
import sys
sys.path.append('../')  # Asume que la carpeta contenedora está un nivel arriba en la jerarquía

from api_airtable import get_table_Airtable
from python_utilities.utils import read_yaml_from_s3, RODAAPP_BUCKET_PREFIX


INCREMENTO_POR_REFERIDO = 0.05  # 10% de incremento por cada referido que cumpla la condición



def buscar_info_referidos(df):

    """
    Esta función busca y asocia los referidos de cada cliente en el DataFrame proporcionado. Para cada cliente, identifica a sus referidos basándose en el 'ID Referidor Nocode' y compila información relevante de estos referidos en un diccionario, que luego se asigna a una nueva columna en el DataFrame.

    Args:
    df (pd.DataFrame): DataFrame que contiene la información de los clientes, incluyendo su ID y el ID de quien los refirió ('ID Referidor Nocode').

    Returns:
    pd.DataFrame: El mismo DataFrame de entrada con una columna adicional ('Referidos') que contiene un diccionario por cada fila/cliente. Este diccionario tiene como claves los IDs de los referidos y como valores otro diccionario con información específica de cada referido (créditos en proceso, días de atraso, ID del último crédito, si tiene crédito perdido y su puntaje final).
    """

    # Crear una columna nueva para almacenar los referidos como diccionarios
    df['Referidos'] = None
    
    # Iterar sobre el DataFrame para actualizar cada fila con su correspondiente diccionario de referidos
    for index, row in df.iterrows():
        id_cliente = row['ID CLIENTE']
        # Buscar todos los referidos que tienen este ID como referidor
        referidos_df = df[df['ID Referidor Nocode'] == id_cliente]
        
        # Crear un diccionario para almacenar la información de cada referido
        referidos_info = {}
        for _, referido_row in referidos_df.iterrows():
            # Extraer la información requerida de cada referido
            id_referido = referido_row['ID CLIENTE']
            creditos_proceso = referido_row['Créditos en Proceso']
            ultimo_dias_atraso = referido_row['Último Días de Atraso']
            ultimo_id_credito = referido_row['Último ID CRÉDITO']
            ultimo_tiene_credito_perdido = referido_row['Tiene Credito Perdido']
            ultimo_puntaje = referido_row['Puntaje_Final']
            
            # Almacenar la información en el diccionario con el ID del referido como clave
            referidos_info[id_referido] = {
                'Créditos en Proceso': creditos_proceso,
                'Último Días de Atraso': ultimo_dias_atraso,
                'Último ID CRÉDITO': ultimo_id_credito,
                'Crédito Perdido' : ultimo_tiene_credito_perdido,
                'Puntaje' : ultimo_puntaje
            }
        
        # Actualizar la columna 'Referidos' con el diccionario de referidos
        df.at[index, 'Referidos'] = referidos_info

    return df

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
    ultimo_credito_por_cliente = df_credito_en_proceso.sort_values(by='Fecha desembolso', ascending=False).drop_duplicates('ID Cliente nocode')

    tiene_credito_en_proceso = df_credito_en_proceso['ID Cliente nocode'].unique()
    df_contacto['Créditos en Proceso'] = df_contacto['ID CLIENTE'].apply(lambda x: 'VERDADERO' if x in tiene_credito_en_proceso else 'FALSO')

    dias_atraso_dict = ultimo_credito_por_cliente.set_index('ID Cliente nocode')['Días de atraso'].to_dict()
    id_credito_dict = ultimo_credito_por_cliente.set_index('ID Cliente nocode')['ID CRÉDITO'].to_dict()

    # Asigna directamente np.nan en lugar de 'N/A' antes de la conversión a numérico
    df_contacto['Último Días de Atraso'] = df_contacto['ID CLIENTE'].map(dias_atraso_dict)
    df_contacto['Último ID CRÉDITO'] = df_contacto['ID CLIENTE'].map(id_credito_dict)

    # Solo reemplazar por np.nan si 'Créditos en Proceso' es 'FALSO' (Omitido ya que np.nan será el valor por defecto si no se encuentra el mapeo)
    # No es necesario reasignar 'N/A' y luego reemplazarlo, ya que el mapeo con .map() ya asignará np.nan a los que no encuentre

    # Convertir las columnas a numérico, asumiendo que np.nan ya está asignado a los valores faltantes
    df_contacto['Último Días de Atraso'] = pd.to_numeric(df_contacto['Último Días de Atraso'], errors='coerce')
    df_contacto['Último ID CRÉDITO'] = pd.to_numeric(df_contacto['Último ID CRÉDITO'], errors='coerce')

    return df_contacto



def calcular_afectaciones(referidos, incremento_por_referido=INCREMENTO_POR_REFERIDO, decremento_por_mora=0.1):
    """
    Calcula el ajuste (incremento o decremento) porcentual del puntaje final de un referidor basado en la condición de sus referidos.
    
    Args:
    referidos (dict): Un diccionario conteniendo información sobre los referidos del cliente.
    incremento_por_referido (float): El incremento porcentual aplicado al puntaje final del referidor por cada referido válido con puntaje > 800.
    decremento_por_mora (float): El decremento porcentual aplicado al puntaje final del referidor por cada referido en mora ('Último Días de Atraso' > 0).
    
    Returns:
    float: El ajuste porcentual a aplicar al puntaje final del referidor. Si algún referido tiene 'Crédito Perdido': 'VERDADERO',
          retorna 0 inmediatamente. Si hay referidos en mora, se resta un 10% del score del referidor por cada uno.
    """

    tiene_credito_perdido = any(info_referido.get('Crédito Perdido') == True for _, info_referido in referidos.items())
    if tiene_credito_perdido:
        # Devolvemos 0 como ajuste y True para indicar que se debe anular el puntaje
        return 0, True

    total_referidos_evaluados = 0
    ajuste_puntaje_final = 0
    
    for _, info_referido in referidos.items():
        if info_referido.get('Créditos en Proceso') == 'VERDADERO':
            total_referidos_evaluados += 1
            dias_atraso = pd.to_numeric(info_referido.get('Último Días de Atraso', 0), errors='coerce')
            
            # Evaluar 'Último Días de Atraso' para referidos con créditos en proceso
            if not pd.isna(dias_atraso) and dias_atraso > 0:
                # En lugar de incrementar, restamos el 10% del score del referidor por cada referido en mora
                ajuste_puntaje_final -= decremento_por_mora
            else:
                # Evaluar incremento solo si el referido tiene un puntaje > 800 y no está en mora
                puntaje_referido = pd.to_numeric(info_referido.get('Puntaje', 0), errors='coerce')
                if not pd.isna(puntaje_referido) and puntaje_referido > 800:
                    ajuste_puntaje_final += incremento_por_referido
    
    # Si no hay referidos con créditos en proceso, no modificar el puntaje final del referidor
    if total_referidos_evaluados == 0:
        return 0, False
    
    return ajuste_puntaje_final, False



def afectaciones_por_referidos(df_contacto,df_credito):
    
    """
    Calcula y aplica las afectaciones al puntaje final de los clientes en el DataFrame de contacto basadas en la información de sus referidos y los créditos en proceso. Limpia y prepara los datos de contacto, valida los créditos en proceso, busca información de referidos, y calcula el ajuste porcentual del puntaje final. Finalmente, ajusta el puntaje final de los referidores basado en la condición de sus referidos y asegura que el puntaje ajustado no exceda los límites establecidos.

    Args:
    df_contacto (pd.DataFrame): DataFrame con información de los clientes, incluyendo su ID, si fue referido por otro cliente, y su puntaje final.
    df_credito (pd.DataFrame): DataFrame con información de los créditos, incluyendo el estado del crédito, el ID del cliente, y otros detalles relevantes.

    Returns:
    pd.DataFrame: El DataFrame de contacto actualizado con las columnas 'Incremento_Puntaje_Final', 'Tiene_Credito_Perdido' y 'Puntaje_Final_Ajustado', este último refleja el puntaje final ajustado basado en las afectaciones calculadas de los referidos y se asegura de que esté dentro del rango de 0 a 1000.
    """

    print("Entró a afectaciones")

    df_contacto['¿Referido RODA?'] = df_contacto['¿Referido RODA?'].replace({'No se encuentra': 'No'})
    df_contacto['ID Referidor Nocode'] = df_contacto['ID Referidor Nocode'].fillna('No tiene')

    print("Cleaning exitoso")

    df_contacto = validacion_creditos_en_proceso(df_contacto, df_credito)
    df_contacto = buscar_info_referidos(df_contacto)

    # Aplicar 'calcular_afectaciones' y ajustar el puntaje final
    resultados = df_contacto['Referidos'].apply(lambda referidos: calcular_afectaciones(referidos))
    df_contacto['Incremento_Puntaje_Final'], df_contacto['Tiene_Credito_Perdido'] = zip(*resultados)

    # Asignar Puntaje_Final_Ajustado basado en el indicador de crédito perdido
    df_contacto['Puntaje_Final_Ajustado'] = df_contacto.apply(
        lambda row: 0 if row['Tiene_Credito_Perdido'] else row['Puntaje_Final'] * (1 + row['Incremento_Puntaje_Final']),
        axis=1
    )

    # Ajustar el puntaje final ajustado para que no exceda 1000 y no sea inferior a 0
    df_contacto['Puntaje_Final_Ajustado'] = np.clip(df_contacto['Puntaje_Final_Ajustado'], 0, 1000)


    print("Proceso completado")

    return df_contacto
