import pandas as pd
import numpy as np
import os
import sys
sys.path.append('../')  # Asume que la carpeta contenedora está un nivel arriba en la jerarquía

from api_airtable import get_table_Airtable
from python_utilities.utils import read_yaml_from_s3, RODAAPP_BUCKET_PREFIX

def actualizar_referidos(df):
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
            
            # Almacenar la información en el diccionario con el ID del referido como clave
            referidos_info[id_referido] = {
                'Créditos en Proceso': creditos_proceso,
                'Último Días de Atraso': ultimo_dias_atraso,
                'Último ID CRÉDITO': ultimo_id_credito,
                'Crédito Perdido' : ultimo_tiene_credito_perdido
            }
        
        # Actualizar la columna 'Referidos' con el diccionario de referidos
        df.at[index, 'Referidos'] = referidos_info

    return df

# Creación de columna en DF_Contactos que me diga si tiene o no créditos en proceso: VERDADERO y FALSO
# Necesito añadir una condición que me diga si el referido tiene creditos EN PROCESO, sisi que me traiga la información de días_de_atraso del último crédito en proceso, si no que me retorne "na" en dias_de_atraso

def validacion_creditos_en_proceso(df_contacto, df_credito): # PENDIENTE VALIDAR

    '''
    El objetivo de esta función es crear una columna en DF_Contactos que diga si el Cliente actualmente tiene o no créditos EN PROCESO
    

    '''
    # Filtramos df_credito para obtener solo aquellos créditos que están EN PROCESO
    df_credito_en_proceso = df_credito[df_credito['ESTADO'] == 'EN PROCESO']
    
    # Agrupamos por 'ID Cliente nocode' y obtenemos el último crédito en proceso por fecha (asumiendo que existe una columna de fecha)
    ultimo_credito_por_cliente = df_credito_en_proceso.sort_values(by='Fecha desembolso', ascending=False).drop_duplicates('ID Cliente nocode')
    
    # Creamos un diccionario para mapear ID Cliente a VERDADERO/FALSO si tiene créditos en proceso
    tiene_credito_en_proceso = df_credito_en_proceso['ID Cliente nocode'].unique()
    df_contacto['Créditos en Proceso'] = df_contacto['ID CLIENTE'].apply(lambda x: 'VERDADERO' if x in tiene_credito_en_proceso else 'FALSO')
    
    # Creamos un diccionario para mapear ID Cliente a su último 'Días de atraso' y 'ID CRÉDITO'
    dias_atraso_dict = ultimo_credito_por_cliente.set_index('ID Cliente nocode')['Días de atraso'].to_dict()
    id_credito_dict = ultimo_credito_por_cliente.set_index('ID Cliente nocode')['ID CRÉDITO'].to_dict()
    
    # Aplicamos el mapeo para crear las nuevas columnas
    df_contacto['Último Días de Atraso'] = df_contacto['ID CLIENTE'].map(dias_atraso_dict).fillna('N/A')
    df_contacto['Último ID CRÉDITO'] = df_contacto['ID CLIENTE'].map(id_credito_dict).fillna('N/A')
    
    # Condición para llenar las nuevas columnas solo si 'Créditos en Proceso' es VERDADERO
    df_contacto['Último Días de Atraso'] = df_contacto.apply(lambda x: x['Último Días de Atraso'] if x['Créditos en Proceso'] == 'VERDADERO' else 'N/A', axis=1)
    df_contacto['Último ID CRÉDITO'] = df_contacto.apply(lambda x: x['Último ID CRÉDITO'] if x['Créditos en Proceso'] == 'VERDADERO' else 'N/A', axis=1)
    
    return df_contacto


def calcular_afectaciones(referidos):
    # Aquí implementarías la lógica de cálculo de afectaciones
    # basada en la información de referidos
    # Por ejemplo, calcular el porcentaje de referidos en mora
    # y ajustar el puntaje del referidor según tus reglas
    pass




def afectaciones_por_referidos(df_contacto,df_credito):
    
    
    '''
    Here we are building social_score

    '''

    print("Entró a afectaciones")

    df_contacto['¿Referido RODA?'] = df_contacto['¿Referido RODA?'].replace({'No se encuentra': 'No'})
    df_contacto['ID Referidor Nocode'] = df_contacto['ID Referidor Nocode'].fillna('No tiene')
    print("Cleaning exitoso")

    df_contacto = validacion_creditos_en_proceso(df_contacto, df_credito)


    # Aplicamos la función a cada fila del DataFrame
    df_contacto = actualizar_referidos(df_contacto)
    print("Proceso completado")
    # Mostrar algunas filas del DataFrame para verificar los resultados
    # print(df_contacto[['ID CLIENTE', 'Referidos']].head())



    '''
    - Si el 20% o más de los referidos están en mora, NINGUN REFERIDO SUMA NADA
        - De lo contrario, por cada referido entre 800 y 1000, el score de referidor aumenta en 10%*(Score-800) (Pendiente definir si 10% está bien)

    - Cualquier referido en mora (O con un puntaje inferior a 400, hay que probar los 2 casos) resta 10% del score del **referidor**
    
    Si existe un referido perdido. Tanto el score del referido como del referidor son 0. Los demás referidos deberían restarleses el 50% de su score (Validar si de por si ya se están viendo afectados)

    '''

    #------------Referidor-----------------------

    '''
    Si mi referidor entra en mora (O tiene un puntaje menor o igual a X número) resta 10% del score del referido
    '''

    return df_contacto
