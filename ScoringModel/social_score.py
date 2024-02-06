import pandas as pd
import numpy as np
import os
import sys
sys.path.append('../')  # Asume que la carpeta contenedora está un nivel arriba en la jerarquía

from api_airtable import get_table_Airtable
from python_utilities.utils import read_yaml_from_s3, RODAAPP_BUCKET_PREFIX


INCREMENTO_POR_REFERIDO = 0.05  # 10% de incremento por cada referido que cumpla la condición



def buscar_info_referidos(df):
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

def validacion_creditos_en_proceso(df_contacto, df_credito): # PENDIENTE VALIDAR

    '''
    El objetivo de esta función es crear una columna en DF_Contactos que diga si el Cliente actualmente tiene o no créditos EN PROCESO
    

    '''
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
    total_referidos_evaluados = 0
    ajuste_puntaje_final = 0
    
    # Revisar si algún referido tiene 'Crédito Perdido': 'VERDADERO'
    for _, info_referido in referidos.items():
        if info_referido.get('Crédito Perdido') == 'VERDADERO': # REVISAR
            # Si algún referido perdió un crédito, el puntaje final ajustado del referidor es 0
            return 0
    
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
        return 0
    
    return ajuste_puntaje_final



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
    df_contacto = buscar_info_referidos(df_contacto)

    # Asumiendo que df es tu DataFrame y 'Referidos' es la columna con los diccionarios
    df_contacto['Incremento_Puntaje_Final'] = df_contacto['Referidos'].apply(calcular_afectaciones)

    # Si deseas aplicar el incremento al Puntaje_Final, primero asegúrate de tener esa columna
    # Por ejemplo, si ya tienes un 'Puntaje_Final' y quieres incrementarlo según la lógica definida:
    df_contacto['Puntaje_Final_Ajustado'] = df_contacto['Puntaje_Final'] * (1 + df_contacto['Incremento_Puntaje_Final'])
    
    # Ajustar el puntaje final ajustado para que no exceda 1000
    df_contacto['Puntaje_Final_Ajustado'] = np.where(df_contacto['Puntaje_Final_Ajustado'] > 1000, 1000, df_contacto['Puntaje_Final_Ajustado'])
    # Que no sea inferior a 0
    df_contacto['Puntaje_Final_Ajustado'] = np.where(df_contacto['Puntaje_Final_Ajustado'] < 0, 0, df_contacto['Puntaje_Final_Ajustado'])

    print("Proceso completado")
    # Mostrar algunas filas del DataFrame para verificar los resultados
    # print(df_contacto[['ID CLIENTE', 'Referidos']].head())



    '''
    Si el 20% o más de los referidos tienen en la variable 'Último Días de Atraso'>0, NINGUN REFERIDO SUMA NADA. De lo contrario, por cada referido que en la variable 'Puntaje' tenga >800, la columna 'Puntaje_Final' del cliente tiene un incremento del 10%
    
    - Cualquier referido en mora (O con un puntaje inferior a 400, hay que probar los 2 casos) resta 10% del score del **referidor**
    
    Si existe un referido perdido. Tanto el score del referido como del referidor son 0. Los demás referidos deberían restarleses el 50% de su score (Validar si de por si ya se están viendo afectados)

    '''

    #------------Referidor-----------------------

    '''
    Si mi referidor entra en mora (O tiene un puntaje menor o igual a X número) resta 10% del score del referido
    '''

    return df_contacto
