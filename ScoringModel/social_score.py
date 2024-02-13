import pandas as pd
import numpy as np
import os
import sys
sys.path.append('../')  # Asume que la carpeta contenedora está un nivel arriba en la jerarquía

from api_airtable import get_table_Airtable
from python_utilities.utils import read_yaml_from_s3, RODAAPP_BUCKET_PREFIX



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



def afectaciones_por_referidos(df_contacto, df_credito, INCREMENTO_POR_REFERIDO, DECREMENTO_POR_REFERIDO, UMBRAL_BONUS):
    """
    Ajusta los puntajes finales de los clientes en base a la condición de sus referidos,
    considerando créditos en proceso, días de atraso, y si algún referido ha perdido un crédito.
    
    Args:
        df_contacto (pd.DataFrame): DataFrame que contiene información de los clientes. 
            Debe incluir las columnas 'ID CLIENTE', 'Puntaje_Final', 'ID Referidor Nocode', 
            'Créditos en Proceso' y 'Tiene Credito Perdido'.
        df_credito (pd.DataFrame): DataFrame que contiene información de los créditos.
            Debe incluir las columnas 'ID Cliente nocode', 'Último Días de Atraso', entre otros.
            Este DataFrame no se utiliza directamente en la función, pero se incluye por completitud
            y posibles usos futuros o validaciones.
        INCREMENTO_POR_REFERIDO (float): Valor que se añadirá al puntaje final del cliente por cada 
            referido con un puntaje final mayor a 800, siempre que menos del UMBRAL_BONUS porcentaje 
            de sus referidos tengan días de atraso.
        DECREMENTO_POR_REFERIDO (float): Valor que se restará del puntaje final del cliente por cada 
            referido con días de atraso.
        UMBRAL_BONUS (float): Umbral utilizado para determinar si se aplican bonificaciones por referidos 
            basado en el porcentaje de referidos sin atrasos. Representa un porcentaje (ej. 0.2 para 20%).
    
    Returns:
        pd.DataFrame: DataFrame 'df_contacto' actualizado con tres nuevas columnas:
            - 'Ajuste_calculado': Muestra el ajuste calculado basado en la lógica de referidos.
            - 'REFERIDO_Perdido': Indica con 'VERDADERO' o 'FALSO' si algún referido ha perdido un crédito.
            - 'Info_Referidos': Cadena de texto que representa una lista de diccionarios con información 
              detallada de cada referido evaluado, incluyendo su ID, puntaje final y días de atraso.
            - 'Puntaje_Final_Ajustado': El nuevo puntaje final ajustado del cliente, calculado basado en 
              los ajustes por referidos, asegurando que se mantenga en el rango de 0 a 1000.

    La función primero limpia y prepara los datos, luego evalúa los referidos de cada cliente para calcular 
    ajustes en su puntaje final. Se consideran tanto bonificaciones por referidos con alto puntaje y sin atrasos, 
    como penalizaciones por referidos con días de atraso. Además, se marca a aquellos clientes que tienen referidos 
    con créditos perdidos, lo cual impacta directamente en su puntaje final ajustado.
    """

    # Convertir ID a float64 y limpiar NaN
    df_contacto['ID CLIENTE'] = pd.to_numeric(df_contacto['ID CLIENTE'], errors='coerce').astype('float64')
    df_credito['ID Cliente nocode'] = pd.to_numeric(df_credito['ID Cliente nocode'], errors='coerce').astype('float64')
    df_contacto.dropna(subset=['ID CLIENTE'], inplace=True)
    df_credito.dropna(subset=['ID Cliente nocode'], inplace=True)

    # Validar créditos en proceso
    df_contacto = validacion_creditos_en_proceso(df_contacto, df_credito)

    def calcular_ajustes(id_referidor, df_contacto):
        # Filtrar referidos que tienen "Créditos en Proceso" = 'VERDADERO'
        referidos = df_contacto[(df_contacto['ID Referidor Nocode'] == id_referidor) & (df_contacto['Créditos en Proceso'] == 'VERDADERO')]
        ajuste_calculado = 0
        referido_perdido = 'FALSO'
        referidos_con_atraso = 0
        info_referidos = []  # Lista para almacenar información de cada referido
        print(f"Calculando ajustes para el referidor {id_referidor}, {len(referidos)} referidos en proceso")

        # Contar referidos con 'Último Días de Atraso' > 0 y recopilar información
        for _, referido in referidos.iterrows():
            info_referido = {'ID CLIENTE': referido['ID CLIENTE'], 'Puntaje_Final': referido['Puntaje_Final'], 'Último Días de Atraso': referido.get('Último Días de Atraso', 0)}
            if info_referido['Último Días de Atraso'] > 0:
                referidos_con_atraso += 1
            if 'Tiene Credito Perdido' in referido and referido['Tiene Credito Perdido'] == True:  # Asumiendo que 'Tiene Credito Perdido' es una columna en df_contacto
                referido_perdido = 'VERDADERO'
                print(f"Referido con crédito perdido encontrado: {referido['ID CLIENTE']}")
            info_referidos.append(info_referido)

        # Calcular el porcentaje de referidos con atraso
        porcentaje_con_atraso = (referidos_con_atraso / len(referidos)) if referidos.shape[0] > 0 else 0
        print(f"Porcentaje con atraso: {porcentaje_con_atraso}, Referido perdido: {referido_perdido}")

        for referido in info_referidos:
            if referido_perdido == 'VERDADERO':
                break  # Si ya se encontró un referido perdido, no se hacen más cálculos
            if porcentaje_con_atraso < UMBRAL_BONUS and referido['Puntaje_Final'] > 800:
                ajuste_calculado += INCREMENTO_POR_REFERIDO
            if referido['Último Días de Atraso'] > 0:
                ajuste_calculado -= DECREMENTO_POR_REFERIDO

        return ajuste_calculado, referido_perdido, info_referidos


    # Aplicar ajustes y almacenar información de referidos
    for id_cliente in df_contacto['ID CLIENTE'].unique():
        ajuste_calculado, referido_perdido, info_referidos = calcular_ajustes(id_cliente, df_contacto)
        df_contacto.loc[df_contacto['ID CLIENTE'] == id_cliente, 'Ajuste_calculado'] = ajuste_calculado
        df_contacto.loc[df_contacto['ID CLIENTE'] == id_cliente, 'REFERIDO_Perdido'] = referido_perdido
        df_contacto.loc[df_contacto['ID CLIENTE'] == id_cliente, 'Info_Referidos'] = str(info_referidos)

    # Calcular Puntaje_Final_Ajustado
    df_contacto['Puntaje_Final_Ajustado'] = df_contacto.apply(
        lambda row: 0 if row['REFERIDO_Perdido'] == 'VERDADERO' or row['Tiene Credito Perdido'] == True 
        else row['Puntaje_Final'] + row['Puntaje_Final'] * row['Ajuste_calculado'],
        axis=1
    )

    # Asegurar que el puntaje final ajustado esté en el rango de 0 a 1000
    df_contacto['Puntaje_Final_Ajustado'] = df_contacto['Puntaje_Final_Ajustado'].clip(lower=0, upper=1000)

    return df_contacto

