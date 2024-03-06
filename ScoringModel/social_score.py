import pandas as pd
import numpy as np
import os
import sys
sys.path.append('../')  # Asume que la carpeta contenedora está un nivel arriba en la jerarquía

from api_airtable import get_table_Airtable
from python_utilities.utils import read_yaml_from_s3, RODAAPP_BUCKET_PREFIX, logger



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

def ajuste_x_referidos_atrasados(id_referidor, dataframe_auxiliar, df_contacto, UMBRAL_BONUS, INCREMENTO_POR_REFERIDO, DECREMENTO_POR_REFERIDO):

    logger.info(f"Calculando ajustes por referidos atrasados para el cliente {id_referidor}...")
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


    return df_contacto


def get_info_referido(id_referidor, df_contacto):

    logger.info(f"Obteniendo información sobre referidos del cliente {id_referidor}")
    # Filtrar referidos que tienen "Créditos en Proceso" = 'VERDADERO'
    referidos = df_contacto[(df_contacto['ID Referidor Nocode'] == id_referidor) & (df_contacto['Créditos en Proceso'] == 'VERDADERO')]

    if not referidos.empty:
        # Extraer la información deseada para cada referido y almacenarla en una lista de diccionarios
        info_referidos_list = referidos.apply(lambda row: {
            'ID CLIENTE': row['ID CLIENTE'],
            'Puntaje_Final': row['Puntaje_Final'],
            'Afectado_x_red': row['Afectado_x_red'],
            'Créditos en Proceso': row['Créditos en Proceso'],
            'Último Días de Atraso': row['Último Días de Atraso']
        }, axis=1).tolist()

        # Convertir la lista de diccionarios a string para almacenamiento
        info_referidos_str = str(info_referidos_list)
        
        # Actualizar la columna 'Info_Referidos' en df_contacto para el cliente especificado
        df_contacto.loc[df_contacto['ID CLIENTE'] == id_referidor, 'Info_Referidos'] = info_referidos_str
    else:
        logger.info(f"No se encontraron referidos en proceso para el cliente {id_referidor}")

    return referidos


def cleaning_social_variables(df_contacto, df_credito):

    logger.info("Cleaning social variables...")

    # Asegura que los IDs se manejen correctamente y limpia valores NaN
    df_contacto['ID CLIENTE'] = pd.to_numeric(df_contacto['ID CLIENTE'], errors='coerce').astype('float64')
    df_credito['ID Cliente nocode'] = pd.to_numeric(df_credito['ID Cliente nocode'], errors='coerce').astype('float64')
    df_contacto['ID Referidor Nocode'] = pd.to_numeric(df_contacto['ID Referidor Nocode'], errors='coerce')
    df_contacto.dropna(subset=['ID CLIENTE'], inplace=True)
    df_credito.dropna(subset=['ID Cliente nocode'], inplace=True)

    return df_contacto, df_credito

def identificacion_referidores_perdidos(df_contacto):

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

    return df_contacto



def afectaciones_por_referidos(df_contacto, df_credito, INCREMENTO_POR_REFERIDO, DECREMENTO_POR_REFERIDO, UMBRAL_BONUS, PARAM_POR_PERDIDO):
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

    try:
        logger.info("Computing social score...")

        df_contacto, df_credito = cleaning_social_variables(df_contacto, df_credito)

        df_contacto = identificacion_referidores_perdidos(df_contacto)
                
        # ---------Sección Validar créditos en proceso-----------------
        df_contacto = validacion_creditos_en_proceso(df_contacto, df_credito)


        def calculo_ajustes(df_contacto, UMBRAL_BONUS, INCREMENTO_POR_REFERIDO, DECREMENTO_POR_REFERIDO):

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

                print("Dataframe_auxiliar creado...")
                print(dataframe_auxiliar)


                df_contacto = ajuste_x_referidos_atrasados(id_cliente, dataframe_auxiliar, df_contacto, UMBRAL_BONUS, INCREMENTO_POR_REFERIDO, DECREMENTO_POR_REFERIDO)

                print(dataframe_auxiliar['Tiene Credito Perdido'])

                if dataframe_auxiliar['Tiene Credito Perdido'].any():

                    logger.info(f"el cliente {id_cliente} tiene un referido perdido")
                    # Si hay algún referido perdido, actualiza 'REFERIDO_Perdido' en df_contacto para el cliente actual
                    df_contacto.loc[df_contacto['ID CLIENTE'] == id_cliente, 'REFERIDO_Perdido'] = 'VERDADERO'
 
                    referidos_sin_perdida = dataframe_auxiliar[~dataframe_auxiliar['Tiene Credito Perdido']]

                    # Para cada cliente referido sin crédito perdido, realizar ajustes en df_contacto
                    for id_referido in referidos_sin_perdida['ID CLIENTE']:
                        # Restar 0.5 del ajuste calculado

                        logger.info(f"Referido {id_referido} fue afectado por su red, dkcmdskjcndskcdscndkjcd")
                        df_contacto.loc[df_contacto['ID CLIENTE'] == id_referido, 'ajuste_calculado'] -= 0.5
                        
                        # Actualizar 'Afectado_x_red' como 'VERDADERO'
                        df_contacto.loc[df_contacto['ID CLIENTE'] == id_referido, 'Afectado_x_red'] = 'VERDADERO'

                        print(f"hOLA ESTE MENSAJE ES SI ENTRÓ A ACTUALIZAR EN EL REFERIDO {id_referido}")


            '''
            para cada cliente:
                obtener información sobre referidos (en un dataframe auxiliar)
                dataframe_auxiliar = get_info_referido(id_cliente, df_contacto)
                inicializar variable 'REFERIDO_Perdido' como 'FALSO' en df_contacto
                identificar si existe algún referido perdido en el dataframe auxiliar

                ajuste_x_referidos_atrasados(id_referidor, dataframe_auxiliar, df_contacto, UMBRAL_BONUS, INCREMENTO_POR_REFERIDO, DECREMENTO_POR_REFERIDO):

                    contar cuantos clientes en dataframe_auxiliar tienen 'Último Días de Atraso' > 0

                    calcular porcentaje de referidos con atraso (Clientes con 'Último Días de Atraso' > 0/total de clientes referidos)

                    para cada cliente en dataframe_auxiliar evaluar:

                        if porcentaje_con_atraso < UMBRAL_BONUS and referido['Puntaje_Final'] > 800:
                            ajuste_calculado += INCREMENTO_POR_REFERIDO
                        if referido['Último Días de Atraso'] > 0:
                            ajuste_calculado -= DECREMENTO_POR_REFERIDO                    
                    
                    
                    actualizar df_contacto['ajuste_calculado']    para los el cliente en cuestión


                    return df_contacto


                si existe referido perdido en dataframe auxiliar:
                    
                    actualizar variable 'REFERIDO_Perdido' al referidor en df_contacto con 'VERDADERO'
                    a todos los clientes presentes en el dataframe auxiliar que no tengan crédito perdido, se debe restar el ajuste calculado (presente en el dataframe original df_contactos) -0.5
                    a todos los clientes presentes en el dataframe auxiliar que no tengan crédito perdido, se debe actualizar la bandera 'Afectado_x_red' como 'VERDADERO'

  
            '''

            return df_contacto

        
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



