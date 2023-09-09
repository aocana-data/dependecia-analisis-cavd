import pandas as pd
from .verificar_data import verificar_data


def estado_columnas(dataframe:any, chars_null:list, option:bool) -> any:
    '''
    _summary_

    Un analisis de cada columna, si contiene ciertos caracteres o lineas de texto de 
    agrupa por la cantidad de cada uno de esos caracteres.

    @params
        dataframe(df) : dataframe a analizar
        chars_null(ls) : lista predeterminada en la intancia de la clase

    @return
        values(df) : dataframe visual del set analizado.
    '''

    list_dfs = []

    
    for col in dataframe.columns :

        resultados_por_col = verificar_data(dataframe , col, chars_null , option)
        row = resultados_por_col.get("chequeo",None)

        if row != None:
            df_row = pd.DataFrame([row],index=[col])
            df_row.index.name = col
            list_dfs.append(df_row)

        #TODO=> cantidad de duplicados

    list_dfs = [frame for frame in list_dfs if frame is not None]
    
    if list_dfs == []:
        print('No existen valores que tengan esos caracteres')
        return None

    return pd.concat(list_dfs)
