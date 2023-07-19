import re

import pandas as pd
from .total_chars_null import total_chars_null

def perc(rows, dataframe):
    return round(rows/len(dataframe) *100 ,2)



def completitud(dataframe:any,nombre_tabla:str , chars_null,filter_nan)->any:
    '''
    _summary_
    Retorna un dataframe con el registro de los campos nulos
    
    @params
        dataframe (pandasDataFrame)
        nombre_tabla (str)  : nombre de la tabla que se realiza el analis

    @return
        tabla_resumen(pd) : dataframe conn el analisis de completitud
    '''

    headers = {
        'COLUMNA' : [] ,
        'NOMBRE TABLA':[],
        'REGISTROS TOTALES' : [],
        'CANTIDAD DE INCOMPLETOS' : [],
        'CANTIDAD DE COMPLETOS' : [],
        'PORCENTAJE COMPLETITUD' : []
    }

    tabla_resumen = pd.DataFrame(headers)

    chars_null.append(filter_nan)
       
    for col in dataframe.columns:

        incompletitud_cantidad_col = total_chars_null(dataframe,col, chars_null )
        completitud_cantidad_col = len(dataframe) - incompletitud_cantidad_col
        
        row_body = {'COLUMNA':[col],
                    'NOMBRE TABLA':[nombre_tabla],
                    'REGISTROS TOTALES' : [len(dataframe)],
                    'CANTIDAD DE INCOMPLETOS' : [incompletitud_cantidad_col],
                    'CANTIDAD DE COMPLETOS' : [completitud_cantidad_col],
                    'PORCENTAJE COMPLETITUD' : [perc(completitud_cantidad_col,dataframe)]
            }

        row = pd.DataFrame(row_body)

        data_types = {
                    'COLUMNA':'str',
                    'NOMBRE TABLA':'str',
                    'REGISTROS TOTALES' : 'int64',
                    'CANTIDAD DE INCOMPLETOS' : 'int64',
                    'CANTIDAD DE COMPLETOS' : 'int64',
                    'PORCENTAJE COMPLETITUD' : 'float64'
                    }

        tabla_resumen=pd.concat([row, tabla_resumen], ignore_index= True).astype(data_types)
        
        #solo para remover le valor que vue asignado como default al software
        chars_null = [x for x in chars_null if x != filter_nan]

    return tabla_resumen
