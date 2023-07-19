import pandas as pd
from .total_chars_null import  total_chars_null

def completitud(    dataframe:any,
                    nombre_tabla:str,
                    data_types:dict,
                    reglas_completitud_config:dict,
                    chars_null:list,
                    opcion:str
                    ):

    # que tipó de completitud estamos buscando

    dict_seleccion = {
        'solo_nan': completitud_solo_nan,
        'con_reglas': completitud_con_rules,
        'default':completitud_default
    }

    # encabezados de la tabla resumen
    headers = {
        'COLUMNA' : [] ,
        'NOMBRE TABLA':[],
        'REGISTROS TOTALES' : [],
        'CANTIDAD DE INCOMPLETOS' : [],
        'CANTIDAD DE COMPLETOS' : [],
        'PORCENTAJE COMPLETITUD' : []
    }

    tabla_resumen = pd.DataFrame(headers)

    # formato de salida de datos

    completitud_types = {
            'COLUMNA':'str',
            'NOMBRE TABLA':'str',
            'REGISTROS TOTALES' : 'int64',
            'CANTIDAD DE INCOMPLETOS' : 'int64',
            'CANTIDAD DE COMPLETOS' : 'int64',
            'PORCENTAJE COMPLETITUD' : 'float64'
    }

    # parametros a pasar 

    options = {
        'dataframe':dataframe,
        'nombre_tabla':nombre_tabla,
        'data_types':data_types,
        'completitud_types':completitud_types,
        'total_registros':len(dataframe),
        'tabla_resumen':tabla_resumen,
        'reglas_completitud_config':reglas_completitud_config,
        'chars_null':chars_null
    }

    
    res_completitud = dict_seleccion.get(opcion)

    tabla_resumen = res_completitud(**options)


    return tabla_resumen


def completitud_con_rules(  dataframe:any,
                            nombre_tabla:str,
                            data_types:dict,
                            completitud_types:dict,
                            total_registros:int,
                            tabla_resumen:any,
                            reglas_completitud_config:dict,
                            chars_null:list
                        ):

    print('Analisis de completitud usando reglas de completitud determinada en el archivo de configuracion')
    
    completitud_reglas = seteador_reglas_columna(dataframe, chars_null ,reglas_completitud_config)

    for col in dataframe.columns:

        columna_data = {
            'dataframe':dataframe,
            'col':col,
            'data_types':data_types
        }
        
        columna_val_nulls= eliminacion_nulls_cantidad(**columna_data)

        cantidad_nulls = columna_val_nulls['cantidad_nulls']
        columna_limpia = columna_val_nulls['columna_limpia']


        res_chars_null = completitud_reglas.get(col, 'None')

        if res_chars_null is None:
            print('Esa columna no existe en el dataframe creado')
            return None


        incompletitud_cantidad_col = total_chars_null(dataframe, col , res_chars_null ) + cantidad_nulls
        completitud_cantidad_col = total_registros - incompletitud_cantidad_col

                
        row_body = {'COLUMNA':[col],
                    'NOMBRE TABLA':[nombre_tabla],
                    'REGISTROS TOTALES' : [total_registros],
                    'CANTIDAD DE INCOMPLETOS' : [incompletitud_cantidad_col],
                    'CANTIDAD DE COMPLETOS' : [completitud_cantidad_col],
                    'PORCENTAJE COMPLETITUD' : [round(completitud_cantidad_col /total_registros * 100 ,2)]
            }

        
        row = pd.DataFrame(row_body)

        tabla_resumen=pd.concat([row, tabla_resumen], ignore_index= True).astype(completitud_types)

    return {
            'tabla_resumen':tabla_resumen,
            'chars_omitir_exactitud':completitud_reglas
    }


def completitud_default( dataframe:any,
                         nombre_tabla:str ,
                         data_types:dict,
                         completitud_types:dict,
                         total_registros:int,
                         tabla_resumen:any,
                         reglas_completitud_config:dict,
                         chars_null:list
                        ):

    print('Analisis de completitud realizado teniendo en cuenta los valores por default')
           
    for col in dataframe.columns:

        columna_data = {
            'dataframe':dataframe,
            'col':col,
            'data_types':data_types
        }
        
        columna_val_nulls= eliminacion_nulls_cantidad(**columna_data)

        cantidad_nulls = columna_val_nulls['cantidad_nulls']
        columna_limpia = columna_val_nulls['columna_limpia']
        
        # cantidad de incompletos considerando los valores pasados como elementos de char + nan
        incompletitud_cantidad_col = total_chars_null(columna_limpia, col, chars_null ) + cantidad_nulls

        # le anexo la cantidad de nulls en la limpieza de la columna
        completitud_cantidad_col = total_registros - incompletitud_cantidad_col
        
        row_body = {'COLUMNA':[col],
                    'NOMBRE TABLA':[nombre_tabla],
                    'REGISTROS TOTALES' : [total_registros],
                    'CANTIDAD DE INCOMPLETOS' : [incompletitud_cantidad_col],
                    'CANTIDAD DE COMPLETOS' : [completitud_cantidad_col],
                    'PORCENTAJE COMPLETITUD' : [round(completitud_cantidad_col /total_registros * 100 ,2)]
            }

        row = pd.DataFrame(row_body)

        tabla_resumen=pd.concat([row, tabla_resumen], ignore_index= True).astype(completitud_types)
               

    return {
            'tabla_resumen':tabla_resumen,
            'chars_omitir_exactitud':chars_null
    }


def completitud_solo_nan( dataframe:any,
                          nombre_tabla:str,
                          data_types:dict,
                          completitud_types:dict,
                          total_registros,
                          tabla_resumen:any,
                          reglas_completitud_config:dict,
                          chars_null:list
                        ):

    print('Analisis de completitud sin tener en cuenta ningún tipo de caracter especial, simplemente por valores nulos')

    for col in dataframe.columns:

        row_body = {'COLUMNA':[col],
                    'NOMBRE TABLA':[nombre_tabla],
                    'REGISTROS TOTALES' : [total_registros],
                    'CANTIDAD DE INCOMPLETOS' : [dataframe[col].isnull().sum(axis = 0)],
                    'CANTIDAD DE COMPLETOS' : [dataframe[col].count()],
                    'PORCENTAJE COMPLETITUD' : [round(dataframe[col].count() /total_registros * 100 ,2)]
            }

        row = pd.DataFrame(row_body)

        tabla_resumen=pd.concat([row, tabla_resumen], ignore_index= True).astype(completitud_types)
        
    return {
            'tabla_resumen':tabla_resumen,
            'chars_omitir_exactitud':chars_null
    }




def eliminacion_nulls_cantidad( dataframe, col ,data_types):

    type_deberia = data_types.get(col,'object')
    
    cantidad_nulls = dataframe[col].isna().sum() # cuenta los nulls de esa columna y lo almacena dentro de la variable contadora

    columna_limpia = dataframe[[col]].dropna() # remueve los nulls

    columna_limpia = columna_limpia.astype(type_deberia).astype('str') # lo castea al valor determinado por el usuario

    return {
        'cantidad_nulls': cantidad_nulls,
        'columna_limpia':columna_limpia
    }





def seteador_reglas_columna(dataframe , chars_null,reglas_completitud_config):

    cols = dataframe.columns
    
    reglas_prefixed = {col:chars_null for col in cols}
    
    res_reglas = {**reglas_prefixed , **reglas_completitud_config}
    
    return res_reglas

