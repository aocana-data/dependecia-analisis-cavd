import re
from typing import Optional

def columns_matcher(dataframe,regx:Optional['list'] = None) -> list:
    '''
    _summary_
    
    Retorna una lista con columnas que hagan match con los patrones ingresados
    Valor default : ['.*documento*','.*sex.*','.*genero.*' ,'.*nacion.*', '.*pais.*']

    @params
        dataframe (pandasDataFrame)
        regex (list<str>): List de expresiones regulares a anexar
    
    @return
        data_columns(list<str>) : List de columnas que hacen primer match con lo requerido
    '''
    
    regexp_list=['.*documento*','.*sex.*','.*genero.*' ,'.*nacion.*', '.*pais.*']
    
    data_columns = []
    
    if regx:
        ''' bloque de extensión de regexp para match de columnas(nombres)'''
        print(f"Se agregan los siguientes Regexp a los elementos de busqueda: {regx}")
        regexp_list.extend(regx)
    

    print('Inicio de busqueda de columnas\n')


    for index , col in enumerate(dataframe.columns):
       
        for exp in regexp_list:
            
            if (re.match(exp,col)):
                data_columns.append(col)

    print(f"\nLas columnas que hicieron match son: \n")
    
    for index, col in enumerate(data_columns, start = 1):
        print(f"{index } : {col}")
        
    return data_columns