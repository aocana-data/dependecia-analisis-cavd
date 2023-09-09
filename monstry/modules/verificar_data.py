
def verificar_data(dataframe, col , lista_chequeo, completo):
    '''
    _summary_

    Permite crear un analisis previo de la columna, usando un valor predeterminado de caracteres
    que se deben contar y visualizar, con lo cual nos otorga información previa del estado de la
    columna

    Parameters : 
        dataframe(df)   : el dataframe usado
        col(str)        : columna a analizar
        completo(bool) -> optional kwarg completo = True para poder visualizar todos
        los campos incluyendo la cantidad de campos nulos y su porcentaje frente al total de registros
        en la columna
    

    TODO => agregar metodo para poder agrandar ese set de caracteres extraños que se puedan visualizar 
    o str que deba hacer como factor de busqueda
    '''
    
    len_set = len( dataframe )
  
    df_chequeo = {}

    # analisis de nulos
   
    if completo == True:
            df_chequeo['CANTIDAD VALORES UNICOS'] = dataframe[col].nunique()
            df_chequeo['CANTIDAD VALORES DUPLICADOS'] = dataframe[col].count() - dataframe[col].nunique()
            df_chequeo['TOTAL DE NULOS'] = dataframe[col].isna().sum() # cantidad de valores nulos
            df_chequeo['PORCENTAJE DE NULOS'] = round(df_chequeo['TOTAL DE NULOS'] * 100/len_set,2)

    for check in lista_chequeo:
        # conteo agrupado de repeticiones de esos caracteres
        df_chequeo[check]  = sum(dataframe[col].apply(lambda x: x.strip() if isinstance(x, str) else x) == check)

        if check == '':
            df_chequeo['VACIO'] = sum(dataframe[col].apply(lambda x: x.strip() if isinstance(x, str) else x) == check)

    total = [ data for data in df_chequeo.values() ]

    if sum(total) == 0 and completo == False:
        return {}
    

    return {
        "chequeo":df_chequeo ,
        "chars_nulls": total
    }


