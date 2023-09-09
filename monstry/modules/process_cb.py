
def process_cb(col:str, cb:any , dataset:any , chars_omitir_exactitud , data_types)->dict:

    '''
    _summary_
    
    Genera un dict con el analisis de exactitud de un dataset determinado, los cuales cuenten con 
    el dict de rules que permitan analizar su calidad de exactitud

    @params
        col(str) :  nombre de la columna a analizar
        cb(fn)      :  funcion que analiza la exactitud
        dataset(df) :  dataframe a analizar
    
    @return 
        data_return : dict de retorno con los analisis acumulados


    * Primera instancia : Remueve los valores nulos.
    * Segunda instancia : Aplica una funcion como callback el cual nos devuelve los valores True/False 
    * Tercera instancia : Cuenta los valores

    FORMATO SUGERIDO PARA DESPLIEGE:

    '''
 
    #Limpieza del valor default de filtrado
    bi_columna = limpieza_de_columna(dataset , col , chars_omitir_exactitud, data_types)
    

    #Data que aplica la regla de chequeo
    bi_data = bi_columna[col].apply(cb).value_counts()
    
    inexacto_dataframe_sample = {}
    exacto_dataframe_sample =   {}
    try:
        if bi_data.get(False,0) != 0:
            inexacto_dataframe_sample[col] = bi_columna[bi_columna[col].apply(cb) == False][col].drop_duplicates().head()
            exacto_dataframe_sample[col]= bi_columna[bi_columna[col].apply(cb) == True][col].drop_duplicates().head()

    except Exception as e:
        print(e)

    data_return = {}

    data_return['COLUMNA'] = col
    data_return['EXACTO'] = bi_data.get(True,0)
    data_return['INEXACTO'] = bi_data.get(False,0)


    
    if len(bi_columna) == 0 :
        print(f'''
              
        --!-->    WARNING:
                    LA COLUMNA "{col}" ES COMPLETAMENTE NULA
                    POR LO TANTO VALOR DE EXACTITUD ES SETEADO A 0
        ''')
        
        data_return['PORCENTAJE DE EXACTITUD'] = 0
        
    else:
        data_return['PORCENTAJE DE EXACTITUD'] = round((bi_data.get(True,0) *100)/len(bi_columna), 2)


    return {
        "data_return"   : data_return , 
        "inexactos"     : inexacto_dataframe_sample,
        "exactos"       : exacto_dataframe_sample
    }


def limpieza_de_columna( dataset,col,chars_omitir_exactitud,data_types):

    type_deberia = data_types.get(col,'object')
    
    columna_limpia = dataset[[col]].dropna()

    if isinstance(chars_omitir_exactitud,list):
        chars = chars_omitir_exactitud
    else :
        chars = chars_omitir_exactitud.get(col)

    for char in chars:
        columna_limpia = columna_limpia[ columna_limpia[col] != char]

   
    columna_limpia = columna_limpia.astype(type_deberia,errors='ignore').astype(str)
    
    return columna_limpia


