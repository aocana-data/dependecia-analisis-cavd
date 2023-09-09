from .process_cb import process_cb



def set_exactitud_cols_func(rule:tuple,dataset:any,chars_omitir_exactitud, data_types)->list:
    """
    _summary_

    Realiza un analisis de exactitud 

    @params 
        rule(tupla): valores con las cols que se debe hacer el analisis y fn, la cual se hace el chequeo
        dataset(dataframe): set de analisis

    @return 
        return_data(list): lista de dicts con los resultados de los analisis de exactitud
    """

    return_data =   []
    inexactos =     {}
    exactos =       {}

    # desacoplamos las partes fundamentales de la tupla#

    cols , cb_function = rule
    
    for col in cols:
        res = process_cb(col,cb_function,dataset[cols],chars_omitir_exactitud,data_types)
        
        if res.get('inexactos',None) is not None:
            
            inexactos = {
                **inexactos,
                **res.get("inexactos")
            }

        if res.get("exactos",None) is not None:
            exactos = {
                **exactos,
                **res.get("exactos")
            }
        
        return_data.append(res['data_return'])
    
    return {
        "return_data"   :   return_data,
        "inexactos"     :   inexactos,
        "exactos"       :   exactos
    }
