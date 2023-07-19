

def total_chars_null(dataframe, col, chars_null):
    ''' _summary_
    Realiza una pasada con el conteo de todos los registros
    Siempre y cuando los registros se encuentren en un tipo str
    '''    

    record = {}
    total_records = 0

    for check in chars_null:
        total_records += sum(dataframe[col].apply(lambda x: x.strip() if isinstance(x, str) else x) == check)
        
    return total_records