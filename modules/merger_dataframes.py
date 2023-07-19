import pandas as pd
from typing import Optional


def merger_dataframes(completitud:any , exactitud:any, pivot_col:Optional['str'] = 'COLUMNA'):
    
    if completitud is None or exactitud is None:
        print('No se puede realizar este merge dado a que alguno de los frames se encuentra faltante')
        return None
    
    return completitud.merge(exactitud, on = pivot_col, how = 'left' )