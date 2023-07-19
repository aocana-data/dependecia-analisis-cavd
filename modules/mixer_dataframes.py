import pandas as pd

def mixer_dataframes(lista_dataframes:list[any], option)->any:
    '''
    _sunmmar_
    Concatenacion masiva de cada dataframe

    @params
        lista_dataframes :
    '''

    typos = {

    'completitud' :{  
        'REGISTROS TOTALES': 'int64',
        'CANTIDAD DE INCOMPLETOS': 'int64',
        'CANTIDAD DE COMPLETOS' : 'int64' }
    }

    if lista_dataframes == []:
        print('No hay valores ingresados')
        return None

    if option == 'completitud':
        return pd.concat(lista_dataframes,axis=0,ignore_index=True).astype(typos[option])
    else:
        return  pd.concat(lista_dataframes,axis=0,ignore_index=True)

    