import pandas as pd

def mixer_exactitud(exactitud_data:list)->any:
    """
    Entrada de lista de dfs, que contienen el resultado de los callbacks
    """
    
    exactitud = []

    for dataframe in exactitud_data:
        exactitud.append(dataframe)
    
    exactitud_dataframe = pd.concat(exactitud, axis=0, ignore_index=True)

    return exactitud_dataframe
