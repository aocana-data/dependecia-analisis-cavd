import pandas as pd
import re

from .set_exactitud_cols_func import set_exactitud_cols_func
from .mixer_exactitud import mixer_exactitud

def list_exactitud(rules:dict , dataframe,chars_omitir_exactitud,data_types)->list:

    total_dicts = []
    inexactos = {}
    exactos ={}

    for rule in rules.values(): 
        
        returned_data = set_exactitud_cols_func( rule, dataframe, chars_omitir_exactitud, data_types)
        set_df = returned_data['return_data']

        inexactos = {
            **inexactos,
            **returned_data['inexactos']
        }

        exactos = {
            **exactos,
            **returned_data['exactos']
        }
        
        total_dicts.append(pd.DataFrame(set_df))
    
    
    exactitud = mixer_exactitud(total_dicts)

    return {
        "exactitud":    exactitud,
        "inexactitud":  inexactos,
        "exactos":      exactos
    }



