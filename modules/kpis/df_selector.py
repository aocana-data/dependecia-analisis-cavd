#   TODO Defnir el entorno en la cual el nuevo dataframe va a tener accion, la cual permitira 
#   el analisis general de los elementos que si cumplan por valores de completitud.

"""
Debe usar:
    *   configuraciones de completitud
    *   set de entorno de cols/variables
    *   transformar cuando una columna es COMPLETO/EXACTO de otra base de datos

Debe regresar:
    *   dataset limpio
    *   dataset valores de completitud
    *   total de valores que cumplen
"""

from .completitud_nan import cleaner_df_nan
from .completitud_nan import nullish_db

def set_df_full(data, cols_status):
    cols = None
    
    # con FULL como cols_status
    con_full = [ col[0] for col in cols_status if col[1] == "FULL"]

    # sin FULL como cols status
    sin_full = [ col[0] for col in cols_status if col[1] != "FULL"]
    
    returned_df = data[sin_full].copy(deep=True)

    for col in con_full:
        returned_df.loc[:,col] = 1
        
    
    return returned_df
        

def database_cleaned(data , config ,completitud_settings):

    database_per_kpi = {}

    for kpi in config:
        
        stored_kpi_name = {"nombre_kpi" : kpi['nombre_kpi']}
        df_full = set_df_full(data , kpi['cols'])
        cleaned_df = cleaner_df_nan(df_full, completitud_settings)
        complete_cleaned = nullish_db(cleaned_df)

        database_per_kpi[kpi['numero_kpi']] = {**stored_kpi_name, **complete_cleaned}
    
    return database_per_kpi

