import pandas as pd

def remover(df):
    
    """
    modificar columnas a retirar
    """
    dfs_cols = ["dataframe_exacto_valido","completitud_df"]
    for dfs_col in dfs_cols:
        df.pop(dfs_col)
    
    return df

def scoring_data_frame(df):

    pop_number = df.pop("numero_kpi")

    scoring_data = pd.DataFrame(df, index=[pop_number])

    rename_cols = {
        'numero_kpi': 'NUMERO DE KPI' ,
        'nombre_kpi': 'NOMBRE DE KPI' ,
        'df_len':  'TOTAL REGISTROS' ,
        'completitud_len': 'REGISTROS FILA COMPLETOS' ,
        'completitud_score': 'SCORE COMPLETITUD' ,
        'exactitud_len': 'REGISTROS FILA EXACTOS' ,
        'exactitud_score':'SCORE EXACTITUD' ,
    }

    scoring_data.rename(columns = rename_cols , inplace = True)

    return scoring_data

def cols_remover(list_dfs):
    return [ scoring_data_frame(remover(df)) for df in list_dfs]


def scoring_df(list_dfs):
    dfs = cols_remover(list_dfs)
    return pd.concat(dfs, axis=0)

