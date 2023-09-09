import numpy as np

def true_value(variable):
    return variable == 1

def transform_booleans( df , col , exactitud_function ):
    df[col] = df[col].apply(exactitud_function).astype(np.int8)

def validos_exactitud(df,col,exactitud_function) :
    df.loc[df[col].apply(exactitud_function) == False, col ] = np.nan
    

def reducer_nan(data):
    return data.dropna().copy(deep=True)


def reducer(data):

    df_exactitud_clear = data[(data!=0).all(1)].copy(deep=True)
    completitud_len =len(data)
    exactitud_len = len(df_exactitud_clear) 

    try:
        exactitud_score = round(exactitud_len*100/completitud_len,2)
    
    except ZeroDivisionError:
        print("warning! El valor del score es 0 por que no hay valores en la tabla de completitud")
        exactitud_score = 0
    except Exception:
        print("warning! El valor del score es 0 por que no hay valores en la tabla de completitud")
        exactitud_score = 0

    resultado = {
        "exactitud_len" :  exactitud_len,
        "exactitud_score" : exactitud_score
    }   

    return resultado


def transform_df(df , df_clean_exactitud,  exactitud_settings  ):
    cols    =   df.columns.to_numpy()

    for col in cols:
        exactitud_function = exactitud_settings.get( col , true_value )
        transform_booleans(df, col , exactitud_function)
        validos_exactitud(df_clean_exactitud, col , exactitud_function)


def exactitud_transform(data, exactitud_settings):
    # iteracion sobre items
    
    for kpi_index, kpi_data in data.items():
        
        df              =   kpi_data['completitud_df'].copy(deep=True)
        df_clean_exactitud =   kpi_data['completitud_df'].copy(deep=True)

        transform_df(df , df_clean_exactitud,exactitud_settings)


        reducer_exactitud           =   reducer(df)
        reducer_clean_exactitud     =   reducer_nan(df_clean_exactitud)
        
        reduccion = {
            **reducer_exactitud,
            **{"dataframe_exacto_valido"   : reducer_clean_exactitud}
        }

        kpi_data.update(reduccion)

    return data
    
