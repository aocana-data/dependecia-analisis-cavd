from process_cb import limpieza_de_columna
import pandas as pd


def process_data_cb():
    return 



def eager_values_no_exactos(**kwargs):

    cols = kwargs['dataframe'].columns()



    for col in cols :
        columna_limpia = limpieza_de_columna(kwargs['database'],
                                            col,
                                            kwargs['chars_omitir_exactitud'],
                                            kwargs['data_types']
                                            )
        print(columna_limpia)

