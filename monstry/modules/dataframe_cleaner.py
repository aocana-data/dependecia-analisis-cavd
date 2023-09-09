import pandas as pd
import json
import re
import yaml

from os.path import dirname, abspath


def get_reglas_casteo(path_config):
    regexp = '(.yml |.yaml)$'

    try:

        if not path_config:
            print(
                f"--!-->    No existe archivo de connfiguracion"
            )
            return {}

        with open(path_config, 'r', encoding="latin-1") as f:
            if re.search(regexp, path_config):
                config = yaml.safe_load(f)

            else:
                config = json.load(f)

        return config

    except Exception as e:
        print("ERROR: Lectura de reglas de casteo")
        error = type(e)
        print(f'Error {error}\nData doc: {error.__doc__}')


def completitud_cleaner(dataframe, data_type, filter_nan='-1'):
    '''
    _summary_

    Limpieza de casteo de devolver a un valor string 
    '''

    data_frame_clean = dataframe.copy().fillna(
        filter_nan).astype(data_type).astype(str)

    return data_frame_clean


def exactitud_cleaner(dataframe, data_type, filter_nan='***'):
    """
    Debe limpiar los NaN & remover los characteres seteados en las reglas de los chars a omitir
    """

    data_frame_clean = dataframe.copy().fillna(
        filter_nan).astype(data_type).astype(str)

    return data_frame_clean
