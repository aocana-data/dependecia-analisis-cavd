import numpy as np

from ..global_data import VALOR_NO_REPRESENTATIVO
from ..global_data import NOMBRES_GENERALES
from .dataframe_cleanner import dataframe_cleanner
from .function_exactitud import exactitud_apply
from .function_exactitud import cleaner_values_exactitud


def matrix_boolean_resumen(**kwargs):

    rules = kwargs['rules']
    dataframe = kwargs['dataframe']
    grouped_by = kwargs["grouped_by"]

    options = {
        "grouped_by": kwargs.get("grouped_by", None),
        "not_nullish": kwargs.get("not_nullish", None),
        "dataframe": kwargs["dataframe"],
        "d_types": kwargs.get("d_types", None),
        "chars_null": kwargs.get("chars_null", None),
    }

    apply_rules = exactitud_apply(rules=rules)

    for col in grouped_by:
        apply_rules.pop(col)

    clean_dataframe = dataframe_cleanner(**options)

    try:

        clean_dataframe.fillna(VALOR_NO_REPRESENTATIVO, inplace=True, axis=1)

        for col, function in apply_rules.items():
            print(f"""
                  >>> VECTORIZANDO LA COLUMNA DE {col}
                  CANTIDAD DE REGISTROS {len(clean_dataframe[col])}
                  """)

            clean_dataframe[col] = clean_dataframe[col].apply(function)

        return clean_dataframe

    except Exception as e:
        print(f"""  
        >>> ERROR AL VECTORIZAR EL DATAFRAME
        {e}""")


def matrix_option_check(**kwargs):
    add_general = kwargs.get("add_general", None)
    grouped_by = kwargs.get("grouped_by", None)
    df = kwargs["dataframe"]

    df_index_total = df.loc[:, grouped_by]

    if add_general is None or add_general == False:
        return [None, df_index_total, df]

    try:
        completitud, exactitud = NOMBRES_GENERALES

        df[completitud] = df\
            .loc[:, ~df.columns.isin(grouped_by)]\
            .apply(lambda x: not x.isnull().values.any(), axis=1)

        df[exactitud] = df\
            .loc[:, ~df.columns.isin(grouped_by)]\
            .apply(cleaner_values_exactitud, axis=1)

        df_general = df.loc[:, grouped_by+NOMBRES_GENERALES]
        df_not_general = df.loc[:, ~df.columns.isin(NOMBRES_GENERALES)]

        return [df_general, df_index_total, df_not_general]

    except Exception as e:
        print(f"""  
        >>> ERROR AL AGREGAR LOS VALORES GENERALES DE COMPLETITUD Y EXACTITUD EN EL DATAFRAME
        {e}""")
