from ..global_data import CHARS_NULL
import numpy as np
import pandas as pd


def dataframe_cleanner(**kwargs):

    grouped_by: list = kwargs.get("grouped_by", None)
    not_nullish = kwargs.get("not_nullish", None)
    dataframe = kwargs["dataframe"]
    d_types = kwargs.get("d_types", None)
    chars_null = kwargs.get("chars_null", None)
    cols = dataframe.columns

    if grouped_by is None:
        print(">>> NO EXISTE PARAMETRO DE AGRUPACION")
        return

    if not all(item in dataframe.columns for item in grouped_by):
        print(">>> Al PARECER ALGUNAS COLUMNAS NO SON PERTENECIENTES AL DATAFRAME")
        return

    D_TYPES_DATA = {col: "string" for col in cols}

    CHARS_NULL_DATA = {col: [] for col in cols} if isinstance(
        chars_null, list) else chars_null

    if d_types is not None:
        D_TYPES_DATA = {**D_TYPES_DATA, **d_types}

    # CHARS NULLLS

    for col, chars in CHARS_NULL_DATA.items():
        if chars != []:
            dataframe = dataframe.replace({pd.NA: np.nan})
            dataframe.loc[dataframe[col].isin(chars), col] = np.nan

    if not_nullish is not None:
        for col, value in not_nullish.items():
            dataframe.loc[dataframe[col].isnull(), col] = value

    dataframe = dataframe.astype(D_TYPES_DATA, errors="ignore")

    return dataframe
