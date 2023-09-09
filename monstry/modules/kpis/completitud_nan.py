import numpy as np
import pandas as pd

from ..global_data import CHARS_NULL


def nullish_db(df):
    nullish = df.dropna()

    return {
        "df_len":   len(df),
        "completitud_len":   len(nullish),
        "completitud_score":   round(len(nullish)*100/len(df), 2),
        "completitud_df":   nullish.reset_index(drop=True),
    }


def cleaner_df_nan(df, completitud):

    dataframe = pd.DataFrame()
    cols_df = df.columns.to_numpy()

    CHARS_DEFAULT_NULL = CHARS_NULL

    for col in cols_df:
        value = completitud.get(col, CHARS_DEFAULT_NULL)
        dataframe[col] = df[col].replace(value, np.nan)

    return dataframe
