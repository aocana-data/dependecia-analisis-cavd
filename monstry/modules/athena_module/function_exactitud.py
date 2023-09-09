import numpy as np

from .rounding_format import round_down
from ..global_data import VALOR_NO_REPRESENTATIVO


def cleaner_values_exactitud(x):
    if x.isnull().values.any():
        return np.nan

    return np.prod(x)


def decorator(cb):
    return lambda x: np.nan if (x in [VALOR_NO_REPRESENTATIVO]) else cb(x)


def exactitud_apply(**kwargs):
    return {
        col: decorator(f)
        for col, f in kwargs["rules"].items()
    }


def exactitud(variable):
    if variable.count() == 0:
        return round_down(0, 2)

    value_exactitud = round_down(
        100*(variable.isin([True])).sum()/variable.count(), 2)
    return value_exactitud
