import pandas as pd

from ..global_data import NOMBRES_GENERALES

from .function_exactitud_general import registros_exactos_por_filas, exactitud_por_filas
from .function_completitud_general import registros_completos_por_filas, completitud_por_filas
from .function_cantidad_registros_bool import registros_totales, registros_completos, completitud, registros_exactos, exactitud


def resumen_agg(**kwargs):

    general, index_total, df = kwargs["dataframe"]
    grouped_by = kwargs["grouped_by"]

    try:

        index_total.loc[:, 'registros'] = 1

        total = index_total\
            .groupby(grouped_by, dropna=False, group_keys=True)\
            .agg({
                "registros": [registros_totales]
            })

        grouped_df = df\
            .groupby(grouped_by, dropna=False, group_keys=True)\
            .agg([registros_completos, completitud, registros_exactos, exactitud])

        grouped_resumen = pd\
            .merge(total, grouped_df, how="inner", left_index=True, right_index=True)

        if general is not None:

            completitud_col, exactitud_col = NOMBRES_GENERALES

            general_df = general\
                .groupby(grouped_by, dropna=False, group_keys=True)\
                .agg({
                    completitud_col: [registros_completos_por_filas, completitud_por_filas],
                    exactitud_col: [registros_exactos_por_filas, exactitud_por_filas],
                }
                )

            grouped_resumen = pd\
                .merge(grouped_resumen, general_df, how="inner", left_index=True, right_index=True)

            return grouped_resumen

    except Exception as e:
        print(f"""
        >>> ERROR AL OBTENER EL RESUMEN POR AGRUPAMIENTO
        {e}""")
