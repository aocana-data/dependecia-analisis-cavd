import json
import pandas as pd

def read_kpi_config(path):
    with open(path,encoding='utf8') as  f:
        data = f.read()
    config_kpi = json.loads(data)

    return config_kpi['kpi']


def validar_existencia_col(cols,kpi_cols):
    """
        TODO Validador no aplicado aun
    """
    into_set        = set( [ col for col in kpi_cols if col != "FULL"] )
    super_set_cols  = set(cols) 
    sub_set_kpi     = set(into_set)

    if sub_set_kpi.issubset(super_set_cols): return True

    return sub_set_kpi.difference(super_set_cols)


def reordenar_values(data):
    
    return [{
        "numero_kpi"    : kpi['numero'],
        "nombre_kpi"    : kpi['alias'],
        # chequea si el valor es "FULL" entonces considera que la columna es un tipo de dato completo
        "cols"          : [ ( k if v == "FULL" else v ,  v if v == "FULL" else k) for k,v in kpi['columnas'].items()]
    } for kpi in data]



def restructuracion(path , cols):
    """
    @returns
    [   {
            "numero_kpi"    :   (int)
            "nombre_kpi"    :   (str)
            "cols"          :   [(status-col , col) ... ]
        }
    ]
    """

    kpis = read_kpi_config(path)

    return reordenar_values(kpis)