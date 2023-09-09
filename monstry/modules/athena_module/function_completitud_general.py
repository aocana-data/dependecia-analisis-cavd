from .rounding_format import round_down


def registros_completos_por_filas(variable):
    return (variable == True).sum()


def completitud_por_filas(variable):
    return round_down(
        (variable == True).sum() * 100 / len(variable), 2
    )
