from .rounding_format import round_down


def registros_exactos_por_filas(variable):
    return variable.isin([True]).sum()


def exactitud_por_filas(variable):
    if variable.isin([True, False]).sum() == 0:
        return round_down(0, 2)

    return round_down(
        (variable == True).sum() * 100 / variable.isin([True, False]).sum(), 2
    )
