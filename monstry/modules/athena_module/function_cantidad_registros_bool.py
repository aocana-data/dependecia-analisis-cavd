from .rounding_format import round_down


def registros_totales(variable):
    return len(variable)


def registros_exactos(variable):
    return variable.isin([True]).sum()


def registros_completos(variable):
    return variable.count()


def completitud(variable):
    if len(variable) == 0:
        return round_down(0, 2)

    return round_down((variable.isin([True, False, 1.0, 0.0, 0, 1])).sum() * 100 / len(variable), 2)


def exactitud(variable):
    denominador = variable.isin([True, False]).sum()
    if denominador == 0:
        return round_down(0, 2)

    value_exactitud = round_down(
        100.00*(variable.isin([True, 1.0, 1]).sum())/denominador, 2)
    return value_exactitud
