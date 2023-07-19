def collapser(reglas):
    
    # seteo una lista de valores unicos de funciones

    if reglas is None:
        print('No se encuentra cargado correctamente el archivo config')
        return None

    set_values= set(reglas.values())

    dict_values = {}

    # registro la estructura
    for func in set_values:
        dict_values[func] = [[],func]

    # 
    for key,func in reglas.items():

        columnas , values= dict_values[func]

        if func == values:
            dict_values[func][0].append(key)


    return dict_values