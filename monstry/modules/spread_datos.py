def spread_datos(pool_objetos,cb):
    '''
    _sumary__
    @return 
        lista con los datos requeridos, dependiendo del método 
    '''
    dict_methods = {
        'completitud_resumen': [objects.completitud_resumen() for objects in pool_objetos.values()],
        'exactitud_resumen':[objects.exactitud_resumen() for objects in pool_objetos.values()]
    }


    data = dict_methods.get(cb , None)

    if data is None:
        print('El objeto no cuenta con ese método')
        return None
    
    return data


