import os

from pathlib             import    Path
from .collapser_rules    import    collapser

def set_validadores_exactitud(**kwargs):
    """
    @params
        path_funciones : 
        path del archivo con las funciones a utilizar para el analisis de exactitud
    """

    path_funciones  =   kwargs.get("path_funciones",None)
    BASE_DIR        =   kwargs["base_dir"]
    PATH_DEFAULT    =   kwargs["path_default"]
    config          =   kwargs["config"]
    rules_defecto   =   kwargs["rules_defecto"]
    
    # VALIDAR SI LAS FUNCIONES VIENEN DESDE ALGUNA CONFIGURACION EXTRA
    # O SI SON DE ALGUN LUGAR ESPECÍFICO, UN ARCHIVO DE CONFIGURACION 
    # O VALIDADORES NUEVOS
    
    
    
    if path_funciones is None or path_funciones == "" or len(path_funciones) == 0:
        # ESTA VALIDACION TOMA EL VALOR POR DEFECTO QUE TIENE
        # LA DEPENDECIA
        BASE_DIR                 =     os.path.dirname(os.path.dirname(__file__))
        path_funciones           =     os.path.join(BASE_DIR,PATH_DEFAULT)

    custom_functions    =   config.get("extra_custom_functions","")
      
    with open(path_funciones,"r",encoding='latin-1') as  f:
        data = f.read()

    if custom_functions != "":
        with open(custom_functions,"r",encoding='latin-1') as  f:
            custom_functions = f.read()

    data += f"\n\n{custom_functions}"
    
    exec(data,globals())
    
    rules       =   config.get('exactitud_reglas', None)    
    
    if (rules is None) or (len(rules) == 0):
        
        print("""
        --!-->  VALIDACION DE DATOS POR DEFAULT
            -------------------------------------------
            Si el proyecto tiene funciones customizadas, deberán
            ser cargada el path en el archivo de configuración JSON
        """)
        
        rules   =   { col:fn for col,fn in rules_defecto.items()}
        
    else:
        print("""
        --!-->  VALIDACION DE DATOS POR CONFIGURACION
            -------------------------------------------
            
        """)
        
        rules       =   { col:eval(fn) for col,fn in rules.items()}
        
    # GENERACION Y DEVOLUCION DE REGLAS
    
    try:
        collapsed_rules =   collapser(rules)

        # OPCION PARA VISUALIZAR LAS COLUMNAS COLAPSADAS
        
        display =   {   
            fn.__name__ : [len(tupla[0]),tupla[0]]
            for fn,tupla
            in
            collapsed_rules.items()
        }


        return collapsed_rules

    except Exception as e:
        print("ERROR al setearse los validadores de exactitud")
        print(e)
        print(f'{type(e)} :  {type(e).__doc__}')


