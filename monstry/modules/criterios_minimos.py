import os
from    .output_gauge_total import  get_limits
from    dotenv              import  load_dotenv

load_dotenv()


def percentil(df_div, df_base):
    return round( 100 * len(df_div) / len(df_base)  ,2)


def criterios_minimos_info(criterio_minimo):
        
    # GLOBAL PARAMETERS

    BOTTOM_LIMITS   =   get_limits("BOTTOM_LIMITS",[0,50])
    MIDDLE_LIMITS   =   get_limits("MIDDLE_LIMITS",[50,90])
    TOP_LIMITS      =   get_limits("TOP_LIMITS",[90,100])

    PARAMS = {
            1:BOTTOM_LIMITS,
            2:MIDDLE_LIMITS,
            3:TOP_LIMITS
    }

    LABEL_NO_CONFIABLE , LABEL_POCO_CONFIABLE ,LABEL_CONFIABLE        =   os.getenv("LABELS","No confiable,Poco confiable,Confiable").split(",")
    
    
    confiable       =   criterio_minimo[
        criterio_minimo["CRITERIO MINIMO"]>= PARAMS.get(3)[0]
    ]
    
    poco_confiable  =   criterio_minimo[
        (criterio_minimo["CRITERIO MINIMO"]>= PARAMS.get(2)[0])
        &
        (criterio_minimo["CRITERIO MINIMO"] < PARAMS.get(2)[1])
    ]
    
    no_confiable    =   criterio_minimo[
        (criterio_minimo["CRITERIO MINIMO"]>= PARAMS.get(1)[0])
        &
        (criterio_minimo["CRITERIO MINIMO"] < PARAMS.get(1)[1])
    ]
    
    
    CRITERIOS_RETURN = {}
    
    COL  = "COLUMNA"
    
    CRITERIOS_RETURN[LABEL_CONFIABLE]       = {
        "columnas"      :   " , ".join(list(confiable[COL].to_numpy())),
        "porcentaje"    :   percentil(confiable,criterio_minimo)
    }
    CRITERIOS_RETURN[LABEL_POCO_CONFIABLE]  = {
        "columnas"      :   " , ".join(list(poco_confiable[COL].to_numpy())),
        "porcentaje"    :   percentil(poco_confiable,criterio_minimo)
    }
    CRITERIOS_RETURN[LABEL_NO_CONFIABLE]    = {
        "columnas"      :   " , ".join(list(no_confiable[COL].to_numpy())),
        "porcentaje"    :   percentil(no_confiable,criterio_minimo)        
    }
 
    
    return CRITERIOS_RETURN
    
        