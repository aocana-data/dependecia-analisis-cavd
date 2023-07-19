import re
import numpy
from .funciones_default.funciones_generales import *

REGEXP_LIB = {
    "cuit_cuil" :"^[2-3][0-9](\.|\/|\-|\s|\_){0,1}[0-9]{8}(\.|\/|\-|\s|\_){0,1}[a0-9]{1}$",
    "date"      : "(^(1[0-2]|0?[1-9])(\/|\-)(3[01]|[12][0-9]|0?[1-9])(\/|\-)(?:[0-9]{2})?[0-9]{2}$|^(?:[0-9]{2})?[0-9]{2}(\/|\-)(3[01]|[12][0-9]|0?[1-9])(\/|\-)(1[0-2]|0?[1-9])$)",
    "strings"   : "^[a-zA-ZÀ-ÿ\u00C0-\u017F]*$",
    "booleans"  : "^(1|0|True|False)$" ,
    "numbers"   : "^[0-9]+$" ,
    "alpha_numeric" : "^\w+$" ,
    "email" : "^(([^<>()\[\]\\.,;:\s@”]+(\.[^<>()\[\]\\.,;:\s@”]+)*)|(“.+”))@((\[[0–9]{1,3}\.[0–9]{1,3}\.[0–9]{1,3}\.[0–9]{1,3}])|(([a-zA-Z\-0–9]+\.)+[a-zA-Z]{2,}))$",
    "float"   : "^\d+[\.|\,]?\d*$",
    "uuid"  : "^[0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12}$",
    "date_time": "^(\d){4}([-|\/|.](\d){2}){2}(|\s(\d){2}(:(\d){2}){2}|\s(\d){2}(:(\d){2}){2}(|.\d{1,5}))$",
    "alpha_numeric_text": "^[\w\s]+$"
}

regexp_list = [
    
{   "data" :"cuit_cuil",
    "dtype": "object",
    "type" : "str",
    "regexp": REGEXP_LIB["cuit_cuil"],
    "default": custom_comprobando_cuit_cuil,  
},
{   "data" :"date",
    "dtype": "object",
    "type" : "str",
    "regexp": REGEXP_LIB["date"],
    "default": comprobando_fechas_date,  
},
    
{   "data" :"string",
    "dtype": "object",
    "type" : "str",
    "regexp":REGEXP_LIB["strings"],
    "default": comprobando_strings,
    
},
{
    "data" : "bool",
    "dtype": "bool",
    "type" : "bool",
    "regexp": REGEXP_LIB["booleans"],
    "default" : comprobando_booleanos
}
,

{   "data" : "number",
    "dtype": "int64",
    "type" : "int",
    "regexp": REGEXP_LIB["numbers"],
    "default" : comprobando_numeros
},
{
    "data" : "alpha_numeric",
    "dtype": "object",
    "type" : "str",
    "regexp":REGEXP_LIB["alpha_numeric"],
    "default" : comprobando_alfanumerico
},
{
    "data" : "email",
    "dtype": "object",
    "type" : "str",
    "regexp" : REGEXP_LIB["email"],
    "default" : comprobando_correo_valido
},
{
    "data" : "decimal",
    "dtype": "float64",
    "type" : "float",
    "regexp" : REGEXP_LIB["float"],
    "default" : comprobando_decimales
},
{
    "data" : "uuid",
    "dtype": "object",
    "type" : "str",
    "regexp" : REGEXP_LIB["uuid"] ,
    "default" : comprobando_uuid
},
{
    "data" : "fechas",
    "dtype": "datetime64",
    "type" : "str",
    "regexp" : REGEXP_LIB["date_time"],
    "default" : comprobando_fecha_general
},

{
    "data" : "alpha_numeric_text",
    "dtype": "object",
    "type" : "str",
    "regexp": REGEXP_LIB["alpha_numeric_text"],
    "default" : comprobando_alfanumerico
},
{
    "data" : "no_determinado",
    "dtype": "object",
    "type" : "str",
    "regexp":".*" ,
    "default" : comprobando_no_determinado
}
]


def matcher_regexp(regexp_list , sample, columna):
    try:
        res_data = None

        for checker in regexp_list:

            res = re.match(checker['regexp'],sample)
            if res is not None:          
                if res_data is None:
                    res_data = {**checker,**{"columna":columna,"sample":sample}}
                    break
        
        if res_data is None:
            res_data = {
                "data" : "uknown",
                "dtype": "object",
                "type" : "str",
                "regexp" :".+",
                "columna":columna,
                "sample":sample
                }
        
        del res_data['regexp']

        return res_data

    except Exception as e:
        print(e)
        return {
            "error" : e,
            "sample":sample
        }



def request_not_na_value(dataframe,col):
    dataframe =  dataframe.dropna(how='all',axis=0,inplace=False)

    dataframe = dataframe.astype('str')

    if len(dataframe) == 0 :
        
        print(f'El SubDataset es vacio con columnas: {col}')
        return ''

    sample = dataframe.sample(n=1)

    [validador,*extra] = sample.isna().to_numpy()


    while validador:
        sample = dataframe.sample(n=1)

    [sample_return, *extra]  = sample.to_numpy()

    return str(sample_return)

def chequeo_valores(dataframe):

    if dataframe is None:
        print('No hay un dataframe válido')
        return

    
    columnas = dataframe.columns
    columnas_lista = []

    for index,col in enumerate(columnas):

        sample = request_not_na_value(dataframe[col],col)

        columnas_lista.append(matcher_regexp(regexp_list, sample, col))
    

    return columnas_lista

