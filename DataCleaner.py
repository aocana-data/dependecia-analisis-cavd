import os
import re
import pandas as pd
from datetime import datetime
from typing import Optional
from itertools import zip_longest


# IMPORT OF INTERNAL MODULES

from .modules.completitud_rules import completitud
from .modules.list_exactitud import list_exactitud
from .modules.variable_categorica import variable_categorica
from .modules.estado_columna import estado_columnas
from .modules.merger_dataframes import merger_dataframes
from .modules.agregado_chars import agregado_chars
from .modules.dataframe_cleaner import get_reglas_casteo
from .modules.collapser_rules import collapser
from .modules.data_types_matcher import chequeo_valores
from .modules.export_modules.export_matriz import insercion_data
from .modules.hidden_prints import HiddenPrints

from .Builder import Builder


class DataCleaner:
    """
    Metodos a cargo de analisis exploratorio:
        data_columns :
            Muestra una lista con el tipo de dato, el nombre de la columna y una muestra de la cual analizó
        get_registros:
            Retorna  los registros con que contienen el texto(string) que se quiere mostrar, en la columna buscada
        estado_columna:
            Data general del estado de las columnas, el cual analiza solo los valores que son del tipo None que nos devuelve información relevante del dato de las columnas
        agregar_caracter_chequeo:
            Agrega a la lista de char nulls
        remover_caracter_chequeo:
            Retira el caracter usado para hacer analisis de completitud
        analisis_por_categoria:
            Analisis de valores globales de la columna inidicada

    Metodos a cargo de analisis de completitud y exactitud:
        set_config:
            Ingesta las configuraciones necesarias para la completitud y exactitud, por medio de un archivo .json
        get_completitud:
            Retorna los valores de completitud del dataframe propuesto
        get_exactitud:
            Utiliza las configuraciones en el archivo de config para hacer el analisis de exactitud
        get_resumen:
            Nos devuelve el valor de exactitud y completitud en el mismo dataframe
    """

    #VALORES POR DEFAULT

    path_default = './monstry/modules/funciones_default/funciones_generales.py'
    chars_null = [",", ".", "'", "-", "_", ""]

    config={
        "chars_null": [",", ".", "'", "-", "_", ""],
        "exactitud_reglas" :{},
        "completitud_reglas":{},
        "dtypes":{}
    }

    exactitud = None
    completitud = None
    resumen = None
    rules = None
    chars_omitir_exactitud:dict = None

    def __init__(self, builder:Builder, nombre_tabla:str = 'DESCONOCIDA')->None:
        # inicia la base de datos para poder ser usada en el constructor

        builder.get_database() 
        
        self.config = builder.data_config
        self.builder_config = builder.cnx
        self.dataframe = builder.database
        self.nombre_tabla = nombre_tabla
        self.columnas = self.dataframe.columns
        self.set_config()
        
        texto =f'''
        Informacion:
            _ get_completitud():    
                Se obtienen la completutid de la tabla
            - get_exactitud():      
                Se obtiene la exactitud de la tabla siempre y cuando los valores por default sean completados
                en el archivo de configuraciones o tengan un valor por defecto
            {"-"*100}
            - get_resumen():
                Se obtiene un dataframe con el análisis de completitud y exactitud del dataset seleccionado
        '''
        print(texto)

    def set_validadores_exactitud(self , path_funciones:Optional[str] = None):
        """
        @params
            path_funciones : 
            path del archivo con las funciones a utilizar para el analisis de exactitud
        """

        try:
                

            if path_funciones is None:
                # le indico que puede tomar los datos desde el archivo config

                BASE_DIR = os.path.dirname(os.path.dirname(__file__))

                path_default = self.config.get('exactitud_validadores',None)
                
                if path_default is None or path_default == '':
                    '''
                        EL PATH DEFAULT DE LAS FUNCIONES
                    '''    
                    PATH_DEFAULT = self.path_default

                
                path_funciones = os.path.join(BASE_DIR,PATH_DEFAULT)


                
            with open(path_funciones,encoding='utf8') as  f:
                data = f.read()

            # exec(compile(data,'funciones','exec'))
            exec(data)

            rules = self.config.get('exactitud_reglas', None)
            rules_set = collapser(rules) 

            if rules_set is None:
                self.raw_rules = rules_set                
                return True

            for col , fn in rules.items():
                rules[col] = eval(fn)


            for key,value in rules_set.items():
                function = value.pop()
                value.append(eval(function))            


            self.rules = rules_set
            self.raw_rules = rules

            if self.rules is  None:
                return None
            
            return True

        except Exception as e:
            
            print(e)
            print(f'{type(e)} :  {type(e).__doc__}')

            return None


    def set_config(self,json_config_path_file:Optional[str]=None):
        '''
        _summary_
        genera las reglas generadas por un archivo json
        '''

        if json_config_path_file is not None:              
            self.config = get_reglas_casteo(json_config_path_file)

        chars_nulabilidad= self.config.get('chars_null',None)

        if chars_nulabilidad is None:
            print('Se setean los valores por default')

        self.chars_null = chars_nulabilidad

        return None


    def get_completitud(self):

        if self.config.get('dtypes',None) is None:
            print('La configuracion de los tipos de datos no están cargados')
            return None

        if len(self.config.get('dtypes')) == 0:
            print('Se toman los valores por defecto')

        data_types= self.config.get('dtypes')

        if self.config['completitud_reglas'].get('rules',None) is None:
            opcion = 'solo_nan'
        
        elif len(self.config['completitud_reglas'].get('rules',None)) == 0 :
            opcion = 'default'

        else:
            opcion = 'con_reglas'


        params = {  'dataframe':self.dataframe,
                    'nombre_tabla':self.nombre_tabla,
                    'data_types':data_types,
                    'reglas_completitud_config':self.config.get('completitud_reglas',{}).get('rules',{}),
                    'chars_null':self.chars_null,
                    'opcion':opcion
        }



        res_completitud = completitud(**params)
        
        self.completitud = res_completitud['tabla_resumen']
        self.chars_omitir_exactitud = res_completitud['chars_omitir_exactitud']

        print('Analisis de completitud finalizado\n * Para observar los resultados solo de "Completitud" visualizar con .completitud')

        return None


    def get_exactitud(self)->any:
        """
        default False: Permite hacer un analisis usando valores que el usuario
        pasa por default
        default True : Permite realizar un analisis con valores por default de
        las funciones declaradas en ./modules/funciones_default/funciones_generales.py
        
        """
        self.get_completitud()
        
        print("SETEADO DE FUNCIONES DE EXACTITUD".center(20))     
        self.set_validadores_exactitud()
        
        # if self.rules is None:
        #     print('La carga de estas reglas se deben realizar, indicandole el archivo de configuración o de manera manual.')
        #     return None

        data_types_default = dict(list(zip_longest(self.dataframe.columns,['object'],fillvalue='object')))

        if self.rules is None or len(self.rules) == 0:
            """
            Se setean los valores por default de reglas para el analisis de exactitud
            """

            print("""
            VALIDACION DE DATOS POR DEFAULT
            ---
            
            Si se desean funciones customizadas, deberán ser cargadas en
            en el archivo de configuración JSON
                
                  """)
            
            reglas = {x["columna"]:x.get('default') for x  in self.data_columnas()}

            
            self.raw_rules = reglas
            self.rules = collapser(reglas)

        
        print("""
            SETEANDO LA CONFIGURACIONES:

            PARAMETROS DE EXACTITUD
              """)
        
        
        exactitud_paramas = {
            'rules' : self.rules,
            'dataframe':    self.dataframe,
            'chars_omitir_exactitud' : self.chars_omitir_exactitud,
            'data_types':   self.config['dtypes'] or {x["columna"]:x.get('dtype') for x  in self.data_columnas()}
        }
        
        
        exactitud_return = list_exactitud(**exactitud_paramas)
        try:    
            self.exactitud = exactitud_return['exactitud']
            self.inexactos = exactitud_return['inexactitud']
            self.exactos = exactitud_return['exactos']

            print('Analisis de exactitud finalizado\n* Para observar los resultados solo de "Exactitud" visualizar con .exactitud')

            return None
        
        except Exception as e:
            print("Excepcion producida en seteo de las listas de exactitud".center(20))
            print(f"==>{e}")

    def get_resumen(self)->any:
        '''
        _summary_
        Asigna un dataframe directamente como atributo del objeto
        Valor por default valor "COLUMNA"
        @return
            dataframe(pandasDF) : los valores unificados
        '''

        self.get_exactitud()
        
        try:
            if self.exactitud is None:
                print('Se debe hacer un analisis de exactitud previamente')
                return None

            self.resumen = merger_dataframes(self.completitud, self.exactitud)
            return self.resumen

        except Exception as e:
            print(e)
            print(f'{type(e)} :  {type(e).__doc__}')
            
    def resumen_export(self,path_to_dump = 'analisis_output.csv'):

        if self.resumen is None:
            print('Aun no se encuentra listo, se deben correr la completitud y exactitud previo a realizar el exportado del resumen total')
            return None
        
        self.resumen.to_csv(path_to_dump,index_label= 'ROW')
        print(f'El resumen fue exportado a {path_to_dump}')

        return None

    #
    # A partir de acá son métodos para el analisis exploratorio del dataset analizado
    # #

    def data_columnas(self):
                
        return chequeo_valores(self.dataframe)

    def get_registros(self, columna:Optional[str] = None , char:Optional[str]=None):

        if columna is None or char is None:
            print('No se ingresaron datos en alguno de los campos')
            return None

        frame = self.dataframe
        records = frame[frame[columna] == char]

        if len(records) == 0:
            print(f'No hay registros que contenga el caracter: \"{char}\"')
            return None

        return records
            
    def estado_columna(self, option:bool = True):
        '''
            @params 
            options : Boolean | Default: True
                - Permite dar la posibilidad de visualizar de manera completa los elementos o de manera parcial
                    * Completa::True -> incluye todas las columnas del dataframe. DEFAULT
                    * Seleccionada::False -> incluye solamente las columnas que tengan valores distintos al null o vacio
        '''

        texto = '''
        Conteo de cantidad de registros por caracteres determinados como nulls por el usuario
        como valores de default.
        '''
        print(texto)

        return estado_columnas(self.dataframe , self.chars_null, option)

    def remover_caracter_chequeo(self, char=None):
        if char is None:
            print('No se ingresaron datos')
            return None
        
        if char not in self.chars_null:
            print('El valor no se encuentra en la lista de nulabilidad')
            return None
        
        self.chars_null = [char for char in self.chars_null if char != char]
        print(f'Se ha removido el valor: \"{char}\" de la lista a considerar nulo')

        return None

    def agregar_caracter_chequeo(self , *chequeo):

        if chequeo is None:
            return

        self.chars_null = agregado_chars(self.chars_null , *chequeo)
        
        return None

    def analisis_por_categoria(self, *cols:Optional[list[str]])->None:
        '''
            Retorna un analisis rápido por variables solicitada
        '''
        cantidad_registros = len(self.dataframe)
        
        if len(cols) == 0:
            print('No se ingresaron columnas')
            return None

        if cols is None:
            print('No se ingresaron columnas')
            return None
        
        for col in cols:
            variable_categorica(self.dataframe, cantidad_registros , col)

    def gather_data(self,file_name = None):
        '''
        @paramas
            file_name: 
            nombre del archivo xlxs que dará como output
        '''
        
        BASE_DIR = os.path.dirname(os.path.dirname(__file__))

        folder_name = 'output'
        folder_path = os.path.join(BASE_DIR,folder_name) 

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        file_path = "monstry/modules/export_modules/matriz_evaluacion_calidad_datos.template.xlsx"


        if file_name is None:
            file_output_path = f"matriz_evaluacion.{datetime.today().strftime('%b_%d_%Y')}_output.xlsx"
        else:
            file_output_path = f"{file_name}.xlsx"


        template_path = os.path.join(BASE_DIR,file_path)
        output_data = os.path.join(folder_path,file_output_path)


        worksheet_config ={
            'template_wb_matriz' : template_path,
            'sheet' : '2.1 Calidad por Variable',
            'output': output_data,
            'completitud':self.completitud,
            'exactitud':self.exactitud,
            'data_columnas':self.data_columnas(),
            'data_builder': self.builder_config
        }
            
        insercion_data(worksheet_config)

 
        return 

    def __inexactos__(self):
        if len(self.inexactos) == 0:
            return 'No data'

        for columna,data in self.inexactos.items():
            print(columna.center(100,'-'))
            print(data)
        return 
    
    def __exactos__(self):
        if len(self.exactos) == 0:
            return 'No data exacta'

        for columna,data in self.exactos.items():
            print(columna.center(100,'-'))
            print(data)
        return 



    def __values_aprox_types__(self):
        return {x["columna"]:x.get('dtype') for x  in self.data_columnas()}
    
    def __values_aprox_function__(self):
        return {x["columna"]:x.get('default').__name__ for x  in self.data_columnas()}
    
    def __values_sample__(self):
        return {x["columna"]:x.get('sample') for x  in self.data_columnas()}
    
    def __exactitud_config__(self):

        opcion = input("Modificamos el set de funciones?\t ") or False
        
        
        with HiddenPrints():
            config =  {settings["columna"] : settings["default"].__name__  for settings in self.data_columnas()}
        
        if not opcion:    
            return config

        returner_value = {}

        for key,value in config.items():
            
            validacion = input("Modificamos la funcion a validar?\nEl valor por defecto va a ser no") or False
            
            if not validacion:
                nombre_funcion = input("Indique el nombre de la funcion") or  "comprobando_no_determinado"
                returner_value[key] = nombre_funcion
            
            returner_value[key] = value
            return returner_value
        
    def __exactitud_funciones_config__(self,path_in,path_out):
        
        
        BASE_DIR = os.path.dirname(os.path.dirname(__file__))
        
        path_funciones_default = os.path.join(BASE_DIR,self.path_default)


        print(path_funciones_default)
        
        with open(path_funciones_default, 'r') as file:
            data = file.read().rstrip()
            
        with open(path_in, 'r') as file:
            new_data = file.read().rstrip()
        
        
        joiner_text_functions = f"{data}\n\n\n{new_data}"
        
        with open(path_out,'w') as output:
            output.write(joiner_text_functions)
        
        
        