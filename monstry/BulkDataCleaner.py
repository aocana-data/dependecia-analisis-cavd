import os
import re

import pandas as pd
import numpy as np
from tabulate import tabulate
from pathlib import Path
from datetime import date, datetime
from dotenv import load_dotenv

from datetime import datetime
from typing import Optional
from itertools import zip_longest


from monstry.BuilderManager import BuilderManager


# IMPORT OF INTERNAL MODULES
from .modules.export_modules.export_matriz import insercion_data
from .modules.set_validadores_exactitud import set_validadores_exactitud

from .modules.completitud_rules import completitud
from .modules.list_exactitud import list_exactitud
from .modules.variable_categorica import variable_categorica
from .modules.estado_columna import estado_columnas
from .modules.merger_dataframes import merger_dataframes
from .modules.agregado_chars import agregado_chars
from .modules.dataframe_cleaner import get_reglas_casteo
from .modules.data_types_matcher import chequeo_valores
from .modules.hidden_prints import HiddenPrints

from .modules.output_process_csv import to_csv_func, \
    validate_output_file

from .modules.output_gauge_total import gauge, \
    arrow_value, \
    score

from .modules.criterios_minimos import criterios_minimos_info
from .modules.get_json_config import get_config_files
from .modules.save_new_config_file import save_new_config_file
from .modules.check_new_config_file import check_new_config_file
from .modules.save_criterios_barh import save_criterios_generales
from .modules.global_data import CHARS_NULL
from .modules.global_data import NOMBRES_GENERALES

# ATHENA MODULE
from .modules.athena_module.matrix_boolean_agg import matrix_boolean_resumen
from .modules.athena_module.matrix_boolean_agg import matrix_option_check
from .modules.athena_module.resumen_agg import resumen_agg

from .BuilderManager import Builder

load_dotenv()


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

    # VALORES POR DEFAULT

    PATH_DEFAULT = './modules/funciones_default/funciones_generales.py'

    chars_null = CHARS_NULL

    config = {
        "chars_null": chars_null,
        "exactitud_reglas": {},
        "completitud_reglas": {},
        "dtypes": {}
    }

    chars_omitir_exactitud: dict = None
    DATA_DEFAULT_COLUMNAS: dict = None
    segregacion_criterios_minimos = None
    score_completitud = None
    criterio_minimo = None
    min_criterio_minimo = None
    score_exactitud = None
    completitud = None
    exactitud = None
    resumen = None
    rules = None

    # GLOBAL PARA OMITIR EL ANALISIS GENERAL
    #     TODO: MEJORAR EL MOTOR DE ANALISIS ESTRUCTURADO

    SKIP_GENERAL = int(os.getenv("SKIP_GENERAL", "0"))

    # RESUMEN GLOBAL DE AGRUPACION
    resumen_agg_df = None
    grouped_by = None

    def vectorizacion_cb(self, completitud, exactitud): return round(
        (completitud * exactitud)/100, 2)

    def __init__(self, **kwargs) -> None:

        builder: Builder = kwargs.get("builder", None)

        if isinstance(builder, BuilderManager):
            builder = builder.build()

        builder.get_database()

        # CONFIGURACIONES DESDE EL BUILDER
        self.config = builder.get_config()

        # LOS DATOS DE CONEXION DIRECTAMENTE DESDE EL BUILDER
        self.builder_config = builder.get_cnx()

        # EL DATAFRAME GENERADO POR EL BUILDER DIRECTAMENTE
        self.dataframe = builder.builder_get_database()
        self.BASE_DIR = builder.get_base_dir()
        self.get_config_file = builder.get_config_path()

        # AGREGADO DE NOMBRE DE TABLA Y COLUMNAS DERIVADAS DEL BUILDER
        if builder.get_table() is not None:
            self.nombre_tabla = kwargs.get("nombre_tabla", "DESCONOCIDA")
        else:
            self.nombre_tabla = builder.get_table()

        self.columnas = self.dataframe.columns
        self.cantidad_registros = len(self.dataframe)

        # SETEADO DE UN ARCHIVO DE CONFIGURACION QUE SE OBTENDRÁ UNICAMENTE
        # CUANDO EL CLEANER EJECUTE EL METODO DE GUARDADO DE CSV Y GAUGES
        # DE ANALISIS DE:
        #     COMPLETITUD
        #     EXACTITUD
        #     CRITERIOS MINIMOS

        self.CONFIG_FILE_PATH = self.__set_CONFIG_FILE_PATH__()
        self.NEW_CONFIG_FILE = self.__set_CONFIG_FILE_PATH__()

        path = check_new_config_file(
            config_file_name=self.CONFIG_FILE_PATH,
            default_config_file=self.get_config_file
        )

        self.CONFIG_FILE_PATH = path

        self.set_config(self.CONFIG_FILE_PATH)
        self.criteria_valores_generales = {}
        self.scoring_to_gauge_paths = {}

        texto = f'''
        TABLA   :   {self.nombre_tabla.upper()}
        
        CANTIDAD DE REGISTROS A  ANALIZAR: {self.cantidad_registros}
        
        {"-"*100}
        Informacion de uso básico:
            _ get_completitud():    
                Se obtienen la completutid de la tabla
            - get_exactitud():      
                Se obtiene la exactitud de la tabla siempre y cuando 
                los valores por default sean completados en el archivo
                de configuraciones o tengan un valor por defecto
            
        {"-"*100}
            
            - get_resumen():
                Se obtiene un dataframe con el análisis de completitud 
                y exactitud del dataset seleccionado
        '''

        if self.SKIP_GENERAL == 1:
            add_ = f"""
                >>> SE EVITA HACER EL ANALISIS COMPLETO DE EXATITUD Y COMPLETITUD POR MOTIVOS 
                DE OBTENER SOLAMENTE LOS VALORES POR DEFECTO O POR ANALISIS POR AGRUPACION
            
            """
            texto = add_ + texto

        print(texto)

    def set_config(self, json_config_path_file):
        '''
        _summary_
        genera las reglas generadas por un archivo json
        '''

        if not json_config_path_file:
            print(f"""
                  
            --!-->  No existe archivo de configuración
                    Se toman los siguientes caracteres como incompletos/nulos
                    {'-'*50}
                    {' | '.join(self.chars_null)}
                  """)
            return None

        self.config = get_reglas_casteo(json_config_path_file)

        chars_nulabilidad = self.config.get('chars_null', None)

        if chars_nulabilidad is None:
            print('Se setean los valores por default')

        self.chars_null = chars_nulabilidad

        return None

    def get_agg_completitud(self, **kwargs):

        with HiddenPrints():
            if self.resumen is None:
                self.get_resumen()

        idx = pd.IndexSlice

        if not (self.resumen_agg_df is not None and (self.config.get("grouped_by", kwargs.get("grouped_by", None)) == self.grouped_by)):
            self.get_agg_resumen()

        print(f"""
            >>> SE USA EL VALOR POR CACHÉ
            """)

        completitud_df = self.resumen_agg_df\
            .loc[:, self.resumen_agg_df.columns.get_level_values(1).isin(["registros_totales", "registros_completos", "completitud"])]

        return completitud_df

    def get_agg_exactitud(self, **kwargs):
        with HiddenPrints():
            if self.resumen is None:
                self.get_resumen()

        if not (self.resumen_agg_df is not None and (self.config.get("grouped_by", kwargs.get("grouped_by", None)) == self.grouped_by)):
            self.get_agg_resumen()

        print(f"""
            >>> SE USA EL VALOR POR CACHÉ
            """)

        exactitud_df = self.resumen_agg_df\
            .loc[:, self.resumen_agg_df.columns.get_level_values(1).isin(["registros_totales", "registros_exactos", "exactitud"])]

        return exactitud_df

    def get_agg_resumen(self, **kwargs):

        self.grouped_by = self.config\
            .get("grouped_by", kwargs.get("grouped_by", None))

        not_nullish = self.config\
            .get("not_nullish", kwargs.get("not_nullish", None))

        add_general = self.config\
            .get("add_general", kwargs.get("add_general", None))

        if self.grouped_by is None:
            print(f"""
            >>> NO EXISTEN COLUMNAS DE AGRUPACION.
            DEBEN SER GENERADAS EN EL ARCHIVO DE CONFIGURACION O COMO PARAMETROS.
            """)
            return

        print(f"""
              >>> SE REALIZA UN ANALISIS DE RESUMEN GENERAL DE COMPLETITUD Y EXACTITUD
              DE TODA LA QUERY EN GENERAL.
              """)

        with HiddenPrints():
            if self.resumen is None:
                self.get_resumen()

        if (self.resumen_agg_df is not None and (self.config.get("grouped_by", kwargs.get("grouped_by", None)) == self.grouped_by)):
            print(f"""
              >>> SE USA EL VALOR POR CACHÉ
              """)
            return self.resumen_agg_df

        options = {
            "dataframe": self.dataframe,
            "grouped_by": self.grouped_by,
            "not_nullish": not_nullish,
            "d_types": self.config["dtypes"],
            "chars_null": self.config.get("completitud_reglas", {}).get("rules", {}),
            "rules": self.rules
        }

        print(f"""
        >>> SE PROCEDE A REALIZAR EL ANALISIS POR AGRUPACION
        """)

        clean_dataframe = matrix_boolean_resumen(**options)

        # OPCIONES DE FILTRO COMPLETITUD EXACTITUD GENERAL

        add_options = {
            "add_general": add_general,
            "dataframe": clean_dataframe,
            "grouped_by": self.grouped_by
        }

        matrix_boolean_df = matrix_option_check(**add_options)

        matrix_grouped_by = resumen_agg(
            dataframe=matrix_boolean_df, grouped_by=self.grouped_by)

        # OPCIONES DE FILTRO CON FILTROS MAYORES A % COMPLETITUD | EXACTITUD

        self.resumen_agg_df = matrix_grouped_by

        return self.resumen_agg_df

    def group_threshold_filter(self, **kwargs):

        with HiddenPrints():
            if self.resumen_agg_df is None:
                self.get_agg_resumen()

        completitud, exactitud = NOMBRES_GENERALES

        df = self.resumen_agg_df

        if len(kwargs) == 0:
            print(">> SIN FILTROS")
            return df

        k, v = list(kwargs.items())[0]

        if k == "exactitud":
            return df[df[exactitud][k] >= v]

        if k == "completitud":
            return df[df[completitud][k] >= v]

        print(">> POSIBLEMENTE ESE CAMPO DE FILTRADO REQUERIDO NO ESTÉ CONTEMPLADO")
        return

    def get_completitud(self):
        print("ANALISIS DE COMPLETITUD")
        print('-'*60)

        print("\nCONFIGURACION DE VALORES dtypes PARA LAS COLUMNAS")

        if self.config.get('dtypes', None) is None:
            print('--!-->   La configuracion de los tipos de datos no están cargados')
            return None

        if len(self.config.get('dtypes')) == 0:
            print('--!-->   Se toman los valores por defecto')

        data_types = self.config.get('dtypes')

        if self.config['completitud_reglas'].get('rules', None) is None:
            opcion = 'solo_nan'

        elif len(self.config['completitud_reglas'].get('rules', None)) == 0:
            opcion = 'default'

        else:
            opcion = 'con_reglas'

        params = {
            'dataframe':   self.dataframe,
            'nombre_tabla':   self.nombre_tabla,
            'chars_null':   self.chars_null,
            'opcion':   opcion,
            'data_types':   data_types,
            'reglas_completitud_config': self.config.get('completitud_reglas', {}).get('rules', {})
        }

        if self.SKIP_GENERAL == 1:
            print(">> SE OMITE EL ANALISIS  GENERAL")
            return None

        res_completitud = completitud(**params)
        self.completitud = res_completitud['tabla_resumen']
        self.chars_omitir_exactitud = res_completitud['chars_omitir_exactitud']

        print('Analisis de completitud finalizado\n * Para observar los resultados solo de "Completitud" visualizar con .completitud')

        return None

    def get_exactitud(self) -> any:
        """
        default False: Permite hacer un analisis usando valores que el usuario
        pasa por default
        default True : Permite realizar un analisis con valores por default de
        las funciones declaradas en ./modules/funciones_default/funciones_generales.py

        """
        if self.completitud is None:
            self.get_completitud()

        print("\nSETEADO DE FUNCIONES DE EXACTITUD")

        # LAS REGLAS DE CASTEO SE PUEDEN TOMAR DESDE:
        #     DEFAULT
        #     JSON CONFIG PATH
        #     UN AGREGADO DE MEZCLA DE FUNCIONES

        # CENTRALIZADO DE VALORES POR DEFECTO

        # BUSCO LOS PATHS DONDE SE ENCUENTRE LAS FUNCIONES
        #     - SI NO HAY FUNCIONES CUSTOM TOMARA LAS DE DEFECTO
        #     - SI HAY TOMARA LA RUTA Y CONVERTIRÁ USANDO ESAS FUNCIONES

        config_path_getter = get_reglas_casteo(self.CONFIG_FILE_PATH)

        path_custom_functions = {}

        if config_path_getter is not None:
            path_custom_functions = config_path_getter.get(
                'exactitud_validadores', None)

        # VERIFICAMOS SI TENEMOS FUNCIONES AGREGADAS

        # VERIFICACION SOBRE LAS FUNCIONES CUSTOMIZADAS
        if path_custom_functions is None or path_custom_functions == '':
            print(
                f" --!--> No existe path de funciones customizadas, se usan las funciones de defecto")

        elif len(path_custom_functions) == 0:
            print(f" --!--> No existe path de funciones customizadas")

        else:
            print(
                f" --!--> Existe path de funciones customizadas, se usa el siguiente path:\n\t{path_custom_functions}")

        # SET VALORES DE ANALISIS POR DEFAULT

        if self.DATA_DEFAULT_COLUMNAS is None:
            self.DATA_DEFAULT_COLUMNAS = self.data_columnas()

        RULES = {x["columna"]: x.get('default')
                 for x in self.DATA_DEFAULT_COLUMNAS}

        get_rules = set_validadores_exactitud(

            path_funciones=path_custom_functions,
            base_dir=self.BASE_DIR,
            path_default=self.PATH_DEFAULT,
            config=self.config,
            rules_defecto=RULES

        )

        collapsed_rules, self.rules = get_rules['collapsed_rules'], get_rules['rules']

        # SET DE DTYPES

        data_types_default = dict(
            list(
                zip_longest(self.dataframe.columns,
                            ['object'],
                            fillvalue='object')
            )
        )

        exactitud_paramas = {
            'data_types':   self.config['dtypes'] or {
                x["columna"]: x.get('dtype')
                for x
                in self.DATA_DEFAULT_COLUMNAS},

            'chars_omitir_exactitud':   self.chars_omitir_exactitud,
            'rules':   collapsed_rules,
            'dataframe':   self.dataframe
        }

        try:
            if self.SKIP_GENERAL == 1:
                print(">> SE OMITE EL ANALISIS EXACTITUD GENERAL")
                return None

            # ANALISIS DE EXACTITUD
            exactitud_return = list_exactitud(**exactitud_paramas)

            self.exactitud = exactitud_return['exactitud']
            self.inexactos = exactitud_return['inexactitud']
            self.exactos = exactitud_return['exactos']

            print('''
            Analisis de exactitud finalizado
                - Para observar los resultados solo de "Exactitud" visualizar con .exactitud''')

            return None

        except Exception as e:
            print(f"==>{e}")

    def get_resumen(self) -> any:
        '''
        _summary_
        Asigna un dataframe directamente como atributo del objeto
        Valor por default valor "COLUMNA"
        @return
            dataframe(pandasDF) : los valores unificados
        '''
        print(f"{'-'*100}\n\nTABLA : {self.nombre_tabla}\n\n")

        self.get_exactitud()

        try:
            if self.exactitud is None:
                print('Se debe hacer un analisis de exactitud previamente')
                return None

            self.resumen = merger_dataframes(self.completitud, self.exactitud)

            self.score_completitud, self.score_exactitud = [
                score(self.resumen, col)
                for col
                in [
                    "PORCENTAJE DE COMPLETITUD",
                    "PORCENTAJE DE EXACTITUD"
                ]
            ]

            self.get_criterios_minimos_resumen()

            return self.resumen

        except Exception as e:
            print(e)
            print(f'{type(e)} :  {type(e).__doc__}')

    def resumen_export(self, path_to_dump='analisis_output.csv'):

        if self.resumen is None:
            print('Aun no se encuentra listo, se deben correr la completitud y exactitud previo a realizar el exportado del resumen total')
            return None

        self.resumen.to_csv(path_to_dump, index_label='ROW')
        print(f'El resumen fue exportado a {path_to_dump}')

        return None

    #
    # A partir de acá son métodos para el analisis exploratorio del dataset analizado
    # #

    def data_columnas(self):
        return chequeo_valores(self.dataframe)

    def get_registros(self, columna: Optional[str] = None, char: Optional[str] = None):
        if columna is None or char is None:
            print('No se ingresaron datos en alguno de los campos')
            return None

        frame = self.dataframe
        records = frame[frame[columna] == char]

        if len(records) == 0:
            print(f'No hay registros que contenga el caracter: \"{char}\"')
            return None

        return records

    def estado_columna(self, option: bool = True):
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

        return estado_columnas(self.dataframe, self.chars_null, option)

    def remover_caracter_chequeo(self, char=None):
        if char is None:
            print('No se ingresaron datos')
            return None

        if char not in self.chars_null:
            print('El valor no se encuentra en la lista de nulabilidad')
            return None

        self.chars_null = [char for char in self.chars_null if char != char]
        print(
            f'Se ha removido el valor: \"{char}\" de la lista a considerar nulo')

        return None

    def agregar_caracter_chequeo(self, *chequeo):

        if chequeo is None:
            return

        self.chars_null = agregado_chars(self.chars_null, *chequeo)

        return None

    def analisis_por_categoria(self, *cols: Optional[list[str]]) -> None:
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
            variable_categorica(self.dataframe, cantidad_registros, col)

    def get_criterios_minimos_resumen(self, **kwargs):
        """
        :params
            metodo_criterio_minimo ==> es una funcion (recomendado una lambda function)
            por defecto 
            -   lambda completitud, exactitud : round((completitud* exactitud)/100,2)
        """

        self.vectorizacion_cb = kwargs.get(
            "metodo_criterio_minimo", self.vectorizacion_cb)

        try:
            if self.resumen is None or len(self.resumen) == 0:
                print(f"Debe correr el metodo de get_resumen() previamente")
                return

            self.criterio_minimo = self.resumen.copy(deep=True)

            self.criterio_minimo["CRITERIO MINIMO"] = np.vectorize(self.vectorizacion_cb)(
                self.criterio_minimo["PORCENTAJE DE COMPLETITUD"], self.criterio_minimo["PORCENTAJE DE EXACTITUD"])

            self.segregacion_criterios_minimos = pd.DataFrame(
                criterios_minimos_info(self.criterio_minimo))

            self.min_criterio_minimo = self.criterio_minimo[[
                "COLUMNA", "PORCENTAJE DE COMPLETITUD", "PORCENTAJE DE EXACTITUD", "CRITERIO MINIMO"]].set_index("COLUMNA").iloc[::-1]

            self.criteria_valores_generales["SCORE COMPLETITUD"] = self.min_criterio_minimo["PORCENTAJE DE COMPLETITUD"].mean(
            )
            self.criteria_valores_generales["SCORE EXACTITUD"] = self.min_criterio_minimo["PORCENTAJE DE EXACTITUD"].mean(
            )
            self.criteria_valores_generales["SCORE CRITERIO"] = self.min_criterio_minimo["CRITERIO MINIMO"].mean(
            )

        except Exception as e:
            print(f"ERROR al generar los criterios mínimos\n{e}")

    def __set_CONFIG_FILE_PATH__(self):

        return os.path.join(
            self.BASE_DIR,
            os.getenv("CONFIG_DIR", "config_files"),
            f"{self.nombre_tabla}",
            f"{self.nombre_tabla}_config.json"
        )

    def __save_last_config_file(self, **kwargs):

        if Path(self.NEW_CONFIG_FILE).is_file():
            print(
                "Archivo de configuracion existe, no se procederá a guardar ninguna nueva")
            return

        if not self.get_config_file:
            config_json_file = self.config

        else:
            config_base_file = Path(self.BASE_DIR) / self.get_config_file
            config_json_file = get_config_files(config_base_file)

        config_json_file["exactitud_reglas"] = self.__values_aprox_function__()

        if isinstance(self.chars_omitir_exactitud, dict):
            config_json_file["completitud_reglas"]["rules"] = self.chars_omitir_exactitud

        validate_output_file(output_dir=Path(
            self.NEW_CONFIG_FILE).resolve().parent)

        save_new_config_file(
            path=self.NEW_CONFIG_FILE,
            json_to_save=config_json_file
        )

    def to_csv_resumen_table(self, **kwargs):
        """
        CSV de Resumen
        """

        print(f"{'-'*70}\nTABLA ANALIZADA : {self.nombre_tabla.upper()}\n")

        TODAY = date.today().strftime("%b-%d-%Y")
        OUTPUT_DIR = Path(self.BASE_DIR) / kwargs.get("output_dir",
                                                      f"{self.nombre_tabla}_analisis") / f"{self.nombre_tabla}" / f"csv"

        path = Path(OUTPUT_DIR)

        FILE_NAME = kwargs.get("file_name", f"{self.nombre_tabla}_{TODAY}")

        FILE_NAME_OUTPUT_RESUMEN = path / f"RESUMEN_{FILE_NAME}.csv"
        FILE_NAME_OUTPUT_CRITERIO_MINIMO = path / \
            f"CRITERIOS_MINIMOS_{FILE_NAME}.csv"
        FILE_NAME_OUTPUT_CRITERIO_MINIMO_SEGREGADO = path / \
            f"CRITERIOS_MINIMOS_SEGREGADO_{FILE_NAME}.csv"

        files_names = [FILE_NAME_OUTPUT_RESUMEN,
                       FILE_NAME_OUTPUT_CRITERIO_MINIMO
                       ]

        if self.resumen is None:
            print("Correr el método get_resumen previo a realizar este metodo ")
            return

        if self.criterio_minimo is None:
            self.get_criterios_minimos_resumen()

        csv_paramas = [
            {
                "resumen":   df.iloc[::-1],
                "output_dir":   OUTPUT_DIR,
                "file_name":   file_name
            }
            for df, file_name
            in zip([self.resumen, self.criterio_minimo], files_names)

        ]

        segregado = [{
            "resumen":   self.segregacion_criterios_minimos.iloc[::-1],
            "output_dir":   OUTPUT_DIR,
            "file_name":   FILE_NAME_OUTPUT_CRITERIO_MINIMO_SEGREGADO,
            "index":   True
        }]

        if self.resumen_agg_df is not None:
            FILE_NAME_OUTPUT_grouped = path / \
                f"grouped_BY{''.join(self.grouped_by).upper()}_{FILE_NAME}.csv"

            grouped_data = [
                {
                    "resumen": self.resumen_agg_df,
                    "output_dir":   OUTPUT_DIR,
                    "file_name":   FILE_NAME_OUTPUT_grouped,
                    "index":   True
                }
            ]

            csv_paramas += grouped_data

        csv_paramas += segregado

        for params in csv_paramas:
            to_csv_func(**params)

        # GUARDA LA CONFIGURACION USADA PARA EL ANALISIS
        self.__save_last_config_file()

        print("-"*100, '\n')

    def total_score_gauge_save(self, **kwargs):
        """
        :params
            columns:list -> lista de columnas para hacer una media de la columna indicada

        """
        print(f"{'-'*70}\nTABLA ANALIZADA : {self.nombre_tabla.upper()}\n")

        name = self.nombre_tabla

        OUTPUT_DIR = Path(self.BASE_DIR) / kwargs.get("output_dir",
                                                      f"{name}_analisis") / f"{name}"/f"gauge_images"
        columnas = kwargs.get(
            "columns", ["PORCENTAJE DE COMPLETITUD", "PORCENTAJE DE EXACTITUD"])

        TODAY = date.today().strftime("%Y-%m-%d")

        labels = os.getenv(
            "LABELS", "No confiable,Poco confiable,Confiable").split(",")
        colors = os.getenv("COLORS", '#E74C3C,#F5CBA7,#58D68D').split(",")

        path = Path(OUTPUT_DIR)

        validate_output_file(output_dir=path)

        SCORING = [
            {"arrow":   arrow_value(score(self.resumen, col)),
             "title":   f"\n{name.upper()}\n{col.upper()}\n{score(self.resumen,col)}%",
             "fname":   path / f"{name}_{col}_{TODAY}.png"
             }
            for col in columnas
        ]

        total_criterio_minimo = round(
            self.criteria_valores_generales["SCORE CRITERIO"], 2)

        DATA_SCORING = {
            "labels": labels,
            "colors": colors,
        }

        TOTAL_SCORING = [
            {**{
                "arrow":   arrow_value(total_criterio_minimo),
                "title":   f"\n{name.upper()}\n{'CRITERIO MINIMO TOTAL'.upper()}\n{total_criterio_minimo}% ",
                "fname":   path / f"{name}_CRITERIO MINIMO TOTAL_{TODAY}.png"
            }, **DATA_SCORING}
        ]

        SCORING += TOTAL_SCORING

        scoring_to_gauge_params = [
            {**DATA_SCORING, **col_score}
            for col_score in SCORING
        ]

        self.scoring_to_gauge_paths = {
            score["title"]: score["fname"]
            for score in scoring_to_gauge_params}

        try:

            for gauge_img in scoring_to_gauge_params:
                gauge(**gauge_img)

            save_criterios_generales(
                criterio_min=self.min_criterio_minimo,
                title=f"{self.nombre_tabla.upper()}",
                path=path / f"{name}_barh_{TODAY}.png"
            )

            print("-"*100, '\n')
        except Exception as e:
            print("Error en la creacion de los gauges")
            print(f"{e}")

    def gather_data(self, file_name=None):
        '''
        @paramas
            file_name: 
            nombre del archivo xlxs que dará como output
        '''

        BASE_DIR = os.path.dirname(os.path.dirname(__file__))

        folder_name = 'output'
        folder_path = os.path.join(BASE_DIR, folder_name)

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        file_path = "monstry/modules/export_modules/matriz_evaluacion_calidad_datos.template.xlsx"

        if file_name is None:
            file_output_path = f"matriz_evaluacion.{datetime.today().strftime('%b_%d_%Y')}_output.xlsx"
        else:
            file_output_path = f"{file_name}.xlsx"

        template_path = os.path.join(BASE_DIR, file_path)
        output_data = os.path.join(folder_path, file_output_path)

        worksheet_config = {
            'template_wb_matriz': template_path,
            'sheet': '2.1 Calidad por Variable',
            'output': output_data,
            'completitud': self.completitud,
            'exactitud': self.exactitud,
            'data_columnas': self.DATA_DEFAULT_COLUMNAS,
            'data_builder': self.builder_config
        }

        insercion_data(worksheet_config)
        return

    def __inexactos__(self):
        if len(self.inexactos) == 0:
            return 'No data'

        for columna, data in self.inexactos.items():
            print(columna.center(100, '-'))
            print(data)
        return

    def __exactos__(self):
        if len(self.exactos) == 0:
            return 'No data exacta'

        for columna, data in self.exactos.items():
            print(columna.center(100, '-'))
            print(data)
        return

    def __values_aprox_types__(self):
        return {x["columna"]: x.get('dtype') for x in self.DATA_DEFAULT_COLUMNAS}

    def __values_aprox_function__(self):
        return {x["columna"]: x.get('default').__name__ for x in self.DATA_DEFAULT_COLUMNAS}

    def __values_sample__(self):
        return {x["columna"]: x.get('sample') for x in self.DATA_DEFAULT_COLUMNAS}

    def __exactitud_config__(self):

        opcion = input("Modificamos el set de funciones?\t ") or False

        with HiddenPrints():
            config = {
                settings["columna"]: settings["default"].__name__ for settings in self.DATA_DEFAULT_COLUMNAS}

        if not opcion:
            return config

        returner_value = {}

        for key, value in config.items():

            validacion = input(
                "Modificamos la funcion a validar?\nEl valor por defecto va a ser no") or False

            if not validacion:
                nombre_funcion = input(
                    "Indique el nombre de la funcion") or "comprobando_no_determinado"
                returner_value[key] = nombre_funcion

            returner_value[key] = value
            return returner_value

    def __exactitud_funciones_config__(self, path_in, path_out):

        BASE_DIR = os.path.dirname(os.path.dirname(__file__))
        path_funciones_default = os.path.join(BASE_DIR, self.PATH_DEFAULT)

        print(path_funciones_default)

        with open(path_funciones_default, 'r') as file:
            data = file.read().rstrip()

        with open(path_in, 'r') as file:
            new_data = file.read().rstrip()

        joiner_text_functions = f"{data}\n\n\n{new_data}"

        with open(path_out, 'w') as output:
            output.write(joiner_text_functions)

    def print_agrupaciones(self):
        if self.resumen_agg_df is None:
            with HiddenPrints():
                self.get_agg_resumen()

        print(f"""
TABLA: {self.nombre_tabla.upper()} 
CANTIDAD DE REGISTROS ANALIZADOS: {self.cantidad_registros} REGISTROS
""")

        print(tabulate(self.resumen_agg_df, headers="keys",
              tablefmt="rounded_outline", floatfmt=".2f"))

    def print_criterios_minimos(self):
        if self.resumen is None:
            with HiddenPrints():
                self.get_resumen()

        print(f"""
TABLA: {self.nombre_tabla.upper()} 
CANTIDAD DE REGISTROS ANALIZADOS: {self.cantidad_registros} REGISTROS

-   COMPLETITUD GENERAL         : {round(self.criteria_valores_generales["SCORE COMPLETITUD"],2)} %
-   EXACTITUD   GENERAL         : {round(self.criteria_valores_generales["SCORE EXACTITUD"],2)} %
-   CRITERIO    MINIMO GENERAL  : {round(self.criteria_valores_generales["SCORE CRITERIO"],2)} %
        """)

        print(tabulate(self.min_criterio_minimo, headers="keys",
              tablefmt="rounded_outline", floatfmt=".2f"))
        print("""
              
        SEGREGACION POR CRITERIOS MINIMOS POR COLUMNAS
        
        """)

        print(tabulate(self.segregacion_criterios_minimos,
              headers="keys", tablefmt="rounded_outline", floatfmt=".2f"))

        print('-'*120)

    def print_analisis_to_markdown(self, **kwargs):

        if self.resumen is None:
            with HiddenPrints():
                self.get_resumen()

        texto_resumen = f"""
TABLA ANALIZADA:  {self.nombre_tabla.upper()} 
---

## CANTIDAD DE REGISTROS ANALIZADOS: {self.cantidad_registros} REGISTROS
    
*   COMPLETITUD GENERAL         : {round(self.criteria_valores_generales["SCORE COMPLETITUD"],2)} %
*   EXACTITUD   GENERAL         : {round(self.criteria_valores_generales["SCORE EXACTITUD"],2)} %
*   CRITERIO    MINIMO GENERAL  : {round(self.criteria_valores_generales["SCORE CRITERIO"],2)} %

---

"""

        criterios_min = tabulate(
            self.min_criterio_minimo, headers="keys", tablefmt="github", floatfmt=".2f")

        segregacion = [
            tabulate(self.segregacion_criterios_minimos[[
                     col]][::-1], headers="keys", tablefmt="github")
            for col
            in ["CONFIABLE", "POCO CONFIABLE", "NO CONFIABLE"]
        ]

        FILE_NAME = kwargs.get("file_name", f"{self.nombre_tabla}.md")
        OUTPUT_DIR = Path(self.BASE_DIR) / kwargs.get("output_dir",
                                                      f"{self.nombre_tabla}_analisis") / f"{self.nombre_tabla}"
        path = Path(OUTPUT_DIR)

        with HiddenPrints():
            validate_output_file(output_dir=path)

        image_scoring = "\n## MEDIDORES\n<p float='left'>"

        for title, path_ in self.scoring_to_gauge_paths.items():
            path_ = f"./gauge_images/{str(path_).split('/')[-1]}"
            title = title.replace("\n", "_")
            image_scoring += f'<img src="{path_}" alt="{title}" style="width:300px;"/>\n'

        image_scoring += "</p>\n\n"

        looper_lines = "\n\n\n---\n\n\n"

        TODAY = datetime.today().strftime("%m/%d/%Y, %H:%M:%S")

        with open(path / FILE_NAME, "w", encoding='utf-8') as f:
            f.writelines(texto_resumen)
            f.writelines(image_scoring)
            f.writelines("\n"*3)
            f.writelines(
                "\n## TABLA DE COMPLETITUD - EXACTITUD - CRITERIO MINIMO \n\n")
            f.writelines(
                criterios_min
            )
            f.writelines("\n"*3)
            f.writelines("\n## SEGREGACION POR COLUMNAS \n\n")
            for _ in segregacion:
                f.writelines("\n\n\n")
                f.writelines(_)
            f.writelines(looper_lines)
            f.writelines(f"__FECHA CREACION__ : {TODAY}")

        print(f"""
        >>> CREACION MARKDOWN FINALIZADA PARA {self.nombre_tabla.upper()}
            UBICACION : {path}
        """)
