import os, sys

from datetime import datetime
from .modules.export_modules.export_matriz import insercion_data
from .modules.template_modules.file_data import file_data
from .modules.hidden_prints import HiddenPrints

class DataAppender:

    __slots__ = []  
    
    def __init__(self) -> None:
        
        print("Se ha inicializado el objeto unificador de datasets")

    def __str__(self):
        return f"Appender con {len(self.__slots__)} datasets ingresados en el objeto"

    def dataset_appender(self,*args):

        if len(args) == 0:
            print('\033[93mNo se agregó ningún dataset al objeto')
            return 
        
        try:

            for index,dataset in enumerate(args):
                appender_data_set = {
                    'nombre':dataset.nombre_tabla if dataset.nombre_tabla is not None else f'tabla_desconocida_{index}',
                    'dataset':dataset

                    }
                self.__slots__.append(appender_data_set)
        
            print(f'Se agregaron: \t{len(args)} DataSets')
            
            return
        
        except Exception as e:
            print(e)
        
        return 


    def dataset_remover(self, nombre_dataset:str)->None:

        nombre_dataset = nombre_dataset.strip()

        if nombre_dataset in ['',None]: return

        nombres_disponibles = [dataset.nombre_tabla for dataset in self.__slots__]

        if nombre_dataset not in nombres_disponibles:
            print('El nombre del dataset seleccionado no pertenece a ninguno de los datasets almacenados')
            return 

        self.__slots__ = [dataset for dataset in self.__slots__ if dataset.nombre_tabla != nombre_dataset.strip()] 

        return
    
    def get_preview(self):
    
        with HiddenPrints():
            
            resumenes = [ data['dataset'].get_resumen() for data in self.__slots__] 
            result = [(data.completitud , data.exactitud) for data in [ data['dataset'] for data in self.__slots__] ]

        for df_resumen in resumenes:
            print('-'*100)
            print(df_resumen)

        return [(data.completitud , data.exactitud) for data in [ data['dataset'] for data in self.__slots__] ]



    def data_printer(self, file_name = None):

        PATHS = file_data(file_name)
        SHEET = '2.1 Calidad por Variable'

        results = None

        with HiddenPrints():            
            resumenes = [ data['dataset'].get_resumen() for data in self.__slots__] 

            result = [ {
                'dataframe'     :   data ,
                'completitud'   :   data.completitud ,
                'exactitud'     :   data.exactitud  ,
                'data_columnas' :   data.data_columnas(),
                'data_builder'  :   data.builder_config
            }  for data in [ data['dataset'] for data in self.__slots__] ]

        worksheet_config = {
            'template_wb_matriz':   PATHS["template_path"],
            'output'            :   PATHS["output_data"],
            'sheet'             :   SHEET,
            'results'           :   result
        }

        try:
            insercion_data(worksheet_config)
        except Exception as e:
            print(e)

        return None