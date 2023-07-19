import numpy as np

from .DataCleaner import DataCleaner
from .modules.hidden_prints import HiddenPrints
from .modules.kpis.completitud_nan import cleaner_df_nan
from .modules.kpis.kpis_config import read_kpi_config
from .modules.kpis.df_selector import database_cleaned
from .modules.kpis.kpis_config import restructuracion
from .modules.kpis.analisis_exactitud import exactitud_transform
from .modules.kpis.remover import scoring_df

class DataKpi:
    
    def __init__(self , **kwargs) -> None:
        """"
        
        
        """
        data_cleaner = kwargs['data']
        path = kwargs['config_path']

        self.data       = data_cleaner.dataframe
        self.columns    = data_cleaner.columnas
        self.path       = path
        self.completitud_settings = data_cleaner.chars_omitir_exactitud
        try:
            with HiddenPrints():
                data_cleaner.get_exactitud()
            self.raw_exactitud      = data_cleaner.raw_rules

        except Exception as e:
            print(e)
        

    def config_kpi(self):
        """
        Lee la configuracion desde el archivo .json

        @Params:
            path (str): ruta relativa del tipo json
        @Returns:
            kpis_config (dict)

        """
            
        if self.path is None:
            return 
        
        kpis_config = restructuracion(self.path,self.columns)

        return kpis_config
    

    def status_por_completitud(self):

        limpieza_info = {
            "data": self.data,
            "config" : self.config_kpi(),
            "completitud_settings" : self.completitud_settings
        }

        return database_cleaned(**limpieza_info)

    

    def status_por_exactitud(self):

        try:
            with HiddenPrints():
                completitud_score = self.status_por_completitud()
        except Exception as e:
            print(e)

        return exactitud_transform(completitud_score , self.raw_exactitud)

    def scoring(self):
        try:
            with HiddenPrints():
                #Engloba todos los procesos anteriores#

                total_score = self.status_por_exactitud()
        except Exception as e:
            print(e)

        data_ = [ {**value , **{"numero_kpi":key}}  for  key ,value in total_score.items() ]

        df_stored_kips_remover = scoring_df(data_)

        return df_stored_kips_remover