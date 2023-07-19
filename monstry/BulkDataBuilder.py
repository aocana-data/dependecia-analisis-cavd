import os

from .BuilderManager import BuilderManager
from .modules.Builders.InteraceGetters import InterfaceGetters
from .modules.Builders.BuilderJsonConfig import BuilderJsonConfig
from .modules.Builders.BuilderLocalFiles import BuilderLocalFiles

from .modules.bulk_data_postgresql  import bulk_dataframes
from .modules.get_json_config       import get_config_files
from .modules.use_env_files         import use_env_files

class LinkerDataBuilder(BuilderManager,InterfaceGetters):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.database   =   kwargs["database"]
        self.table      =   kwargs["table"]

    def build(self):
       
        if self.config_path is not None:
            print("CONSTRUCTOR CON ARCHIVO DE CONFIGURACION")
            
            return BuilderJsonConfig(
                base_dir    =   self.BASE_DIR   ,
                config_path =   self.config_path,
                table       =   self.table      ,
                database    =   self.database
            )



class BulkDataBuilder:
    
    databases   =   None
    engine      =   None
    cnx         =   None
    
    
    def __init__(self, **kwargs):
        self.config_path    =   kwargs.get("config_path",None)
        self.limit          =   kwargs.get("limit",None)
        self.config         =   get_config_files(self.config_path)
        self.directory_files =   kwargs.get("directory_files","input_files")
        
        
    def get_databases_postgresql(self):
        
        try:
            
            builder =   self.config["builder"]
            
            if self.config is not None:
                cnx             = builder.get('cnx', None)                
                self.engine     = builder.get('engine')
                self.cnx        = use_env_files(cnx = cnx)
                        
            
            self.databases  = bulk_dataframes(
                cnx     = self.cnx ,
                engine  = self.engine ,
                limit   = self.limit 
            )
            
            if self.databases is None or len(self.databases) ==0:
                print('No se han generado los dataframes')
                return

            print('Se ha cargado efectivamente el dataframe')

            return self.values_to_release_cleanner()
        
        except Exception as e:
            print(e)    
    

    def values_to_release_cleanner(self):
        
        return [
            LinkerDataBuilder(
                database    =   database    ,
                table       =   table       ,
                config_path =   self.config_path ,
            )
            for 
            table,database
            in
            self.databases.items()
        ]


    def get_databases_local_files(self):
        get_files   =   os.listdir(self.directory_files)

        return[
            BuilderLocalFiles( 
                file_path       =   file ,
                directory_files  =   self.directory_files
            )
            for file in get_files
        ]