import os
import re
from uuid                   import  uuid4
from pathlib                import  Path
from .Builder               import  Builder       
from .InteraceGetters       import  InterfaceGetters
from ..dataframes_uri_def   import  read_db_engine

class BuilderLocalFiles(Builder,InterfaceGetters):
    
    database = None
    EXTENSION_AVAILABLE = "xlsx,csv".split(",")
    
    
    def __init__(self,**kwargs):
    
        self.BASE_DIR   =   kwargs.get("base_dir","./")
        self.file_path  =   kwargs.get("file_path",None)
        self.directory_files    =   kwargs.get("directory_files",None)
        
        self.table      =   kwargs.get("table",None)
        
        if self.file_path is None:
            print("No se ha cargado la ruta del archivo")
            return
        
        regexp_file = '\.[^.\\/:*?"<>|\r\n]+$'
        regexp_file_name = '[ \w-]+?(?=\.)'
        
        
        file_name       =   None
        
        if self.table is None:
            self.table       =   re.search(regexp_file_name, self.file_path)[0]
            
        file_extension   = re.search(regexp_file,self.file_path)[0][1:]
        
        self.engine     =   file_extension
      
        if self.engine not in self.EXTENSION_AVAILABLE:
            print("Temporalmente no se encuentra disponible esa extension")
            return 
      
        self.config     =   {    
            "builder" : {
                "engine":   self.engine,
                "cnx"   :   {}
            },
            "exactitud_validadores": "",
            "exactitud_reglas": {},
            "chars_null": [",", ".", "'", "-", "_", ""],
            "completitud_reglas": {"rules": {} },
            "dtypes": {}
        }
        
        
        self.builder    =   self.config["builder"]
        self.cnx        =   self.builder["cnx"]

        if self.directory_files is not None:
            self.config["builder"][f"{file_extension}_path"] =  os.path.join(
                self.directory_files,
                self.file_path
            )
            
        else:
            self.config["builder"][f"{file_extension}_path"] = self.file_path
            
        

        
    def get_database(self):
        try:
            self.database = read_db_engine(
                builder =   self.builder,
                engine  =   self.engine
            )
        
            if self.database is None:
                print('No se ha generado el dataframe')
                return
            
            print('Se ha cargado efectivamente el dataframe')

        except Exception as e:
            print(f"ERROR producido al generar el dataframe\n\t{e}")    
        