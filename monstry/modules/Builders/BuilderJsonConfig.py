import re
from pathlib    import  Path

from .Builder           import  Builder     
from .InteraceGetters   import  InterfaceGetters  

from ..get_json_config  import  get_config_files
from ..use_env_files    import  use_env_files
from ..get_matcher      import  get_matcher


class BuilderJsonConfig(Builder,InterfaceGetters):
    ENGINE_AVAILABLE = "mysql,postgres,oracle".split(",")
    EXTENSION_AVAILABLE = "xlsx,csv".split(",")


    def __init__(self, **kwargs):
        
        self.BASE_DIR   =   kwargs.get("base_dir","./")
        self.config_path=   kwargs.get("config_path",None)

        self.config     =   get_config_files(self.config_path)

        self.builder    =   self.config["builder"]
        self.engine     =   self.builder["engine"]
        self.table      =   kwargs.get("table",None)  
        
        self.database   =   kwargs.get("database",None)
        
        try:
            if self.engine in self.ENGINE_AVAILABLE:
                self.cnx        =   use_env_files(cnx = self.builder["cnx"])
                                
                if self.builder.get("path",None) is not None:
                    self.query      =   self.get_query(
                            path    =   self.builder["path"],
                            base_dir=   self.BASE_DIR
                    )
                    
                else:
                    self.query  =   kwargs.get("query_path",None)
                                  
            elif self.engine in self.EXTENSION_AVAILABLE:
                self.cnx    =   {}
                path_file   =   get_matcher(builder=self.builder)
                
                if not path_file : 
                    print("No se ha encontrado una ruta para el archivo")
                    return 

                self.query_path =   self.builder[path_file]  
                self.query      =   None
                
                if self.table is None:
                    regexp_file_name = '[ \w-]+?(?=\.)'
                    self.table       =   re.search(regexp_file_name, self.query_path)[0]
                
        except Exception as e:
            print(f"ERROR producido en la lectura del archivo de configuracion\n\t{e}")
            
            
    def get_query(self,**kwargs):
        
        path  =   kwargs["path"]
        BASE_DIR =  kwargs["base_dir"]
        
        query_path  =   Path(BASE_DIR) / path
        
        if not query_path.is_file():
            print("No existe el archivo seteado ")
        
        try:
            with open(query_path,"r") as query:
                return query.read()
            
        except Exception as e:
            print(f"ERROR producido en la obtencion de la query desde un archivo\n\t{e}")
