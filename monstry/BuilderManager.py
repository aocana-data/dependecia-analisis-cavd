from uuid import uuid4

from    dotenv  import  load_dotenv
from    .modules.Builders.Builder   import Builder
from    .modules.Builders.BuilderCnxQuery   import BuilderCnxQuery
from    .modules.Builders.BuilderJsonConfig import BuilderJsonConfig
from    .modules.Builders.BuilderLocalFiles import BuilderLocalFiles


load_dotenv()

class BuilderManager:
    
    def __init__(self,**kwargs):
        """
        Genera tres tipos de lectores y constructores de lectores de configuraciones
        
        -   BuilderLocalFiles   :   Constructor para realizar construcciones de configuraciones
                                    para archivos locales con extension .csv .xlsx 
                                    
                                    :params
                                        -   base_dir  : ruta raiz del proyecto default ./
                                        -   engine    : motor de base del tipo
                                        -   file_path : ruta del archivo 
                                    
        -   BuilderJsonConfig   :   Constructor usando configuraciones por medio de un archivo 
                                    json
                                    
                                    :params
                                        -   base_dir  : ruta raiz del proyecto default ./
                                        -   config_path:  ruta del archivo de configuracion
                                    
        -   BuilderCnxQuery     :   Constructor para analisis con solo queries directas desde 
                                    un archivo path /path/file.sql o una query directa como string
                                    
                                    :params
                                        -   base_dir :  ruta raiz del proyecto default ./
                                        -   query | query_path: ruta del archivo .sql para analisis
                                        tambien ser usado una query como string
                                        -   cnx : dict de conexion:
                                                    :cnx = {    'host': 'localhost',
                                                                'port': 3306,
                                                                'database':'database',
                                                                'schema': 'database.schema',
                                                                'table': '',
                                                                'password': 'admin',
                                                                'user':'root'
                                                        }
                                        -   engine: tipo de motor utilizado para hacer la conexion
                                        UPDATE 12 JULIO : solamente posible realizado para mysql,postgresql,oracle
        
        """
        
        
        self.BASE_DIR       =   kwargs.get("base_dir","./")
        
        self.config_path    =   kwargs.get("config_path",None)
        self.file_path      =   kwargs.get("file_path",None)
        
        self.query_path     =   kwargs.get("query_path",None)
        self.query          =   kwargs.get("query",None)
        self.cnx            =   kwargs.get('cnx',None)
        self.engine         =   kwargs.get("engine",None)
        self.table          =   kwargs.get("table",None)

        
    def build(self):
    
        if self.file_path is not None:
            print("CONSTRUCTOR CON ARCHIVOS LOCALES")
            return BuilderLocalFiles(
                base_dir    =   self.BASE_DIR   ,
                file_path   =   self.file_path  ,
                table       =   self.table      
            )
        
        if self.config_path is not None:
            print("CONSTRUCTOR CON ARCHIVO DE CONFIGURACION")
            
            return BuilderJsonConfig(
                base_dir    =   self.BASE_DIR   ,
                config_path =   self.config_path,
                table       =   self.table      
            )
            
            
        if self.query is not None:
            print("CONSTRUCTOR CON QUERY TEXT")
            return BuilderCnxQuery(
                query_path  =   self.query_path ,
                base_dir    =   self.BASE_DIR   ,
                engine      =   self.engine     ,
                query       =   self.query      ,
                cnx         =   self.cnx        ,
                table       =   self.table
            )
        