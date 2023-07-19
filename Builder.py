import os
from dotenv import load_dotenv
from typing import Optional
from .modules.dataframes_uri_def import read_db_engine
from .modules.dataframes_uri_def import read_db_csv , read_db_xlsx
from .modules.dataframe_cleaner import get_reglas_casteo

load_dotenv()


class Builder:

    query = None
    
    def __init__(self, **kwargs):
        '''
        @params:
            query_path = ruta al archivo query
            config_path = ruta al archivo de configuracion
            cnx = diccionario de conexion
            ie: {   'host': 'localhost',
                    'port': 3306,
                    'database':'typ_sys',
                    'schema': 'typ_sys',
                    'table': '',
                    'password': 'admin',
                    'user':'root'
            }
        '''

        self.query_path = kwargs.get('query_path',None)
        self.cnx = kwargs.get('cnx',None)

        config_path_exits = kwargs.get('config_path',None)
        config_path_exits = os.path.join(os.getcwd(),config_path_exits)


        if config_path_exits is None:
            self.config = kwargs.get('config_path',None)
            print('Se deberán setear el path y la conexion a la fuente de datos')
            return 

        config = get_reglas_casteo(config_path_exits)

        self.config = config['builder']

        self.data_config = {
            k : config[k] for k in set( list(config.keys() - ['builder']) )
        }

        print('Se ha creado usando un archivo de configuracion')



    def get_database(self):
 
        try:
            if self.config is not None:
                self.query_path = self.config.get('query', None)
                cnx = self.config.get('cnx', None)
                
                self.cnx = self.use_env_files(cnx)
                engine = self.config.get('engine')
            
            
            if engine == "csv":
                csv_path = self.config.get('csv_path', None)                
                self.database = read_db_csv(csv_path)

            elif engine == 'xlsx':
                xlsx_path = self.config.get('xlsx_path',None)
                self.database = read_db_xlsx(xlsx_path)
                
            else:
                self.get_query()   
                self.database = read_db_engine(self.cnx, self.query, engine)
            
            
            if self.database is None:
                print('No se ha generado el dataframe')
                return
            
            print('Se ha cargado efectivamente el dataframe')

        except Exception as e:
            print(e)    
        
        
        
    def get_query(self):
        db = self.cnx.get('database', None) or self.config['cnx']['database']

        try:
   
            with open(self.query_path, 'r') as file:
                data = file.read()
                self.query = eval(data)
                print('Se ha cargado la query satisfactoriamente')
                return None

        except Exception as e :
            error = type(e)
            print(e)
            print(f'Error {error}\nData doc: {error.__doc__}')
            return 
    
    def use_env_files(self,cnx_json):
        
        if cnx_json is None: return None
        
        return { k:os.getenv(v) if v!='' else ''  for  k,v in cnx_json.items()}
        
