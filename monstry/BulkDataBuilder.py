from .Builder import Builder
from .modules.bulk_data_postgresql import bulk_dataframes

class LinkerDataBuilder:
    def __init__(self,**kwargs) -> None:
        self.cnx            =   kwargs["cnx"]
        self.database       =   kwargs["database"]
        self.table_name     =   kwargs["table_name"]        
        self.data_config    =   kwargs["data_config"]

class BulkDataBuilder(Builder):
    databases   =   None
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.limit = kwargs.get("limit",None)
    
    def get_database(self):
        try:
            if self.config is not None:
                cnx = self.config.get('cnx', None)                
                self.cnx = self.use_env_files(cnx)
                engine = self.config.get('engine')
                        
            self.databases  = bulk_dataframes(cnx = self.cnx , engine = engine , limit = self.limit )
            
            if self.databases is None or len(self.databases) ==0:
                print('No se ha generado el dataframe')
                return
            
            print('Se ha cargado efectivamente el dataframe')

        except Exception as e:
            print(e)    
    
    def values_to_release_cleanner(self):
        return [
            LinkerDataBuilder(
                database    =   database    ,
                table_name  =   table       ,
                cnx         =   self.cnx    ,
                data_config =   self.data_config 
            )
            for 
            table,database
            in
            self.databases.items()
        ]
