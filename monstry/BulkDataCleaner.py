from monstry.Builder    import Builder

from monstry.DataCleaner       import DataCleaner
from monstry.BulkDataBuilder   import LinkerDataBuilder

class BulkDataCleaner(DataCleaner):
    
    def __init__(self, builder:LinkerDataBuilder) -> None:
        
        self.config         =   builder.data_config
        self.builder_config =   builder.cnx
        self.dataframe      =   builder.database

        self.nombre_tabla   =   builder.table_name
        self.columnas       =   self.dataframe.columns
        
        self.set_config()
    