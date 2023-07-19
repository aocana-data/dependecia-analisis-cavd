
from monstry.DataCleaner       import DataCleaner
from monstry.BulkDataBuilder   import LinkerDataBuilder
from .modules.check_new_config_file import  check_new_config_file


class BulkDataCleaner(DataCleaner):
    
    def __init__(self, **kwargs) -> None: 
        self.builder:LinkerDataBuilder = kwargs["builder"]
        
        
     
        # CONFIGURACIONES DESDE EL BUILDER
        self.nombre_tabla:str   =   self.builder.get_table()
        self.config             =   self.builder.get_config()
        
        # LOS DATOS DE CONEXION DIRECTAMENTE DESDE EL BUILDER
        self.builder_config     =   self.builder.get_cnx()
        
        # EL DATAFRAME GENERADO POR EL BUILDER DIRECTAMENTE
        self.dataframe          =   self.builder.builder_get_database()
        self.BASE_DIR           =   self.builder.get_base_dir()
        self.get_config_file    =   self.builder.get_config_path()
        
        # AGREGADO DE NOMBRE DE TABLA Y COLUMNAS DERIVADAS DEL BUILDER
        self.columnas           =   self.dataframe.columns
        self.cantidad_registros =   len(self.dataframe)
        
        
        # SETEADO DE UN ARCHIVO DE CONFIGURACION QUE SE OBTENDRÁ UNICAMENTE 
        # CUANDO EL CLEANER EJECUTE EL METODO DE GUARDADO DE CSV Y GAUGES
        # DE ANALISIS DE:
        #     COMPLETITUD
        #     EXACTITUD
        #     CRITERIOS MINIMOS
            
        self.CONFIG_FILE_PATH   =   self.__set_CONFIG_FILE_PATH__()
        self.NEW_CONFIG_FILE    =   self.__set_CONFIG_FILE_PATH__()
        
                
        path    =   check_new_config_file(  
            config_file_name    =   self.CONFIG_FILE_PATH   ,
            default_config_file =   self.get_config_file
        )
        
        self.CONFIG_FILE_PATH   =   path
        
        self.set_config(self.CONFIG_FILE_PATH)
        
        data_print  =   f"""
        OBJETO DE ANALISIS:
        
            TABLA:
                {self.nombre_tabla.upper()}
        {'-'*70}
            CANTIDAD DE REGISTROS   
                {self.cantidad_registros}
        {'-'*70}
        """
        
        print(data_print)


