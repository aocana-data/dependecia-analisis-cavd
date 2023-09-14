from ..dataframes_uri_def import read_db_engine


class Builder:
 
    def get_database(self):
        try:
            if self.database is None :
                self.database = read_db_engine(
                                        builder = self.builder,
                                        cnx     = self.cnx,
                                        query   = self.query,
                                        engine  = self.engine,
                                        nombre_tabla    = self.table
                    )
                
            if self.database is None:
                print('No se ha generado el dataframe')
                return
            
        
            
            print('Se ha cargado efectivamente el dataframe')

        except Exception as e:
            print(f"ERROR producido al generar el dataframe\n\t{e}")    
        