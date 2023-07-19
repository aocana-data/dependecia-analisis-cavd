from typing             import  List
from .BulkDataBuilder   import  BulkDataBuilder
from .BulkDataCleaner   import  BulkDataCleaner
from .DataCleaner       import  DataCleaner


from .modules.bulk_cleaner_files    import bulk_cleaner_postgresql
from .modules.bulk_cleaner_files    import bulk_cleaner_files



class BulkIterator:
    
    cleaners = None
    
    def bulk_analisis_iterator_config_file(self,**kwargs):
        builders:BulkDataBuilder    =   kwargs.get("builders",None)
        
        if builders is None:
            print("No hay builders creados")
            return 
        
        self.cleaners:List[BulkDataCleaner]    =   bulk_cleaner_postgresql(builders = builders)

        
    def bulk_analisis_iterator_local_files(self,**kwargs):
        builders:BulkDataBuilder    =   kwargs.get("builders",None)
        if builders is None:
            print("No hay builders creados")
            return 
        
        builders    =  [ builder for builder in builders.get_databases_local_files()]

        self.cleaners:List[DataCleaner]    =   bulk_cleaner_files(builders = builders)

        
        
    def bulk_resumen_save(self,**kwargs):
        output_dir  =   kwargs.get("output_dir",None)
        
        for n,cleaner in enumerate(self.cleaners):
            try:
                cleaner.get_resumen()
                
                cleaner.to_csv_resumen_table(
                    output_dir  =   output_dir 
            )

                
            except Exception as e:
                print(f"ERROR en el {n+1} cleaner\n{e}")

    def bulk_gauge_save(self,**kwargs):
        output_dir  =   kwargs.get("output_dir",None)
        
        for n,cleaner in enumerate(self.cleaners):
            if cleaner.resumen is None:
                cleaner.get_resumen()
            try:
                
                cleaner.total_score_gauge_save(
                    output_dir  =   output_dir ,
                )

                
            except Exception as e:
                print(f"ERROR en el {n+1} cleaner\n{e}")        

        
        
    