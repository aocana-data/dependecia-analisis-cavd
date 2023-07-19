from typing import List
from ..BulkDataCleaner  import BulkDataCleaner
from ..DataCleaner      import DataCleaner

def bulk_cleaner_postgresql(**kwargs):
    builders    =    kwargs["builders"]
    
    builders    =    builders.get_databases_postgresql()
        
    cleaners:List[BulkDataCleaner]    =   [
        
        BulkDataCleaner(
            builder         =   builder.build(),
            nombre_tabla    =   builder.table
        )
        for builder
        in  builders
    ]
    
    return cleaners


def bulk_cleaner_files(**kwargs):
    builders    =    kwargs["builders"]
    
    cleaners:List[DataCleaner]    =   [
        
        DataCleaner(
            builder         =   builder,
            nombre_tabla     =   builder.get_table()
        )
        for builder
        in  builders
    ]

    
    return cleaners