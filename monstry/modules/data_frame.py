
import pandas as pd
from sqlalchemy import text


def data_frame(**kwargs):
    
    query       =   kwargs["query"]
    engine_cnx  =   kwargs["engine_cnx"]
    
        
    engine_ = engine_cnx.execution_options(stream_results=True)
    data_frame = pd.DataFrame()


    print(f"""{'-'*50}""")
    for index,chunks in enumerate(pd.read_sql(text(query),engine_,chunksize=100_000)):
        """
        realiza consumo por medio de chunks de registros
        """
        data_frame = pd.concat([data_frame,chunks],ignore_index=True,axis=0)
        print(f""" 
            !>>  Chunk {index+1} 
                Dataframe: {query.split("FROM")[1].split("LIMIT")[0]} Cantidad de {len(chunks)} registros    
        """)
    return data_frame