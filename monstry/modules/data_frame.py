
import pandas as pd
from sqlalchemy import text


def data_frame(**kwargs):
    
    query       =   kwargs["query"]
    engine_cnx  =   kwargs["engine_cnx"]
    
        
    engine_ = engine_cnx.execution_options(stream_results=True)
    data_frame = pd.DataFrame()



    for index,chunks in enumerate(pd.read_sql(text(query),engine_,chunksize=100_000)):
        """
        realiza consumo por medio de chunks de registros
        """
        data_frame = pd.concat([data_frame,chunks],ignore_index=True,axis=0)
        print(f"{index} : dataframe with {len(chunks)} rows")

    return data_frame