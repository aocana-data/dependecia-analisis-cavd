
'''
Funciones para la conexion a las bases de datos
'''

import pandas as pd
from sqlalchemy.engine import create_engine

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy     import text

from .get_engine    import get_engine_df
from .data_frame    import data_frame


# def oracle_cnx(cnx, query:str = None , engine:str = None):
#     try:
#         URI_ORACLE = f"oracle+cx_oracle://{cnx.get('user')}:{cnx.get('password')}@{cnx.get('host')}:{cnx.get('port')}/?service_name={cnx.get('service_name')}&encoding=UTF-8&nencoding=UTF-8"
#         engine = create_engine(URI_ORACLE).connect()

#         return pd.read_sql_query(text(query), engine)
    
#     except SQLAlchemyError as e:
#         print(e)
 
        
def read_db_csv(csv_path):
    return pd.read_csv(csv_path)

def read_db_xlsx(xlsx_path):
    return pd.read_excel(xlsx_path)

def read_db_engine(**kwargs):
 
    engine      =   kwargs.get("engine",None)
    builder     =   kwargs.get("builder",None)
    cnx         =   kwargs.get("cnx",None)
    query       =   kwargs.get("query",None)
    
               
    if engine == "csv":
        path_local  =   builder["csv_path"]
        return read_db_csv(path_local)
    
    if engine == "xlsx":    
        path_local  =   builder["xlsx_path"]
        return read_db_xlsx(path_local)
    
    if query is None or engine is None:
        print('No se han cargado el query o el engine')
        return
    
    engine_sql_alchemy  = get_engine_df(
        cnx         =   cnx,
        engine_db   =   engine
    )
        
    return  data_frame(
                query = query ,
                engine_cnx =  engine_sql_alchemy.connect() 
            )
    
    
