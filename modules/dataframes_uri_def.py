
'''
Funciones para la conexion a las bases de datos
'''

import pandas as pd
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text


def oracle_cnx(cnx, query:str = None , engine:str = None):
    try:

        URI_ORACLE = f"oracle+cx_oracle://{cnx.get('user')}:{cnx.get('password')}@{cnx.get('host')}:{cnx.get('port')}/?service_name={cnx.get('service_name')}&encoding=UTF-8&nencoding=UTF-8"
        engine = create_engine(URI_ORACLE).connect()

        return pd.read_sql_query(text(query), engine)
    except SQLAlchemyError as e:
        print(e)
 
        
def read_db_csv(csv_path):
    df = pd.read_csv(csv_path)

    return df


def read_db_xlsx(xlsx_path):
    return pd.read_excel(xlsx_path)

def read_db_engine(cnx, query:str = None , engine:str = None):
    if query is None or engine is None:
        print('No se han cargado el query o el engine')
        return

    try:
        db_URIConector={
            "oracle":f"oracle+cx_oracle://{cnx.get('user')}:{cnx.get('password')}@{cnx.get('host')}:{cnx.get('port')}/?service_name={cnx.get('service_name')}&encoding=UTF-8&nencoding=UTF-8",
            "postgres":f"postgresql://{cnx.get('user')}:{cnx.get('password')}@{cnx.get('host')}:{cnx.get('port')}/{cnx.get('database')}",
            "mysql":f"mysql+pymysql://{cnx.get('user')}:{cnx.get('password')}@{cnx.get('host')}:{cnx.get('port')}/{cnx.get('database')}"
        }

        URI = db_URIConector.get(engine,None)
            
        if URI is None:
            print('No se encuentra ese motor')
            return 
        
        print(URI)
        engine_ = create_engine(URI).connect()
        
        
        engine_ = engine_.execution_options(stream_results=True)
        data_frame = pd.DataFrame()

        """
        Nuevas actualizaciones que se realizaron con SQLAlchemy > 2.0.0
        """

        for index,chunks in enumerate(pd.read_sql(text(query),engine_,chunksize=100_000)):
            data_frame = pd.concat([data_frame,chunks],ignore_index=True,axis=0)
            print(f"{index} : dataframe with {len(chunks)} rows")

        return data_frame

        
        

    except SQLAlchemyError as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        engine_.close()
        print('Cerrando conexion')
