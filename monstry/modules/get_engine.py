from sqlalchemy.engine import create_engine
from sqlalchemy.exc import SQLAlchemyError


def get_engine_df(cnx,engine_db):

    try:
        db_URIConector={
            "oracle":f"oracle+cx_oracle://{cnx.get('user')}:{cnx.get('password')}@{cnx.get('host')}:{cnx.get('port')}/?service_name={cnx.get('service_name')}&encoding=UTF-8&nencoding=UTF-8",
            "postgres":f"postgresql://{cnx.get('user')}:{cnx.get('password')}@{cnx.get('host')}:{cnx.get('port')}/{cnx.get('database')}",
            "mysql":f"mysql+pymysql://{cnx.get('user')}:{cnx.get('password')}@{cnx.get('host')}:{cnx.get('port')}/{cnx.get('database')}"
        }

        URI = db_URIConector.get(engine_db,None)
            
        if URI is None:
            print('No se encuentra ese motor')
            return 
        
        return create_engine(URI)
    
    except SQLAlchemyError as e:
        print(e)
    except Exception as e:
        print(e)