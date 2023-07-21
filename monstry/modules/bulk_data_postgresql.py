from sqlalchemy     import inspect

from .get_engine    import get_engine_df
from .data_frame    import data_frame


def postgresql_database(engine):
    insp = inspect(engine)
    schemas = insp.get_schema_names()
    
    if not schemas or len(schemas)==0:
        return False
    
    return {
            schema: insp.get_table_names(schema=schema) \
            for schema in schemas 
    }
    
def get_query_list(database_schema, tables , limit ):
    ## TODO => mejorar la forma como se piden los datos, permitiendo obtener ciertas cantidades muestrales
    if limit is not None:
        return [
                f'''
                SELECT * 
                FROM
                "{database_schema}"."{table}"
                LIMIT {limit}
                '''
                for table in tables 
            ] 
        
    return [
        f'''
        SELECT * 
        FROM
        "{database_schema}"."{table}"
        '''
        for table in tables 
    ] 

def get_all_df(query_list:list,engine_cnx):
    return [
        data_frame(query=query,engine_cnx=engine_cnx) for query in query_list
    ]


# FUNCION PRINCIPAL DE ANALISIS EN POSTGRESQL PARA CONOCER TODAS LAS TABLAS DE UN SCHEMA EN PARTICULAR

def bulk_dataframes(**kwargs):
    """
    :params
        cnx:dict ->     dict de conexion
        engine:str ->   motor de base de datos
    
    :return
        dataframes:list -> lista de dataframes masivos de la base de datos
    """
    
    cnx     =   kwargs["cnx"]
    engine  =   kwargs["engine"]
    limit   =   kwargs.get("limit",None)
    
    schema  =   cnx.get("schema", cnx["database"])
    
    # OBTENCION DE LAS TABLAS QUE COMPONEN AL SCHEMA
    engine_sql_alchemy  =   get_engine_df(cnx = cnx , engine_db = engine)
    tables_  =   postgresql_database(engine_sql_alchemy)
    
    if not tables_:
        print("No se pudieron leer las tablas de la base de datos")
        return None
    # SCHEMA ESPECIFICADO
    tables  =   tables_[schema]
    query_list = get_query_list(schema , tables , limit)
    engine_sql_alchemy_connection   =   engine_sql_alchemy.connect()
    
    return  { 
        
        table:df 
        for df,table 
        in zip( get_all_df(query_list,engine_sql_alchemy_connection) , tables) 
    }

def bulk_local_dataframes(**kwargs):
    ...
    
