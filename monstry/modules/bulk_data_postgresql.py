from sqlalchemy import inspect

from .get_engine import get_engine_df
from .data_frame import data_frame


def db_inspector(engine):
    insp = inspect(engine)
    schemas = insp.get_schema_names()

    if not schemas or len(schemas) == 0:
        return False

    return {
        schema: insp.get_table_names(schema=schema)
        for schema in schemas
    }

    j


def oracle_dialect(**kwargs):
    database_schema = kwargs["database_schema"]
    tables = kwargs["tables"]

    return [
        f'''
        SELECT * 
        FROM    {database_schema}.{table}
        '''
        for table in tables
    ]


def postgresql_dialect(**kwargs):

    database_schema = kwargs["database_schema"]
    tables = kwargs["tables"]

    return [
        f'''
        SELECT * 
        FROM    "{database_schema}"."{table}"
        '''
        for table in tables
    ]


def get_query_list(**kwargs):

    database_schema = kwargs["database_schema"]
    engine = kwargs["engine"]
    tables = kwargs["tables"]
    limit = kwargs["limit"]

    query_set = []

    if engine == "oracle":

        ORACLE_LIMIT = f"""FETCH NEXT {limit} ROWS ONLY""" if limit is not None else ""

        query_set = [
            query + ORACLE_LIMIT for query in
            oracle_dialect(
                database_schema=database_schema,   tables=tables
            )
        ]

    if engine == "postgres":
        PG_LIMIT = f"""
            LIMIT {limit} ;
        """ if limit is not None \
            else ";"

        query_set = [
            query + PG_LIMIT for query in
            postgresql_dialect(
                database_schema=database_schema,   tables=tables
            )
        ]

    return query_set


def get_all_df(query_list: list, engine_cnx):
    return [
        data_frame(query=query, engine_cnx=engine_cnx) for query in query_list
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

    cnx = kwargs["cnx"]
    engine = kwargs["engine"]
    limit = kwargs.get("limit", None)

    schema = cnx.get("schema", cnx["database"])

    # OBTENCION DE LAS TABLAS QUE COMPONEN AL SCHEMA
    engine_sql_alchemy = get_engine_df(cnx=cnx, engine_db=engine)
    tables_ = db_inspector(engine_sql_alchemy)

    if not tables_:
        print("No se pudieron leer las tablas de la base de datos")
        return None

    # SCHEMA ESPECIFICADO
    tables = tables_[schema]

    query_list = get_query_list(
        database_schema=schema,   engine=engine,   tables=tables,   limit=limit
    )

    engine_sql_alchemy_connection = engine_sql_alchemy.connect()

    all_dfs = get_all_df(query_list, engine_sql_alchemy_connection)

    return {
        table: df
        for df, table
        in zip(all_dfs, tables)
    }
