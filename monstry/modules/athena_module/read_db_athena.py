import os
import pandas as pd
import awswrangler as wr
from dotenv import load_dotenv


load_dotenv()


def read_db_athena(**kwargs):

    NOMBRE_TABLA = os.getenv("NOMBRE_TABLA", "cache")
    PARQUET_CSV = f"athena_{NOMBRE_TABLA}.parquet"

    if os.path.isfile(PARQUET_CSV):
        print(f">>> LECTURA DEL ARCHIVO USANDO CACHÉ : {PARQUET_CSV}")
        df_athena = pd.read_parquet(PARQUET_CSV)
        return df_athena

    query = kwargs["query"]

    database = os.getenv("ATHENA_DATABASE")
    workgroup = os.getenv("ATHENA_WORKGROUP")
    ctas_approach = os.getenv("ATHENA_CTAS_APPROACH", False)
    chunk_size = os.getenv("ATHENA_CHUNKSIZE", None)

    options = {
        "sql":   query,
        "database":   database,
        "workgroup":   workgroup,
        "ctas_approach":   ctas_approach,
    }

    if chunk_size is not None:
        chunk_size = int(chunk_size) if chunk_size.isnumeric() else True
        options = {**options, **{"chunksize":  chunk_size}}

    try:

        df_athena = wr.athena.read_sql_query(
            **options
        )

        if isinstance(chunk_size, int):
            data_frame = pd.DataFrame()
            for index, chunk in enumerate(df_athena):
                data_frame = pd.concat(
                    [data_frame, chunk], ignore_index=True, axis=0)
                print(f""" 
                    !>>  Chunk {index+1} con size de:{len(chunk)} registros.
                        Dataframe: {database}    
                """)
            return data_frame

        if int(os.getenv("CACHE", 0)) == 1:
            df_athena.to_parquet(PARQUET_CSV, index=False)

        return df_athena

    except Exception as e:
        print(f"""
        >>> ERROR 
        {e}""")
