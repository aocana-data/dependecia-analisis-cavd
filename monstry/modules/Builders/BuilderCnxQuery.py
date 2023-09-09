from uuid import uuid4
from pathlib import Path


from .Builder import Builder
from .InteraceGetters import InterfaceGetters
from ..dataframes_uri_def import read_db_engine
from ..global_data import CHARS_NULL


class BuilderCnxQuery(Builder, InterfaceGetters):
    """
    Genera un Builder con datos ingresados
    de manera manual

    :params

        Ejemplo de cnx

            :cnx = { 'host': 'localhost',
                    'port': 3306,
                    'database':'database',
                    'schema': 'database.schema',
                    'table': '',
                    'password': 'admin',
                    'user':'root'
            }

            :engine     =   [mysql|postgresql|oracle]
            :query_path =   /path/query.sql
            :base_dir   =   raíz_del_proyecto => default ./
    """

    def __init__(self, **kwargs):

        self.BASE_DIR = kwargs.get("base_dir", "./")
        self.query_path = kwargs.get("query_path")
        self.query = kwargs.get("query", None)
        self.cnx: dict = kwargs.get('cnx', None)
        self.engine: str = kwargs.get("engine")
        self.table = kwargs.get("table", f"DESCONOCIDA{uuid4().hex}")

        self.config = {
            "builder": {
                "engine":   self.engine,
                "cnx":   self.cnx,

            },
            "exactitud_validadores": "",
            "exactitud_reglas": {},
            "chars_null": CHARS_NULL,
            "completitud_reglas": {"rules": {}},
            "dtypes": {}
        }

    def get_query(self):
        if self.query is not None:
            return self.query

        query_path = Path(self.BASE_DIR) / self.query_path

        try:
            with open(query_path, "r", encoding="latin-1") as query:
                return query.read()

        except Exception as e:
            print(
                f"ERROR producido en la obtencionde la query desde un archivo\n\t{e}")

    def get_database(self):
        query = self.get_query()
        self.database = read_db_engine(
            cnx=self.cnx, query=query, engine=self.engine)

        try:

            if self.database is None:
                print('No se ha generado el dataframe')
                return

            print('Se ha cargado efectivamente el dataframe')

        except Exception as e:
            print(f"ERROR producido al generar el dataframe\n\t{e}")
