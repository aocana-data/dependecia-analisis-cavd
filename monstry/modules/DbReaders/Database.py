from ..bulk_data_postgresql import bulk_dataframes
from ..use_env_files import use_env_files


class Database:

    def get_databases(self):

        try:

            builder = self.config["builder"]

            if self.config is not None:
                cnx = builder.get('cnx', None)
                self.engine = builder.get('engine')
                self.cnx = use_env_files(cnx=cnx)

            self.databases = bulk_dataframes(
                cnx=self.cnx,
                engine=self.engine,
                limit=self.limit
            )

            if self.databases is None or len(self.databases) == 0:
                print('No se han generado los dataframes')
                return

            print(f"""
            >>> CANTIDAD DE TABLAS A ANALIZAR : {len(self.databases)}<<<
            """)

            print('Se ha cargado efectivamente el dataframe')

            return self.values_to_release_cleanner()

        except Exception as e:
            print("ERROR OBTENCION DE DB")
            print(e)
