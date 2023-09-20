from monstry.DataCleaner import DataCleaner
from monstry.BulkDataBuilder import LinkerDataBuilder
from .modules.check_new_config_file import check_new_config_file


class BulkDataCleaner(DataCleaner):
    PATH_DEFAULT = './modules/funciones_default/funciones_generales.py'

    chars_null = [",", ".", "'", "-", "_", ""]

    config = {
        "chars_null": [",", ".", "'", "-", "_", ""],
        "exactitud_reglas": {},
        "completitud_reglas": {},
        "dtypes": {}
    }

    chars_omitir_exactitud: dict = None
    DATA_DEFAULT_COLUMNAS: dict = None
    segregacion_criterios_minimos = None
    score_completitud = None
    criterio_minimo = None
    min_criterio_minimo = None
    score_exactitud = None
    completitud = None
    exactitud = None
    resumen = None
    rules = None

    def vectorizacion_cb(self, completitud, exactitud): return round(
        (completitud * exactitud)/100, 2)

    def __init__(self, **kwargs) -> None:
        self.builder: LinkerDataBuilder = kwargs["builder"]

        # CONFIGURACIONES DESDE EL BUILDER
        self.nombre_tabla: str = self.builder.get_table()
        self.config = self.builder.get_config()

        # LOS DATOS DE CONEXION DIRECTAMENTE DESDE EL BUILDER
        self.builder_config = self.builder.get_cnx()

        # EL DATAFRAME GENERADO POR EL BUILDER DIRECTAMENTE
        self.dataframe = self.builder.builder_get_database()
        self.BASE_DIR = self.builder.get_base_dir()
        self.get_config_file = self.builder.get_config_path()

        # AGREGADO DE NOMBRE DE TABLA Y COLUMNAS DERIVADAS DEL BUILDER
        self.columnas = self.dataframe.columns
        self.cantidad_registros = len(self.dataframe)

        # SETEADO DE UN ARCHIVO DE CONFIGURACION QUE SE OBTENDRÁ UNICAMENTE
        # CUANDO EL CLEANER EJECUTE EL METODO DE GUARDADO DE CSV Y GAUGES
        # DE ANALISIS DE:
        #     COMPLETITUD
        #     EXACTITUD
        #     CRITERIOS MINIMOS

        self.CONFIG_FILE_PATH = self.__set_CONFIG_FILE_PATH__()
        self.NEW_CONFIG_FILE = self.__set_CONFIG_FILE_PATH__()

        path = check_new_config_file(
            config_file_name=self.CONFIG_FILE_PATH,
            default_config_file=self.get_config_file
        )

        self.CONFIG_FILE_PATH = path

        self.set_config(self.CONFIG_FILE_PATH)
        self.criteria_valores_generales = {}
        self.scoring_to_gauge_paths = {}

        data_print = f"""
        OBJETO DE ANALISIS:
        
            TABLA:
                {self.nombre_tabla.upper()}
        {'-'*70}
            CANTIDAD DE REGISTROS   
                {self.cantidad_registros}
        {'-'*70}
        """

        print(data_print)
