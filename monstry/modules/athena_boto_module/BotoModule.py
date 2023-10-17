import os
import os.path
import time
import datetime
import logging
import json
import boto3
import pandas as pd
from botocore.exceptions import ClientError 
from io import StringIO, BytesIO


from .boto_query_athena import query_athena

class BotoModule:

    def __init__(self, **kwargs):   
        """
        Inicializa los valores de la clase Boto3Connect
        param: log  :bool por defecto false (por si se requiere log) 
        param: profile_name : str nombre de usuario segun el profile del cli
        param: database : str base de datos de athena
        param: bucket_output_location   : str bucket S3 donde se almacenan los archivos
        param: region   : str region de aws account
        param: key_path : str path donde se almacenarán los datos
        param: query_bucket : str bucket donde son almacenadas las queries, por defecto 'development-athena-queries-workgroups-piba-dl'
        """

        self.log:bool= kwargs.get("log",False)
        self.profile_name:str = kwargs.get("profile_name","development") 
        self.database:str = kwargs.get("database")
        self.bucket_output_location:str =  kwargs.get("bucket_output_location")
        self.region:str = kwargs.get("region")
        self.key_path:str = kwargs.get("key_path","modeler/athena/")
        self.query_bucket : str = kwargs.get("query_bucket","development-athena-queries-workgroups-piba-dl")

    def cleaner_dataframe_to_athena(self, df):
        df.columns = [   
            "_".join(col) for col in df.columns.to_flat_index()
        ]   
        
        return df.astype(str).reset_index()

    def restore_connection(self):
        q_status = 'SELECT 1 FROM "information_schema"."schemata" LIMIT 1'
        boto3.setup_default_session(profile_name=self.profile_name)
        client = boto3.client('athena', region_name='us-east-1')

        params = {
            "QueryString":q_status,
            "QueryExecutionContext":{"Database": "caba-piba-raw-zone-db"},
            "ResultConfiguration":{
                "OutputLocation": 's3://development-athena-queries-workgroups-piba-dl/modeler/athena/',
                "EncryptionConfiguration": 
                    {"EncryptionOption": "SSE_S3"},
            }
        }

        try:
            response = client.start_query_execution(**params)
            print("Sesion restaurada", response['ResponseMetadata']['HTTPStatusCode'])

        except ClientError as e:
            AWS_AZURE_LOGIN = f'aws-azure-login -p {self.profile_name} --mode=gui'

            error_code = e.response.get("Error", {}).get("Code")

            if error_code == "ExpiredTokenException":
                if self.log:
                    print("Restaurando sesión")
                
                os.system(AWS_AZURE_LOGIN)
                
                if self.log:
                    print("Sesión restaurada")
                    # Creación del diccionario con los datos del error
                    error_dict = {
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "timestamp": str(datetime.datetime.now())
                    }
                    # Conversión del diccionario a formato JSON
                    error_json = json.dumps(error_dict)
                    # Registro del error en el log
                    logging.error(error_json)
        return None

    def run_query(self, query):
        """
        Ejecuta una sentencia DDL o DML en boto3
        :param query: Sentencia DDL o DML a ejecutar
        :return: DataFrame con los resultados en caso de ser una sentencia SELECT, de lo contrario devuelve el estado de la ejecución
        """
        self.restore_connection()

        return query_athena(self,query)

    def save_df_s3(self,**kwargs):
        self.restore_connection()

        dataframe= kwargs.get("dataframe")
        format= kwargs.get("format", "parquet")
        filename= kwargs.get("filename", "test")
        index = kwargs.get("index",False)
        bucket = kwargs.get("bucket",False)

        boto3.setup_default_session(profile_name=self.profile_name)
        filename = filename + '.' + format

        s3 = boto3.client("s3")
        out_buffer = None
 
        if format == 'parquet':
                out_buffer = BytesIO()
                dataframe.to_parquet(out_buffer, index=index)

        elif format == 'csv':
                out_buffer = StringIO()
                dataframe.to_csv(out_buffer,index=index)

        params = {
             "Bucket"   :   bucket,
             "Key"      :   self.key_path + filename,
             "Body"     :   out_buffer.getvalue()
        }

        s3.put_object(**params)


    def upload_to_athena(self,**kwargs):
        if self.__create_table_from_df(**kwargs) != 1:
            print("Falla en la creación de la tabla")
            return 
        try:
            self.__insert_from_dataframe_to_athena(**kwargs)    
            print("Inserción de datos en el dataframe con éxito")
        except Exception as e:
            print(f"""
            Error en la inserción de los datos
            {e}
            """)
         

    def __create_table_from_df(self, **kwargs):
            """
            Crea una tabla en Athena a partir de un dataframe.

            Parámetros
            df : pandas.DataFrame
            Dataframe a partir del cual se creará la tabla.
            schema : str, opcional
            Nombre del esquema en Athena donde se creará la tabla, por defecto 'caba-piba-staging-zone-db'.
            table : str, opcional
            Nombre de la tabla a crear, por defecto 'borrar_tabla'.

            Retorno
            None
            """

            table       =   kwargs.get("table") 
            dataframe   =   kwargs.get("dataframe",) 
            schema      =   kwargs.get("schema","caba-piba-staging-zone-db") 


            nombres_columnas = dataframe.columns

            q_create = f'''
            CREATE TABLE "{schema}"."{table}"
            AS SELECT
            '''
            for index,col in enumerate(nombres_columnas):
                    q_create += f'''
                    CAST(\'\' AS VARCHAR) {col}''' 
                    if index != (len(nombres_columnas) - 1):
                        q_create += ','
            
            try:
                print("Query de tabla:\n",q_create)
                self.run_query(q_create)

                return 1
            
            except Exception as e:
                
                print(f"""
                Creación de tabla truncada
                    ERROR:
                    {e}
                """)
                
                return 0


    def __get_n_insert_from_df(self, df, desde=0, n=100):
        """
        Genera la cadena de texto para la inserción de datos en una tabla de una cantidad determinada de filas a partir de una posición específica en un dataframe.

        Parámetros:
        - df (pandas.DataFrame): Dataframe con los datos a insertar en la tabla.
        - desde (int): Posición desde la cual se inicia la generación de la cadena para la inserción de datos en la tabla. Por defecto, es 0.
        - n (int): Cantidad de filas a incluir en la cadena de texto para la inserción de datos. Por defecto, el valor es 100.

        Retorno:
        La cadena de texto resultante de la generación para la inserción de datos en la tabla.
        """
        sql_texts = ''
        n_aux = 0

        for index, row in df.iterrows():
            n_aux += 1
            if index < desde:
                continue
            else:
                sql_texts += str(tuple(row.values)) 

                if index != (len(df.index)-1) and n_aux <= (desde + n - 1):
                    sql_texts += ', \n'
                else:
                    return sql_texts


    def __insert_from_dataframe_to_athena(self, **kwargs):
        """
        Inserta los datos de un DataFrame en una tabla de Athena.

        Este método permite insertar los datos de un DataFrame en una tabla de Athena,
        con un control opcional sobre la cantidad de filas insertadas en una sola ejecución de consulta SQL.

        Parámetros:
        df (pandas.DataFrame): El DataFrame que contiene los datos a insertar.
        schema (str): El nombre del esquema en Athena en el que se encuentra la tabla.
        table (str): El nombre de la tabla en Athena en la que se insertarán los datos.
        n (int, opcional): El número de filas que se insertarán en una sola ejecución de consulta SQL. Por defecto, n=100.

        Retorno:
        None

        """

        table       =   kwargs.get("table") 
        dataframe   =   kwargs.get("dataframe",) 
        schema      =   kwargs.get("schema","caba-piba-staging-zone-db") 

        n = kwargs.get("n",100)
        target_table = f'\n"{schema}"."{table}" ' 
        
        sql_texts = 'INSERT INTO '+target_table+'\n('+ str(', '.join(dataframe.columns))+ ') \nVALUES \n'

        partes = len(dataframe) // n
        ultimo_n = len(dataframe) % n

        for i in range(partes):
                q_insert = sql_texts + self.__get_n_insert_from_df(dataframe, i*n, n)
                print(q_insert)
                self.run_query(q_insert)

        if ultimo_n > 0:
                q_insert = sql_texts + self.__get_n_insert_from_df(dataframe, partes * n, ultimo_n)
                print(q_insert)
                self.run_query(q_insert)
        


    def delete_file_s3(self, **kwargs):
        """
        Elimina un archivo en un bucket S3.

        Parámetros
        format : str, opcional
            Formato del archivo a eliminar, por defecto 'parquet'.
        filename : str, opcional
            Nombre del archivo a eliminar, por defecto 'test'.
        path_key : str, opcional
            Carpeta dentro del bucket donde se encuentra el archivo a eliminar, por defecto 'modeler/athena/'.
        bucket : str, opcional
            Nombre del bucket donde se encuentra el archivo a eliminar, por defecto 'development-athena-queries-workgroups-piba-dl'.

        Retorno
        None
        """
        self.restore_connection()

        filename = kwargs.get("filename")
        format = kwargs.get("format","parquet")
        query_bucket = kwargs.get("query_bucket", self.query_bucket)
        key_path = kwargs.get("key_path",self.key_path)

        s3 = boto3.client("s3")
        boto3.setup_default_session(profile_name=self.profile_name)

        filename = f"{filename}.{format}"

        result = s3.delete_object(Bucket=query_bucket, Key=key_path + filename)
        
        if self.log:
              print(result)


    def drop_table(self, schema='caba-piba-staging-zone-db', table='borrar_tabla'):
        """
        Elimina una tabla específica en la base de datos seleccionada.

        Parámetros:
        - schema (str): Nombre del esquema al que pertenece la tabla. Por defecto, el valor es "caba-piba-staging-zone-db".
        - table (str): Nombre de la tabla a eliminar. Por defecto, el valor es "borrar_tabla".

        Retorno:
        El resultado de la ejecución del query para la eliminación de la tabla.
        """
        query_drop = f'''
        DROP TABLE IF EXISTS `{schema}`.`{table}`
        '''

        return self.run_query(query_drop)
    