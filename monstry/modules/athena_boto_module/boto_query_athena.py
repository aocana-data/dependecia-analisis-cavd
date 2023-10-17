import boto3
import time
import pandas as pd

def query_athena(cls,query):
        #cls.restore_connection()
        
        try:

            # Verifica que la query contenga una palabra permitida como primer elemento
            tipos_permitidos = ['with', 'select', 'insert', 'update', 'detele', 'create', 'drop']
            tipo = query.split()[0].lower()
            
            
            if tipo not in tipos_permitidos:
                raise SyntaxError(
                    "La query debe contener como primer palabra una de las siguientes: {}".format(tipos_permitidos))

            # Configura la sesión de Boto3
            boto3.setup_default_session(profile_name=cls.profile_name)
            client = boto3.client('athena', region_name=cls.region)


            # Inicia la ejecución de la query
            response = client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": cls.database},
                ResultConfiguration={
                    "OutputLocation": f"{cls.bucket_output_location}{cls.key_path}",
                    "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
                }
            )


            query_execution_id = response["QueryExecutionId"]

            # Espera hasta que la ejecución de la query haya finalizado

            query_state = client.get_query_execution(QueryExecutionId=query_execution_id)
            
            while query_state["QueryExecution"]["Status"]["State"] != "SUCCEEDED":
                if query_state["QueryExecution"]["Status"]["State"] in ["QUEUED", "RUNNING"]:
                    time.sleep(1)
                    query_state = client.get_query_execution(QueryExecutionId=query_execution_id)
                else:
                    break

            # Devuelve los resultados o un mensaje de error
            if query_state["QueryExecution"]["Status"]["State"] == "SUCCEEDED":
                
                if tipo == 'select' or tipo == 'with':

                    s3path = query_state["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
                    name = s3path.split("/")[-1]
                    df_response = boto3.client("s3").get_object(
                        Bucket=cls.query_bucket, 
                        Key=cls.key_path + name
                    )
                    dataframe = pd.read_csv(df_response.get("Body"))
                    return dataframe
                
                else:
                    return query_state["QueryExecution"]["Status"]["State"]
                
            else:
                raise Exception("Error al realizar query en metodo run_query\n\nQuery:\n{}".format(query))

        except SyntaxError as ex:
            print(ex)
            raise

        except Exception as ex:
            print(ex)
            print('\nQuery con error:\n' + query)

