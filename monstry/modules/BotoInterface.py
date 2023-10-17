from .athena_boto_module.BotoModule import BotoModule

class BotoInterface(BotoModule):
    def __init__(self,**kwargs):
        print(""" GENERANDO CONECTOR DE BOTO

            Methodos 
            --------
            restore_connection > Inicia la conexion con aws
            run_query > Ejecuta una query
            save_df_s3 > Guarda un dataframe de pandas en un Bucket
            upload_to_athena > Ejecuta un upload de este dataframe
            drop_table > Drop de una tabla creada
            delete_file_s3 > Borra el archivo en el Bucket
        """)
        super().__init__(**kwargs)