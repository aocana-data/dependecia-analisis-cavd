from    .validate_output_file   import validate_output_file


def to_csv_func(**kwargs):
    """
    :params
        output_dir :   directorio donde se va a realizar el output
        se crea uno si no existe
        file_name:      nombre del archivo como output como archivo csv  
    
    :return 
        void
    """
    resumen             =   kwargs["resumen"]
    OUTPUT_FILE_NAME    =   kwargs["file_name"]
    OUTPUT_DIR          =   kwargs["output_dir"]
    INDEX               =   kwargs.get("index", False)


    try:
        validate_output_file(output_dir = OUTPUT_DIR)
        resumen.to_csv(OUTPUT_FILE_NAME,index=INDEX)
        
    except Exception as e:
        print("Error producido al intentar crear el directorio")
        print(f"{e}")
    
    

