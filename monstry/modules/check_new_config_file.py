from pathlib import Path

def check_new_config_file(**kwargs):
    new_config_file     =   kwargs["config_file_name"]
    default_config_file  =   kwargs.get("default_config_file", None)
    
    if not default_config_file:
        print(f"""
    --!-> No hay archivo de configuración.
        - Analisis realizado por medio de Query Directa
        - Analisis por Archivos Locales
            
        Si se realiza el guardado de los archivos de output 
        se generará un archivo de configuración, el proximo
        analisis se realizará usando ese archivo de configuracion
        """)
        
    

    if not Path(new_config_file).is_file():
        header  =   "--!--> No existe un archivo construido por la dependencia"
        if not default_config_file:
            print(f"""
            {header}""")
        else:
            print(f"""
            {header}
            Se usará el archivo
                {default_config_file}
            """)
        return default_config_file


    print(f"""
        --!--> Las configuracion se tomarán desde el archivo construido
            {new_config_file}
    """)
    
    return new_config_file
    