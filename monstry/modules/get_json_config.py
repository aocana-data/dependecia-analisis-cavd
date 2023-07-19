import json

def get_config_files(path):
    
    if path is None:
        print("No hay archivos de configuracion")
        return
        
    try:    
        with open(path,"r") as config:
            config_file = json.load(config)

        return config_file

    except Exception as e:
        print("Fallo en carga de configuraciones")
        print(e)