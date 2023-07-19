from pathlib import Path

def validate_output_file(**kwargs):
    OUTPUT_DIR  =   kwargs["output_dir"] 
    
    try:
        if not Path.exists(OUTPUT_DIR):
            print(f"La ruta {OUTPUT_DIR} no existe, procedemos a crearla")
            path = Path(OUTPUT_DIR)
            path.mkdir(parents=True)
        
        print(f"La ruta {OUTPUT_DIR} existe.")
        
        return                         
    except Exception as e:
        
        print(f"Error en la creacion de la carpeta: \n{e}")
        
        