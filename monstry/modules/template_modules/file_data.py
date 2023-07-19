import os
from datetime import datetime

def file_data(file_name = None):

    # TODO ==> Mejorar este proceso

    BASE_DIR = os.path.dirname(os.path.dirname(__file__))
    ROOT_DIR = os.path.abspath( os.path.join(__file__,'../../../..'))


    folder_name = 'output'
    folder_path = os.path.join(ROOT_DIR, folder_name) 

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    file_path = "export_modules/matriz_evaluacion_calidad_datos.template.xlsx"


    if file_name is None:
        file_output_path = f"matriz_evaluacion.{datetime.today().strftime('%b_%d_%Y')}_output.xlsx"
    else:
        file_output_path = f"{file_name}.xlsx"
    


    return {
        "template_path" :   os.path.join(BASE_DIR,file_path),
        "output_data"   :   os.path.join(folder_path,file_output_path)
    }