CAVD HERRAMIENTA DE ANALISIS
---
__AKA: monstry__


Todos los procesos de analisis, cuentan con dos pasos principales:
- Creación de builder       : 
    *   Se encarga principalmente de setear los configuradores si es que llegan a hacer por default o customizados
    *   Seleccionar el archivo de configuración si es que llega a existir
    *   Determina por medio del Buildermanager el tipo de Builder debe ser creado
        *   BuilderJsonNConfig  -> A cargo de generar el builder por medio de un archivo de configuración del tipo .json
        *   BuilderLocalFile    -> A cargo de generar el builder usando un archivo local, tanto csv o xlsx
        *   BuilderCnxQuery     -> A cargo de generar el builder por medio de elementos o queries, pasando los datos de configuracion,
            la cual se recomienda usar variables de entorno cuando son datos sensibles tables como user,passwords,y URI de conexión.
  
- Creación de un cleaner    :
    *  Principal tarea es de poder generar las funciones básicas de completitud, exactitud y criterio mínimo, teniendo en consideración:
       *  Si el analisis se realiza de manera default o se realiza por medios custom:
            *  DEFAULT : Utiliza un engine de busqueda por patrones aproximados por REGEXP del tipo de datos aproximados\
            estos permiten tener unos caracteres de nulidad, las cuales por defectos son considerados : __[",", ".", "'", "-", "_", ""]__\
            También tiene por defectos funciones de analisis **bool**, estas son funciones que validad si son válidos como patrones aceptados como exactos,\
            deben tener como valores de retorno bool, ejemplo de una funcion: \
            
            ```
            # analiza si empieza con a o b, retornando un valor boolean (forma lambda)
            custom_comprobando_inicio   =   lambda variable: str(variable).strip()[0] in ["a","b"]

            # forma regular
            # valida que sea un string con patrones regulares
            def custom_comprobando_strings(variable):

                variable = variable.strip()
                reg_exp = "^[a-zA-ZÀ-ÿ\u00C0-\u017F]*$"

                return True if re.search(reg_exp, variable) else False
            
            ```
            * CUSTOM : Utiliza un archivo de configuracion .json, los cuales deben tener como mínimo los siguientes parámetros:
            
            ```
            {
                //  DATOS DE CONFIGURACION Y CONEXION SI SE TRATA DE UNA CONEXION A UNA DB
                "builder": {
                    //MOTOR: CSV,XLSX,MYSQL,POSTGRES,ORACLE
                    "engine": "csv",
                    "cnx": {},
                    // PATH >> PUEDE SER UNA QUERY O EL ARCHIVO A ANALISAR
                    "csv_path": "./input-data/path.csv"
                },
                // FUNCIONES DE VALIDACION COMPLETAMENTE CUSTOMIZADA SIN UTILIZAR LOS DEFAULT
                "exactitud_validadores": "",
                // FUNCIONES QUE SE ANEXAN A LOS QUE VIENEN POR DEFAULT
                "extra_custom_functions": "./config_files/proyecto/custom_functions_extra.py",
                // MAP DE LAS COLUMNAS CON SU FUNCION DE VALIDACION
                "exactitud_reglas": {
                    "apellido": "comprobando_strings",
                    "nombre": "comprobando_strings",
                    "dni_numero": "custom_validador_dni"
                },
                // CHARS DE NULIDAD GLOABL
                "chars_null": [",", ".", "'", "-", "_", ""],
                // CHARS DE NULIDAD INDIVIDUAL
                "completitud_reglas": {
                    "rules": {
                        "dni_numero": [
                            "28863658",
                            "38358368",
                            "26428738",
                            "34239721",
                            ",",
                            ".",
                            "'",
                            "-",
                            "_",
                            ""
                        ]
                    }
                },
                // TIPO DE DATOS PARA PODER REALIZAR UN LEVE MEJORA EN LA MEMORIA POR EL ALMACENAJE DEL TIPO DE DATO
                "dtypes": {}
            }

            ```

            ---
            
            * Criterio minimos: Es el producto del primer campo con el segundo:
            
                ejemplo : 
            
            
                
            |           |   porcentaje completitud  |   porcentaje exactitud    |   criterio minimo |
            |   :---    |   :---:   |   :---:   |   :---:   |
            |   columna |   98%     |   100%    |   98%     |    
                    
                    
    *   Funciones principales:
        - get_resumen() -> Devuelve un analisis de completitud y exactitud
        - print_criterios_minimos() -> Devuelve por medio de un print por consola los valores de criterios minimos junto con los valores percentiles
        - to_csv_resumen_table()    -> Genera un directorio con archivos de analisis del tipo csv, junto con los archivos de configuracion por la cual se hicieron esos analisis
        - total_score_gauge_save()  -> Genera unos medidores, grafico de bar horizontal los cuales nos dan los valores de criterio minimo.
        - print_analisis_to_markdown-> Genera un archivo markdown con todas las graficas necesarias para muestra del proyecto, esto se genera dentro del archivo output, por lo cual si sea el caso
            que se quiera mover al inicio del repositorio debe tener en cuenta la ruta donde está guardado las imágenes.
    *   Al correr el método to_csv_resumen(), se crea un par de carpetas:
        -   config_files: 
            Esta nos genera de un archivo de configuración con las funciones de analisis por defecto reflejandose para volver a retomar
            las mismas validaciones y/o ser modificadas para un nuevo analisis; estos archivos no se volverán a crear al correr el archivo.
        -   output_dir(carpeta default, pero se puede configurar una nueva):
            Generan una carpeta con el nombre de la tabla con los csv de resumen (exactitud, completitud y criterios minimos).
            Tambien al correr el resto de los métodos tales como los gauges y los markdown, persistiran en esta carpeta.
            Al correr nuevamente estos métodos, los archivos csv, png y md serán actualizados.
            
            
COMO INSTALAR LA DEPENDENCIA
---

- crear el entorno virtual  :   python3 -m venv venv 
- activar el entrono        :   . ./venv/bin/activate
- instalar la dependencia   :   pip install git+https://repositorio-asi.buenosaires.gob.ar/ssppbe/ssppbe-dgcinfo-cdd/herramienta-analitica-cavd.git

_Te va a pedir el user y el password, siempre y cuando tengas acceso_
