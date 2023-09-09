def variable_categorica(dataframe, len_dataframe, columna:str)->None:
    """
    _summary_

    Retorna breve resumen sin limpieza por categoria o columna necesaria 
    @params:
        dataframe (pandasDataFrame)
        len_dataframe (int)
        columna (str) : columna debe ser valida si no falla
    
    @return:
        None
    """    
    try:
        if columna in dataframe.columns:

            print("Filas totales:\n", len_dataframe)
            print(f"\nRegistros con variable \"{columna}\":\n", dataframe[columna].count())
            print("\nCantidad de nulos:\n", len_dataframe - dataframe[columna].count())
            print("\nValores distintos:\n", dataframe[columna].drop_duplicates().values)
            print("\nCantidad de filas por valor:\n", dataframe.groupby(columna)[columna].count().sort_values().head(20))
            
        
        else:
            print(f" \"{columna}\" | No se encuentra en el dataframe seleccionado")
    
    except Exception as e:
        
        print(e)