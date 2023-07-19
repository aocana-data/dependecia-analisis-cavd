def read_file_query(path,):

    with open(path, 'r') as file:
        query_text = file.read()
        data = eval(query_text)
        print('Se ha cargado la query satisfactoriamente')
        return data



