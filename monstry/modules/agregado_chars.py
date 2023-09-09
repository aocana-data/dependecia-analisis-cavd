from typing import Optional

def agregado_chars(lista_orginal,*args)->Optional[list[str]]:

    no_repeated  = list(set(list(args) + lista_orginal))
    print(no_repeated)
    return no_repeated

    