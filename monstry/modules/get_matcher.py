import re

def get_matcher(**kwargs):
    regexp  =   "path$"
    builder     =   kwargs["builder"]
    
    for path_key,value in builder.items():
        if re.search(regexp ,path_key , re.IGNORECASE): return path_key
    
    return False
             