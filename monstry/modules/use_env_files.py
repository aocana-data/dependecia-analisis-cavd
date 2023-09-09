import os

def use_env_files(**kwargs):
    cnx =   kwargs["cnx"]
    if cnx is None: return None 
    return { k:os.getenv(v) if v!='' else ''  for  k,v in cnx.items()}
