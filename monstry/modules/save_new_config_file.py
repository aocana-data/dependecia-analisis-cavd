import json

def save_new_config_file(**kwargs):
    path    = kwargs["path"]
    json_to_save    = kwargs["json_to_save"]

    with open(path,"w",encoding="latin-1") as config_file_last:
        json.dump(json_to_save , config_file_last)
        